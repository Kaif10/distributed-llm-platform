"""A real HuggingFace causal-LM backend with genuine prefix KV-cache reuse.

This is the piece that makes Phase 5's routing story real rather than
simulated. `infer/mock` and `--backend mock` model a prefix cache: they hash
64-character blocks and pretend prefill is cheaper when a prefix was seen
before. This module does the actual thing.

# What a prefix cache actually is

A causal LM's attention over a prompt produces, for every layer and every
token, a key and a value tensor. Those are the `past_key_values` (the "KV
cache"). Generating token N+1 needs the KV of tokens 1..N — and crucially,
the KV for a given token depends only on the tokens BEFORE it. So if two
prompts share a leading token sequence, the KV tensors for that shared span
are identical, and the second request can skip recomputing them entirely.

That is the whole trick behind vLLM's automatic prefix caching and SGLang's
RadixAttention, and it is why routing requests with a shared system prompt
to the SAME worker matters: a worker can only reuse a prefix it personally
still holds.

# What this implementation does

    tokenize(prompt) -> [t0, t1, ... tn]
    find the longest cached prefix of that token sequence
    prefill only the remaining suffix, seeded with the cached KV
    after generating, store the prompt's KV for later reuse

Cache keys are cumulative hashes of token-id prefixes taken at fixed block
boundaries (BLOCK_TOKENS), which bounds how many entries one prompt adds and
mirrors how real paged-attention caches work in whole blocks rather than at
arbitrary offsets. Eviction is LRU, bounded by entry count, because each
entry pins real tensors.

# Honest limits

Single request at a time (a lock around the forward pass), greedy decoding,
no batching, no paged allocation, CPU-friendly small models. A production
server batches across requests and manages KV memory in pages. The point
here is that the prefix-reuse SEMANTICS the gateway routes on are real: a
reported `prefix_cache_hit` means genuinely skipped computation, and the
`prefill_ms` difference is measured, not modelled.
"""

from __future__ import annotations

import copy
import hashlib
import logging
import threading
import time
from collections import OrderedDict
from typing import Iterator

log = logging.getLogger("infer.hf")

# Prefix-cache block size, in TOKENS. A prompt contributes one cache entry
# per completed block, so this bounds per-request cache growth. 32 tokens is
# roughly 100-130 characters of English.
BLOCK_TOKENS = 32


class PrefixCache:
    """LRU over (block-aligned token prefix) -> past_key_values.

    Entries hold real KV tensors, so `max_entries` is a memory bound, not a
    tuning knob to set casually: each entry costs roughly
    2 * n_layers * n_kv_heads * head_dim * n_tokens * dtype_bytes.
    """

    def __init__(self, max_entries: int = 32) -> None:
        self._lru: OrderedDict[str, tuple[object, int]] = OrderedDict()
        self._max = max_entries
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    @staticmethod
    def _key(token_ids: list[int]) -> str:
        h = hashlib.blake2b(digest_size=16)
        for t in token_ids:
            h.update(t.to_bytes(4, "little"))
        return h.hexdigest()

    def longest_prefix(self, token_ids: list[int]) -> tuple[object | None, int]:
        """Return (past_key_values, n_tokens_covered) for the longest cached
        block-aligned prefix of token_ids, or (None, 0)."""
        best: tuple[object | None, int] = (None, 0)
        with self._lock:
            # Walk block boundaries from longest to shortest; first hit wins.
            n_blocks = len(token_ids) // BLOCK_TOKENS
            for blocks in range(n_blocks, 0, -1):
                n = blocks * BLOCK_TOKENS
                entry = self._lru.get(self._key(token_ids[:n]))
                if entry is not None:
                    self._lru.move_to_end(self._key(token_ids[:n]))
                    best = (entry[0], entry[1])
                    break
            if best[0] is None:
                self.misses += 1
            else:
                self.hits += 1
        return best

    def put_block(self, token_ids: list[int], n_tokens: int, past: object) -> None:
        """Store one entry covering exactly the first n_tokens (block-aligned).

        Callers should store an entry at EVERY block boundary of a prompt, not
        just the longest one — see HFBackend.generate's comment on why storing
        only the longest boundary makes shorter shared prefixes un-matchable.
        """
        key = self._key(token_ids[:n_tokens])
        with self._lock:
            self._lru[key] = (past, n_tokens)
            self._lru.move_to_end(key)
            while len(self._lru) > self._max:
                self._lru.popitem(last=False)

    def stats(self) -> tuple[int, int, int]:
        with self._lock:
            return self.hits, self.misses, len(self._lru)


class HFBackend:
    """Greedy decoding from a HuggingFace causal LM with real prefix reuse.

    One forward pass at a time (self.lock): batching across concurrent
    requests is exactly what a real inference server adds on top, and
    deliberately out of scope here — see the module docstring.
    """

    def __init__(self, args, infer_pb2) -> None:
        try:
            import torch
            from transformers import AutoModelForCausalLM, AutoTokenizer
        except ImportError:
            raise SystemExit(
                "--backend hf needs torch and transformers:\n"
                "  .venv/Scripts/python.exe -m pip install "
                "--index-url https://download.pytorch.org/whl/cpu torch\n"
                "  .venv/Scripts/python.exe -m pip install transformers"
            )
        self.torch = torch
        self.pb = infer_pb2
        self.model_name = args.model or "HuggingFaceTB/SmolLM2-135M-Instruct"
        self.model = self.model_name

        log.info("loading %s on CPU (first run downloads weights)", self.model_name)
        t0 = time.monotonic()
        self.tok = AutoTokenizer.from_pretrained(self.model_name)
        self.lm = AutoModelForCausalLM.from_pretrained(
            self.model_name, dtype=torch.float32, low_cpu_mem_usage=True
        )
        self.lm.eval()
        torch.set_num_threads(max(1, getattr(args, "torch_threads", 2)))
        log.info("loaded in %.1fs", time.monotonic() - t0)

        self.cache = PrefixCache(max_entries=getattr(args, "kv_cache_blocks", 32))
        self.lock = threading.Lock()

    # -- KV cache plumbing ---------------------------------------------------
    # transformers has moved from tuple-of-tuples to Cache objects; support
    # both, and fail loudly rather than silently skipping reuse.

    def _crop(self, past, n_tokens: int):
        """Trim a KV cache to its first n_tokens positions.

        transformers' Cache.crop took a positive target length historically
        and switched to "negative = remove this many" in 5.x (positive is
        deprecated and slated for removal in 5.18). Try the new spelling
        first and fall back, so this works across both.
        """
        if past is None:
            return None
        if hasattr(past, "crop"):
            current = self._seq_len(past)
            if current > n_tokens:
                try:
                    past.crop(-(current - n_tokens))
                except (TypeError, ValueError):
                    past.crop(n_tokens)
            return past
        # Legacy tuple-of-tuples layout.
        return tuple(
            (k[:, :, :n_tokens, :], v[:, :, :n_tokens, :]) for k, v in past
        )

    def _clone(self, past):
        """Deep-copy a KV cache so a cached entry is not mutated by the
        generation that reuses it.

        This is the subtle part. Decoding APPENDS to whatever cache object it
        is handed, so handing a cached entry straight to the model would grow
        that entry by every token generated and corrupt it for the next
        request that matched the same prefix. copy.deepcopy handles both the
        modern Cache objects (whose internal layout has changed across
        versions — 5.x iterates as 3-tuples, not the 2-tuples older code
        assumed) and the legacy tuple layout, without this module needing to
        know either. Measured at ~15ms for a 135M model's cache, which is
        cheap next to the prefill it saves.
        """
        if past is None:
            return None
        return copy.deepcopy(past)

    def _seq_len(self, past) -> int:
        if past is None:
            return 0
        if hasattr(past, "get_seq_length"):
            return int(past.get_seq_length())
        return int(past[0][0].shape[2])

    # -- generation ----------------------------------------------------------

    def generate(self, req, active) -> Iterator[object]:
        torch = self.torch
        pb = self.pb
        max_tokens = req.max_tokens if req.max_tokens > 0 else 64
        eos = self.tok.eos_token_id

        token_ids: list[int] = self.tok(req.prompt).input_ids
        t0 = time.monotonic()

        with self.lock:
            with torch.no_grad():
                cached_past, n_cached = self.cache.longest_prefix(token_ids)
                past = self._clone(cached_past) if cached_past is not None else None
                if past is not None:
                    past = self._crop(past, n_cached)

                # Prefill only what the cache did not already cover.
                suffix = token_ids[n_cached:] or token_ids[-1:]
                ids = torch.tensor([suffix], dtype=torch.long)
                out = self.lm(input_ids=ids, past_key_values=past, use_cache=True)
                past = out.past_key_values
                prefill_ms = int((time.monotonic() - t0) * 1000)

                # Remember this prompt's KV for later requests that share a
                # prefix with it, storing an entry at EVERY block boundary —
                # not just the longest one.
                #
                # Storing only the longest boundary is a bug that end-to-end
                # testing caught and a standalone test had missed by luck:
                # two prompts sharing a 60-token system prompt diverge inside
                # the 32..64 block, so their 64-token keys differ, and with
                # nothing stored at 32 the lookup walks down and finds
                # nothing. Every boundary must be its own entry for a shorter
                # shared prefix to be matchable at all.
                #
                # The cost is that this duplicates KV memory across entries
                # (a 96-token prompt stores 32-, 64- and 96-token copies).
                # Real paged implementations avoid that by storing immutable
                # per-block pages and sharing them between sequences; that
                # page table is exactly the piece this deliberately doesn't
                # build. LRU max_entries is what bounds the damage here.
                n_full_blocks = len(token_ids) // BLOCK_TOKENS
                if n_full_blocks >= 1 and self._seq_len(past) >= BLOCK_TOKENS:
                    for blocks in range(1, n_full_blocks + 1):
                        n = blocks * BLOCK_TOKENS
                        self.cache.put_block(
                            token_ids, n, self._crop(self._clone(past), n)
                        )

                # cached_prefix_chars is reported in CHARACTERS because that
                # is the unit the gateway's router and the mock both speak.
                cached_chars = (
                    len(self.tok.decode(token_ids[:n_cached])) if n_cached else 0
                )

                for i in range(max_tokens):
                    if not active():
                        return
                    nxt = out.logits[:, -1, :].argmax(dim=-1, keepdim=True)
                    tid = int(nxt)
                    tok = pb.Token(text=self.tok.decode([tid]), index=i)
                    if i == 0:
                        tok.prefill_ms = prefill_ms
                        tok.prefix_cache_hit = n_cached > 0
                        tok.cached_prefix_chars = cached_chars
                    stop = tid == eos or i == max_tokens - 1
                    if stop:
                        tok.done = True
                        tok.finish_reason = "stop" if tid == eos else "length"
                    yield tok
                    if stop:
                        return
                    out = self.lm(
                        input_ids=nxt, past_key_values=past, use_cache=True
                    )
                    past = out.past_key_values

    def cache_stats(self) -> str:
        hits, misses, entries = self.cache.stats()
        total = hits + misses
        rate = (hits / total) if total else 0.0
        return f"prefix cache: {hits}/{total} hits ({rate:.0%}), {entries} entries"
