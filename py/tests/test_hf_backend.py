"""Regression test for the real-model backend's prefix cache.

Skipped unless torch and transformers are installed (they are optional).
Loads SmolLM2-135M on CPU, so it takes a few seconds and ~0.7 GB.

    .venv/Scripts/python.exe -m unittest py.tests.test_hf_backend -v
"""
from __future__ import annotations

import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
PY = os.path.dirname(HERE)
for p in (PY, os.path.join(PY, "dsys_infer")):
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    import torch  # noqa: F401
    import transformers  # noqa: F401
    HAVE_HF = True
except ImportError:
    HAVE_HF = False


@unittest.skipUnless(HAVE_HF, "torch/transformers not installed")
class FullPrefixHitTest(unittest.TestCase):
    """A prompt whose length is an exact multiple of BLOCK_TOKENS is cached
    in full after one request. The second request must not re-feed the
    last prompt token on top of a cache that already contains it: that
    duplicates its key/value and changes the output. Greedy decoding is
    deterministic, so a cold run and a full-cache-hit run must agree."""

    @classmethod
    def setUpClass(cls):
        from dsys_infer.infer.v1 import infer_pb2
        import hf_backend
        cls.pb = infer_pb2
        cls.hf = hf_backend
        args = types.SimpleNamespace(model="", kv_cache_blocks=32, torch_threads=2,
                                     stall_prob=0.0, stall_ms=0.0, seed=1)
        cls.backend = hf_backend.HFBackend(args, infer_pb2)

    def generate(self, prompt: str):
        req = self.pb.GenerateRequest(prompt=prompt, max_tokens=12)
        toks = list(self.backend.generate(req, lambda: True))
        return "".join(t.text for t in toks), toks[0]

    def block_aligned_prompt(self) -> str:
        words = ("consensus replica leader follower election term log entry commit "
                 "apply snapshot lease fence quorum partition retry").split()
        prompt = "Explain briefly:"
        i = 0
        while True:
            n = len(self.backend.tok(prompt).input_ids)
            if n >= 2 * self.hf.BLOCK_TOKENS and n % self.hf.BLOCK_TOKENS == 0:
                return prompt
            prompt += " " + words[i % len(words)]
            i += 1

    def test_full_cache_hit_matches_cold_generation(self):
        prompt = self.block_aligned_prompt()
        n = len(self.backend.tok(prompt).input_ids)
        cold, first_cold = self.generate(prompt)
        warm, first_warm = self.generate(prompt)
        self.assertFalse(first_cold.prefix_cache_hit, "first request should be cold")
        self.assertTrue(first_warm.prefix_cache_hit, "second request should hit the cache")
        self.assertEqual(cold, warm,
                         f"{n}-token prompt fully cached: output changed\ncold: {cold!r}\nwarm: {warm!r}")


if __name__ == "__main__":
    unittest.main()
