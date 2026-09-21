"""Render a captured terminal session into an animated GIF.

Input is a file of `<unix_timestamp>|<line>` records (see scripts/record_demo.sh),
so the GIF's pacing reflects how the real command actually behaved rather than a
made-up cadence. Frames are drawn with Pillow in a terminal-like palette; no
ffmpeg, asciinema, or other external tooling required.

Usage:
    python scripts/make_demo_gif.py <capture.txt> <out.gif> [--title "..."] [--cols 100] [--rows 28]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

# A dark terminal palette, close to what the project's own logs look like.
BG = (13, 17, 23)
FG = (201, 209, 217)
DIM = (110, 118, 129)
GREEN = (63, 185, 80)
RED = (248, 81, 73)
YELLOW = (210, 153, 34)
BLUE = (88, 166, 255)
PURPLE = (188, 140, 255)
TITLEBAR = (22, 27, 34)

FONT_PATH = r"C:\Windows\Fonts\consola.ttf"
FONT_BOLD = r"C:\Windows\Fonts\consolab.ttf"

# Substrings that tint a whole line, in priority order.
RULES: list[tuple[re.Pattern[str], tuple[int, int, int]]] = [
    (re.compile(r"PASS|no invariant violated|✓"), GREEN),
    (re.compile(r"FAIL|violation|panic"), RED),
    (re.compile(r"\bcrash\b"), RED),
    (re.compile(r"\bpartition\b"), YELLOW),
    (re.compile(r"\bpause\b"), YELLOW),
    (re.compile(r"\bheal\b"), GREEN),
    (re.compile(r"^simrun:|^---|report"), PURPLE),
    (re.compile(r"kv ops|scheduler|gateway|nemesis events"), BLUE),
]


def colour_for(line: str) -> tuple[int, int, int]:
    for pattern, colour in RULES:
        if pattern.search(line):
            return colour
    return FG


def load_capture(path: Path) -> list[tuple[float, str]]:
    rows: list[tuple[float, str]] = []
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        ts, _, text = raw.partition("|")
        try:
            rows.append((float(ts), text))
        except ValueError:
            # A line without a timestamp: attach it to the previous moment.
            rows.append((rows[-1][0] if rows else 0.0, raw))
    return rows


def render(
    capture: Path,
    out: Path,
    title: str,
    cols: int,
    rows: int,
    speed: float,
    max_frame_ms: int,
) -> None:
    lines = load_capture(capture)
    if not lines:
        sys.exit(f"{capture}: no lines captured")

    font = ImageFont.truetype(FONT_PATH, 15)
    bold = ImageFont.truetype(FONT_BOLD, 15)
    cw = int(font.getlength("M"))
    ch = 21
    pad = 14
    bar = 30

    width = pad * 2 + cw * cols
    height = bar + pad * 2 + ch * rows

    t0 = lines[0][0]
    frames: list[Image.Image] = []
    durations: list[int] = []

    # One frame per output line, showing the last `rows` lines, with the frame
    # held for however long the real command waited before the next line.
    for i in range(len(lines)):
        visible = [text for _, text in lines[max(0, i - rows + 1) : i + 1]]

        img = Image.new("RGB", (width, height), BG)
        d = ImageDraw.Draw(img)

        # Title bar with the usual three dots.
        d.rectangle([0, 0, width, bar], fill=TITLEBAR)
        for n, dot in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
            d.ellipse([pad + n * 18, 11, pad + n * 18 + 9, 20], fill=dot)
        d.text((pad + 70, 8), title, font=bold, fill=DIM)

        y = bar + pad
        for text in visible:
            d.text((pad, y), text[:cols], font=font, fill=colour_for(text))
            y += ch

        # Quantize to a small fixed palette: these frames use ~8 colours, so a
        # 16-colour palette is lossless here and cuts the GIF's size by roughly
        # an order of magnitude versus saving full RGB frames.
        frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=16))

        if i + 1 < len(lines):
            gap = (lines[i + 1][0] - lines[i][0]) / speed
        else:
            gap = 2.5  # hold the final frame so the result is readable
        durations.append(max(40, min(int(gap * 1000), max_frame_ms)))

    frames[0].save(
        out,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    total = sum(durations) / 1000
    size_kb = out.stat().st_size / 1024
    print(f"{out}: {len(frames)} frames, {total:.1f}s, {size_kb:.0f} KB, {width}x{height}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("capture", type=Path)
    ap.add_argument("out", type=Path)
    ap.add_argument("--title", default="dsys")
    ap.add_argument("--cols", type=int, default=100)
    ap.add_argument("--rows", type=int, default=28)
    ap.add_argument("--speed", type=float, default=1.0, help=">1 plays faster than real time")
    ap.add_argument("--max-frame-ms", type=int, default=1200, help="cap on any single frame's hold")
    args = ap.parse_args()
    render(args.capture, args.out, args.title, args.cols, args.rows, args.speed, args.max_frame_ms)


if __name__ == "__main__":
    main()
