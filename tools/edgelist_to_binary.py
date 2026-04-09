#!/usr/bin/env python3
"""Convert a text edgelist (CSV/TSV/space-delimited) to binary .bedgelist format.

Binary format:
    [8 bytes: uint64 num_edges]
    [8 bytes per edge: int32 source, int32 target] x num_edges

Usage:
    python edgelist_to_binary.py input.edgelist output.bedgelist
    python edgelist_to_binary.py input.edgelist              # writes input.bedgelist
"""

import argparse
import struct
import sys
from pathlib import Path


def detect_delimiter(header: str) -> str:
    for delim in [",", "\t", " "]:
        if delim in header:
            return delim
    raise ValueError(f"Cannot detect delimiter from header: {header!r}")


def convert(input_path: str, output_path: str) -> None:
    edges = []
    with open(input_path, "r") as f:
        header = f.readline().strip()
        delim = detect_delimiter(header)

        for lineno, line in enumerate(f, start=2):
            line = line.strip()
            if not line:
                continue
            parts = line.split(delim)
            if len(parts) < 2:
                print(f"Warning: skipping malformed line {lineno}: {line!r}", file=sys.stderr)
                continue
            edges.append((int(parts[0]), int(parts[1])))

    num_edges = len(edges)
    with open(output_path, "wb") as f:
        f.write(struct.pack("<Q", num_edges))
        for src, tgt in edges:
            f.write(struct.pack("<ii", src, tgt))

    text_size = Path(input_path).stat().st_size
    bin_size = Path(output_path).stat().st_size
    reduction = (1 - bin_size / text_size) * 100 if text_size > 0 else 0

    print(f"Converted {num_edges} edges")
    print(f"  {input_path}: {text_size:,} bytes")
    print(f"  {output_path}: {bin_size:,} bytes ({reduction:.1f}% smaller)")


def main():
    parser = argparse.ArgumentParser(description="Convert text edgelist to binary .bedgelist format")
    parser.add_argument("input", help="Input text edgelist file")
    parser.add_argument("output", nargs="?", default=None, help="Output binary file (default: input with .bedgelist extension)")
    args = parser.parse_args()

    output = args.output
    if output is None:
        output = str(Path(args.input).with_suffix(".bedgelist"))

    convert(args.input, output)


if __name__ == "__main__":
    main()
