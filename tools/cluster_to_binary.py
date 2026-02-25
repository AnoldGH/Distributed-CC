#!/usr/bin/env python3
"""Convert a text cluster file (CSV/TSV/space-delimited) to binary .bcluster format.

Binary format:
    [4 bytes: uint32 num_entries]
    [8 bytes per entry: int32 node_id, int32 cluster_id] x num_entries

Usage:
    python cluster_to_binary.py input.cluster output.bcluster
    python cluster_to_binary.py input.cluster              # writes input.bcluster
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
    entries = []
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
            entries.append((int(parts[0]), int(parts[1])))

    num_entries = len(entries)
    with open(output_path, "wb") as f:
        f.write(struct.pack("<I", num_entries))
        for node_id, cluster_id in entries:
            f.write(struct.pack("<ii", node_id, cluster_id))

    text_size = Path(input_path).stat().st_size
    bin_size = Path(output_path).stat().st_size
    reduction = (1 - bin_size / text_size) * 100 if text_size > 0 else 0

    print(f"Converted {num_entries} entries")
    print(f"  {input_path}: {text_size:,} bytes")
    print(f"  {output_path}: {bin_size:,} bytes ({reduction:.1f}% smaller)")


def main():
    parser = argparse.ArgumentParser(description="Convert text cluster file to binary .bcluster format")
    parser.add_argument("input", help="Input text cluster file")
    parser.add_argument("output", nargs="?", default=None, help="Output binary file (default: input with .bcluster extension)")
    args = parser.parse_args()

    output = args.output
    if output is None:
        output = str(Path(args.input).with_suffix(".bcluster"))

    convert(args.input, output)


if __name__ == "__main__":
    main()
