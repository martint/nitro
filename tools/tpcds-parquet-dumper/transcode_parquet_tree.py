#!/usr/bin/env python3

import argparse
import shutil
from pathlib import Path

import pyarrow.parquet as parquet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite a parquet dataset tree with a different compression codec."
    )
    parser.add_argument("--input-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--compression", default="lz4")
    return parser.parse_args()


def should_skip(path: Path) -> bool:
    return path.name.endswith(".crc")


def should_copy_without_transcoding(path: Path) -> bool:
    if path.name.startswith("."):
        return True
    try:
        parquet.ParquetFile(path)
        return False
    except Exception:
        return True


def main() -> None:
    arguments = parse_args()
    input_root = arguments.input_root.resolve()
    output_root = arguments.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    rewritten_files = 0
    copied_files = 0

    for source in sorted(input_root.rglob("*")):
        relative_path = source.relative_to(input_root)
        target = output_root / relative_path

        if source.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue

        if should_skip(source):
            continue

        target.parent.mkdir(parents=True, exist_ok=True)

        if should_copy_without_transcoding(source):
            shutil.copy2(source, target)
            copied_files += 1
            print(f"copied    {relative_path}")
            continue

        table = parquet.read_table(source)
        parquet.write_table(table, target, compression=arguments.compression)
        rewritten_files += 1
        print(f"rewritten {relative_path}")

    print(
        f"done: rewritten {rewritten_files} parquet files, copied {copied_files} metadata files"
    )


if __name__ == "__main__":
    main()
