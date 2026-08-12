#!/usr/bin/env python3

import csv
import html
import re
import sys
from collections import defaultdict
from pathlib import Path


WORK = re.compile(
    r"\[source-work\] (?:source=(\S+) )?column=(\S+) "
    r"fullDecoded=(\d+) selectedDecoded=(\d+) dictionaryExamined=(\d+) "
    r"skipped=(\d+) published=(\d+) copied=(\d+) lazyOmitted=(\d+) nullExamined=(\d+)")
DECOMPRESSION = re.compile(
    r"\[decompression\] (?:source=(\S+) column=)?(\S+) pages=(\d+)"
    r"(?: compressed=(\d+) uncompressed=(\d+) .*)?")


def main() -> None:
    if len(sys.argv) not in (3, 4) or (len(sys.argv) == 4 and sys.argv[3] != "--by-source"):
        raise SystemExit("usage: summarize_source_work.py INPUT OUTPUT.csv [--by-source]")
    by_source = len(sys.argv) == 4
    text = html.unescape(Path(sys.argv[1]).read_text())
    columns = defaultdict(lambda: [0] * 11)
    for match in WORK.finditer(text):
        source, column = match.group(1), match.group(2)
        key = (source or "legacy", column) if by_source else column
        values = columns[key]
        for index, value in enumerate(match.groups()[2:]):
            values[index] += int(value)
    for match in DECOMPRESSION.finditer(text):
        source, column = match.group(1), match.group(2)
        key = (source or "legacy", column) if by_source else column
        values = columns[key]
        values[8] += int(match.group(3))
        values[9] += int(match.group(4) or 0)
        values[10] += int(match.group(5) or 0)

    headers = (["source"] if by_source else []) + [
        "column", "full_decoded", "selected_decoded", "dictionary_examined",
        "skipped", "published", "copied", "lazy_omitted", "null_examined",
        "decompressed_pages", "compressed_bytes", "uncompressed_bytes"]
    with Path(sys.argv[2]).open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(headers)
        for key, values in sorted(columns.items(), key=lambda item: sum(item[1][:3]), reverse=True):
            if by_source:
                source, column = key
                writer.writerow([source, column, *values])
            else:
                writer.writerow([key, *values])


if __name__ == "__main__":
    main()
