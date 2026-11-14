#!/usr/bin/env python3
"""Gzips one built bus version and lists its files for `wrangler r2 bulk put`.

The files are stored gzipped in R2, which is about a sixth of their plain size. The
Worker sends them as stored.
"""

import gzip
import json
import sys
from pathlib import Path


def pack(version_dir: Path, staging_dir: Path) -> list:
    entries = []
    for source in sorted(version_dir.rglob("*.json")):
        relative = source.relative_to(version_dir)
        packed = staging_dir / f"{relative}.gz"
        packed.parent.mkdir(parents=True, exist_ok=True)
        # A fixed mtime keeps the bytes the same from one build of a version to the next.
        with open(packed, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=9, mtime=0) as f:
            f.write(source.read_bytes())
        entries.append({"key": f"bus/{version_dir.name}/{relative.as_posix()}", "file": str(packed)})
    return entries


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("usage: bus_upload_list.py <version dir> <staging dir> <list file>", file=sys.stderr)
        sys.exit(1)
    entries = pack(Path(sys.argv[1]), Path(sys.argv[2]))
    if not entries:
        print(f"no files found in {sys.argv[1]}", file=sys.stderr)
        sys.exit(1)
    Path(sys.argv[3]).write_text(json.dumps(entries))
    print(f"packed {len(entries)} files")
