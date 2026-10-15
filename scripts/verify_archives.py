"""Compare archive content per partition, independently of inter-partition order."""
import argparse
import hashlib
import json
from pathlib import Path


def digest(path):
    hashes, counts, offsets = {}, {}, {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            partition = record["partition"]
            if record["offset"] <= offsets.get(partition, -1):
                raise RuntimeError(f"Non-increasing offsets in {path}: {partition}")
            offsets[partition] = record["offset"]
            hashes.setdefault(partition, hashlib.sha256()).update(
                json.dumps(record, sort_keys=True, separators=(",", ":")).encode())
            counts[partition] = counts.get(partition, 0) + 1
    return {"counts": counts, "partition_sha256": {p: h.hexdigest() for p, h in hashes.items()}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("reference", type=Path)
    parser.add_argument("archives", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference = digest(args.reference)
    for path in args.archives:
        if digest(path) != reference:
            raise RuntimeError(f"Archive content differs: {path}")
    result = {"status": "passed", "reference": str(args.reference),
              "archives": [str(p) for p in args.archives], **reference}
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
