"""Compare acknowledged replay through the supported CLI, with fresh processes."""
import argparse
import json
import re
import statistics
import sys
from pathlib import Path
from benchmark import ROOT, measure


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--offset-project", type=Path, default=ROOT / "benchmarks/offset-replay.json")
    args = parser.parse_args()
    platform = "windows" if sys.platform == "win32" else "linux"
    offset = ROOT / ".tools/offset-explorer/app/commandline.exe" if platform == "windows" else Path.home() / ".local/opt/offset-explorer-bench/commandline"
    lens = ROOT / "target/release/kafunekafka-bench.exe" if platform == "windows" else Path.home() / ".cache/kafunekafka-target/release/kafunekafka-bench"
    topic = "kafunekafka-perf-balanced-20261014-source"
    destination = "kafunekafka-perf-balanced-20261014-destination"
    commands = {"offset-explorer-4.0.5": [offset, "-exportMessages", args.offset_project],
                "kafunekafka-0.2.0": [lens, "replay", topic, "12000", "unused", destination]}
    rows = []
    for index in range(-1, args.trials):
        for product in (list(commands) if index % 2 == 0 else list(reversed(commands))):
            tag = f"replay-{platform}-{product}-{index}"
            row = measure(commands[product], tag)
            if product.startswith("offset"):
                output = (ROOT / f"artifacts/performance/raw/{tag}.stdout").read_text()
                count = int(re.search(r"Total number of messages exported:\s*(\d+)", output)[1])
                core = int(re.search(r"Time taken:\s*(\d+) ms", output)[1]) / 1000
            else:
                count = row["backend"]["report"]["delivered"]
                core = row["backend"]["elapsed_seconds"]
            if count != 12000:
                raise RuntimeError(f"{tag}: only {count} messages delivered")
            row.update(product=product, warmup=index < 0, delivered=count, operation_seconds_reported=core)
            rows.append(row)
            print(json.dumps({"trial": tag, "wall_seconds": row["wall_seconds"], "operation_seconds_reported": core}), flush=True)
    summary = {}
    for product in commands:
        trials = [r for r in rows if r["product"] == product and not r["warmup"]]
        summary[product] = {key: {"median": statistics.median(r[key] for r in trials),
                                 "min": min(r[key] for r in trials), "max": max(r[key] for r in trials)}
                            for key in ["wall_seconds", "operation_seconds_reported", "cpu_seconds_sampled", "peak_rss_bytes_sampled"]}
    result = {"platform": platform, "count": 12000, "partitions": 3, "samples": rows, "summary": summary,
              "clock_scope": "Wall time includes process startup. Product-reported operation clocks have different boundaries and are not an equivalent GUI benchmark."}
    (ROOT / f"artifacts/performance/replay-comparison-{platform}.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
