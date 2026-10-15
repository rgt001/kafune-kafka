"""Alternate old/new native GUI sessions; keep every warmup and measured read."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
from benchmark import ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--batches", type=int, default=5)
    parser.add_argument("--trials", type=int, default=9)
    args = parser.parse_args()
    assert 1 <= args.trials <= 9 and args.batches > 0
    env = os.environ.copy()
    env.pop("KAFKALENS_TRACE", None)
    runs = {"baseline": [], "optimized": []}
    for batch in range(args.batches):
        order = ["baseline", "optimized"] if batch % 2 == 0 else ["optimized", "baseline"]
        for name in order:
            binary = args.baseline if name == "baseline" else args.binary
            label = f"gui-ab-{name}-{batch}"
            subprocess.run([sys.executable, ROOT / "scripts/benchmark_native_ui.py", "--binary", binary,
                            "--same-window", "--trials", str(args.trials), "--label", label],
                           cwd=ROOT, env=env, check=True, stdout=subprocess.DEVNULL)
            path = ROOT / f"artifacts/performance/{label}-same-window.json"
            runs[name].append(json.loads(path.read_text()))
            print(json.dumps({"batch": batch, "application": name, "summary": runs[name][-1]["summary"]}), flush=True)
    summary = {}
    for name, batches in runs.items():
        rows = [row for batch in batches for row in batch["samples"] if not row["warmup"]]
        summary[name] = {}
        for phase in ["data_ready", "frame_captured"]:
            values = [e["elapsed_seconds"] for row in rows for e in row["events"] if e["phase"] == phase]
            summary[name][phase] = {"count": len(values), "median": statistics.median(values),
                                    "mean": statistics.mean(values), "min": min(values), "max": max(values),
                                    "individual_seconds": values}
        for metric in ["peak_rss_bytes_sampled", "peak_private_bytes_sampled", "cpu_seconds_sampled"]:
            # Resources belong to an entire GUI process, not to each read.
            values = [batch["samples"][0][metric] for batch in batches]
            summary[name][metric] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
    result = {"platform": "windows", "topic": "kafkalens-perf-balanced-20261014-source", "count": 12000,
              "baseline_sha256": hashlib.sha256(args.baseline.read_bytes()).hexdigest(),
              "optimized_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "runs": runs, "summary": summary,
              "scope": "Same native production UI, table and JSON formatter. Alternating old/new process order. Each window connects once, discards one warmup, then measures repeated reads. Timings include worker scheduling and UI data/layout; frame timings additionally include screenshot readback. Trace logging disabled. No outliers removed. Process resource peaks include startup and all reads; they are not idle measurements.",
              "offset_explorer_user_reported_mean_seconds": .180,
              "offset_explorer_limit": "User-reported mean from the prior manual run; individual values and its timer boundaries are unavailable. This is not a fresh automated comparison with Offset Explorer."}
    (ROOT / "artifacts/performance/gui-windows-optimization.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
