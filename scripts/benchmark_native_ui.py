"""Exercise the production native GUI worker, table and selected-message formatter."""
import argparse
import hashlib
import json
import os
import statistics
from pathlib import Path
from benchmark import ROOT, measure


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--topic", default="kafkalens-perf-balanced-20261014-source")
    parser.add_argument("--count", type=int, default=12000)
    parser.add_argument("--same-window", action="store_true")
    parser.add_argument("--label", default="native-ui-windows-lens")
    args = parser.parse_args()
    os.environ["KAFKALENS_CONFIG"] = str(ROOT / ".tools/perf-gui-profiles.json")
    os.environ["KAFKALENS_SMOKE_TOPIC"] = args.topic
    os.environ["KAFKALENS_SMOKE_LIMIT"] = str(args.count)
    os.environ["KAFKALENS_SMOKE_REPEATS"] = str(args.trials + 1 if args.same_window else 1)
    samples = []
    for index in ([-1] if args.same_window else range(-1, args.trials)):
        tag = f"{args.label}-{'same-' if args.same_window else ''}{index}"
        screenshot = ROOT / f"artifacts/performance/{tag}.png"
        row = measure([args.binary, screenshot], tag)
        events = [json.loads(line.removeprefix("UI_BENCH ")) for line in
                  (ROOT / f"artifacts/performance/raw/{tag}.stdout").read_text().splitlines()
                  if line.startswith("UI_BENCH ")]
        assert {e["phase"] for e in events} == {"data_ready", "frame_captured"}
        assert all(e["records"] == args.count for e in events)
        assert screenshot.exists()
        if args.same_window:
            for trial in range(args.trials + 1):
                samples.append({**row, "warmup": trial == 0,
                                "events": [e for e in events if e["trial"] == trial],
                                "screenshot": screenshot.name})
        else:
            row.update(warmup=index < 0, events=events, screenshot=screenshot.name)
            samples.append(row)
        print(json.dumps({"trial": tag, "events": events}), flush=True)
    summary = {phase: {"median": statistics.median(values), "mean": statistics.mean(values), "min": min(values), "max": max(values)}
               for phase in ["data_ready", "frame_captured"]
               for values in [[e["elapsed_seconds"] for s in samples if not s["warmup"]
                              for e in s["events"] if e["phase"] == phase]]}
    result = {"platform": "windows", "topic": args.topic, "count": args.count,
              "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "binary": str(args.binary), "samples": samples, "summary": summary,
              "same_window": args.same_window,
              "scope": "Test runner uses the production native UI and worker with smoke instrumentation. Read timers exclude connection/process startup. One discarded warmup. Frame timer includes screenshot readback. Repeated operation events in one window share whole-process resource readings."}
    name = args.label + ("-same-window" if args.same_window else "")
    (ROOT / f"artifacts/performance/{name}.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
