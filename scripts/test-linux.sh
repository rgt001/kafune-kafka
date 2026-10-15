#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-8}"
cargo test --locked --lib
cargo test --locked --test kafka_end_to_end -- --ignored --nocapture
cargo build --locked
./target/debug/kafka-lens --smoke-ui artifacts/ui-linux.png
