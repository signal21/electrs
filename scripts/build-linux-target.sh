#!/bin/bash

set +e
set +x

export OPENSSL_DIR=/opt/homebrew/Cellar/openssl@3/
cargo build --bin btcdb --target aarch64-unknown-linux-gnu --release

scp -i ~/.ssh/dev_rsa -P 19342  target/aarch64-unknown-linux-gnu/release/btcdb   ubuntu@129.146.148.10:/mnt/opt/electrs2/