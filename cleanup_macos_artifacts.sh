#!/usr/bin/env bash
set -euo pipefail
find . -name "._*" -delete
find . -name ".DS_Store" -delete
find . -type d -name "__MACOSX" -prune -exec rm -rf {} +
echo "Removed macOS artifact files."
