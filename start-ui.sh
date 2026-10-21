#!/usr/bin/env sh
# dip - Docker Image Puller  (Web UI launcher, Linux / macOS)
cd "$(dirname "$0")" || exit 1
exec python3 -u dip.py web "$@"
