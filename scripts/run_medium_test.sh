#!/usr/bin/env bash
set -euo pipefail
python3 crawler.py -s seeds/seeds-teste.txt -n 1000 --threads 16
