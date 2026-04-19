#!/usr/bin/env bash
set -euo pipefail
python3 crawler.py -s seeds/seeds-teste.txt -n 50 -d --threads 8
