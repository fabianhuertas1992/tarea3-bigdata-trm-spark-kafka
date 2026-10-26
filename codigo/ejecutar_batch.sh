#!/usr/bin/env bash
set -euo pipefail
source "$HOME/.spark-env"
cd "$(dirname "$0")/.."
spark-submit codigo/batch_trm.py --entrada hdfs://localhost:9000/Tarea3_TRM_2026/trm_original.csv --fecha-corte 2026-10-25 --salida resultados/batch
