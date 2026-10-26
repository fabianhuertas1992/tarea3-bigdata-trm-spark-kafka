#!/usr/bin/env bash
set -euo pipefail
source "$HOME/.spark-env"
cd "$(dirname "$0")/.."
if [ ! -f runtime/demo_actual.env ]; then echo 'Ejecute primero bash codigo/preparar_demostracion.sh'; exit 1; fi
source runtime/demo_actual.env
spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 codigo/spark_streaming_consumer.py --topic "$DEMO_TOPIC" --salida "$DEMO_SALIDA" --checkpoint "$DEMO_CHECKPOINT" --duracion 240
