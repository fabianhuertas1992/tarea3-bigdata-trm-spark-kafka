#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
BASE="$PWD"
ID="$(date +%Y%m%d_%H%M%S)"
TOPIC="sensor_data_demo_$ID"
SALIDA="resultados/demostraciones/$ID"
CHECKPOINT="runtime/checkpoint_$ID"
mkdir -p "$SALIDA"
printf 'DEMO_TOPIC=%q\nDEMO_SALIDA=%q\nDEMO_CHECKPOINT=%q\n' "$TOPIC" "$SALIDA" "$CHECKPOINT" > runtime/demo_actual.env
"$BASE/runtime/kafka_2.12-3.9.2/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 1 --replication-factor 1
printf 'Demostración nueva: %s\nTopic: %s\nResultados: %s\n' "$ID" "$TOPIC" "$SALIDA"
