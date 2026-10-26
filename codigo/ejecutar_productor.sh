#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f runtime/demo_actual.env ]; then echo 'Ejecute primero bash codigo/preparar_demostracion.sh'; exit 1; fi
source runtime/demo_actual.env
"$HOME/venvs/spark/bin/python" -u codigo/kafka_producer.py --topic "$DEMO_TOPIC" --salida "$DEMO_SALIDA/productor.jsonl" --eventos 120 --intervalo 1
