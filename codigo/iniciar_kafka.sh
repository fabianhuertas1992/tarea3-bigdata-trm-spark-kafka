#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
BASE="$PWD"
KAFKA="$BASE/runtime/kafka_2.12-3.9.2"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH="$JAVA_HOME/bin:$PATH"
export LOG_DIR="$BASE/registros/kafka"
mkdir -p "$LOG_DIR" "$BASE/runtime/zookeeper" "$BASE/runtime/kafka-logs"
if [ ! -x "$KAFKA/bin/kafka-server-start.sh" ]; then echo 'Falta instalar Kafka 3.9.2 en runtime. Consultar README.'; exit 1; fi
cat > "$BASE/runtime/zookeeper.properties" <<EOF
tickTime=2000
dataDir=$BASE/runtime/zookeeper
clientPort=2181
clientPortAddress=127.0.0.1
maxClientCnxns=0
admin.enableServer=false
EOF
cat > "$BASE/runtime/server.properties" <<EOF
broker.id=0
listeners=PLAINTEXT://127.0.0.1:9092
advertised.listeners=PLAINTEXT://127.0.0.1:9092
log.dirs=$BASE/runtime/kafka-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
zookeeper.connect=127.0.0.1:2181
zookeeper.connection.timeout.ms=18000
log.retention.hours=24
EOF
if ! ss -ltnH 'sport = :2181' | grep -q .; then
 KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' "$KAFKA/bin/zookeeper-server-start.sh" -daemon "$BASE/runtime/zookeeper.properties"
fi
for i in $(seq 1 30); do ss -ltnH 'sport = :2181' | grep -q . && break; sleep 1; done
if ! ss -ltnH 'sport = :9092' | grep -q .; then
 KAFKA_HEAP_OPTS='-Xms256m -Xmx512m' "$KAFKA/bin/kafka-server-start.sh" -daemon "$BASE/runtime/server.properties"
fi
for i in $(seq 1 45); do
 if "$KAFKA/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then break; fi
 sleep 1
done
"$KAFKA/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic sensor_data --partitions 1 --replication-factor 1
"$KAFKA/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic sensor_data
