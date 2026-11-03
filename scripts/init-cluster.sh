#!/bin/bash
set -euo pipefail
CFG=/etc/kafka/secrets/admin.properties
A=kafka-a1:9092
B=kafka-b1:9092

wait_for() {
  local bootstrap="$1"
  local ready=0
  for _ in $(seq 1 40); do
    if kafka-topics --bootstrap-server "$bootstrap" --command-config "$CFG" --list >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 3
  done
  if [ "$ready" -ne 1 ]; then
    echo "not ready: $bootstrap" >&2
    exit 1
  fi
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  shift 2
  kafka-topics --bootstrap-server "$A" --command-config "$CFG" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    "$@"
}

wait_for "$A"
wait_for "$B"

create_topic shop.products 3 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config segment.bytes=1073741824
create_topic shop.products.clean 3 --config cleanup.policy=delete
create_topic shop.banned 3 --config cleanup.policy=compact
create_topic client.searches 3 --config cleanup.policy=delete
create_topic client.recommendation-requests 3 --config cleanup.policy=delete
create_topic recommendations 3 --config cleanup.policy=delete
create_topic connect-configs 1 --config cleanup.policy=compact
create_topic connect-offsets 3 --config cleanup.policy=compact
create_topic connect-status 3 --config cleanup.policy=compact

kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --allow-principal User:shop --operation Write --operation Describe --topic shop.products
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --deny-principal User:shop --operation Write --topic shop.products.clean
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --deny-principal User:shop --operation Write --topic recommendations
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --allow-principal User:client --operation Write --operation Describe --topic client.searches
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --allow-principal User:client --operation Write --operation Describe --topic client.recommendation-requests
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --allow-principal User:client --operation Read --operation Describe --topic recommendations
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --allow-principal User:client --operation Read --operation Describe --group client-recommendations
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --deny-principal User:client --operation Write --topic shop.products
kafka-acls --bootstrap-server "$A" --command-config "$CFG" \
  --add --allow-principal User:spark --operation Write --operation Describe --topic recommendations

kafka-topics --bootstrap-server "$A" --command-config "$CFG" --describe > /evidence/topics.txt
kafka-acls --bootstrap-server "$A" --command-config "$CFG" --list > /evidence/acls.txt
echo "cluster B answers" >> /evidence/topics.txt
kafka-topics --bootstrap-server "$B" --command-config "$CFG" --list >> /evidence/topics.txt
echo "topics and ACLs are ready"
