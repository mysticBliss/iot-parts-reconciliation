#!/usr/bin/env bash
# Deploy Connectors to Kafka Connect REST API

CONNECT_URL="http://localhost:8083"

echo "Checking Kafka Connect status at $CONNECT_URL..."
curl -s -f "$CONNECT_URL/" > /dev/null || {
  echo "Error: Kafka Connect is not reachable at $CONNECT_URL."
  exit 1
}

echo -e "\nDeploying MQTT Source Connector..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @../connectors/mqtt-source-connector.json \
  "$CONNECT_URL/connectors" | jq . || true

echo -e "\nActive Connectors:"
curl -s "$CONNECT_URL/connectors" | jq .
