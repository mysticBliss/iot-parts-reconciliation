#!/usr/bin/env python3
"""
Deploy Connectors to Kafka Connect REST API
Reads connector JSON definitions, interpolates environment variables from .env,
and deploys or updates connectors on the Kafka Connect cluster.
"""

import os
import sys
import json
import re
import requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_DIR, ".env")
CONNECTORS_DIR = os.path.join(PROJECT_DIR, "connectors")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")

load_dotenv(ENV_PATH)

def replace_env_vars(text: str) -> str:
    """Replace ${VAR} with environment variables."""
    def replacer(match):
        var_name = match.group(1)
        val = os.getenv(var_name)
        if val is None:
            print(f"[WARN] Environment variable '{var_name}' not set in .env")
            return match.group(0)
        return val
    return re.sub(r'\$\{([A-Za-z0-9_]+)\}', replacer, text)

def check_connect_health():
    print(f"Checking Kafka Connect cluster at {CONNECT_URL}...")
    try:
        r = requests.get(f"{CONNECT_URL}/", timeout=10)
        r.raise_for_status()
        info = r.json()
        print(f"[OK] Kafka Connect is UP (Version: {info.get('version')}, Cluster ID: {info.get('kafka_cluster_id')})")
        return True
    except Exception as e:
        print(f"[ERROR] Cannot connect to Kafka Connect at {CONNECT_URL}: {e}")
        return False

def deploy_connector(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        raw_content = f.read()

    interpolated = replace_env_vars(raw_content)
    try:
        data = json.loads(interpolated)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON in {file_path}: {e}")
        return

    connector_name = data.get("name")
    config = data.get("config", {})

    print(f"\n--- Deploying {connector_name} ---")

    # Check if connector exists
    status_url = f"{CONNECT_URL}/connectors/{connector_name}/status"
    check_r = requests.get(status_url)

    if check_r.status_code == 200:
        print(f"Connector '{connector_name}' already exists. Updating configuration...")
        update_r = requests.put(f"{CONNECT_URL}/connectors/{connector_name}/config", json=config)
        if update_r.status_code in [200, 201]:
            print(f"[OK] Connector '{connector_name}' updated successfully.")
        else:
            print(f"[ERROR] Update failed ({update_r.status_code}): {update_r.text}")
    else:
        create_r = requests.post(f"{CONNECT_URL}/connectors", json=data)
        if create_r.status_code in [200, 201]:
            print(f"[OK] Connector '{connector_name}' created successfully.")
        else:
            print(f"[ERROR] Creation failed ({create_r.status_code}): {create_r.text}")

def main():
    if not check_connect_health():
        sys.exit(1)

    # The JDBC sink to PostgreSQL belongs to the earlier Superset design and is no
    # longer part of the stack - docker-compose does not start Postgres.
    connectors_to_deploy = [
        "mqtt-source-connector.json",
    ]

    for c in connectors_to_deploy:
        path = os.path.join(CONNECTORS_DIR, c)
        if os.path.exists(path):
            deploy_connector(path)
        else:
            print(f"[WARN] File not found: {path}")

    # List active connectors
    r = requests.get(f"{CONNECT_URL}/connectors")
    print(f"\nActive Connectors on Cluster: {r.json()}")

if __name__ == "__main__":
    main()
