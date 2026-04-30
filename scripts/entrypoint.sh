#!/bin/bash
set -e

# ============================================================
# Airflow Custom Entrypoint
# ============================================================
# 1. Waits for Postgres to be ready
# 2. Creates Databricks + AWS connections from env vars
# 3. Starts the requested Airflow component
# ============================================================

# ── Wait for Postgres ────────────────────────────────────────
wait_for_postgres() {
    echo "Waiting for PostgreSQL at ${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}..."
    # Use Python (always available in Airflow image) instead of netcat
    while ! python3 -c "
import socket, sys
try:
    s = socket.create_connection(('${POSTGRES_HOST:-postgres}', ${POSTGRES_PORT:-5432}), timeout=2)
    s.close()
except Exception:
    sys.exit(1)
" 2>/dev/null; do
        sleep 1
    done
    echo "PostgreSQL is ready!"
}

# ── Create Airflow connections ───────────────────────────────
setup_connections() {
    echo "Setting up Airflow connections..."

    # -- Databricks connection --
    # Uses --conn-password for the PAT token (Airflow Databricks provider convention)
    if [ -n "$DATABRICKS_HOST" ] && [ -n "$AIRFLOW_DATABRICKS_TOKEN" ]; then
        airflow connections delete databricks_default 2>/dev/null || true

        airflow connections add databricks_default \
            --conn-type databricks \
            --conn-host "https://${DATABRICKS_HOST}" \
            --conn-password "${AIRFLOW_DATABRICKS_TOKEN}"

        echo "✅ Created connection: databricks_default -> https://${DATABRICKS_HOST}"
    else
        echo "⚠️  Skipping Databricks connection — DATABRICKS_HOST or AIRFLOW_DATABRICKS_TOKEN not set"
    fi

    # -- AWS connection --
    if [ -n "$AWS_ACCESS_KEY_ID" ] && [ -n "$AWS_SECRET_ACCESS_KEY" ]; then
        airflow connections delete aws_default 2>/dev/null || true

        airflow connections add aws_default \
            --conn-type aws \
            --conn-login "${AWS_ACCESS_KEY_ID}" \
            --conn-password "${AWS_SECRET_ACCESS_KEY}" \
            --conn-extra "{\"region_name\": \"${AWS_REGION:-ap-south-1}\"}"

        echo "✅ Created connection: aws_default -> ${AWS_REGION:-ap-south-1}"
    else
        echo "⚠️  Skipping AWS connection — AWS credentials not set"
    fi
}

# ── Main ─────────────────────────────────────────────────────
wait_for_postgres

# Only set up connections for webserver (avoid race with scheduler)
if [ "$1" = "webserver" ]; then
    setup_connections
fi

# Hand off to the official Airflow entrypoint
exec airflow "$@"
