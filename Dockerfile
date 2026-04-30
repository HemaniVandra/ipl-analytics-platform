# ============================================================
# Airflow + Databricks  —  Custom Docker Image
# ============================================================
# Base: Official Apache Airflow with Python 3.11
# Adds: Databricks provider, AWS provider, project dependencies
# ============================================================

FROM apache/airflow:2.9.3-python3.11

USER root

# Install system deps needed by some Python packages (e.g., selenium)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy the entrypoint script
COPY scripts/entrypoint.sh /entrypoint.sh

USER root
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]
