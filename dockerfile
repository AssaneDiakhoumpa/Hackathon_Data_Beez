FROM apache/airflow:2.9.1

USER root

# Installer des packages système
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && update-ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copier requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Installer les dépendances Python en tant qu'utilisateur airflow
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt



