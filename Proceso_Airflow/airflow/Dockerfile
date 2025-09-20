FROM apache/airflow:2.10.5-python3.8

ENV AIRFLOW_HOME=/opt/airflow
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

USER root
RUN set -eux; \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        unixodbc \
        unixodbc-dev \
        curl \
        gnupg && \
    . /etc/os-release; \
    if [ "${VERSION_CODENAME:-}" = "bookworm" ]; then export MS_DEB_VER=12; \
    elif [ "${VERSION_CODENAME:-}" = "bullseye" ]; then export MS_DEB_VER=11; \
    else export MS_DEB_VER=12; fi; \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -; \
    curl -fsSL "https://packages.microsoft.com/config/debian/${MS_DEB_VER}/prod.list" \
        > /etc/apt/sources.list.d/mssql-release.list; \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        mssql-tools18 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.8
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"
