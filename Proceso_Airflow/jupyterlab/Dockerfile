FROM jupyter/base-notebook:python-3.11

USER root
SHELL ["/bin/bash", "-lc"]

RUN set -euxo pipefail \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      curl gnupg ca-certificates \
      unixodbc unixodbc-dev odbcinst \
 && rm -rf /var/lib/apt/lists/*

RUN set -euxo pipefail \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
 && curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 \
 && rm -rf /var/lib/apt/lists/*

RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' > /etc/profile.d/mssql-tools.sh

RUN ldconfig && ldconfig -p | grep -q 'libodbc\.so\.2'

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER $NB_UID
