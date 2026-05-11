FROM apache/superset:latest

USER root

# Install dependency untuk mysqlclient
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# copy requirements
COPY requirements.txt /app/requirements.txt

# Install pip ke venv + dependency
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir -r /app/requirements.txt

USER superset