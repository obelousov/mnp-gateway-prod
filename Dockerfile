FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
# alembic MySQL driver dependency
    default-libmysqlclient-dev \
# alembic dependency    
    pkg-config \
    libssl-dev \
    curl wget \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Clean up build dependencies
# RUN apt-get purge -y gcc libssl-dev && \
# UPDATED from alembic Dockerfile
RUN apt-get purge -y gcc libssl-dev pkg-config default-libmysqlclient-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "main:app", "--bind", "0.0.0.0:8080"]