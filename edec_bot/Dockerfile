FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Data directory symlinked to HA persistent storage at /data
RUN mkdir -p /data/edec

CMD ["/bin/bash", "/app/run.sh"]
