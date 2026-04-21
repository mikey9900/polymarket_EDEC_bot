FROM rust:1.91-slim AS polymarket-cli-builder

ARG POLYMARKET_CLI_REF=4b5a749d5bf04f23611544a059e2a15c7281ae83

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/polymarket-cli
RUN git clone https://github.com/Polymarket/polymarket-cli.git .
RUN git checkout ${POLYMARKET_CLI_REF}
RUN cargo build --release --locked

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install pinned Polymarket CLI binary for operator tooling
COPY --from=polymarket-cli-builder /tmp/polymarket-cli/target/release/polymarket /usr/local/bin/polymarket
RUN polymarket --version

# Copy bot code
COPY . .

# Data directory symlinked to HA persistent storage at /data
RUN mkdir -p /data/edec

CMD ["/bin/bash", "/app/run.sh"]
