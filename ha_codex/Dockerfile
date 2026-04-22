FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /runner

COPY run.sh /runner/run.sh

RUN chmod +x /runner/run.sh

CMD ["/bin/bash", "/runner/run.sh"]
