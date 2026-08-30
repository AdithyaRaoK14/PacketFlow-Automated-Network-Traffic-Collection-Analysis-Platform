FROM haproxy:2.9

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends tcpdump iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
