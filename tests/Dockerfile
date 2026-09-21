# syntax=docker/dockerfile:1.7

ARG INTEGRATION_BASE_IMAGE=ghcr.io/voismart/mod-gemini-audio-stream-ci:integration
FROM ${INTEGRATION_BASE_IMAGE}

WORKDIR /work
COPY . /work

CMD ["./tests/run-ci.sh"]
