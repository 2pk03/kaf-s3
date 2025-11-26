# Changelog

## v1.0.2
- Packaging/metadata fixes to support PyPI trusted publishing flow.

## v1.0.1
- Fixed packaging metadata and release automation for PyPI/GitHub releases.
- Docker pushes now target ghcr.io/2pk03/kaf-s3-connector; GHCR pull instructions added.
- Added badges, issue templates, and contributing guide for better adoption.
- Added Helm chart, metrics samples, and hardened non-root image.

## v1.0.0
- Hardened non-root Docker image with healthcheck and Prometheus `/metrics`.
- Env-driven runtime entrypoint for producer/consumer, DLQ + metrics hooks.
- Added gzip compression, deterministic keys, TTL metadata, SSE/SSE-KMS support.
- DLQ handling on producer delivery errors and consumer integrity failures.
- Optional inline handling, bucket/prefix enforcement, SHA-256 + ETag integrity checks.
- Helm chart for deployment, Prometheus scrape config, Grafana dashboard sample.
- GitHub Actions for tests and Docker build/push.
