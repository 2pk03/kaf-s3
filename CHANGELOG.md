# Changelog

## v1.2.5
- Add version validation to PyPI workflow to prevent dev version uploads
- Note: v1.2.3 and v1.2.4 dev versions were incorrectly published due to setuptools-scm detecting commits after tags

## v1.2.4
- Fix setuptools-scm configuration to remove local version identifiers for PyPI compatibility

## v1.2.3
- Fix PyPI metadata configuration (add [project] section to pyproject.toml)
- Implement setuptools-scm for fully automated version management from git tags
- No manual version updates needed - version now derived automatically from git tags
- Update PyPI workflow to support setuptools-scm with full git history

## v1.2.2
- Version bump for release with setuptools pin and PyPI checks.

## v1.2.1
- Pin setuptools<75 for builds (avoid Metadata-Version 2.4 issues) and add twine check in PyPI workflow.
- Workflow triggers limited to tags or manual dispatch (CI/Docker/Release/PyPI).
- Multi-arch Docker builds (amd64, arm64) on tags.

## v1.1.0
- Packaging/metadata fixes to support PyPI trusted publishing flow.
- Docker pushes target ghcr.io/2pk03/kaf-s3-connector with tagged images.
- Added badges, issue templates, contributing guide, Helm chart, metrics samples, hardened non-root image.

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
