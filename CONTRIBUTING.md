# Contributing

Thanks for considering a contribution to `kaf-s3-connector`!

## Quick start
- Use Python 3.12+
- Install deps: `pip install -r requirements.txt pytest pytest-mock`
- Run tests: `python -m pytest`

## Workflow
- Fork and branch from `main`.
- Add tests for new behavior or bug fixes.
- Keep PRs focused and small when possible.

## Coding standards
- Follow existing style; add concise comments only where code isn’t self-evident.
- Prefer pure Python + stdlib; avoid extra deps unless necessary.
- Ensure new configuration is documented in README.

## Security
- Do not commit secrets. Use env vars for credentials (Kafka/S3).
- Preserve non-root runtime in Docker and Helm.

## Releases
- Use semantic versioning (v1.x.y). Tags trigger Docker builds and GitHub Releases. A changelog entry is required for releases.
