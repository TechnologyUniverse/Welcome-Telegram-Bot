# Changelog

## 1.6.0

Production preparation:

- Added a hardened Docker container and standalone Compose service.
- Moved persistent registry data to `/data` and registry backups to `/data/backups/`.
- Added registry size/structure validation and protected corrupted files from overwrite.
- Preserved atomic registry writes and made schema updates atomic.
- Added an exclusive startup lock, URL validation, HTML escaping and secret redaction.
- Pinned dependencies and added hash-verified runtime and test lock files.
- Added unit tests and GitHub Actions CI/GHCR publishing workflows with SHA-pinned actions.
- Standardized the documented/runtime Python version on 3.13.4.

No VPS configuration or production deployment is included in this release preparation.
