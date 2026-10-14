# Deployment 1.6.1 — 2026-10-14

Deployed on the existing Contabo VPS with owner approval.

- Existing Welcome Bot polling instance replaced; existing token retained.
- Bot image: technology-universe-welcome:1.6.1.
- Storage image: technology-universe-files:2.0.0-storage-feed-161.
- Both services passed Docker health checks before enabling notifications.
- Scoped metadata endpoint ready; 159 existing files baselined without announcements.
- Authenticated object HEAD succeeded.
- Explicit test photo delivered to the configured supergroup; Telegram message ID 7210.
- Automatic notifications enabled after the successful test.
- Original compose/environment and bot data backup retained on VPS.
- No other production services changed.

The first live check exposed partial chunked-response JSON decoding; fixed by bounded chunk aggregation and covered by a regression test. One object HEAD request timed out; the subsequent check succeeded. No publication was attempted on that timeout.

No real user files were uploaded, replaced or deleted during deployment. New-file batching/replacement/restart/error paths are covered by automated tests; future real uploads exercise the production end-to-end path.

The deployed storage artifact patches only the existing server bundle with the metadata route; public assets are unchanged. The source-level integration patch is included alongside this document. No credentials are included in this repository.
