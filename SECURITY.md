# Security policy

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | yes |

## Reporting a vulnerability

Please **do not open a public issue** for security problems. Use GitHub's
[private vulnerability reporting](../../security/advisories/new) for this repository.
Include steps to reproduce, the affected component (API, CDC pipeline, Spark job) and
the impact. You will get an acknowledgement within 3 business days.

## Security design

- PII (SSN, Luhn-valid card numbers, email) is blocked at ingestion and **redacted**
  before anything is written to quarantine.
- Entitlements are enforced before retrieval scoring, so restricted text cannot leak
  through ranking, snippets or timing.
- `X-User-Groups` is a development stand-in. Production deployments must derive groups
  from validated identity-provider claims.
- Dependencies are monitored by Dependabot; code is scanned by CodeQL on every push.
