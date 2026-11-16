# Security

- Never commit credentials. Secrets live in `.env` (git-ignored) or a secret store; Databricks reaches Azure through managed identities and Unity Catalog credentials.
- CI runs a secret scan (gitleaks) on every push and pull request.
- If a secret is ever exposed, rotate it immediately, then remove it from history.
- Report a vulnerability privately through GitHub's *Security → Report a vulnerability*.
