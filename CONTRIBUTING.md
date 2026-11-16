# Contributing

1. Branch from `main` (`feature/<topic>` or `fix/<topic>`), open a pull request; CODEOWNERS review is required.
2. `uv sync`, then `make check` (ruff + pytest) before pushing.
3. Commit messages: short imperative subject (`Add ...`, `Fix ...`), blank line, then the why. One logical change per commit.
4. Pipeline changes (`src/sample_clames_pipeline/`): keep one table per file, document expectations, and describe the downstream impact in the PR.
5. Never commit secrets, `.env`, photos or third-party datasets. See [SECURITY.md](SECURITY.md).
6. Add or update a decision record in `docs/adr/` for architectural choices.
