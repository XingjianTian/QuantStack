# Privacy checks

This is the sole maintained repository: https://github.com/XingjianTian/QuantStack-Public.
The previous private repository is archival recovery material. Never merge old
branches or copy their `.git` directory into this repository.

After cloning, install Python 3 and Gitleaks 8.25+ (`brew install gitleaks` on
macOS), set your GitHub noreply email, and run `make privacy-setup`. Git does not
activate versioned hooks automatically in new clones. Run `make privacy-check`
for the same check manually. Missing scanner dependencies block the commit.

The pre-commit hook checks author and committer emails and exports the complete
Git index to a temporary directory. It rejects private configuration/evidence
paths, compares local private values, and runs the default Gitleaks rules on the
staged snapshot. Unstaged edits cannot hide a secret that remains in the index.
Private data and scanner reports are never uploaded; secret values are not printed.

The ignored `.privacy.local.json` can contain `{"forbidden_literals": [...]}` for
personal paths, email addresses, hostnames or other confidential strings. Keep
this policy local and never copy its real contents into documentation. Literal
credential values from the local `.env` are also checked. Each workstation must
configure its own local policy. The shared scanner allows only the two exact,
fictional curl credentials in the gateway verification script; it excludes no
whole file and disables no default detection rule.

Hooks can be bypassed and pattern detection is incomplete. Review staged changes
before committing, especially screenshots, binary files, prose and new secrets
that no rule recognizes. Keep real credentials in ignored `.env` files, never in
frontend variables. Do not force-add ignored configuration or private evidence.
