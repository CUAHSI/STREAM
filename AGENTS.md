## Agent skills

## Communication

- Explain questions, choices, and progress in plain language for a PhD-level researcher who is developing software skills. Avoid software-engineering jargon; when a technical term is useful, explain it briefly.

## Shell and local operations

- Do not ask for permission for routine, non-destructive work in the repository, including `pwd`, `cd`, `ls`, reading/searching files, and inspecting local project state.
- Do not ask each time before read-only Git inspection such as `git status`, `git diff`, or `git log`.
- Avoid unnecessary shell commands, redundant checks, and repeated tool calls.
- Do not install dependencies or global tools unless necessary for the requested work.

## Git safety

- When asked to work on a GitHub issue, read the issue and create a new branch for that issue before editing files. Name the branch `issue-<number>-<short-description>`, using lowercase words separated by hyphens.
- Get explicit authorization before running `git reset --hard`, `git clean -fd`, `git push`, `git commit`, or any command that discards user changes or publishes changes.
- Never discard or overwrite user changes without explicit authorization.

### Issue tracker

Issues are tracked in GitHub Issues via `gh`. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the default five triage labels. See `docs/agents/triage-labels.md`.

### Domain docs

Use the single-context domain docs layout. See `docs/agents/domain.md`.