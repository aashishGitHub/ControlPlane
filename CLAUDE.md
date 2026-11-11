# CLAUDE.md — working rules for this repo

This is the Avid take-home: a tenant provisioning control plane (Go + Postgres +
RabbitMQ). Full plan, milestones, and progress tracking live **outside this repo** at
`c:/PROJECT/aashishGithub/Avid/plan.md` — read that first each session, not this file
alone.

## Source of truth

- `docs/PRD.md` — requirements (FR/NFR), multi-tenant rationale.
- `docs/DESIGN.md` — schema, state machines, API table, message contracts, decisions
  table. Read only the section the current milestone needs; don't re-read the whole file
  every session.
- `../plan.md` (one level up) — milestones, learning-mode rules, progress checklist.

## Learning mode (non-negotiable)

The user is preparing for a live interview on this code and must run every command
themselves and be able to explain every line.

- **Never run install, build, test, docker, or git commands.** Only read/write files.
  Show the exact command, say what it does and what output to expect, then ask the user
  to run it and paste the output.
- Before writing a key piece of logic (a conditional UPDATE, the outbox relay, the inbox
  insert, the rank check, nack classification), ask the user one short prediction
  question first.
- At the end of a milestone, quiz the user with 2-3 questions before moving on.
- Keep replies terse. The code comments carry the explanation — don't repeat them in
  chat.

## Conventions

- Layout: `cmd/controlplane`, `cmd/worker`, `internal/{domain,store,service,httpapi,messaging,outbox}`.
- `domain` has zero I/O — pure functions and types only.
- Every package gets a `doc.go` with a "Patterns used here" list.
- At each decision point in code, comment `// Pattern: ... Why: ... Trade-off: ...` and
  point at the relevant row in `docs/DESIGN.md` §7 (decisions table).
- Comment the *why*, never the *what*.
- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`), one commit per
  milestone, written by the user.
