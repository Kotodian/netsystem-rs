# Domain Docs

This repo uses a single-context domain documentation layout.

## Before Exploring

Read these when a task touches architecture, domain language, TCP/VPP semantics, or triage wording:

- `CONTEXT.md` at the repo root
- ADRs under `docs/adr/`

If a file is absent, proceed silently. The domain-modeling flow creates or updates domain docs when new vocabulary or decisions are resolved.

## Vocabulary

Use the glossary vocabulary from `CONTEXT.md` when naming issues, plans, tests, or code concepts. Avoid terms explicitly listed as "Avoid" in the glossary.

## ADR Conflicts

If a proposal or implementation conflicts with an ADR under `docs/adr/`, surface the conflict explicitly instead of silently overriding the decision.
