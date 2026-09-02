---
name: issue-to-pr-lifecycle
description: Automate an issue-driven Rust change from branch creation through scoped commits, verification, PR merge, and cleanup.
---

# Issue To PR Lifecycle

Use this skill when a user gives a tracked issue and asks for the complete local delivery
cycle. It is for repository changes, not issue triage alone or general Git tutorials.

## Workflow

1. Read the complete issue, repository `AGENTS.md`, `CONTEXT.md`, relevant ADRs, and the
   issue-tracker instructions. Confirm acceptance criteria, non-goals, and the target base
   branch before editing.
2. Inspect `git status`, remotes, and the current branch. Preserve unrelated user changes.
   Create the repository's required issue branch name (`bug/<issue>`, `feature/<issue>`, or
   `enhance/<issue>` unless local policy says otherwise).
   Resolve the canonical GitHub repository before any PR or issue API call (for example,
   compare `git remote -v` with `gh repo view --json nameWithOwner,url`). Treat a redirect or
   renamed repository as authoritative and use its owner/name for all later remote operations.
3. Search existing callers and implementations before adding types or APIs. Prefer deletion,
   existing helpers, and standard-library facilities. Keep each commit scoped to its owning
   crate/module and order commits by dependency direction. Every issue commit includes
   `Refs #<issue>` or `Fixes/Closes #<issue>` as appropriate.
4. Before the final test gate, use focused compile checks while developing. At the final
   commit boundary run the repository-required format, diff, test/check, and lint commands.
   Run Cargo gates serially to avoid lock contention and misleading failures. Do not run tests
   during unfinished implementation when repository policy forbids it. If a required command
   fails, compare it with the base branch or a clean baseline when practical; classify
   pre-existing failures separately from regressions and do not call the gate passing while
   required checks remain unresolved.
   For non-Cargo validators, use the repository's active toolchain or virtual environment and
   record the actual executable (`command -v`, `sys.executable`, or equivalent) before blaming
   a missing dependency. Check configured alternate environments before reinstalling packages.
5. Commit immediately after a passing final gate. Push with an explicit upstream branch.
   Verify the pushed commit and branch contents with the canonical remote (`git ls-remote`,
   `gh pr view`, or equivalent), including after any retry caused by a network error. Create
   the PR against the requested base, including behavior summary, affected crates,
   verification commands, and issue linkage.
6. Inspect PR checks. Merge only when the user authorized merging and the repository's
   required checks or an explicit no-checks decision are understood. If GitHub reports no
   checks, state that fact instead of implying CI passed. Do not infer merge, issue-close,
   branch-delete, or destructive-cleanup authority from permission to implement or open a PR;
   obtain explicit authorization for each of those actions immediately before performing it.
7. After merge, verify the merge commit is an ancestor of the canonical base branch and that
   the PR and issue have the intended states. Verify local `main`, remote branch deletion, and
   the final worktree explicitly. Run destructive cleanup such as `cargo clean` only when
   explicitly requested, and report what was removed; never use cleanup to hide unrelated
   changes.

## Boundaries

- Never stage unrelated modified, deleted, or untracked files merely to make the worktree
  clean. Name them in the handoff and leave them untouched.
- Do not force-push, rewrite history, close issues, delete branches, or merge a PR without
  explicit user authorization for that action. A PR can be created without merging when merge
  authority is absent.
- Treat a moved repository or redirect as a real outcome: use the canonical repository for
  PR operations and report the resulting URL.
- If checks fail, keep the branch unmerged, fix the issue, and rerun the final gate. If no
  checks are configured, distinguish local verification from remote CI. A command that was
  skipped, blocked, or failed on the baseline is not a passing check; report it with its exact
  status and impact on acceptance.

## Output

Report the branch, scoped commit list, verification commands and results, PR URL, merge
commit, cleanup performed, and any remaining acceptance gaps. Keep the report concise unless
the user asks for a retrospective.
