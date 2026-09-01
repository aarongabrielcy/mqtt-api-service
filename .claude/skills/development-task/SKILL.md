---
name: development-task
description: Execute an SDO Development Task in this repository (mqtt-api-service) — Authority Gate, immutable FR/AC, minimal implementation, deterministic validation, restricted Notion updates.
---

# development-task — SDO V1 reusable execution procedure

This Skill is the reusable procedure for executing an SDO Development Task in this repository. It
supersedes ad-hoc task execution; use it whenever a Notion Development Task in this repository is
handed to you, unless the execution prompt is itself an explicit bootstrap exception that says not
to invoke this Skill (as was the case for the task that created this Skill).

## 1. Authority Gate — before any edit

1. Read the task from the Notion Development Tasks database.
2. Verify exactly, against the Notion page's properties:
   - Task ID matches what was given to you.
   - Project = `saas-system-iot`.
   - Repository = `mqtt-api-service`.
   - Status matches what the execution prompt claims (typically `READY FOR DEVELOPMENT`, or
     `CHANGES REQUESTED` for a corrective iteration explicitly authorized to skip back to
     `READY FOR DEVELOPMENT`).
   - Prompt Version matches what the execution prompt claims.
3. Read the complete task specification. Treat every FR-* and AC-* as immutable: do not renumber,
   merge, split, weaken, strengthen, or reinterpret them.
4. Read the authoritative Notion `Project Context — saas-system-iot` page for project-level facts
   and SDO policy.
5. Inspect the current Git branch and the complete working tree, including untracked files
   (`git status --short`, `git status --porcelain`, `git ls-files`). If unrelated,
   not-yet-authorized user changes make safe scope isolation impossible, STOP without modifying
   any file.
6. Re-verify every repository fact the task relies on directly against current repository files
   — do not trust the task's or Notion's description of repository behavior without checking. See
   [docs/sdo/PROJECT_CONTEXT.md](../../../docs/sdo/PROJECT_CONTEXT.md) for the last-confirmed deep
   context, but re-confirm anything the task depends on against the actual current files, since
   that document can go stale.
7. If repository evidence materially contradicts the task in a way that would require changing
   FR/AC, architecture, public API/proto contracts, database schema, dependencies, or security
   policy, STOP and report the blocker. Do not invent a resolution.
8. If the gate passes, transition the task's Status in Notion to the authorized "in progress"
   state (typically `READY FOR DEVELOPMENT` → `IN DEVELOPMENT`) before editing any file.

## 2. Implementation

- Change only the files the task's Authorized Changes / Expected Result section names. Follow
  [CLAUDE.md](../../../CLAUDE.md) for the repository's permanent operating rules (scope, secrets,
  Git-write authorization, architecture/contract preservation, configuration verification).
- Prefer the smallest change that satisfies every FR/AC. Do not add unrelated refactors, new
  abstractions, or speculative future-proofing.
- Mark any repository fact you cannot directly verify as UNKNOWN in whatever you write, rather
  than asserting it.

## 3. Deterministic validation

Run the validation commands the task specifies (for this repository, typically):
```
go test ./...
go vet ./...
go build ./...
git diff --check
git status --short
git diff --name-only
git ls-files --others --exclude-standard
```
`go build ./cmd/api` is the canonical entrypoint build and is what `Dockerfile` uses for the
production image; `go build ./...` covers the same single `main` package plus every other package
in one command. Record the **actual** exit codes and output — a claim of "tests pass" without
having run the command in this session is not evidence and must not be reported as such. Review
the complete diff and the full contents of any new untracked file for scope and secret exposure
before reporting completion.

## 4. Notion updates — restricted authority

- Update only the fields the task's execution prompt authorizes (typically: Status, Build, Tests,
  Claude Report, Implementation Notes / Final Development Report).
- Only move Status through the transitions the task's Execution Authority explicitly authorizes
  (typically `READY FOR DEVELOPMENT` → `IN DEVELOPMENT` → `READY FOR VALIDATION`, or the
  equivalent corrective-iteration path).
- Set `Build=Pass` / `Tests=Pass` only when the corresponding commands were actually run in this
  session and exited 0. Otherwise leave them as-is or mark the failure explicitly.
- **Never set `AI Review=Pass`, `APPROVED`, or `DONE`.** Those transitions require a human
  decision and are outside Claude's authority under SDO V1, regardless of how confident the
  validation evidence looks.

## Final Report Consolidation and READY FOR VALIDATION Read-Back

Before transitioning an authorized Development Task to READY FOR VALIDATION:

1. Maintain exactly one current authoritative Final Claude Report for the Task. Interim, completion, or corrective conclusions must not remain simultaneously authoritative. Replace stale conclusions or explicitly mark them superseded while preserving useful historical evidence.

2. Ensure the current Final Claude Report is internally consistent with the implementation state, executed validation evidence, remaining human gates, and the transition being requested. Do not claim a previously pending human gate is satisfied without current evidence.

3. Re-fetch the authoritative Notion Task immediately before the READY FOR VALIDATION transition. Verify that the current Final Claude Report is durably present and internally consistent and that any previously pending human gate claimed as satisfied has current supporting evidence in the Task/report.

4. If the Notion read-back fails or reveals stale, contradictory, missing, or unpersisted report state, do not transition to READY FOR VALIDATION. Correct and persist the report state first, then read it back again.

5. This procedure grants no SDO review or approval authority. Claude must not set AI Review, APPROVED, or DONE and must not independently expand its authorized Task scope or architecture authority.

## 5. Final Development Report

When all FR/AC gates executable by Claude pass, append a Final Development Report (in the task's
Implementation Notes / Claude Report field) containing:
- The complete changed-file set (tracked + untracked, deduplicated).
- The exact validation commands run and their exact results/exit codes.
- The repository evidence used for each non-trivial documentation/implementation claim.
- An FR-by-FR and AC-by-AC mapping to concrete evidence (not just a restated claim).
- Any UNKNOWN or blocking items discovered.

Then move Status to `READY FOR VALIDATION` (or the task's equivalent authorized terminal state for
Claude). Do not claim completion solely because files were created — completion requires the
recorded deterministic evidence above.

## 6. STOP on scope expansion

STOP without expanding scope if, during implementation, you discover that satisfying the task
would require: modifying a file outside the task's Authorized Changes; adding a dependency;
changing a public API/proto/Kafka/database contract; changing the LG vendor-control or
confirmation behavior; designing a multi-bridge command ownership/routing strategy or the
project-level `confirmationStrategy` abstraction; a Git write operation without explicit
authorization; or a change to another repository in the `saas-system-iot` polyrepo. Report the
blocker instead of proceeding.
