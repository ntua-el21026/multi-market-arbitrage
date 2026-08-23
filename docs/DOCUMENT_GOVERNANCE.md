# Documentation Governance

## 1. Purpose

The repository contains current normative specifications, historical design-freeze reports, ADRs, implementation/evidence documents, external-reference backlogs, and future planning documents. They have different authority.

## 2. Canonical current documents

The current architecture is defined by:

1. `PROJECT_SPEC.md`
2. `SYSTEM_DESIGN.md`
3. `INVARIANTS.md`
4. relevant current topic-specific specifications under `docs/*.md`
5. accepted, non-superseded ADRs

These documents must agree.

Current phase specifications may impose narrower scope/gates without reopening the frozen architecture. A planning document that explicitly blocks implementation until a prerequisite gate passes is authoritative for that scope boundary.

## 3. Historical documents

Files under:

```text
docs/design_freeze/
```

are historical/audit evidence.

They explain how decisions were reached and may intentionally contain terminology that was later superseded.

They are **not** implementation authority after the final v0.1 review.

Design Freeze v0.1 remains immutable. Current additive ADRs/specifications extend later phases without rewriting historical freeze evidence.

## 4. ADR authority

ADRs record design decisions and their lifecycle.

Status values:

```text
Proposed
Accepted
Superseded
Rejected
Deprecated
```

When an ADR is superseded, the newer ADR is authoritative for that decision.

A conflict between a current topic document and a current accepted ADR is a documentation defect; implementation must stop and report it rather than choosing silently.

Future ADR candidates named in roadmaps/backlogs are not accepted decisions. They become authoritative only after a real ADR is written and accepted.

## 5. Evidence/reference notes

Files named `*_OFFICIAL_DOCS_NOTES.md` are dated evidence snapshots.

They do not convert changeable external venue behavior into permanent architecture.

Implementation must re-check current official venue documentation for fees, rate limits, eligibility, market mechanics, authentication, settlement, and protocol behavior.

Gate evidence and measured performance documents report what was actually observed for an exact code/config/data identity. They do not automatically generalize to other venues, markets, hosts, or later commits.

## 6. External-reference and backlog documents

Documents explicitly marked non-normative or advisory—such as `EXTERNAL_REFERENCE_BACKLOG.md`—preserve research findings, external patterns, and decision candidates.

They:

- may inform roadmap, experiments, specifications, and future ADRs;
- do not override current specifications, invariants, or accepted ADRs;
- do not make external repository behavior or dated venue facts authoritative;
- must distinguish adopt/adapt/defer/reject recommendations from implemented behavior.

## 7. Precedence during ambiguity

Do not silently "resolve" a real contradiction by precedence.

Instead:

1. identify the conflict;
2. fail closed for implementation of the affected behavior;
3. open/update an ADR or normative document;
4. make the documents consistent;
5. continue only after resolution.

The ordering in section 2 is primarily navigational and scope-defining.

## 8. Current vs historical terminology

Current code and new documentation use terminology from `GLOSSARY.md`.

Historical design-freeze files are not rewritten solely to update terminology because they are an audit trail.

Candidate terms in planning/backlog documents do not become canonical glossary terms until their design is accepted.

## 9. Documentation changes

Every material architecture change should update, as applicable:

- `PROJECT_SPEC.md`;
- `SYSTEM_DESIGN.md`;
- topic document;
- `INVARIANTS.md`;
- `GLOSSARY.md`;
- `ADR_INDEX.md`;
- one new/superseding ADR.

Roadmap sequencing, phase-status, and planning-scope changes that preserve the existing architecture may update `ROADMAP.md`, `IMPLEMENTATION_PLAN.md`, phase specifications, phase gates, and `DESIGN_INDEX.md` without reopening Design Freeze v0.1.

Do not edit acceptance criteria after observing results merely to make evidence pass. Status may be recorded separately, but the gate is evaluated against the requirements that governed the run unless a documented defect requires an explicit new decision and rerun.

## 10. Implementation agents

Codex must:

- read `DESIGN_INDEX.md` first;
- treat current normative docs as constraints;
- report contradictions rather than inventing resolution;
- not edit historical pass reports except for explicit correction annotations;
- not alter accepted architecture without an ADR;
- treat advisory external-reference/backlog documents as research inputs, not authority;
- respect phase specifications that block implementation pending a prerequisite gate.
