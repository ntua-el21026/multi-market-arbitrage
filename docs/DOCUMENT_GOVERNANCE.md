# Documentation Governance

## 1. Purpose

The repository contains current normative specifications, historical design-freeze reports, ADRs, evidence notes, and future implementation documentation. They have different authority.

## 2. Canonical current documents

The current architecture is defined by:

1. `PROJECT_SPEC.md`
2. `SYSTEM_DESIGN.md`
3. `INVARIANTS.md`
4. relevant topic-specific top-level `docs/*.md`
5. accepted, non-superseded ADRs

These documents must agree.

## 3. Historical documents

Files under:

```text
docs/design_freeze/
```

are historical/audit evidence.

They explain how decisions were reached and may intentionally contain terminology that was later superseded.

They are **not** implementation authority after the final v0.1 review.

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

## 5. Evidence/reference notes

Files named `*_OFFICIAL_DOCS_NOTES.md` are dated evidence snapshots.

They do not convert changeable external venue behavior into permanent architecture.

Implementation must re-check current official venue documentation for fees, rate limits, eligibility, market mechanics, authentication, settlement, and protocol behavior.

## 6. Precedence during ambiguity

Do not silently "resolve" a real contradiction by precedence.

Instead:

1. identify the conflict;
2. fail closed for implementation of the affected behavior;
3. open/update an ADR or normative document;
4. make the documents consistent;
5. continue only after resolution.

The ordering in section 2 is primarily navigational and scope-defining.

## 7. Current vs historical terminology

Current code and new documentation use terminology from `GLOSSARY.md`.

Historical design-freeze files are not rewritten solely to update terminology because they are an audit trail.

## 8. Documentation changes

Every material architecture change should update, as applicable:

- `PROJECT_SPEC.md`;
- `SYSTEM_DESIGN.md`;
- topic document;
- `INVARIANTS.md`;
- `GLOSSARY.md`;
- `ADR_INDEX.md`;
- one new/superseding ADR.

## 9. Implementation agents

Codex must:

- read `DESIGN_INDEX.md` first;
- treat current normative docs as constraints;
- report contradictions rather than inventing resolution;
- not edit historical pass reports except for explicit correction annotations;
- not alter accepted architecture without an ADR.
