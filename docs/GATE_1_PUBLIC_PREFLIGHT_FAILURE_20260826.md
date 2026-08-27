# Gate-1 Public-Preflight Failure Evidence — 2026-08-26

## 1. Status and authority

This document records the independently validated public preflight result for
exact candidate `40acc02c3e6499a38675c3f8368fab3399488279`.

It is evidence and status documentation. The normative acceptance contract
remains `GATE_1_SOAK_SPEC.md`; no Gate criterion or formula is changed by this
result.

**Six-hour authorization disposition:** `NOT READY`

The exact-SHA 60-second smoke passed. The subsequent exact-SHA 7,200-second
stability observation completed normally and passed every non-memory
diagnostic, but failed the mandatory post-saturation RSS net-growth predicate.
Gate 1 therefore remains `FAIL`. No six-hour rerun was performed.

## 2. Candidate and campaign identity

| Field | Value |
|---|---|
| Candidate commit | `40acc02c3e6499a38675c3f8368fab3399488279` |
| Candidate preservation ref | `gate1-memory-failure-40acc02c` |
| Live `origin/main` during qualification | `ee240c51184cccfc6e425906097758b5ca019a95` |
| Design Freeze commit | `e7d69aedc328ceda220120377307d5ebcc1a80d9` |
| Draft pull request | `#1`, open, draft, unmerged and mergeable at the evidence cutoff |
| Exact-SHA CI | run `33012631070`, attempt 3, `SUCCESS` |
| Gamma market ID | `665374` |
| Condition ID | `0x5db999fad322cea2914535aae5517060c3f80ad6d8c0231cde2124a434d16846` |
| Smoke run ID | `01a04002-29e3-7f13-a837-d552c9073dbf` |
| Stability run ID | `01a04007-fa69-79b9-90a0-4a6dc0f99e8b` |

The target was explicitly revalidated for each run as an open, active,
unresolved, order-book-enabled, non-negative-risk ordinary binary YES/NO market
with two-sided books and sufficient remaining lifetime. No target fallback was
used.

## 3. Retained evidence

The immutable public campaign is retained outside Git at:

```text
/home/peppaspan/mma_gate1_public_preflight/
  20260826T205301Z_40acc02c3e64/
```

The memory-remediation and forensic campaign is retained outside Git at:

```text
/home/peppaspan/mma_gate1_memory_remediation/
  20260827T010509Z_40acc02c3e64/
```

The remediation campaign created independent SHA-256 inventories for the raw,
analytical, health, interval, contribution, replay, rebuild, post-run-audit and
wrapper evidence. Its bootstrap manifest is
`f405c8430bb1e758c421c951568d3b42ed672919595028f43da08b81e42e4985`.
The retained public data and its manifests remain authoritative and were not
modified by the forensic work.

## 4. Passing 60-second smoke

The smoke requested 60 seconds and completed `60.004228964 s` of monotonic
observation with process exit zero.

Verified evidence includes:

- 376 received, appended, durable, sealed and verified journal records;
- no journal writer failure, ingress overflow, truncation or backpressure loss;
- 141 accepted/processed analytical records, zero rejection and healthy clean
  completion;
- six complete, healthy and Gate-evidence-eligible checkpoints;
- complete empty interval/contribution streams with zero terminal active causal
  keys and exact reconciliation;
- healthy terminal capture and research eligibility;
- successful original-data validation, two byte-identical replays, terminal
  hash `e9200559a66e99968fa3edbb8ee24f664c2acd24952b3dc138d36210f3da7992`,
  analytical rebuild, rebuilt-data validation and scan-semantic comparison;
- raw/evidence immutability; and
- a passing no-live negative-capability audit.

The smoke is valid evidence for candidate `40acc02c...`; it does not compensate
for the later stability failure and cannot qualify a source-modified successor.

## 5. Completed 7,200-second stability observation

The stability run requested 7,200 seconds, completed `7,200.003120449 s` of
monotonic observation and exited zero. Its observation, terminal health and
post-run audit were otherwise healthy:

- 51,860 raw records, exact contiguous range `0..51859`, all received,
  appended, durable, sealed and verified;
- no raw loss, duplicate, gap, recovery, writer failure, overflow or analytical
  rejection;
- 18,909 accepted/processed analytical records with healthy clean completion;
- 719 complete health checkpoints;
- 11,443 audit matches, 35 mismatches, 250 raced outcomes and zero failed
  outcomes;
- 21 availability intervals, 38 independently completed contributions, zero
  terminal active causal keys and exact cross-stream reconciliation digest
  `ea2f32c16423b442f9881af00a5a014e15c9353c0b3aea42d329846292ec1a30`;
- healthy terminal books, audit eligibility and research eligibility;
- successful original/rebuilt validation, two byte-identical replays with
  terminal hash `dcccec1c89aa344118e852e775de24aacd0e659e5150518de19accf04cffdf10`,
  analytical rebuild and equal scan semantics;
- 12,736 scan rows: 5,722 `BLOCKED`, 7,014 `NO_CANDIDATE`, and zero candidates,
  opportunities or lifecycles;
- exact raw immutability and passing no-live audit.

The post-run audit helper returned `PASSED` for its evidence mechanics and
deliberately emitted no Gate verdict. That result cannot override a failed
stability predicate.

## 6. Mandatory failure

The stability validator evaluated 886 predicates: 885 passed, one failed and
none were unknown. The sole failed predicate was:

```text
post-saturation RSS net growth
```

| Measurement | Actual | Unchanged limit | Result |
|---|---:|---:|---|
| Post-saturation RSS net growth | `535,859,200 B` | `134,217,728 B` | `FAIL` |
| Excess | `401,641,472 B` | — | `3.99246x` the limit |
| Post-saturation RSS OLS slope | `64,221.052 B/s` | `65,536 B/s` | `PASS` |
| Post-saturation traced-Python net growth | `13,578,387 B` | existing bound | `PASS` |
| Terminal PyArrow current allocation | `0 B` | existing bound | `PASS` |

The independent net-growth requirement remains mandatory even though the OLS
slope was narrowly within its separate limit. A projected value below host RAM
is not a demonstrated plateau.

The machine-readable hard-fail matrix contains 198 `PASS`, nine `FAIL` and zero
`UNKNOWN` requirements. The nine failures are direct or transitive consequences
of this one measured resource breach; they do not describe nine independent
runtime defects.

## 7. Memory forensics

The forensic timeline reconstructs all 719 health-resource samples. Across the
post-saturation window, approximately 97.47% of RSS net growth was outside
traced-Python-current and active-PyArrow-current attribution and therefore
appeared as native/allocator residual. This classification is diagnostic; it
does not assign every residual byte to one library.

The strongest reproducible storage-boundary findings were:

- five full 10,000-record journal rotations were followed by positive RSS
  steps totaling `293,711,872 B`, 54.81% of the failed post-saturation net
  growth;
- the old live seal path materialized the whole uncompressed segment, a whole
  compressed output and a 10,000-envelope verification tuple;
- on one retained 10,000-record segment, an isolated materializing verifier
  retained approximately `76.55 MiB` of RSS after cleanup, versus approximately
  `1.55 MiB` for otherwise equivalent streaming verification;
- the old analytical fragment hash materialized one canonical JSON array; on a
  retained 1,000-row fragment its isolated hash-specific traced peak was
  `40,875,861 B`, versus `141,002 B` for incremental sequence hashing, while
  producing the same digest;
- a separate 50-flush production-sized analytical probe completed and exactly
  validated 50,000 rows without positive whole-probe RSS or native-residual net
  growth.

These probes identify bounded but allocator-retentive storage-finalization
transients and justify the narrow correction. They are not a substitute for a
fresh exact-SHA public stability observation.

## 8. Scoped correction and identity boundary

The successor correction changes allocation behavior, not the journal's
logical content contract or any Gate threshold:

- journal payload hashing and Zstandard compression stream fixed-size chunks
  into an atomically published temporary output;
- live post-seal and startup verification exhaust the record iterator without
  retaining the decoded segment inventory;
- analytical `records_sha256` hashes one canonical sequence element at a time
  rather than materializing the entire canonical array;
- no explicit garbage collection, allocator trimming, process restart, queue
  increase, evidence weakening or resource-limit increase is used as the
  correction.

For a fixed logical segment, the uncompressed JSONL bytes, payload digest,
decoded envelope order and replay identity remain exact. The segment manifest
binds the actual deterministic compressed bytes, compressed size and segment
digest produced by the exact implementation, compressor configuration and
input. Compressed frame bytes are **not** promised to be byte-identical to
legacy one-shot-compressor frames; retained legacy segments remain immutable
and verifiable under their own manifests.

The analytical streaming sequence hash is required to equal the pre-correction
canonical-array digest exactly, preserving fragment and completion identity for
the same records.

## 9. Required successor qualification

The corrected working tree passed the authoritative pre-freeze dual-book v4
capacity run with exit zero and all 65 acceptance predicates true. The retained
result is
`/home/peppaspan/mma_gate1_memory_remediation/20260827T010509Z_40acc02c3e64/offline/final-capacity-v4-publication-guard-final.json`
(SHA-256 `aa626aa03eff5ee4c9d2cf767f7cf93eb42565e5422f8bfcf578f4f755340df2`);
the command/result logs are under
`offline/logs/full-capacity-v4-publication-guard-final/` in the same campaign. Earlier
v4-named artifacts in that campaign are intermediate and are not qualification
evidence. Because this run preceded a clean successor commit, it validates the
scoped correction and strengthened workload but does not qualify the eventual
exact candidate.

Because source changes invalidate candidate `40acc02c...` public qualification,
the successor must, from one new exact SHA:

1. pass all local/static/SCA/build/no-live and retained-corpus gates;
2. pass the v4 full-capacity workload with 6,096 scan-driving events that update
   both sides of both required books, 6,096 audit outcomes, two production-sized
   post-saturation analytical batch phases, and at least 80,001 journal records
   with exactly 1,400 application-payload bytes each across eight full
   10,000-record segments plus a tail record;
3. pass the v4 first-to-last full-segment net-growth bounds: RSS no more than
   128 MiB, traced Python no more than 64 MiB, and native residual no more than
   128 MiB;
4. pass a fresh exact-SHA 60-second public smoke and complete post-run audit;
5. pass a fresh exact-SHA 7,200-second public stability observation and complete
   post-run audit under every unchanged criterion; and
6. receive independent six-hour-soak authorization review.

Only then may a separate 21,600-second run be authorized. This remediation does
not run or authorize that soak.

**THE SIX-HOUR GATE-1 SOAK WAS NOT RUN.**
