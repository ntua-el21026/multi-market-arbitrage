"""Pure deterministic integrity scheduling from authoritative receive boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class IntegrityDeadlineReason(StrEnum):
    RUN_DEADLINE = "RUN_DEADLINE"
    PERIODIC_AUDIT = "PERIODIC_AUDIT"
    BOOK_FRESHNESS = "BOOK_FRESHNESS"


@dataclass(frozen=True, slots=True)
class IntegrityDeadline:
    deadline_ns: int
    reason: IntegrityDeadlineReason


def next_integrity_deadline(
    *,
    run_deadline_ns: int,
    periodic_audit_deadline_ns: int,
    valid_book_receive_ns: tuple[int, ...],
    required_books: int,
    maximum_book_age_ns: int,
    now_ns: int,
) -> IntegrityDeadline:
    """Return min(run, periodic, earliest actual required-book expiry)."""

    if required_books <= 0 or maximum_book_age_ns <= 0:
        raise ValueError("freshness scheduling bounds must be positive")
    if min(run_deadline_ns, periodic_audit_deadline_ns, now_ns) < 0:
        raise ValueError("monotonic scheduling values cannot be negative")
    if len(valid_book_receive_ns) < required_books:
        freshness_deadline_ns = now_ns
    else:
        freshness_deadline_ns = min(valid_book_receive_ns) + maximum_book_age_ns
    deadline_ns = min(
        run_deadline_ns,
        periodic_audit_deadline_ns,
        freshness_deadline_ns,
    )
    if deadline_ns == run_deadline_ns:
        reason = IntegrityDeadlineReason.RUN_DEADLINE
    elif deadline_ns == periodic_audit_deadline_ns:
        reason = IntegrityDeadlineReason.PERIODIC_AUDIT
    else:
        reason = IntegrityDeadlineReason.BOOK_FRESHNESS
    return IntegrityDeadline(deadline_ns=deadline_ns, reason=reason)


__all__ = ["IntegrityDeadline", "IntegrityDeadlineReason", "next_integrity_deadline"]
