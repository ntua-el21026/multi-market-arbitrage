from __future__ import annotations

from multi_market_arbitrage.pipeline.freshness import (
    IntegrityDeadlineReason,
    next_integrity_deadline,
)


def _deadline(
    receives: tuple[int, ...],
    *,
    now: int = 1_000,
    periodic: int = 31_000,
) -> tuple[int, IntegrityDeadlineReason]:
    result = next_integrity_deadline(
        run_deadline_ns=100_000,
        periodic_audit_deadline_ns=periodic,
        valid_book_receive_ns=receives,
        required_books=2,
        maximum_book_age_ns=5_000,
        now_ns=now,
    )
    return result.deadline_ns, result.reason


def test_active_books_move_freshness_until_periodic_audit_wins() -> None:
    assert _deadline((27_000, 28_000)) == (
        31_000,
        IntegrityDeadlineReason.PERIODIC_AUDIT,
    )


def test_idle_or_missing_leg_uses_actual_earliest_expiry() -> None:
    assert _deadline((1_000, 2_000)) == (
        6_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )
    assert _deadline((2_000,), now=4_000) == (
        4_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )


def test_one_fresh_leg_does_not_mask_other_stale_leg() -> None:
    assert _deadline((1_000, 25_000)) == (
        6_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )


def test_update_at_exact_expiry_advances_deadline_deterministically() -> None:
    # Equality is still fresh. Once the accepted update boundary becomes 6,000,
    # the next due time advances rather than forcing a fixed five-second socket.
    assert _deadline((6_000, 6_000), now=6_000) == (
        11_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )


def test_run_deadline_has_priority_on_exact_tie() -> None:
    result = next_integrity_deadline(
        run_deadline_ns=10_000,
        periodic_audit_deadline_ns=10_000,
        valid_book_receive_ns=(5_000, 5_000),
        required_books=2,
        maximum_book_age_ns=5_000,
        now_ns=5_000,
    )
    assert result.deadline_ns == 10_000
    assert result.reason is IntegrityDeadlineReason.RUN_DEADLINE
