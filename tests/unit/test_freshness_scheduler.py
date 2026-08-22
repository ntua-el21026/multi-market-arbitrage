from __future__ import annotations

import pytest

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
        freshness_audit_guard_ns=1_000,
    )
    return result.deadline_ns, result.reason


def test_active_books_move_freshness_until_periodic_audit_wins() -> None:
    assert _deadline((27_000, 28_000)) == (
        31_000,
        IntegrityDeadlineReason.PERIODIC_AUDIT,
    )


def test_idle_or_missing_leg_uses_guarded_earliest_audit_launch() -> None:
    assert _deadline((1_000, 2_000)) == (
        5_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )
    assert _deadline((2_000,), now=4_000) == (
        4_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )


def test_one_fresh_leg_does_not_mask_other_stale_leg() -> None:
    assert _deadline((1_000, 25_000)) == (
        5_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )


def test_update_at_exact_hard_expiry_advances_guarded_launch_deterministically() -> None:
    # The hard acceptance rule still treats equality as fresh. Once that accepted
    # boundary becomes 6,000, the next audit launches one second before expiry.
    assert _deadline((6_000, 6_000), now=6_000) == (
        10_000,
        IntegrityDeadlineReason.BOOK_FRESHNESS,
    )


def test_run_deadline_has_priority_on_exact_tie() -> None:
    result = next_integrity_deadline(
        run_deadline_ns=10_000,
        periodic_audit_deadline_ns=10_000,
        valid_book_receive_ns=(6_000, 6_000),
        required_books=2,
        maximum_book_age_ns=5_000,
        now_ns=5_000,
        freshness_audit_guard_ns=1_000,
    )
    assert result.deadline_ns == 10_000
    assert result.reason is IntegrityDeadlineReason.RUN_DEADLINE


def test_guard_changes_only_freshness_audit_launch() -> None:
    assert _deadline((1_000, 2_000), periodic=4_500) == (
        4_500,
        IntegrityDeadlineReason.PERIODIC_AUDIT,
    )


@pytest.mark.parametrize("guard", [-1, 5_000, 5_001])
def test_guard_must_be_non_negative_and_below_maximum_book_age(guard: int) -> None:
    with pytest.raises(ValueError, match="freshness audit guard"):
        next_integrity_deadline(
            run_deadline_ns=100_000,
            periodic_audit_deadline_ns=31_000,
            valid_book_receive_ns=(1_000, 2_000),
            required_books=2,
            maximum_book_age_ns=5_000,
            now_ns=1_000,
            freshness_audit_guard_ns=guard,
        )
