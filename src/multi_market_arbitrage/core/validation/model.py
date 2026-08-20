"""Typed validation outcomes and fail-closed composition."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Self

from ..ids import TypedId, ValidatorId, ValidatorVersion
from ..provenance import Provenance
from ..time import UtcTimestamp


class ValidationStatus(StrEnum):
    PASS = "PASS"  # noqa: S105 - domain validation status, not a credential
    FAIL = "FAIL"
    INDETERMINATE = "INDETERMINATE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ValidationReasonCode(StrEnum):
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    UNSUPPORTED_SCHEMA = "UNSUPPORTED_SCHEMA"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    CHECKSUM_FAILURE = "CHECKSUM_FAILURE"
    PROTOCOL_VIOLATION = "PROTOCOL_VIOLATION"
    BACKPRESSURE_DATA_LOSS = "BACKPRESSURE_DATA_LOSS"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    IMPOSSIBLE_STATE = "IMPOSSIBLE_STATE"
    UNKNOWN_INSTRUMENT = "UNKNOWN_INSTRUMENT"
    UNKNOWN_PRECISION = "UNKNOWN_PRECISION"
    UNKNOWN_REFERENCE_DATA = "UNKNOWN_REFERENCE_DATA"
    UNKNOWN_COST = "UNKNOWN_COST"
    UNKNOWN_FEE = "UNKNOWN_FEE"
    UNKNOWN_REBATE_ELIGIBILITY = "UNKNOWN_REBATE_ELIGIBILITY"
    MARKET_NOT_OPEN = "MARKET_NOT_OPEN"
    UNHEALTHY_BOOK = "UNHEALTHY_BOOK"
    STALE_BOOK = "STALE_BOOK"
    STALE_SNAPSHOT = "STALE_SNAPSHOT"
    INSUFFICIENT_DEPTH = "INSUFFICIENT_DEPTH"
    MINIMUM_ORDER_SIZE_NOT_MET = "MINIMUM_ORDER_SIZE_NOT_MET"
    CAPACITY_EVALUATION_LIMIT = "CAPACITY_EVALUATION_LIMIT"
    COST_RULE_MISMATCH = "COST_RULE_MISMATCH"
    NEGATIVE_RISK_UNSUPPORTED = "NEGATIVE_RISK_UNSUPPORTED"
    PRECISION_VIOLATION = "PRECISION_VIOLATION"
    REFERENCE_VERSION_MISMATCH = "REFERENCE_VERSION_MISMATCH"
    SEMANTIC_AMBIGUITY = "SEMANTIC_AMBIGUITY"
    UNPROVEN_ECONOMIC_RELATION = "UNPROVEN_ECONOMIC_RELATION"
    INTERLEG_SKEW_EXCEEDED = "INTERLEG_SKEW_EXCEEDED"
    UNSUPPORTED_ORDER_POLICY = "UNSUPPORTED_ORDER_POLICY"
    OOS_CONTAMINATED = "OOS_CONTAMINATED"
    DATA_COVERAGE_TOO_LOW = "DATA_COVERAGE_TOO_LOW"
    SHARED_LIQUIDITY_CONFLICT = "SHARED_LIQUIDITY_CONFLICT"
    REQUIRED_VALIDATION_NOT_APPLICABLE = "REQUIRED_VALIDATION_NOT_APPLICABLE"


def _normalize_evidence(
    evidence: tuple[tuple[str, str], ...],
) -> tuple[tuple[str, str], ...]:
    normalized: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in evidence:
        key, value = item
        if not key:
            raise ValueError("validation evidence key must be a non-empty string")
        if key in seen:
            raise ValueError(f"duplicate validation evidence key: {key}")
        seen.add(key)
        normalized.append((key, value))
    return tuple(sorted(normalized))


@dataclass(frozen=True, slots=True)
class ValidationReason:
    code: ValidationReasonCode
    message: str
    evidence: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.message:
            raise ValueError("validation reason message must not be empty")
        object.__setattr__(self, "evidence", _normalize_evidence(self.evidence))


@dataclass(frozen=True, slots=True)
class ValidationResult:
    validator_id: ValidatorId
    validator_version: ValidatorVersion
    subject_id: TypedId | str
    status: ValidationStatus
    observed_at: UtcTimestamp
    reasons: tuple[ValidationReason, ...] = ()
    evidence: tuple[tuple[str, str], ...] = ()
    provenance: Provenance = field(default_factory=Provenance)

    def __post_init__(self) -> None:
        if isinstance(self.subject_id, str) and not self.subject_id:
            raise ValueError("subject_id string must not be empty")
        if (
            self.status in (ValidationStatus.FAIL, ValidationStatus.INDETERMINATE)
            and not self.reasons
        ):
            raise ValueError(f"{self.status} validation result requires at least one reason")
        if self.status is ValidationStatus.PASS and self.reasons:
            raise ValueError("PASS validation result cannot contain failure reasons")
        object.__setattr__(self, "evidence", _normalize_evidence(self.evidence))

    @property
    def reason_codes(self) -> tuple[ValidationReasonCode, ...]:
        return tuple(reason.code for reason in self.reasons)

    @property
    def is_eligible(self) -> bool:
        return self.status is ValidationStatus.PASS

    @classmethod
    def passed(
        cls,
        *,
        validator_id: ValidatorId,
        validator_version: ValidatorVersion,
        subject_id: TypedId | str,
        observed_at: UtcTimestamp,
        evidence: tuple[tuple[str, str], ...] = (),
        provenance: Provenance | None = None,
    ) -> Self:
        return cls(
            validator_id=validator_id,
            validator_version=validator_version,
            subject_id=subject_id,
            status=ValidationStatus.PASS,
            observed_at=observed_at,
            evidence=evidence,
            provenance=provenance or Provenance(),
        )

    @classmethod
    def failed(
        cls,
        *,
        validator_id: ValidatorId,
        validator_version: ValidatorVersion,
        subject_id: TypedId | str,
        observed_at: UtcTimestamp,
        reasons: tuple[ValidationReason, ...],
        evidence: tuple[tuple[str, str], ...] = (),
        provenance: Provenance | None = None,
    ) -> Self:
        return cls(
            validator_id=validator_id,
            validator_version=validator_version,
            subject_id=subject_id,
            status=ValidationStatus.FAIL,
            observed_at=observed_at,
            reasons=reasons,
            evidence=evidence,
            provenance=provenance or Provenance(),
        )

    @classmethod
    def indeterminate(
        cls,
        *,
        validator_id: ValidatorId,
        validator_version: ValidatorVersion,
        subject_id: TypedId | str,
        observed_at: UtcTimestamp,
        reasons: tuple[ValidationReason, ...],
        evidence: tuple[tuple[str, str], ...] = (),
        provenance: Provenance | None = None,
    ) -> Self:
        return cls(
            validator_id=validator_id,
            validator_version=validator_version,
            subject_id=subject_id,
            status=ValidationStatus.INDETERMINATE,
            observed_at=observed_at,
            reasons=reasons,
            evidence=evidence,
            provenance=provenance or Provenance(),
        )

    @classmethod
    def not_applicable(
        cls,
        *,
        validator_id: ValidatorId,
        validator_version: ValidatorVersion,
        subject_id: TypedId | str,
        observed_at: UtcTimestamp,
        reasons: tuple[ValidationReason, ...] = (),
        evidence: tuple[tuple[str, str], ...] = (),
        provenance: Provenance | None = None,
    ) -> Self:
        return cls(
            validator_id=validator_id,
            validator_version=validator_version,
            subject_id=subject_id,
            status=ValidationStatus.NOT_APPLICABLE,
            observed_at=observed_at,
            reasons=reasons,
            evidence=evidence,
            provenance=provenance or Provenance(),
        )


@dataclass(frozen=True, slots=True)
class ValidationRequirement:
    result: ValidationResult
    required: bool = True


def compose_validation_results(
    requirements: Iterable[ValidationRequirement],
    *,
    validator_id: ValidatorId,
    validator_version: ValidatorVersion,
    subject_id: TypedId | str,
    observed_at: UtcTimestamp,
    provenance: Provenance | None = None,
) -> ValidationResult:
    """Compose required validators with logical AND and fail-closed unknowns."""

    items = tuple(requirements)
    blocking = tuple(item.result for item in items if item.required)
    child_evidence = tuple(
        (f"child_{index}", f"{item.result.validator_id.value}:{item.result.status.value}")
        for index, item in enumerate(items)
    )
    if not blocking:
        return ValidationResult.not_applicable(
            validator_id=validator_id,
            validator_version=validator_version,
            subject_id=subject_id,
            observed_at=observed_at,
            evidence=child_evidence,
            provenance=provenance or Provenance(),
        )

    failed = tuple(result for result in blocking if result.status is ValidationStatus.FAIL)
    if failed:
        return ValidationResult.failed(
            validator_id=validator_id,
            validator_version=validator_version,
            subject_id=subject_id,
            observed_at=observed_at,
            reasons=tuple(reason for result in failed for reason in result.reasons),
            evidence=child_evidence,
            provenance=provenance or Provenance(),
        )

    unresolved = tuple(
        result
        for result in blocking
        if result.status in (ValidationStatus.INDETERMINATE, ValidationStatus.NOT_APPLICABLE)
    )
    if unresolved:
        reasons: list[ValidationReason] = []
        for result in unresolved:
            if result.status is ValidationStatus.INDETERMINATE:
                reasons.extend(result.reasons)
            else:
                reasons.append(
                    ValidationReason(
                        code=ValidationReasonCode.REQUIRED_VALIDATION_NOT_APPLICABLE,
                        message=(
                            f"required validator {result.validator_id.value} was not applicable"
                        ),
                    )
                )
        return ValidationResult.indeterminate(
            validator_id=validator_id,
            validator_version=validator_version,
            subject_id=subject_id,
            observed_at=observed_at,
            reasons=tuple(reasons),
            evidence=child_evidence,
            provenance=provenance or Provenance(),
        )

    return ValidationResult.passed(
        validator_id=validator_id,
        validator_version=validator_version,
        subject_id=subject_id,
        observed_at=observed_at,
        evidence=child_evidence,
        provenance=provenance or Provenance(),
    )
