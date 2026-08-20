import pytest

from multi_market_arbitrage.core.ids import ValidatorId, ValidatorVersion
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.core.validation import (
    ValidationReason,
    ValidationReasonCode,
    ValidationRequirement,
    ValidationResult,
    ValidationStatus,
    compose_validation_results,
)

_VALIDATOR = ValidatorId("book-health")
_VERSION = ValidatorVersion("v1")
_OBSERVED = UtcTimestamp(1_700_000_000_000_000_000)


def _reason(code: ValidationReasonCode = ValidationReasonCode.STALE_BOOK) -> ValidationReason:
    return ValidationReason(code=code, message="validation failed")


def _result(status: ValidationStatus, validator: str = "child") -> ValidationResult:
    validator_id = ValidatorId(validator)
    if status is ValidationStatus.PASS:
        return ValidationResult.passed(
            validator_id=validator_id,
            validator_version=_VERSION,
            subject_id="subject",
            observed_at=_OBSERVED,
        )
    if status is ValidationStatus.FAIL:
        return ValidationResult.failed(
            validator_id=validator_id,
            validator_version=_VERSION,
            subject_id="subject",
            observed_at=_OBSERVED,
            reasons=(_reason(),),
        )
    if status is ValidationStatus.INDETERMINATE:
        return ValidationResult.indeterminate(
            validator_id=validator_id,
            validator_version=_VERSION,
            subject_id="subject",
            observed_at=_OBSERVED,
            reasons=(_reason(ValidationReasonCode.UNKNOWN_REFERENCE_DATA),),
        )
    return ValidationResult.not_applicable(
        validator_id=validator_id,
        validator_version=_VERSION,
        subject_id="subject",
        observed_at=_OBSERVED,
    )


def test_required_invalid_results_need_machine_readable_reasons() -> None:
    with pytest.raises(ValueError, match="requires at least one reason"):
        ValidationResult(
            validator_id=_VALIDATOR,
            validator_version=_VERSION,
            subject_id="subject",
            status=ValidationStatus.INDETERMINATE,
            observed_at=_OBSERVED,
        )
    with pytest.raises(ValueError, match="cannot contain failure reasons"):
        ValidationResult(
            validator_id=_VALIDATOR,
            validator_version=_VERSION,
            subject_id="subject",
            status=ValidationStatus.PASS,
            observed_at=_OBSERVED,
            reasons=(_reason(),),
        )


def test_reason_codes_expose_phase1_specific_failures() -> None:
    assert ValidationReasonCode.MARKET_NOT_OPEN.value == "MARKET_NOT_OPEN"
    assert ValidationReasonCode.COST_RULE_MISMATCH.value == "COST_RULE_MISMATCH"
    assert ValidationReasonCode.CAPACITY_EVALUATION_LIMIT.value == "CAPACITY_EVALUATION_LIMIT"
    assert ValidationReasonCode.REFERENCE_VERSION_MISMATCH.value == ("REFERENCE_VERSION_MISMATCH")


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ((ValidationStatus.PASS, ValidationStatus.PASS), ValidationStatus.PASS),
        ((ValidationStatus.PASS, ValidationStatus.FAIL), ValidationStatus.FAIL),
        (
            (ValidationStatus.PASS, ValidationStatus.INDETERMINATE),
            ValidationStatus.INDETERMINATE,
        ),
        (
            (ValidationStatus.PASS, ValidationStatus.NOT_APPLICABLE),
            ValidationStatus.INDETERMINATE,
        ),
    ],
)
def test_required_composition_is_fail_closed(
    statuses: tuple[ValidationStatus, ...], expected: ValidationStatus
) -> None:
    requirements = tuple(
        ValidationRequirement(_result(status, f"child-{index}"))
        for index, status in enumerate(statuses)
    )
    result = compose_validation_results(
        requirements,
        validator_id=_VALIDATOR,
        validator_version=_VERSION,
        subject_id="subject",
        observed_at=_OBSERVED,
    )
    assert result.status is expected
    assert result.is_eligible is (expected is ValidationStatus.PASS)


def test_optional_not_applicable_does_not_block_required_pass() -> None:
    result = compose_validation_results(
        (
            ValidationRequirement(_result(ValidationStatus.PASS), required=True),
            ValidationRequirement(_result(ValidationStatus.NOT_APPLICABLE), required=False),
        ),
        validator_id=_VALIDATOR,
        validator_version=_VERSION,
        subject_id="subject",
        observed_at=_OBSERVED,
    )
    assert result.status is ValidationStatus.PASS
    assert len(result.evidence) == 2


def test_no_required_validators_composes_to_not_applicable() -> None:
    result = compose_validation_results(
        (ValidationRequirement(_result(ValidationStatus.NOT_APPLICABLE), required=False),),
        validator_id=_VALIDATOR,
        validator_version=_VERSION,
        subject_id="subject",
        observed_at=_OBSERVED,
    )
    assert result.status is ValidationStatus.NOT_APPLICABLE


def test_evidence_is_sorted_and_duplicate_keys_are_rejected() -> None:
    result = ValidationResult.passed(
        validator_id=_VALIDATOR,
        validator_version=_VERSION,
        subject_id="subject",
        observed_at=_OBSERVED,
        evidence=(("z", "2"), ("a", "1")),
    )
    assert result.evidence == (("a", "1"), ("z", "2"))
    with pytest.raises(ValueError, match="duplicate validation evidence key"):
        ValidationReason(
            code=ValidationReasonCode.STALE_BOOK,
            message="duplicate",
            evidence=(("age", "1"), ("age", "2")),
        )
