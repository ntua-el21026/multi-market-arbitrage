"""Cross-cutting Phase 1 resource limits used by validation and calibration."""

from __future__ import annotations

# Calibration projections must use the same inventory ceiling enforced by
# analytical publication and read-only validation. Local aliases remain
# intentionally patchable in focused tests.
MAX_ARTIFACT_PATHS_PER_ROOT = 65_536

__all__ = ["MAX_ARTIFACT_PATHS_PER_ROOT"]
