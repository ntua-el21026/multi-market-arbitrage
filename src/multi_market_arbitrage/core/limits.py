"""Cross-cutting Phase 1 resource limits used by validation and calibration."""

from __future__ import annotations

# Calibration projections must use the same inventory ceiling enforced by
# analytical publication and read-only validation. Local aliases remain
# intentionally patchable in focused tests.
MAX_ARTIFACT_PATHS_PER_ROOT = 65_536

# The streaming scan-report reader and every analytical dataset writer share
# this ceiling.  Completion heads retain the exact ordered fragment-manifest
# identities, so the producer must reject before a 4,097th fragment rather
# than truncate terminal authority.
MAX_ANALYTICAL_FRAGMENTS_PER_DATASET = 4_096

__all__ = [
    "MAX_ANALYTICAL_FRAGMENTS_PER_DATASET",
    "MAX_ARTIFACT_PATHS_PER_ROOT",
]
