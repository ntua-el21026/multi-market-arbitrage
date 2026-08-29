#!/usr/bin/env -S python3 -I
"""Create one fail-closed completion and raw-inventory seal for a public run."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import tomllib
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, cast

DESIGN_FREEZE_SHA = "e7d69aedc328ceda220120377307d5ebcc1a80d9"
SEAL_SCHEMA = "mma-gate1-run-completion-seal-v1"
INVENTORY_SCHEMA = "mma-gate1-immutable-inventory-v1"
COMMAND_SCHEMA = "mma-gate1-observer-command-v1"
MAXIMUM_INVENTORY_PATHS = 65_536
MAXIMUM_JSON_BYTES = 256 * 1024 * 1024
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
TARGET_PATTERN = re.compile(r"[1-9][0-9]*")
HOST_RUNTIME_IDENTITY_SCHEMA = "gate1-host-runtime-identity-v1"
RAW_SOURCE_IDENTITY_SCHEMA = "mma-gate1-raw-source-identity-v2"
JOURNAL_MANIFEST_VERSION = "raw-journal-manifest-v1"
JOURNAL_RUN_COMPLETION_VERSION = "raw-journal-run-completion-v2"
JOURNAL_SOURCE_AUTHORITY_VERSION = "raw-journal-source-authority-v1"
JOURNAL_SOURCE_AUTHORITY_MAX_BYTES = 64 * 1024
JOURNAL_COMPLETION_MAX_BYTES = 8 * 1024 * 1024
SAFE_COMPONENT_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")
SOURCE_AUTHORITY_FIELDS = frozenset(
    {
        "authority_origin",
        "authority_sha256",
        "authority_version",
        "collector_run_id",
        "git_commit",
        "group_commit_max_delay_seconds_hex",
        "group_commit_max_events",
        "max_events_per_segment",
        "max_pending_seal_age_nanoseconds",
        "max_pending_seal_age_seconds_hex",
        "max_pending_seal_uncompressed_bytes",
        "max_uncompressed_bytes_per_run",
        "max_uncompressed_bytes_per_segment",
        "predecessor_manifest_sha256",
        "queue_capacity",
        "seal_queue_capacity",
        "stream",
        "submit_timeout_seconds_hex",
        "venue_id",
        "writer_id",
        "writer_software_version",
        "zstd_level",
    }
)
MANIFEST_FIELDS = frozenset(
    {
        "collector_run_id",
        "compressed_bytes",
        "created_at_utc_ns",
        "end_time_utc_ns",
        "event_count",
        "first_ingest_seq",
        "git_commit",
        "hash_algorithm",
        "journal_format_version",
        "last_ingest_seq",
        "manifest_sha256",
        "payload_digest",
        "previous_segment_digest",
        "recovery_evidence",
        "segment_digest",
        "start_time_utc_ns",
        "stream",
        "uncompressed_bytes",
        "venue",
        "writer_id",
        "writer_software_version",
    }
)
COMPLETION_V2_FIELDS = frozenset(
    {
        "collector_run_id",
        "completed_at_utc_ns",
        "completion_sha256",
        "completion_version",
        "expected_event_count",
        "expected_segment_count",
        "expected_uncompressed_bytes",
        "first_ingest_seq",
        "git_commit",
        "hash_algorithm",
        "last_ingest_seq",
        "max_uncompressed_bytes_per_run",
        "max_uncompressed_bytes_per_segment",
        "source_authority_sha256",
        "stream",
        "terminal_manifest_sha256",
        "venue",
        "writer_id",
        "writer_software_version",
    }
)
SOURCE_IDENTITY_FIELDS = frozenset(
    {
        "code_commit",
        "code_commit_matches_repository",
        "imported_package_root",
        "repository_head_commit",
        "repository_root",
        "source_binding_valid",
        "source_root",
        "tracked_source_clean",
        "untracked_source_clean",
    }
)


class SealFailure(RuntimeError):
    """The run could not be bound to a complete immutable identity."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_config_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SealFailure("configuration contains a non-finite float")
        return value
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, list):
        return [_canonical_config_value(item) for item in cast(list[object], value)]
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        if not all(isinstance(key, str) for key in mapping):
            raise SealFailure("configuration contains a non-text key")
        return {cast(str, key): _canonical_config_value(item) for key, item in mapping.items()}
    raise SealFailure(f"configuration contains unsupported value type: {type(value).__name__}")


def _runtime_config_sha256(path: Path) -> str:
    try:
        with path.open("rb") as handle:
            document: object = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SealFailure("cannot load the strict Phase-1 configuration") from error
    return hashlib.sha256(_canonical_bytes(_canonical_config_value(document))).hexdigest()


def _atomic_create(path: Path, payload: bytes) -> None:
    if path.exists() or path.is_symlink():
        raise SealFailure(f"refusing to replace an existing seal: {path}")
    partial = path.with_name(f".{path.name}.partial")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(partial, flags, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("seal write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.link(partial, path, follow_symlinks=False)
    partial.unlink()
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _require_directory(path: Path, *, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise SealFailure(f"{label} is absent or symbolic: {path}")
    return path.resolve(strict=True)


def _require_file(path: Path, *, label: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise SealFailure(f"{label} is absent or symbolic: {path}")
    resolved = path.resolve(strict=True)
    status = resolved.stat(follow_symlinks=False)
    if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
        raise SealFailure(f"{label} is not a single-link regular file: {path}")
    return resolved


def _sha256_file(path: Path) -> str:
    file_path = _require_file(path, label="artifact")
    before = file_path.stat(follow_symlinks=False)
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
        after = os.fstat(handle.fileno())
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after:
        raise SealFailure(f"artifact changed while hashing: {path}")
    return digest.hexdigest()


def _fingerprint(path: Path) -> dict[str, object]:
    file_path = _require_file(path, label="sealed artifact")
    return {
        "bytes": file_path.stat().st_size,
        "path": str(file_path),
        "sha256": _sha256_file(file_path),
    }


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    file_path = _require_file(path, label=label)
    if file_path.stat().st_size > MAXIMUM_JSON_BYTES:
        raise SealFailure(f"{label} exceeds the supported byte bound")
    try:
        value: object = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SealFailure(f"{label} is not valid JSON") from error
    if not isinstance(value, dict):
        raise SealFailure(f"{label} must be a JSON object")
    return cast(dict[str, Any], value)


def _git_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for name in tuple(environment):
        if name.startswith("GIT_"):
            environment.pop(name)
    environment.update(
        {
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_COUNT": "4",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_KEY_0": "core.hooksPath",
            "GIT_CONFIG_KEY_1": "core.fsmonitor",
            "GIT_CONFIG_KEY_2": "core.attributesFile",
            "GIT_CONFIG_KEY_3": "core.excludesFile",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_CONFIG_VALUE_0": "/dev/null",
            "GIT_CONFIG_VALUE_1": "false",
            "GIT_CONFIG_VALUE_2": "/dev/null",
            "GIT_CONFIG_VALUE_3": "/dev/null",
            "GIT_NO_REPLACE_OBJECTS": "1",
        }
    )
    return environment


def _git(repository: Path, *arguments: str) -> str:
    git = shutil.which("git")
    if git is None:
        raise SealFailure("git is unavailable")
    completed = subprocess.run(  # noqa: S603 - absolute executable and fixed boundary
        (git, "-C", str(repository), *arguments),
        capture_output=True,
        check=False,
        env=_git_environment(),
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "unknown error"
        raise SealFailure(f"git {' '.join(arguments)} failed: {detail}")
    return completed.stdout.strip()


def _git_blob_sha256(repository: Path, commit: str, relative: str) -> str:
    git = shutil.which("git")
    if git is None:
        raise SealFailure("git is unavailable")
    completed = subprocess.run(  # noqa: S603 - absolute executable and fixed boundary
        (git, "-C", str(repository), "cat-file", "blob", f"{commit}:{relative}"),
        capture_output=True,
        check=False,
        env=_git_environment(),
    )
    if completed.returncode != 0:
        raise SealFailure(f"candidate does not contain required path: {relative}")
    return hashlib.sha256(completed.stdout).hexdigest()


def _inventory(root: Path) -> dict[str, object]:
    resolved_root = _require_directory(root, label="raw inventory root")
    records: list[dict[str, object]] = []
    total_bytes = 0
    for path in sorted(resolved_root.rglob("*")):
        if path.is_symlink():
            raise SealFailure(f"raw inventory contains a symbolic link: {path}")
        status = path.stat(follow_symlinks=False)
        if stat.S_ISDIR(status.st_mode):
            continue
        if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
            raise SealFailure(f"raw inventory contains an unsafe path: {path}")
        if len(records) >= MAXIMUM_INVENTORY_PATHS:
            raise SealFailure("raw inventory path count exceeds the supported bound")
        records.append(
            {
                "bytes": status.st_size,
                "path": path.relative_to(resolved_root).as_posix(),
                "sha256": _sha256_file(path),
            }
        )
        total_bytes += status.st_size
    return {
        "path_count": len(records),
        "records": records,
        "root": str(resolved_root),
        "schema_version": INVENTORY_SCHEMA,
        "sha256": hashlib.sha256(_canonical_bytes(records)).hexdigest(),
        "total_bytes": total_bytes,
    }


def _parse_input_hashes(path: Path) -> dict[str, str]:
    file_path = _require_file(path, label="input-files.sha256")
    values: dict[str, str] = {}
    for line in file_path.read_text(encoding="utf-8").splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) != 2 or SHA256_PATTERN.fullmatch(parts[0]) is None:
            raise SealFailure("input-files.sha256 is malformed")
        relative = parts[1].lstrip(" *")
        if relative in values:
            raise SealFailure("input-files.sha256 repeats one path")
        values[relative] = parts[0]
    return values


def _resolve_storage_paths(source: Path, config: Path) -> tuple[Path, Path, Path]:
    try:
        with config.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SealFailure("cannot load the strict Phase-1 configuration") from error
    storage_value = document.get("storage")
    if not isinstance(storage_value, dict):
        raise SealFailure("configuration has no storage table")
    storage = cast(dict[str, object], storage_value)

    def resolve(field: str) -> Path:
        value = storage.get(field)
        if not isinstance(value, str) or not value:
            raise SealFailure(f"storage.{field} must be a non-empty path")
        unresolved = source / value
        if unresolved.is_symlink():
            raise SealFailure(f"storage.{field} escapes or is symbolic")
        candidate = unresolved.resolve(strict=True)
        if not candidate.is_relative_to(source):
            raise SealFailure(f"storage.{field} escapes or is symbolic")
        return candidate

    return resolve("root"), resolve("journal_directory"), resolve("analytical_directory")


def _read_binding(evidence: Path, name: str) -> tuple[str, dict[str, object]]:
    path = _require_file(evidence / name, label=f"run binding {name}")
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as error:
        raise SealFailure(f"cannot read run binding {name}") from error
    return value, _fingerprint(path)


def _numeric_duration(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SealFailure(f"{field} must be a finite numeric duration")
    duration = float(value)
    if not math.isfinite(duration) or duration <= 0:
        raise SealFailure(f"{field} must be a finite positive duration")
    return duration


def _strict_text(record: dict[str, Any], field: str, *, label: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise SealFailure(f"{label} {field} is invalid")
    return value


def _strict_integer(record: dict[str, Any], field: str, *, label: str) -> int:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SealFailure(f"{label} {field} is invalid")
    return value


def _verify_canonical_record_hash(
    record: dict[str, Any],
    *,
    hash_field: str,
    label: str,
) -> str:
    claimed = _strict_text(record, hash_field, label=label)
    if SHA256_PATTERN.fullmatch(claimed) is None:
        raise SealFailure(f"{label} {hash_field} is malformed")
    content = dict(record)
    del content[hash_field]
    observed = hashlib.sha256(_canonical_bytes(content)).hexdigest()
    if claimed != observed:
        raise SealFailure(f"{label} {hash_field} differs from canonical content")
    return claimed


def _verify_canonical_json_file(path: Path, record: dict[str, Any], *, label: str) -> None:
    if path.read_bytes() != _canonical_bytes(record) + b"\n":
        raise SealFailure(f"{label} bytes are non-canonical")


def _expected_source_authority_values(config: Path) -> dict[str, object]:
    try:
        with config.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SealFailure("cannot load the strict Phase-1 configuration") from error
    system_value = document.get("system")
    storage_value = document.get("storage")
    if not isinstance(system_value, dict) or not isinstance(storage_value, dict):
        raise SealFailure("configuration omits system or storage authority")
    system = cast(dict[str, object], system_value)
    storage = cast(dict[str, object], storage_value)

    def integer(mapping: dict[str, object], field: str) -> int:
        value = mapping.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise SealFailure(f"configuration {field} is not an integer")
        return value

    def finite_float(mapping: dict[str, object], field: str) -> float:
        value = mapping.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SealFailure(f"configuration {field} is not numeric")
        result = float(value)
        if not math.isfinite(result):
            raise SealFailure(f"configuration {field} is non-finite")
        return result

    code_version = system.get("code_version")
    if not isinstance(code_version, str) or not code_version:
        raise SealFailure("configuration system.code_version is invalid")
    submit_timeout = finite_float(storage, "journal_submit_timeout_seconds")
    group_delay = finite_float(storage, "journal_group_commit_max_delay_seconds")
    pending_age = finite_float(storage, "journal_pending_seal_max_age_seconds")
    return {
        "group_commit_max_delay_seconds_hex": group_delay.hex(),
        "group_commit_max_events": integer(storage, "journal_group_commit_max_events"),
        "max_events_per_segment": integer(storage, "journal_segment_max_events"),
        "max_pending_seal_age_nanoseconds": int(pending_age * 1_000_000_000),
        "max_pending_seal_age_seconds_hex": pending_age.hex(),
        "max_pending_seal_uncompressed_bytes": integer(
            storage, "journal_pending_seal_max_uncompressed_bytes"
        ),
        "max_uncompressed_bytes_per_run": integer(storage, "journal_run_max_uncompressed_bytes"),
        "max_uncompressed_bytes_per_segment": integer(
            storage, "journal_segment_max_uncompressed_bytes"
        ),
        "queue_capacity": integer(system, "queue_capacity"),
        "seal_queue_capacity": integer(storage, "journal_seal_queue_capacity"),
        "submit_timeout_seconds_hex": submit_timeout.hex(),
        "writer_software_version": code_version,
        # Phase-1 observe does not override JournalWriterConfig's frozen default.
        "zstd_level": 9,
    }


def _verify_host_runtime_identity(
    summary: dict[str, Any],
    *,
    source: Path,
    expected_sha: str,
    runtime_config_sha256: str,
    lock_sha256: str,
) -> dict[str, object]:
    value = summary.get("host_runtime_identity")
    if not isinstance(value, dict):
        raise SealFailure("run summary omits host_runtime_identity")
    identity = cast(dict[str, object], value)
    expected_package_root = (source / "src" / "multi_market_arbitrage").resolve(strict=True)
    exact_values: dict[str, object] = {
        "code_commit": expected_sha,
        "config_sha256": runtime_config_sha256,
        "imported_package_root": str(expected_package_root),
        "lock_sha256": lock_sha256,
        "repository_head_commit": expected_sha,
        "repository_root": str(source),
        "schema_version": HOST_RUNTIME_IDENTITY_SCHEMA,
        "source_root": str(source),
    }
    for field, expected in exact_values.items():
        if identity.get(field) != expected:
            raise SealFailure(f"host_runtime_identity {field} differs from exact source")
    for field in (
        "code_commit_matches_repository",
        "source_binding_valid",
        "tracked_source_clean",
        "untracked_source_clean",
    ):
        if identity.get(field) is not True:
            raise SealFailure(f"host_runtime_identity {field} is not exactly true")
    unknown_value = identity.get("unknown_fields")
    if not isinstance(unknown_value, list):
        raise SealFailure("host_runtime_identity unknown_fields is malformed")
    unknown_items = cast(list[object], unknown_value)
    if not all(isinstance(item, str) for item in unknown_items):
        raise SealFailure("host_runtime_identity unknown_fields is malformed")
    unknown_source_fields = SOURCE_IDENTITY_FIELDS.intersection(cast(list[str], unknown_items))
    if unknown_source_fields:
        fields = ", ".join(sorted(unknown_source_fields))
        raise SealFailure(f"host_runtime_identity has unknown source fields: {fields}")
    return identity


def _run_key(collector_run_id: str) -> str:
    return hashlib.sha256(collector_run_id.encode("utf-8")).hexdigest()[:16]


def _verify_raw_source_identity(
    raw_root: Path,
    *,
    collector_run_id: str,
    expected_sha: str,
    config: Path,
) -> dict[str, object]:
    run_key = _run_key(collector_run_id)
    prefix = f"run-{run_key}."
    segment_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.zst")))
    manifest_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.zst.manifest.json")))
    expected_manifests = {path.with_name(f"{path.name}.manifest.json") for path in segment_paths}
    if set(manifest_paths) != expected_manifests:
        raise SealFailure("selected run segment/manifest inventory differs")
    partial_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.partial")))
    if partial_paths:
        raise SealFailure("selected completed run retains a partial journal segment")
    pending_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.seal-pending*")))
    if pending_paths:
        raise SealFailure("selected completed run retains seal-pending journal evidence")
    authority_temps = tuple(sorted(raw_root.glob(f".{prefix}*.source-authority.json.*.tmp")))
    if authority_temps:
        raise SealFailure("selected run source-authority publication is incomplete")
    completion_temps = tuple(sorted(raw_root.glob(f".{prefix}*.completed.json.*.tmp")))
    if completion_temps:
        raise SealFailure("selected run completion publication is incomplete")

    authority_paths = tuple(sorted(raw_root.glob(f"{prefix}*.source-authority.json")))
    if not authority_paths:
        raise SealFailure("selected run has no exact source-authority artifact")
    completion_paths = tuple(sorted(raw_root.glob(f"{prefix}*.completed.json")))
    if not completion_paths:
        raise SealFailure("selected run has no journal completion header")

    expected_authority_values = _expected_source_authority_values(config)
    authorities: dict[str, dict[str, Any]] = {}
    authority_records: list[dict[str, object]] = []
    for path in authority_paths:
        if path.stat().st_size > JOURNAL_SOURCE_AUTHORITY_MAX_BYTES:
            raise SealFailure("selected run source authority exceeds its byte bound")
        record = _read_json(path, label="selected run source authority")
        if set(record) != set(SOURCE_AUTHORITY_FIELDS):
            raise SealFailure("selected run source authority fields differ from v1")
        stream = _strict_text(record, "stream", label="selected run source authority")
        if SAFE_COMPONENT_PATTERN.fullmatch(stream) is None:
            raise SealFailure("selected run source authority stream is invalid")
        if path.name != f"{prefix}{stream}.source-authority.json":
            raise SealFailure("selected run source-authority filename differs")
        if record.get("authority_version") != JOURNAL_SOURCE_AUTHORITY_VERSION:
            raise SealFailure("selected run source-authority version differs")
        if record.get("collector_run_id") != collector_run_id:
            raise SealFailure("selected run source-authority scope differs")
        if record.get("git_commit") != expected_sha:
            raise SealFailure("selected run source-authority git_commit differs")
        if record.get("venue_id") != "polymarket":
            raise SealFailure("selected run source-authority venue differs")
        _strict_text(record, "writer_id", label="selected run source authority")
        if record.get("authority_origin") != "NEW_RUN":
            raise SealFailure("new qualification run source authority is not NEW_RUN")
        if record.get("predecessor_manifest_sha256") is not None:
            raise SealFailure("new qualification run source authority claims a predecessor")
        for field, expected in expected_authority_values.items():
            observed = record.get(field)
            if isinstance(expected, int) and (
                isinstance(observed, bool) or not isinstance(observed, int)
            ):
                raise SealFailure(f"selected run source-authority {field} is invalid")
            if observed != expected:
                raise SealFailure(
                    f"selected run source-authority {field} differs from configuration"
                )
        authority_sha256 = _verify_canonical_record_hash(
            record,
            hash_field="authority_sha256",
            label="selected run source authority",
        )
        _verify_canonical_json_file(path, record, label="selected run source authority")
        if stream in authorities:
            raise SealFailure("selected run repeats a source-authority stream")
        authorities[stream] = record
        authority_records.append(
            {
                **_fingerprint(path),
                "authority_sha256": authority_sha256,
                "git_commit": expected_sha,
                "stream": stream,
            }
        )

    manifest_records: list[dict[str, object]] = []
    manifests_by_stream: dict[str, list[tuple[dict[str, Any], Path]]] = {
        stream: [] for stream in authorities
    }
    for path in manifest_paths:
        record = _read_json(path, label="selected run segment manifest")
        if record.get("git_commit") != expected_sha:
            raise SealFailure("selected run segment manifest git_commit differs")
        if set(record) != set(MANIFEST_FIELDS):
            raise SealFailure("selected run segment manifest fields differ")
        stream = _strict_text(record, "stream", label="selected run segment manifest")
        first_sequence = _strict_integer(
            record, "first_ingest_seq", label="selected run segment manifest"
        )
        last_sequence = _strict_integer(
            record, "last_ingest_seq", label="selected run segment manifest"
        )
        event_count = _strict_integer(record, "event_count", label="selected run segment manifest")
        uncompressed_bytes = _strict_integer(
            record, "uncompressed_bytes", label="selected run segment manifest"
        )
        compressed_bytes = _strict_integer(
            record, "compressed_bytes", label="selected run segment manifest"
        )
        if first_sequence < 0 or last_sequence < first_sequence or event_count <= 0:
            raise SealFailure("selected run segment manifest identity is malformed")
        expected_name = (
            f"{prefix}{stream}.{first_sequence:020d}-{last_sequence:020d}.jsonl.zst.manifest.json"
        )
        if path.name != expected_name or record.get("collector_run_id") != collector_run_id:
            raise SealFailure("selected run segment manifest scope differs")
        authority = authorities.get(stream)
        if authority is None:
            raise SealFailure("selected run segment has no source authority")
        if (
            record.get("journal_format_version") != JOURNAL_MANIFEST_VERSION
            or record.get("hash_algorithm") != "sha256"
        ):
            raise SealFailure("selected run segment manifest version differs")
        if (
            record.get("venue") != authority["venue_id"]
            or record.get("writer_id") != authority["writer_id"]
            or record.get("writer_software_version") != authority["writer_software_version"]
        ):
            raise SealFailure("selected run segment provenance differs from source authority")
        if event_count != last_sequence - first_sequence + 1:
            raise SealFailure("selected run segment event count differs from its range")
        if (
            event_count > cast(int, authority["max_events_per_segment"])
            or uncompressed_bytes <= 0
            or uncompressed_bytes > cast(int, authority["max_uncompressed_bytes_per_segment"])
        ):
            raise SealFailure("selected run segment exceeds source-authority bounds")
        segment_path = path.with_name(path.name.removesuffix(".manifest.json"))
        if segment_path not in segment_paths:
            raise SealFailure("selected run segment manifest has no exact segment")
        if compressed_bytes != segment_path.stat().st_size:
            raise SealFailure("selected run segment compressed byte count differs")
        segment_digest = _strict_text(
            record, "segment_digest", label="selected run segment manifest"
        )
        payload_digest = _strict_text(
            record, "payload_digest", label="selected run segment manifest"
        )
        if (
            SHA256_PATTERN.fullmatch(segment_digest) is None
            or SHA256_PATTERN.fullmatch(payload_digest) is None
            or segment_digest != _sha256_file(segment_path)
        ):
            raise SealFailure("selected run segment digest identity differs")
        manifest_sha256 = _verify_canonical_record_hash(
            record,
            hash_field="manifest_sha256",
            label="selected run segment manifest",
        )
        _verify_canonical_json_file(path, record, label="selected run segment manifest")
        recovery_evidence = record.get("recovery_evidence")
        if not isinstance(recovery_evidence, list) or not all(
            isinstance(item, str) for item in cast(list[object], recovery_evidence)
        ):
            raise SealFailure("selected run segment recovery evidence is malformed")
        for timestamp_field in (
            "created_at_utc_ns",
            "end_time_utc_ns",
            "start_time_utc_ns",
        ):
            if _strict_integer(record, timestamp_field, label="selected run segment manifest") < 0:
                raise SealFailure("selected run segment timestamp is invalid")
        manifests_by_stream[stream].append((record, path))
        manifest_records.append(
            {
                **_fingerprint(path),
                "git_commit": expected_sha,
                "manifest_sha256": manifest_sha256,
                "stream": stream,
            }
        )

    completion_records: list[dict[str, object]] = []
    completion_streams: set[str] = set()
    for path in completion_paths:
        if path.stat().st_size > JOURNAL_COMPLETION_MAX_BYTES:
            raise SealFailure("selected run completion header exceeds its byte bound")
        record = _read_json(path, label="selected run completion header")
        if record.get("completion_version") != JOURNAL_RUN_COMPLETION_VERSION:
            raise SealFailure("new qualification run requires journal completion v2")
        if record.get("git_commit") != expected_sha:
            raise SealFailure("selected run completion header git_commit differs")
        if set(record) != set(COMPLETION_V2_FIELDS):
            raise SealFailure("selected run completion-v2 fields differ")
        stream = _strict_text(record, "stream", label="selected run completion header")
        if path.name != f"{prefix}{stream}.completed.json":
            raise SealFailure("selected run completion header filename differs")
        if record.get("collector_run_id") != collector_run_id:
            raise SealFailure("selected run completion header scope differs")
        authority = authorities.get(stream)
        if authority is None:
            raise SealFailure("selected run completion has no source authority")
        if (
            record.get("source_authority_sha256") != authority["authority_sha256"]
            or record.get("venue") != authority["venue_id"]
            or record.get("writer_id") != authority["writer_id"]
            or record.get("writer_software_version") != authority["writer_software_version"]
            or record.get("max_uncompressed_bytes_per_segment")
            != authority["max_uncompressed_bytes_per_segment"]
            or record.get("max_uncompressed_bytes_per_run")
            != authority["max_uncompressed_bytes_per_run"]
        ):
            raise SealFailure("selected run completion provenance differs from source authority")
        if record.get("hash_algorithm") != "sha256":
            raise SealFailure("selected run completion hash algorithm differs")
        completion_sha256 = _verify_canonical_record_hash(
            record,
            hash_field="completion_sha256",
            label="selected run completion header",
        )
        _verify_canonical_json_file(path, record, label="selected run completion header")
        if stream in completion_streams:
            raise SealFailure("selected run repeats a completion stream")
        completion_streams.add(stream)

        ordered = sorted(
            manifests_by_stream[stream],
            key=lambda item: cast(int, item[0]["first_ingest_seq"]),
        )
        next_sequence = 0
        previous_digest: str | None = None
        total_events = 0
        total_uncompressed_bytes = 0
        for manifest, _manifest_path in ordered:
            if manifest["first_ingest_seq"] != next_sequence:
                raise SealFailure("selected run segment sequence is gapped or reordered")
            if manifest["previous_segment_digest"] != previous_digest:
                raise SealFailure("selected run segment digest chain differs")
            next_sequence = cast(int, manifest["last_ingest_seq"]) + 1
            previous_digest = cast(str, manifest["segment_digest"])
            total_events += cast(int, manifest["event_count"])
            total_uncompressed_bytes += cast(int, manifest["uncompressed_bytes"])
        if total_uncompressed_bytes > cast(int, authority["max_uncompressed_bytes_per_run"]):
            raise SealFailure("selected run exceeds source-authority run bytes")
        expected_segment_count = _strict_integer(
            record, "expected_segment_count", label="selected run completion header"
        )
        expected_event_count = _strict_integer(
            record, "expected_event_count", label="selected run completion header"
        )
        expected_uncompressed_bytes = _strict_integer(
            record, "expected_uncompressed_bytes", label="selected run completion header"
        )
        if (
            expected_segment_count != len(ordered)
            or expected_event_count != total_events
            or expected_uncompressed_bytes != total_uncompressed_bytes
        ):
            raise SealFailure("selected run completion inventory differs from manifests")
        if (
            _strict_integer(record, "completed_at_utc_ns", label="selected run completion header")
            < 0
        ):
            raise SealFailure("selected run completion timestamp is invalid")
        if ordered:
            if (
                record.get("first_ingest_seq") != 0
                or record.get("last_ingest_seq") != total_events - 1
                or record.get("terminal_manifest_sha256") != ordered[-1][0]["manifest_sha256"]
            ):
                raise SealFailure("selected run completion terminal inventory differs")
        elif any(
            record.get(field) is not None
            for field in ("first_ingest_seq", "last_ingest_seq", "terminal_manifest_sha256")
        ):
            raise SealFailure("empty selected run completion has non-null terminal inventory")
        completion_records.append(
            {
                **_fingerprint(path),
                "completion_sha256": completion_sha256,
                "completion_version": JOURNAL_RUN_COMPLETION_VERSION,
                "git_commit": expected_sha,
                "source_authority_sha256": authority["authority_sha256"],
                "stream": stream,
            }
        )
    if completion_streams != set(authorities):
        raise SealFailure("selected run completion/source-authority stream sets differ")
    return {
        "collector_run_id": collector_run_id,
        "completion_headers": completion_records,
        "expected_git_commit": expected_sha,
        "run_key": run_key,
        "schema_version": RAW_SOURCE_IDENTITY_SCHEMA,
        "segment_manifests": manifest_records,
        "source_authorities": authority_records,
    }


def _expected_command(
    source: Path, expected_sha: str, target_id: str, duration_seconds: int
) -> dict[str, object]:
    return {
        "argv": [
            "uv",
            "run",
            "--offline",
            "--frozen",
            "--no-sync",
            "--all-groups",
            "mma",
            "observe",
            "polymarket",
            "--gamma-market-id",
            target_id,
            "--duration",
            str(duration_seconds),
            "--config",
            "config/phase1.toml",
        ],
        "config_relative_path": "config/phase1.toml",
        "cwd": str(source),
        "duration_seconds": duration_seconds,
        "environment": {"PYTHONHASHSEED": "0", "PYTHONUNBUFFERED": "1"},
        "expected_source_sha": expected_sha,
        "schema_version": COMMAND_SCHEMA,
        "target_id": target_id,
    }


def _command_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write the exact observer command contract consumed by the run sealer."
    )
    parser.add_argument("--source-worktree", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _write_command_record(arguments: argparse.Namespace) -> Path:
    expected_sha: str = arguments.expected_sha
    target_id: str = arguments.target_id
    duration_seconds: int = arguments.duration_seconds
    if SHA_PATTERN.fullmatch(expected_sha) is None:
        raise SealFailure("--expected-sha must be a full lowercase 40-character Git SHA")
    if TARGET_PATTERN.fullmatch(target_id) is None:
        raise SealFailure("--target-id must be an explicit positive numeric Gamma market ID")
    if isinstance(duration_seconds, bool) or duration_seconds <= 0:
        raise SealFailure("--duration-seconds must be a positive integer")

    source_argument: Path = arguments.source_worktree
    source = _require_directory(source_argument, label="source worktree")
    if source_argument != source:
        raise SealFailure("--source-worktree must be an absolute canonical path")
    if _git(source, "rev-parse", "--show-toplevel") != str(source):
        raise SealFailure("--source-worktree is not a Git worktree root")
    if _git(source, "for-each-ref", "--format=%(refname)", "refs/replace"):
        raise SealFailure("source repository contains forbidden Git replacement refs")
    if _git(source, "status", "--porcelain=v1", "--untracked-files=all"):
        raise SealFailure("source worktree is not clean")
    if _git(source, "rev-parse", "HEAD") != expected_sha:
        raise SealFailure("source worktree HEAD differs from --expected-sha")
    if _git(source, "rev-parse", "design-freeze-v0.1^{commit}") != DESIGN_FREEZE_SHA:
        raise SealFailure("Design Freeze tag identity differs")

    script = Path(__file__).resolve(strict=True)
    if not script.is_relative_to(source):
        raise SealFailure("command writer must be invoked from the exact source worktree")
    script_relative = script.relative_to(source).as_posix()
    if _sha256_file(script) != _git_blob_sha256(source, expected_sha, script_relative):
        raise SealFailure("executed command-writer bytes differ from the exact candidate")

    output_argument: Path = arguments.output
    if not output_argument.is_absolute():
        raise SealFailure("--output must be absolute")
    if output_argument.is_symlink() or output_argument.parent.is_symlink():
        raise SealFailure("--output cannot be symbolic")
    output_parent = _require_directory(output_argument.parent, label="output directory")
    if output_argument.parent != output_parent:
        raise SealFailure("--output parent must be an absolute canonical path")
    output = output_parent / output_argument.name
    if output.name != "soak-command.json":
        raise SealFailure("--output must be named soak-command.json")
    if (
        output_parent == source
        or output_parent.is_relative_to(source)
        or source.is_relative_to(output_parent)
    ):
        raise SealFailure("observer command evidence must be outside the source worktree")

    record = _expected_command(source, expected_sha, target_id, duration_seconds)
    payload = json.dumps(record, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    _atomic_create(output, payload)
    return output


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-worktree", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--exit-code-file", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--command-file", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _seal(arguments: argparse.Namespace) -> Path:
    expected_sha: str = arguments.expected_sha
    target_id: str = arguments.target_id
    duration_seconds: int = arguments.duration_seconds
    exit_code: int = arguments.exit_code
    if SHA_PATTERN.fullmatch(expected_sha) is None:
        raise SealFailure("--expected-sha must be a full lowercase 40-character Git SHA")
    if TARGET_PATTERN.fullmatch(target_id) is None:
        raise SealFailure("--target-id must be an explicit positive numeric Gamma market ID")
    if isinstance(duration_seconds, bool) or duration_seconds <= 0:
        raise SealFailure("--duration-seconds must be a positive integer")
    if isinstance(exit_code, bool) or not 0 <= exit_code <= 255:
        raise SealFailure("--exit-code must be between 0 and 255")

    source = _require_directory(arguments.source_worktree, label="source worktree")
    evidence = _require_directory(arguments.evidence_dir, label="evidence directory")
    if evidence == source or evidence.is_relative_to(source) or source.is_relative_to(evidence):
        raise SealFailure("evidence directory must not overlap the source worktree")

    output_argument: Path = arguments.output
    if output_argument.is_symlink() or output_argument.parent.is_symlink():
        raise SealFailure("--output cannot be symbolic")
    output = output_argument.resolve(strict=False)
    if output.parent.resolve(strict=True) != evidence:
        raise SealFailure("--output must be a direct child of the evidence directory")
    if output.name != "run-completion-seal.json":
        raise SealFailure("--output must be named run-completion-seal.json")

    if _git(source, "rev-parse", "--show-toplevel") != str(source):
        raise SealFailure("--source-worktree is not a Git worktree root")
    if _git(source, "for-each-ref", "--format=%(refname)", "refs/replace"):
        raise SealFailure("source repository contains forbidden Git replacement refs")
    if _git(source, "status", "--porcelain=v1", "--untracked-files=all"):
        raise SealFailure("source worktree is not clean")
    if _git(source, "rev-parse", "HEAD") != expected_sha:
        raise SealFailure("source worktree HEAD differs from --expected-sha")
    if _git(source, "rev-parse", "design-freeze-v0.1^{commit}") != DESIGN_FREEZE_SHA:
        raise SealFailure("Design Freeze tag identity differs")

    script = Path(__file__).resolve(strict=True)
    if not script.is_relative_to(source):
        raise SealFailure("seal helper must be invoked from the exact source worktree")
    script_relative = script.relative_to(source).as_posix()
    if _sha256_file(script) != _git_blob_sha256(source, expected_sha, script_relative):
        raise SealFailure("executed seal-helper bytes differ from the exact candidate")

    config = _require_file(arguments.config, label="configuration")
    lock = _require_file(arguments.lock, label="uv.lock")
    if not config.is_relative_to(source) or not lock.is_relative_to(source):
        raise SealFailure("configuration and lock must be inside the exact source worktree")
    config_relative = config.relative_to(source).as_posix()
    lock_relative = lock.relative_to(source).as_posix()
    if config_relative != "config/phase1.toml" or lock_relative != "uv.lock":
        raise SealFailure("configuration or lock path differs from the launcher contract")
    config_sha = _sha256_file(config)
    runtime_config_sha = _runtime_config_sha256(config)
    lock_sha = _sha256_file(lock)
    if config_sha != _git_blob_sha256(source, expected_sha, config_relative):
        raise SealFailure("configuration differs from the exact candidate")
    if lock_sha != _git_blob_sha256(source, expected_sha, lock_relative):
        raise SealFailure("uv.lock differs from the exact candidate")

    bindings: dict[str, dict[str, object]] = {}
    expected_bindings = {
        "design-freeze-commit.txt": DESIGN_FREEZE_SHA,
        "gamma-market-id.txt": target_id,
        "git-head.txt": expected_sha,
        "soak-worktree.txt": str(source),
    }
    for name, expected in expected_bindings.items():
        value, fingerprint = _read_binding(evidence, name)
        if value != expected:
            raise SealFailure(f"run evidence identity differs: {name}")
        bindings[name] = fingerprint
    input_hash_path = evidence / "input-files.sha256"
    input_hashes = _parse_input_hashes(input_hash_path)
    if set(input_hashes) != {config_relative, lock_relative}:
        raise SealFailure("run evidence input-hash path set differs")
    if input_hashes.get(config_relative) != config_sha:
        raise SealFailure("run evidence configuration hash differs")
    if input_hashes.get(lock_relative) != lock_sha:
        raise SealFailure("run evidence lock hash differs")
    bindings["input-files.sha256"] = _fingerprint(input_hash_path)

    data_root, configured_raw_root, analytical_root = _resolve_storage_paths(source, config)
    raw_root = _require_directory(arguments.raw_root, label="raw root")
    if raw_root != configured_raw_root:
        raise SealFailure("--raw-root differs from the configured authoritative journal")

    summary_path = _require_file(arguments.summary, label="run summary")
    command_path = _require_file(arguments.command_file, label="observer command record")
    exit_code_path = _require_file(arguments.exit_code_file, label="observer exit-code artifact")
    expected_artifacts = {
        summary_path: evidence / "soak-summary.json",
        command_path: evidence / "soak-command.json",
        exit_code_path: evidence / "soak-exit-code.txt",
    }
    for observed, expected in expected_artifacts.items():
        if observed != expected:
            raise SealFailure(f"run artifact is outside its exact evidence path: {observed}")

    summary = _read_json(summary_path, label="run summary")
    collector_run_id = summary.get("collector_run_id")
    if not isinstance(collector_run_id, str) or RUN_ID_PATTERN.fullmatch(collector_run_id) is None:
        raise SealFailure("run summary omits a bounded CollectorRunId")
    requested_duration = _numeric_duration(
        summary.get("requested_duration_seconds"), field="summary requested duration"
    )
    if requested_duration != float(duration_seconds):
        raise SealFailure("run summary requested duration differs from the sealed duration")
    actual_duration = summary.get("actual_duration_nanoseconds")
    if (
        isinstance(actual_duration, bool)
        or not isinstance(actual_duration, int)
        or actual_duration < duration_seconds * 1_000_000_000
    ):
        raise SealFailure("run summary does not prove the requested monotonic duration")
    target = summary.get("target")
    if not isinstance(target, dict):
        raise SealFailure("run summary omits target identity")
    target_identity = cast(dict[str, object], target)
    if (
        target_identity.get("requested_gamma_market_id") != target_id
        or target_identity.get("resolved_gamma_market_id") != target_id
    ):
        raise SealFailure("run summary target identity differs")
    host_runtime_identity = _verify_host_runtime_identity(
        summary,
        source=source,
        expected_sha=expected_sha,
        runtime_config_sha256=runtime_config_sha,
        lock_sha256=lock_sha,
    )

    command = _read_json(command_path, label="observer command record")
    if command != _expected_command(source, expected_sha, target_id, duration_seconds):
        raise SealFailure("observer command record differs from the exact sealed command")

    try:
        recorded_exit = int(exit_code_path.read_text(encoding="utf-8").strip())
    except (OSError, UnicodeError, ValueError) as error:
        raise SealFailure("observer exit-code artifact is malformed") from error
    if recorded_exit != exit_code:
        raise SealFailure("observer exit-code artifact differs from --exit-code")

    raw_source_identity = _verify_raw_source_identity(
        raw_root,
        collector_run_id=collector_run_id,
        expected_sha=expected_sha,
        config=config,
    )
    raw_inventory = _inventory(raw_root)
    seal: dict[str, object] = {
        "analytical_root": str(analytical_root),
        "collector_run_id": collector_run_id,
        "command": _fingerprint(command_path),
        "config": {
            **_fingerprint(config),
            "relative_path": config_relative,
            "runtime_sha256": runtime_config_sha,
        },
        "data_root": str(data_root),
        "design_freeze_sha": DESIGN_FREEZE_SHA,
        "duration_seconds": duration_seconds,
        "evidence_bindings": bindings,
        "evidence_directory": str(evidence),
        "exit_code": exit_code,
        "exit_code_artifact": _fingerprint(exit_code_path),
        "host_runtime_identity": host_runtime_identity,
        "lock": {
            **_fingerprint(lock),
            "relative_path": lock_relative,
        },
        "raw_inventory": raw_inventory,
        "raw_root": str(raw_root),
        "raw_source_identity": raw_source_identity,
        "schema_version": SEAL_SCHEMA,
        "sealed_at_utc": datetime.now(UTC).isoformat(),
        "source_sha": expected_sha,
        "source_worktree": str(source),
        "summary": _fingerprint(summary_path),
        "target_id": target_id,
    }
    _atomic_create(output, _canonical_bytes(seal) + b"\n")
    return output


def main() -> int:
    if not sys.flags.isolated:
        print("RUN SEAL FAILED: invoke this helper with Python -I", file=sys.stderr)
        return 1
    if sys.argv[1:2] == ["write-command"]:
        arguments = _command_argument_parser().parse_args(sys.argv[2:])
        try:
            output = _write_command_record(arguments)
        except (OSError, SealFailure, ValueError) as error:
            print(f"OBSERVER COMMAND RECORD FAILED: {error}", file=sys.stderr)
            return 1
        print(f"OBSERVER COMMAND RECORD CREATED: {output}")
        return 0

    arguments = _argument_parser().parse_args()
    try:
        output = _seal(arguments)
    except (OSError, SealFailure, ValueError) as error:
        print(f"RUN SEAL FAILED: {error}", file=sys.stderr)
        return 1
    print(f"RUN SEAL CREATED: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
