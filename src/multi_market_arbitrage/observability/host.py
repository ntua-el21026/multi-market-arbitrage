"""Bounded, explicit host/runtime identity for Gate-1 evidence."""

from __future__ import annotations

import hashlib
import os
import platform
import select
import shutil
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from multi_market_arbitrage.core.ids import is_sha256_hex

HOST_RUNTIME_IDENTITY_SCHEMA_VERSION: Final = "gate1-host-runtime-identity-v1"
_GIT_COMMIT_HEX_LENGTH: Final = 40
_MAXIMUM_SOURCE_ROOT_BYTES: Final = 4096
_SOURCE_PROBE_TIMEOUT_SECONDS: Final = 5.0


def normalize_git_commit(value: str | None) -> str | None:
    """Return an exact Git commit identity or explicit unknown.

    Git emits lowercase hexadecimal object IDs.  Sentinel labels such as
    ``unavailable`` and abbreviated or otherwise malformed IDs are provenance
    hints, not exact Gate evidence.
    """

    if value is None or len(value) != _GIT_COMMIT_HEX_LENGTH:
        return None
    return value if all(character in "0123456789abcdef" for character in value) else None


def _git_executable() -> Path | None:
    discovered = shutil.which("git")
    if discovered is None:
        return None
    try:
        executable = Path(discovered).resolve(strict=True)
    except OSError:
        return None
    return executable if executable.is_file() else None


def _repository_root(git: Path, source_root: Path) -> str | None:
    try:
        completed = subprocess.run(  # noqa: S603 - resolved Git executable, fixed argv
            [str(git), "-C", str(source_root), "rev-parse", "--show-toplevel"],
            check=False,
            capture_output=True,
            text=True,
            timeout=_SOURCE_PROBE_TIMEOUT_SECONDS,
        )
    except OSError, subprocess.SubprocessError:
        return None
    value = completed.stdout.strip()
    if completed.returncode != 0 or not value:
        return None
    if len(value.encode("utf-8")) > _MAXIMUM_SOURCE_ROOT_BYTES:
        return None
    try:
        resolved = Path(value).resolve(strict=True)
    except OSError:
        return None
    return str(resolved) if resolved.is_dir() else None


def _repository_head_commit(git: Path, repository_root: str) -> str | None:
    try:
        completed = subprocess.run(  # noqa: S603 - resolved Git executable, fixed argv
            [str(git), "-C", repository_root, "rev-parse", "--verify", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=_SOURCE_PROBE_TIMEOUT_SECONDS,
        )
    except OSError, subprocess.SubprocessError:
        return None
    if completed.returncode != 0:
        return None
    return normalize_git_commit(completed.stdout.strip())


def _imported_package_root() -> str | None:
    try:
        package_root = Path(__file__).resolve(strict=True).parents[1]
    except OSError, IndexError:
        return None
    value = str(package_root)
    if not package_root.is_dir() or len(value.encode("utf-8")) > _MAXIMUM_SOURCE_ROOT_BYTES:
        return None
    return value


def _tracked_source_clean(git: Path, repository_root: str) -> bool | None:
    try:
        completed = subprocess.run(  # noqa: S603 - resolved Git executable, fixed argv
            [
                str(git),
                "-C",
                repository_root,
                "diff",
                "--quiet",
                "--no-ext-diff",
                "--ignore-submodules=none",
                "HEAD",
                "--",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=_SOURCE_PROBE_TIMEOUT_SECONDS,
        )
    except OSError, subprocess.SubprocessError:
        return None
    if completed.returncode == 0:
        return True
    return False if completed.returncode == 1 else None


def _untracked_source_clean(git: Path, repository_root: str) -> bool | None:
    """Detect any untracked path while reading at most one output byte."""

    try:
        process = subprocess.Popen(  # noqa: S603 - resolved Git executable, fixed argv
            [
                str(git),
                "-C",
                repository_root,
                "ls-files",
                "--others",
                "--exclude-standard",
                "--directory",
                "-z",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        return None
    try:
        stdout = process.stdout
        if stdout is None:
            return None
        readable, _, _ = select.select(
            (stdout,),
            (),
            (),
            _SOURCE_PROBE_TIMEOUT_SECONDS,
        )
        if not readable:
            return None
        first_byte = os.read(stdout.fileno(), 1)
        if first_byte:
            return False
        try:
            return process.wait(timeout=1) == 0
        except subprocess.TimeoutExpired:
            return None
    finally:
        if process.poll() is None:
            process.kill()
        with suppress(OSError, subprocess.TimeoutExpired):
            process.wait(timeout=1)
        if process.stdout is not None:
            process.stdout.close()


def _source_identity(
    workspace_root: Path,
) -> tuple[
    str | None,
    str | None,
    str | None,
    str | None,
    bool | None,
    bool | None,
    bool | None,
]:
    try:
        source_root_path = workspace_root.resolve(strict=True)
    except OSError:
        return None, None, None, None, None, None, None
    source_root = str(source_root_path)
    if (
        not source_root_path.is_dir()
        or len(source_root.encode("utf-8")) > _MAXIMUM_SOURCE_ROOT_BYTES
    ):
        return None, None, None, None, None, None, None
    git = _git_executable()
    imported_package_root = _imported_package_root()
    if git is None:
        return source_root, None, None, imported_package_root, None, None, None
    repository_root = _repository_root(git, source_root_path)
    if repository_root is None:
        return source_root, None, None, imported_package_root, None, None, None
    repository_head_commit = _repository_head_commit(git, repository_root)
    try:
        repository_path = Path(repository_root).resolve(strict=True)
        imported_path = None if imported_package_root is None else Path(imported_package_root)
        expected_package_path = (repository_path / "src" / "multi_market_arbitrage").resolve(
            strict=True
        )
        source_binding_valid = (
            None
            if imported_path is None
            else (
                source_root_path == repository_path
                and expected_package_path.is_relative_to(repository_path)
                and imported_path.resolve(strict=True) == expected_package_path
            )
        )
    except OSError, ValueError:
        source_binding_valid = None
    return (
        source_root,
        repository_root,
        repository_head_commit,
        imported_package_root,
        source_binding_valid,
        _tracked_source_clean(git, repository_root),
        _untracked_source_clean(git, repository_root),
    )


def _sha256_file(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def _decode_mount_field(value: str) -> str:
    for encoded, decoded in (
        ("\\040", " "),
        ("\\011", "\t"),
        ("\\012", "\n"),
        ("\\134", "\\"),
    ):
        value = value.replace(encoded, decoded)
    return value


def filesystem_identity(target: Path) -> str | None:
    """Identify the longest matching Linux mount without shell parsing."""

    try:
        resolved = target.resolve(strict=False)
    except OSError:
        return None
    probe = resolved
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    if not probe.exists():
        return None
    candidates: list[tuple[int, str]] = []
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        try:
            return f"statvfs-fsid={os.statvfs(probe).f_fsid}"
        except OSError:
            return None
    for line in lines:
        fields = line.split()
        try:
            separator = fields.index("-")
        except ValueError:
            continue
        if separator + 2 >= len(fields) or len(fields) < 6:
            continue
        mount = Path(_decode_mount_field(fields[4]))
        if resolved == mount or resolved.is_relative_to(mount):
            fs_type = fields[separator + 1]
            source = _decode_mount_field(fields[separator + 2])
            candidates.append((len(mount.parts), f"{fs_type};source={source};mount={mount}"))
    return None if not candidates else max(candidates, key=lambda item: item[0])[1]


def _cpu_model() -> str | None:
    try:
        lines = Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    for line in lines:
        key, separator, value = line.partition(":")
        if separator and key.strip() in {"model name", "Hardware", "Processor"}:
            normalized = value.strip()
            if normalized:
                return normalized
    fallback = platform.processor().strip()
    return fallback or None


def _total_memory_bytes() -> int | None:
    try:
        lines = Path("/proc/meminfo").read_text(encoding="ascii").splitlines()
    except OSError:
        lines = []
    for line in lines:
        if not line.startswith("MemTotal:"):
            continue
        fields = line.split()
        if len(fields) == 3 and fields[1].isdigit() and fields[2] == "kB":
            return int(fields[1]) * 1024
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
    except OSError, ValueError:
        return None
    return pages * page_size if pages > 0 and page_size > 0 else None


def _gil_enabled() -> bool | None:
    probe = getattr(sys, "_is_gil_enabled", None)
    if probe is None:
        return None
    try:
        value = probe()
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        return None
    return value if isinstance(value, bool) else None


def _uv_identity() -> str | None:
    configured = os.environ.get("UV")
    executable = Path(configured) if configured else None
    if executable is None:
        discovered = shutil.which("uv")
        if discovered is None:
            return None
        executable = Path(discovered)
    try:
        executable = executable.resolve(strict=True)
    except OSError:
        return None
    if not executable.is_file():
        return None
    try:
        completed = subprocess.run(  # noqa: S603 - resolved local executable, fixed argv
            [str(executable), "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except OSError, subprocess.SubprocessError:
        return None
    version = completed.stdout.strip()
    return None if not version else f"{version};executable={executable}"


@dataclass(frozen=True, slots=True)
class HostRuntimeIdentity:
    """One startup-captured host/toolchain/code/config identity."""

    code_commit: str | None
    config_sha256: str
    lock_sha256: str | None
    python: str
    python_gil_enabled: bool | None
    uv: str | None
    hostname: str | None
    cpu_model: str | None
    logical_cpu_count: int | None
    total_memory_bytes: int | None
    kernel: str
    filesystem: str | None
    source_root: str | None
    repository_root: str | None
    repository_head_commit: str | None
    imported_package_root: str | None
    code_commit_matches_repository: bool | None
    source_binding_valid: bool | None
    tracked_source_clean: bool | None
    untracked_source_clean: bool | None
    unknown_fields: tuple[str, ...]
    schema_version: str = HOST_RUNTIME_IDENTITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HOST_RUNTIME_IDENTITY_SCHEMA_VERSION:
            raise ValueError("unsupported host-runtime identity schema version")
        if not self.config_sha256 or not self.python or not self.kernel:
            raise ValueError("required host-runtime identity fields must be non-empty")
        if self.code_commit is not None and normalize_git_commit(self.code_commit) is None:
            raise ValueError("host-runtime code identity must be an exact Git commit")
        if not is_sha256_hex(self.config_sha256):
            raise ValueError("host-runtime config identity must be SHA-256")
        if self.lock_sha256 is not None and not is_sha256_hex(self.lock_sha256):
            raise ValueError("host-runtime lock identity must be SHA-256")
        if self.logical_cpu_count is not None and self.logical_cpu_count <= 0:
            raise ValueError("logical CPU count must be positive when known")
        if self.total_memory_bytes is not None and self.total_memory_bytes <= 0:
            raise ValueError("total memory must be positive when known")
        if self.repository_head_commit is not None and (
            normalize_git_commit(self.repository_head_commit) is None
        ):
            raise ValueError("repository HEAD identity must be an exact Git commit")
        for root in (self.source_root, self.repository_root, self.imported_package_root):
            if root is not None and (
                not root or len(root.encode("utf-8")) > _MAXIMUM_SOURCE_ROOT_BYTES
            ):
                raise ValueError("source identity roots must be non-empty and bounded")
        if tuple(sorted(set(self.unknown_fields))) != self.unknown_fields:
            raise ValueError("unknown host fields must be unique and sorted")
        optional_identity = {
            "code_commit": self.code_commit,
            "code_commit_matches_repository": self.code_commit_matches_repository,
            "imported_package_root": self.imported_package_root,
            "repository_head_commit": self.repository_head_commit,
            "repository_root": self.repository_root,
            "source_root": self.source_root,
            "source_binding_valid": self.source_binding_valid,
            "tracked_source_clean": self.tracked_source_clean,
            "untracked_source_clean": self.untracked_source_clean,
        }
        if any(
            (value is None) != (name in self.unknown_fields)
            for name, value in optional_identity.items()
        ):
            raise ValueError("unknown source identity must be represented explicitly")

    @property
    def source_identity_known(self) -> bool:
        return all(
            value is not None
            for value in (
                self.source_root,
                self.repository_root,
                self.repository_head_commit,
                self.imported_package_root,
                self.source_binding_valid,
                self.tracked_source_clean,
                self.untracked_source_clean,
            )
        )

    @property
    def source_identity_mismatch(self) -> bool:
        return self.code_commit_matches_repository is False or self.source_binding_valid is False

    @property
    def source_tree_clean(self) -> bool:
        return self.tracked_source_clean is True and self.untracked_source_clean is True

    def to_record(self) -> dict[str, object]:
        return {
            "code_commit": self.code_commit,
            "code_commit_matches_repository": self.code_commit_matches_repository,
            "config_sha256": self.config_sha256,
            "cpu_model": self.cpu_model,
            "filesystem": self.filesystem,
            "hostname": self.hostname,
            "kernel": self.kernel,
            "lock_sha256": self.lock_sha256,
            "logical_cpu_count": self.logical_cpu_count,
            "python": self.python,
            "python_gil_enabled": self.python_gil_enabled,
            "imported_package_root": self.imported_package_root,
            "repository_head_commit": self.repository_head_commit,
            "repository_root": self.repository_root,
            "schema_version": self.schema_version,
            "source_root": self.source_root,
            "source_binding_valid": self.source_binding_valid,
            "total_memory_bytes": self.total_memory_bytes,
            "tracked_source_clean": self.tracked_source_clean,
            "unknown_fields": self.unknown_fields,
            "untracked_source_clean": self.untracked_source_clean,
            "uv": self.uv,
        }


def capture_host_runtime_identity(
    *,
    workspace_root: Path,
    storage_path: Path,
    code_commit: str,
    config_sha256: str,
) -> HostRuntimeIdentity:
    """Capture best-effort host evidence, retaining every missing field explicitly."""

    claimed_code_commit = normalize_git_commit(code_commit)
    (
        source_root,
        repository_root,
        repository_head_commit,
        imported_package_root,
        source_binding_valid,
        tracked_source_clean,
        untracked_source_clean,
    ) = _source_identity(workspace_root)
    code_commit_matches_repository = (
        None
        if claimed_code_commit is None or repository_head_commit is None
        else claimed_code_commit == repository_head_commit
    )
    exact_code_commit = claimed_code_commit if code_commit_matches_repository is True else None
    lock_sha256 = _sha256_file(workspace_root / "uv.lock")
    uv = _uv_identity()
    hostname = platform.node().strip() or None
    cpu_model = _cpu_model()
    logical_cpu_count = os.cpu_count()
    total_memory_bytes = _total_memory_bytes()
    filesystem = filesystem_identity(storage_path)
    gil_enabled = _gil_enabled()
    values: dict[str, object | None] = {
        "code_commit": exact_code_commit,
        "code_commit_matches_repository": code_commit_matches_repository,
        "cpu_model": cpu_model,
        "filesystem": filesystem,
        "hostname": hostname,
        "lock_sha256": lock_sha256,
        "logical_cpu_count": logical_cpu_count,
        "python_gil_enabled": gil_enabled,
        "imported_package_root": imported_package_root,
        "repository_head_commit": repository_head_commit,
        "repository_root": repository_root,
        "source_root": source_root,
        "source_binding_valid": source_binding_valid,
        "total_memory_bytes": total_memory_bytes,
        "tracked_source_clean": tracked_source_clean,
        "untracked_source_clean": untracked_source_clean,
        "uv": uv,
    }
    unknown_fields = tuple(sorted(name for name, value in values.items() if value is None))
    build = platform.python_build()
    python = (
        f"{platform.python_implementation()} {platform.python_version()};"
        f"implementation={sys.implementation.name};build={build[0]} {build[1]};"
        f"compiler={platform.python_compiler()};executable={Path(sys.executable).resolve()}"
    )
    kernel = (
        f"{platform.system()} {platform.release()} {platform.version()};"
        f"machine={platform.machine()}"
    )
    return HostRuntimeIdentity(
        code_commit=exact_code_commit,
        config_sha256=config_sha256,
        lock_sha256=lock_sha256,
        python=python,
        python_gil_enabled=gil_enabled,
        uv=uv,
        hostname=hostname,
        cpu_model=cpu_model,
        logical_cpu_count=logical_cpu_count,
        total_memory_bytes=total_memory_bytes,
        kernel=kernel,
        filesystem=filesystem,
        source_root=source_root,
        repository_root=repository_root,
        repository_head_commit=repository_head_commit,
        imported_package_root=imported_package_root,
        code_commit_matches_repository=code_commit_matches_repository,
        source_binding_valid=source_binding_valid,
        tracked_source_clean=tracked_source_clean,
        untracked_source_clean=untracked_source_clean,
        unknown_fields=unknown_fields,
    )


__all__ = [
    "HOST_RUNTIME_IDENTITY_SCHEMA_VERSION",
    "HostRuntimeIdentity",
    "capture_host_runtime_identity",
    "filesystem_identity",
    "normalize_git_commit",
]
