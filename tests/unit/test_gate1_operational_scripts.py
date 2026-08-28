from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import textwrap
import tomllib
from datetime import date, datetime, time
from pathlib import Path
from typing import cast

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_LAUNCHER_RELATIVE = Path("scripts/gate1/run_polymarket_soak.sh")
_AUDITOR_RELATIVE = Path("scripts/gate1/audit_polymarket_soak.py")
_SEALER_RELATIVE = Path("scripts/gate1/seal_polymarket_run.py")
_DESIGN_FREEZE_SHA = "e7d69aedc328ceda220120377307d5ebcc1a80d9"
_TERMINAL_HASH = "7" * 64


def _run(
    arguments: list[str | Path],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed test-owned commands and paths
        [str(argument) for argument in arguments],
        capture_output=True,
        check=check,
        cwd=cwd,
        env=env,
        text=True,
    )


def _git(repository: Path, *arguments: str) -> str:
    completed = _run(["git", "-C", repository, *arguments], check=True)
    return completed.stdout.strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_record_sha256(record: dict[str, object]) -> str:
    return hashlib.sha256(_canonical_record_bytes(record)).hexdigest()


def _canonical_record_bytes(record: dict[str, object]) -> bytes:
    return json.dumps(
        record,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _write_canonical_record(path: Path, record: dict[str, object]) -> None:
    path.write_bytes(_canonical_record_bytes(record) + b"\n")


def _self_hash(record: dict[str, object], field: str) -> dict[str, object]:
    result = dict(record)
    result[field] = _canonical_record_sha256(record)
    return result


def _canonical_config_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        assert math.isfinite(value)
        return value
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, list):
        return [_canonical_config_value(item) for item in cast(list[object], value)]
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        return {str(key): _canonical_config_value(item) for key, item in mapping.items()}
    raise AssertionError(f"unsupported fixture config value: {type(value).__name__}")


def _runtime_config_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        document = tomllib.load(handle)
    payload = json.dumps(
        _canonical_config_value(document),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _raw_inventory_record(root: Path) -> dict[str, object]:
    records: list[dict[str, object]] = []
    total_bytes = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        size = path.stat().st_size
        records.append(
            {
                "bytes": size,
                "path": path.relative_to(root).as_posix(),
                "sha256": _sha256(path),
            }
        )
        total_bytes += size
    canonical = json.dumps(
        records,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return {
        "path_count": len(records),
        "records": records,
        "root": str(root.resolve()),
        "schema_version": "mma-gate1-immutable-inventory-v1",
        "sha256": hashlib.sha256(canonical).hexdigest(),
        "total_bytes": total_bytes,
    }


def _refresh_fingerprint(record: dict[str, object], path: Path) -> None:
    record["bytes"] = path.stat().st_size
    record["sha256"] = _sha256(path)


def test_operational_runtime_config_hash_matches_application_semantics() -> None:
    from multi_market_arbitrage.core.config import load_phase1_config

    config = _ROOT / "config/phase1.toml"
    assert _runtime_config_sha256(config) == load_phase1_config(config).config_hash.value


def _candidate_repository(tmp_path: Path) -> tuple[Path, str]:
    repository = tmp_path / "source"
    _run(["git", "clone", "--shared", _ROOT, repository], check=True)
    _git(repository, "config", "user.name", "Gate Script Test")
    _git(repository, "config", "user.email", "gate-script-test@example.invalid")
    for relative in (_LAUNCHER_RELATIVE, _AUDITOR_RELATIVE, _SEALER_RELATIVE):
        destination = repository / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_ROOT / relative, destination)
    config_relative = Path("config/phase1.toml")
    shutil.copy2(_ROOT / config_relative, repository / config_relative)
    _git(
        repository,
        "add",
        "--force",
        str(_LAUNCHER_RELATIVE),
        str(_AUDITOR_RELATIVE),
        str(_SEALER_RELATIVE),
        str(config_relative),
    )
    # The scripts may already match the exact committed candidate when this
    # suite runs from a detached public-qualification worktree.  The fixture
    # still needs its own explicit candidate identity in that case.
    _git(repository, "commit", "--allow-empty", "-m", "test: freeze operational scripts")
    candidate = _git(repository, "rev-parse", "HEAD")
    # A pull-request checkout need not materialize refs/remotes/origin/main.
    # The fixture candidate's parent is the exact cloned source commit and is
    # the deterministic distinct-main authority this fixture needs.
    fixture_main = _git(repository, "rev-parse", f"{candidate}^")
    _git(repository, "update-ref", "refs/remotes/origin/main", fixture_main)
    assert _git(repository, "rev-parse", "design-freeze-v0.1^{commit}") == _DESIGN_FREEZE_SHA
    assert not _git(repository, "status", "--porcelain=v1", "--untracked-files=all")
    return repository, candidate


def _configure_candidate_remote(
    tmp_path: Path,
    repository: Path,
    candidate: str,
    *,
    main_sha: str | None = None,
) -> Path:
    remote = tmp_path / "candidate-origin.git"
    _run(["git", "init", "--bare", remote], check=True)
    _git(repository, "remote", "set-url", "origin", str(remote))
    expected_main = candidate if main_sha is None else main_sha
    _git(repository, "push", "origin", f"{expected_main}:refs/heads/main")
    _git(
        repository,
        "push",
        "origin",
        "refs/tags/design-freeze-v0.1:refs/tags/design-freeze-v0.1",
    )
    _git(repository, "fetch", "origin", "--tags", "--prune")
    assert _git(repository, "rev-parse", "origin/main") == expected_main
    return remote


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _install_uv_shim(directory: Path) -> Path:
    log = directory / "uv-calls.jsonl"
    shim = directory / "uv"
    _write_executable(
        shim,
        f"""#!{sys.executable}
import json
import os
from pathlib import Path
import subprocess
import sys

arguments = sys.argv[1:]
with Path(os.environ["MMA_TEST_UV_LOG"]).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(arguments) + "\\n")
if arguments == ["--version"]:
    print("uv 0.12.5 (test-fixture)")
    raise SystemExit(0)
if arguments[:2] == ["python", "find"]:
    print(os.environ["MMA_TEST_PYTHON"])
    raise SystemExit(0)
if arguments and arguments[0] in {{"lock", "sync"}}:
    raise SystemExit(0)
if "python" in arguments:
    python_index = arguments.index("python")
    python_arguments = arguments[python_index + 1:]
    if (
        len(python_arguments) >= 3
        and python_arguments[0] == "-I"
        and python_arguments[2] == "write-command"
    ):
        python = os.environ["MMA_TEST_PYTHON"]
        os.execv(python, [python, *python_arguments])
if "python" in arguments and arguments[-2:] == ["python", "-"]:
    python = os.environ["MMA_TEST_PYTHON"]
    os.execv(python, [python, "-"])
if "mma" not in arguments:
    raise SystemExit("unexpected uv fixture invocation")
mma = arguments.index("mma")
command = arguments[mma + 1:]
if command[:2] == ["observe", "polymarket"]:
    raw = Path.cwd() / "data" / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    if os.environ.get("MMA_TEST_PREFLIGHT_SUCCESS"):
        target = command[command.index("--gamma-market-id") + 1]
        (raw / "successful-preflight.raw").write_bytes(b"successful preflight\\n")
        print(json.dumps({{
            "actual_duration_nanoseconds": 60_000_000_000,
            "collector_run_id": "01a99999-2222-7333-8444-555555555555",
            "health_decision": {{
                "capture_healthy": True,
                "gate1_eligible_so_far": True,
                "research_eligible": True,
            }},
            "health_evidence": {{"completed": True, "gate1_evidence_eligible": True}},
            "requested_duration_seconds": 60,
            "status": "HEALTHY",
            "target": {{
                "requested_gamma_market_id": target,
                "resolved_gamma_market_id": target,
            }},
            "target_selection_mode": "EXPLICIT_GATE1",
        }}, indent=2, sort_keys=True))
        raise SystemExit(0)
    (raw / "failed-preflight.raw").write_bytes(b"preserved failed preflight\\n")
    raise SystemExit(int(os.environ.get("MMA_TEST_PREFLIGHT_RC", "23")))
if command[0] == "validate-data":
    if os.environ.get("MMA_TEST_ADVANCE_REMOTE"):
        subprocess.run(
            [
                "git",
                "--git-dir",
                os.environ["MMA_TEST_ADVANCE_REMOTE"],
                "update-ref",
                "refs/heads/main",
                os.environ["MMA_TEST_ADVANCE_SHA"],
            ],
            check=True,
        )
    print(json.dumps({{"errors": [], "root": command[1]}}, sort_keys=True))
    raise SystemExit(0)
if command[0] == "replay":
    if "--expect-terminal-hash" in command and os.environ.get("MMA_TEST_FAIL_REPLAY_TWO"):
        print("fixture replay-two failure", file=sys.stderr)
        raise SystemExit(37)
    if os.environ.get("MMA_TEST_MUTATE_RAW_DURING_AUDIT"):
        Path(os.environ["MMA_TEST_MUTATE_RAW_DURING_AUDIT"]).write_bytes(
            b"mutated during audit\\n"
        )
    print(json.dumps({{
        "collector_run_id": os.environ.get(
            "MMA_TEST_REPLAY_RUN_ID", os.environ["MMA_TEST_RUN_ID"]
        ),
        "processor": {{"terminal_hash": "{_TERMINAL_HASH}"}},
    }}, indent=2, sort_keys=True))
    raise SystemExit(0)
if command[0] == "rebuild-analytical":
    output = Path(command[command.index("--output") + 1])
    output.mkdir()
    (output / "rebuilt.marker").write_bytes(b"rebuilt\\n")
    print(json.dumps({{
        "replay": {{"processor": {{"terminal_hash": "{_TERMINAL_HASH}"}}}},
    }}, indent=2, sort_keys=True))
    raise SystemExit(0)
if command[0] == "scan-report":
    print(json.dumps({{
        "analytical_root": command[1],
        "candidates": 0,
        "dispositions": [{{"count": 2, "disposition": "NO_CANDIDATE"}}],
        "resources": {{"compressed_bytes": 1}},
        "schema_version": "phase1-scan-report-v2",
        "source_inventory_sha256": "8" * 64,
        "terminal_hash_count": 2,
        "terminal_hash_order": "observed_at_utc_ns_then_terminal_hash",
        "terminal_hash_samples": [],
        "terminal_hash_sequence_sha256": "9" * 64,
        "terminal_hashes": ["{_TERMINAL_HASH}", "{_TERMINAL_HASH}"],
        "terminal_hashes_complete": True,
        "total_scans": 2,
        "unique_lifecycles": 0,
        "unique_opportunity_observations": 0,
        "validated_opportunities": 0
    }}, indent=2, sort_keys=True))
    raise SystemExit(0)
raise SystemExit(f"unexpected mma fixture command: {{command!r}}")
""",
    )
    return log


def _shim_environment(shim_directory: Path, log: Path) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "MMA_TEST_PYTHON": sys.executable,
            "MMA_TEST_UV_LOG": str(log),
            "PATH": f"{shim_directory}:{environment['PATH']}",
        }
    )
    return environment


def _launcher_arguments(
    repository: Path,
    candidate: str,
    tmp_path: Path,
    *,
    validate_only: bool,
) -> list[str | Path]:
    arguments: list[str | Path] = [
        repository / _LAUNCHER_RELATIVE,
        "--expected-sha",
        candidate,
        "--target-id",
        "665374",
        "--source-repo",
        repository,
        "--worktree-parent",
        tmp_path / "worktrees",
        "--evidence-root",
        tmp_path / "evidence",
    ]
    if validate_only:
        arguments.append("--validate-only")
    return arguments


def test_launcher_contract_has_no_embedded_candidate_or_target_default() -> None:
    content = (_ROOT / _LAUNCHER_RELATIVE).read_text(encoding="utf-8")
    assert "a382b93220011428703fe02483f7034f59c1f045" not in content
    assert "DEFAULT_TARGET" not in content
    assert "merge-base" not in content
    assert 'GATE1_DURATION_SECONDS="21600"' in content
    assert "origin/main to equal --expected-sha exactly" in content
    assert '"$RUN_SEAL_RELATIVE" \\\n  write-command' in content
    assert '"schema_version": "mma-gate1-observer-command-v1"' not in content
    assert "for environment_prefix in GIT_ PYTHON UV_" in content
    assert _run(["bash", "-n", _ROOT / _LAUNCHER_RELATIVE]).returncode == 0


def test_evidence_helpers_require_isolated_python() -> None:
    sealer = _run([sys.executable, _ROOT / _SEALER_RELATIVE])
    assert sealer.returncode != 0
    assert "invoke this helper with Python -I" in sealer.stderr

    auditor = _run([sys.executable, _ROOT / _AUDITOR_RELATIVE])
    assert auditor.returncode != 0
    assert "invoke this helper with Python -I" in auditor.stderr

    direct_auditor_help = _run([_ROOT / _AUDITOR_RELATIVE, "--help"])
    assert direct_auditor_help.returncode == 0
    assert "--collector-run-id" in direct_auditor_help.stdout


def test_exact_candidate_sealer_writes_canonical_observer_command(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    evidence = tmp_path / "evidence-command"
    evidence.mkdir()
    environment = dict(os.environ)
    environment.update(
        {
            "GIT_CONFIG_PARAMETERS": "'core.fsmonitor=true'",
            "GIT_DIR": str(tmp_path / "attacker-git-dir"),
            "GIT_OBJECT_DIRECTORY": str(tmp_path / "attacker-object-dir"),
        }
    )
    completed = _run(
        [
            sys.executable,
            "-I",
            repository / _SEALER_RELATIVE,
            "write-command",
            "--source-worktree",
            repository,
            "--expected-sha",
            candidate,
            "--target-id",
            "665374",
            "--duration-seconds",
            "7200",
            "--output",
            evidence / "soak-command.json",
        ],
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads((evidence / "soak-command.json").read_text(encoding="utf-8")) == {
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
            "665374",
            "--duration",
            "7200",
            "--config",
            "config/phase1.toml",
        ],
        "config_relative_path": "config/phase1.toml",
        "cwd": str(repository),
        "duration_seconds": 7200,
        "environment": {"PYTHONHASHSEED": "0", "PYTHONUNBUFFERED": "1"},
        "expected_source_sha": candidate,
        "schema_version": "mma-gate1-observer-command-v1",
        "target_id": "665374",
    }


def test_observer_command_writer_rejects_non_exact_identity_and_overwrite(
    tmp_path: Path,
) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    evidence = tmp_path / "evidence-command"
    evidence.mkdir()
    output = evidence / "soak-command.json"
    arguments: list[str | Path] = [
        sys.executable,
        "-I",
        repository / _SEALER_RELATIVE,
        "write-command",
        "--source-worktree",
        repository,
        "--expected-sha",
        candidate,
        "--target-id",
        "665374",
        "--duration-seconds",
        "60",
        "--output",
        output,
    ]
    abbreviated = list(arguments)
    abbreviated[abbreviated.index(candidate)] = candidate[:12]
    rejected_identity = _run(abbreviated)
    assert rejected_identity.returncode != 0
    assert "full lowercase 40-character" in rejected_identity.stderr

    created = _run(arguments)
    assert created.returncode == 0, created.stderr
    original = output.read_bytes()
    rejected_overwrite = _run(arguments)
    assert rejected_overwrite.returncode != 0
    assert "refusing to replace" in rejected_overwrite.stderr
    assert output.read_bytes() == original


def test_launcher_and_command_writer_reject_git_replacement_refs(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    _git(repository, "replace", candidate, f"{candidate}^")
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    uv_log = _install_uv_shim(shim_directory)

    launcher = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=True),
        env=_shim_environment(shim_directory, uv_log),
    )
    assert launcher.returncode != 0
    assert "forbidden Git replacement refs" in launcher.stderr
    assert not uv_log.exists()
    assert not (tmp_path / "worktrees").exists()
    assert not (tmp_path / "evidence").exists()

    evidence = tmp_path / "evidence-command"
    evidence.mkdir()
    writer = _run(
        [
            sys.executable,
            "-I",
            repository / _SEALER_RELATIVE,
            "write-command",
            "--source-worktree",
            repository,
            "--expected-sha",
            candidate,
            "--target-id",
            "665374",
            "--duration-seconds",
            "60",
            "--output",
            evidence / "soak-command.json",
        ]
    )
    assert writer.returncode != 0
    assert "forbidden Git replacement refs" in writer.stderr
    assert not (evidence / "soak-command.json").exists()


def test_auditor_rejects_git_replacement_refs(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, _ = _prepare_audit_fixture(
        tmp_path
    )
    _git(repository, "replace", candidate, f"{candidate}^")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id),
        env=environment,
    )
    assert completed.returncode != 0
    assert "forbidden Git replacement refs" in completed.stderr
    assert not analysis.exists()


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (["--target-id", "665374"], "--expected-sha"),
        (["--expected-sha", "f0c1124", "--target-id", "665374"], "40-character"),
        (["--expected-sha", "f" * 40], "--target-id"),
    ],
)
def test_launcher_rejects_missing_or_abbreviated_identity(
    arguments: list[str], expected: str
) -> None:
    completed = _run([_ROOT / _LAUNCHER_RELATIVE, *arguments])
    assert completed.returncode != 0
    assert expected in completed.stderr


def test_launcher_rejects_wrong_design_freeze(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    _git(repository, "tag", "--force", "design-freeze-v0.1", "HEAD")
    completed = _run(_launcher_arguments(repository, candidate, tmp_path, validate_only=True))
    assert completed.returncode != 0
    assert "design-freeze-v0.1" in completed.stderr


def test_real_launch_rejects_candidate_not_equal_to_origin_main(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    fixture_main = _git(repository, "rev-parse", f"{candidate}^")
    _configure_candidate_remote(tmp_path, repository, candidate, main_sha=fixture_main)
    completed = _run(_launcher_arguments(repository, candidate, tmp_path, validate_only=False))
    assert completed.returncode != 0
    assert "origin/main to equal" in completed.stderr
    assert not (tmp_path / "evidence").exists()


def test_launcher_rejects_dirty_source(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    (repository / "operator-untracked.txt").write_text("dirty\n", encoding="utf-8")
    completed = _run(_launcher_arguments(repository, candidate, tmp_path, validate_only=True))
    assert completed.returncode != 0
    assert "not clean" in completed.stderr


def test_launcher_rejects_non_native_filesystem(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    _write_executable(
        shim_directory / "stat",
        "#!/usr/bin/env bash\nprintf '%s\\n' drvfs\n",
    )
    environment = dict(os.environ)
    environment["PATH"] = f"{shim_directory}:{environment['PATH']}"
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=True),
        env=environment,
    )
    assert completed.returncode != 0
    assert "native Linux filesystem" in completed.stderr


def test_launcher_rejects_inadequate_disk(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    _write_executable(
        shim_directory / "df",
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            printf 'Avail\\n1\\n'
            """
        ),
    )
    environment = dict(os.environ)
    environment["PATH"] = f"{shim_directory}:{environment['PATH']}"
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=True),
        env=environment,
    )
    assert completed.returncode != 0
    assert "less than 10 GiB" in completed.stderr


def test_validate_only_is_offline_and_never_starts_a_process(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    log = _install_uv_shim(shim_directory)
    nohup_sentinel = tmp_path / "nohup-was-called"
    _write_executable(
        shim_directory / "nohup",
        f"#!/usr/bin/env bash\nprintf called > {shlex_quote(nohup_sentinel)}\nexit 99\n",
    )
    fetch_sentinel = tmp_path / "remote-was-contacted"
    ssh_shim = shim_directory / "ssh"
    _write_executable(
        ssh_shim,
        f"#!/usr/bin/env bash\nprintf called > {shlex_quote(fetch_sentinel)}\nexit 98\n",
    )
    _git(repository, "remote", "set-url", "origin", "ssh://offline.invalid/repository.git")
    environment = _shim_environment(shim_directory, log)
    environment["GIT_SSH_COMMAND"] = str(ssh_shim)
    environment["PYTHONPATH"] = str(tmp_path / "attacker-pythonpath")
    environment["UV_PROJECT"] = str(tmp_path / "attacker-pyproject.toml")
    environment["UV_WORKING_DIR"] = str(tmp_path / "attacker-working-directory")
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=True),
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
    assert "NO PUBLIC MARKET-DATA REQUEST WAS PERFORMED." in completed.stdout
    assert "NO SIX-HOUR SOAK WAS LAUNCHED." in completed.stdout
    assert not nohup_sentinel.exists()
    assert not fetch_sentinel.exists()
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert not any("mma" in call or "observe" in call for call in calls)
    evidence_directories = tuple((tmp_path / "evidence").iterdir())
    assert len(evidence_directories) == 1
    assert not tuple(evidence_directories[0].glob("*.pid"))
    assert not (evidence_directories[0] / "run-soak.sh").exists()


def shlex_quote(path: Path) -> str:
    return "'" + str(path).replace("'", "'\\''") + "'"


def test_failed_preflight_is_preserved_and_never_launches_soak(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    _configure_candidate_remote(tmp_path, repository, candidate)
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    log = _install_uv_shim(shim_directory)
    environment = _shim_environment(shim_directory, log)
    environment["MMA_TEST_PREFLIGHT_RC"] = "23"
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=False),
        env=environment,
    )
    assert completed.returncode == 23
    assert "No six-hour soak was launched" in completed.stderr
    evidence = next((tmp_path / "evidence").iterdir())
    assert (evidence / "preflight" / "data" / "raw" / "failed-preflight.raw").is_file()
    assert (evidence / "preflight" / "exit-code.txt").read_text().strip() == "23"
    assert not (evidence / "nohup-runner.pid").exists()
    assert not (evidence / "run-soak.sh").exists()


def test_real_launch_fetches_and_rejects_a_stale_origin_main(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    remote = _configure_candidate_remote(tmp_path, repository, candidate)
    _git(repository, "commit", "--allow-empty", "-m", "test: advance remote main")
    advanced = _git(repository, "rev-parse", "HEAD")
    _git(repository, "push", "origin", f"{advanced}:refs/heads/future")
    _git(remote, "update-ref", "refs/heads/main", advanced)
    _git(repository, "update-ref", "refs/remotes/origin/main", candidate)
    assert _git(repository, "rev-parse", "origin/main") == candidate

    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    log = _install_uv_shim(shim_directory)
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=False),
        env=_shim_environment(shim_directory, log),
    )
    assert completed.returncode != 0
    assert "origin/main to equal" in completed.stderr
    assert _git(repository, "rev-parse", "origin/main") == advanced
    assert not log.exists()
    assert not (tmp_path / "evidence").exists()


def test_real_launch_refetches_origin_after_successful_preflight(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    remote = _configure_candidate_remote(tmp_path, repository, candidate)
    _git(repository, "commit", "--allow-empty", "-m", "test: future remote main")
    advanced = _git(repository, "rev-parse", "HEAD")
    _git(repository, "push", "origin", f"{advanced}:refs/heads/future")
    assert _git(repository, "rev-parse", "origin/main") == candidate

    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    log = _install_uv_shim(shim_directory)
    nohup_sentinel = tmp_path / "nohup-was-called"
    _write_executable(
        shim_directory / "nohup",
        f"#!/usr/bin/env bash\nprintf called > {shlex_quote(nohup_sentinel)}\nexit 99\n",
    )
    environment = _shim_environment(shim_directory, log)
    environment.update(
        {
            "MMA_TEST_ADVANCE_REMOTE": str(remote),
            "MMA_TEST_ADVANCE_SHA": advanced,
            "MMA_TEST_PREFLIGHT_SUCCESS": "1",
        }
    )
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=False),
        env=environment,
    )
    assert completed.returncode != 0
    assert "origin/main changed after preflight" in completed.stderr
    assert _git(repository, "rev-parse", "origin/main") == advanced
    assert not nohup_sentinel.exists()
    evidence = next((tmp_path / "evidence").iterdir())
    assert (evidence / "preflight" / "verification.json").is_file()
    assert not (evidence / "run-soak.sh").exists()


def test_successful_preflight_generates_sealed_exact_duration_runner_without_soak(
    tmp_path: Path,
) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    _configure_candidate_remote(tmp_path, repository, candidate)
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    log = _install_uv_shim(shim_directory)
    nohup_sentinel = tmp_path / "nohup-was-called"
    _write_executable(
        shim_directory / "nohup",
        f"#!/usr/bin/env bash\nprintf called > {shlex_quote(nohup_sentinel)}\nexit 99\n",
    )
    environment = _shim_environment(shim_directory, log)
    environment["MMA_TEST_PREFLIGHT_SUCCESS"] = "1"
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=False),
        env=environment,
    )
    assert completed.returncode != 0
    assert "runner exited during startup" in completed.stderr
    assert nohup_sentinel.is_file()
    evidence = next((tmp_path / "evidence").iterdir())
    runner = evidence / "run-soak.sh"
    assert _run(["bash", "-n", runner]).returncode == 0
    runner_text = runner.read_text(encoding="utf-8")
    assert "--duration 21600" in runner_text
    assert "scripts/gate1/seal_polymarket_run.py" in runner_text
    assert "run-completion-seal.json" in runner_text
    command = json.loads((evidence / "soak-command.json").read_text(encoding="utf-8"))
    assert command["duration_seconds"] == 21_600
    assert command["target_id"] == "665374"
    assert not (evidence / "observer.pid").exists()


@pytest.mark.parametrize("overlap_kind", ["worktree", "evidence", "parents"])
def test_launcher_rejects_path_overlap_before_creation(tmp_path: Path, overlap_kind: str) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    arguments = _launcher_arguments(repository, candidate, tmp_path, validate_only=True)
    if overlap_kind == "worktree":
        arguments[arguments.index("--worktree-parent") + 1] = repository / "nested-worktrees"
    elif overlap_kind == "evidence":
        arguments[arguments.index("--evidence-root") + 1] = repository / "nested-evidence"
    else:
        shared = tmp_path / "shared"
        arguments[arguments.index("--worktree-parent") + 1] = shared
        arguments[arguments.index("--evidence-root") + 1] = shared / "evidence"
    completed = _run(arguments)
    assert completed.returncode != 0
    assert "must not overlap" in completed.stderr


def test_launcher_rejects_non_exact_uv_version(tmp_path: Path) -> None:
    repository, candidate = _candidate_repository(tmp_path)
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    _write_executable(shim_directory / "uv", "#!/usr/bin/env bash\nprintf 'uv 0.12.50\\n'\n")
    environment = dict(os.environ)
    environment["PATH"] = f"{shim_directory}:{environment['PATH']}"
    completed = _run(
        _launcher_arguments(repository, candidate, tmp_path, validate_only=True),
        env=environment,
    )
    assert completed.returncode != 0
    assert "uv 0.12.5 is required" in completed.stderr


def _seal_arguments(
    repository: Path,
    candidate: str,
    evidence: Path,
    *,
    duration_seconds: int,
    exit_code: int,
) -> list[str | Path]:
    return [
        sys.executable,
        "-I",
        repository / _SEALER_RELATIVE,
        "--source-worktree",
        repository,
        "--evidence-dir",
        evidence,
        "--expected-sha",
        candidate,
        "--target-id",
        "665374",
        "--duration-seconds",
        str(duration_seconds),
        "--exit-code",
        str(exit_code),
        "--exit-code-file",
        evidence / "soak-exit-code.txt",
        "--summary",
        evidence / "soak-summary.json",
        "--config",
        repository / "config/phase1.toml",
        "--lock",
        repository / "uv.lock",
        "--command-file",
        evidence / "soak-command.json",
        "--raw-root",
        repository / "data/raw",
        "--output",
        evidence / "run-completion-seal.json",
    ]


def _prepare_audit_fixture(
    tmp_path: Path,
    *,
    duration_seconds: int = 7_200,
    exit_code: int = 0,
) -> tuple[Path, str, Path, Path, str, dict[str, str], Path]:
    repository, candidate = _candidate_repository(tmp_path)
    run_id = "01a11111-2222-7333-8444-555555555555"
    raw = repository / "data" / "raw"
    analytical = repository / "data" / "analytical"
    raw.mkdir(parents=True)
    analytical.mkdir(parents=True)
    (raw / "sealed.raw").write_bytes(b"immutable raw fixture\n")
    run_key = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    with (repository / "config/phase1.toml").open("rb") as handle:
        config_document = tomllib.load(handle)
    system = cast(dict[str, object], config_document["system"])
    storage = cast(dict[str, object], config_document["storage"])
    writer_id = "01a22222-3333-7444-8555-666666666666"
    submit_timeout = float(cast(float, storage["journal_submit_timeout_seconds"]))
    group_delay = float(cast(float, storage["journal_group_commit_max_delay_seconds"]))
    pending_age = float(cast(float, storage["journal_pending_seal_max_age_seconds"]))
    authority = _self_hash(
        {
            "authority_origin": "NEW_RUN",
            "authority_version": "raw-journal-source-authority-v1",
            "collector_run_id": run_id,
            "git_commit": candidate,
            "group_commit_max_delay_seconds_hex": group_delay.hex(),
            "group_commit_max_events": storage["journal_group_commit_max_events"],
            "max_events_per_segment": storage["journal_segment_max_events"],
            "max_pending_seal_age_nanoseconds": int(pending_age * 1_000_000_000),
            "max_pending_seal_age_seconds_hex": pending_age.hex(),
            "max_pending_seal_uncompressed_bytes": storage[
                "journal_pending_seal_max_uncompressed_bytes"
            ],
            "max_uncompressed_bytes_per_run": storage["journal_run_max_uncompressed_bytes"],
            "max_uncompressed_bytes_per_segment": storage["journal_segment_max_uncompressed_bytes"],
            "predecessor_manifest_sha256": None,
            "queue_capacity": system["queue_capacity"],
            "seal_queue_capacity": storage["journal_seal_queue_capacity"],
            "stream": "market",
            "submit_timeout_seconds_hex": submit_timeout.hex(),
            "venue_id": "polymarket",
            "writer_id": writer_id,
            "writer_software_version": system["code_version"],
            "zstd_level": 9,
        },
        "authority_sha256",
    )
    _write_canonical_record(
        raw / f"run-{run_key}.market.source-authority.json",
        authority,
    )
    segment = raw / f"run-{run_key}.market.{0:020d}-{0:020d}.jsonl.zst"
    segment_payload = b"test-owned sealed segment fixture\n"
    segment.write_bytes(segment_payload)
    manifest = _self_hash(
        {
            "collector_run_id": run_id,
            "compressed_bytes": len(segment_payload),
            "created_at_utc_ns": 3,
            "end_time_utc_ns": 2,
            "event_count": 1,
            "first_ingest_seq": 0,
            "git_commit": candidate,
            "hash_algorithm": "sha256",
            "journal_format_version": "raw-journal-manifest-v1",
            "last_ingest_seq": 0,
            "payload_digest": "6" * 64,
            "previous_segment_digest": None,
            "recovery_evidence": [],
            "segment_digest": _sha256(segment),
            "start_time_utc_ns": 1,
            "stream": "market",
            "uncompressed_bytes": 37,
            "venue": "polymarket",
            "writer_id": writer_id,
            "writer_software_version": system["code_version"],
        },
        "manifest_sha256",
    )
    _write_canonical_record(
        segment.with_name(f"{segment.name}.manifest.json"),
        manifest,
    )
    completion = _self_hash(
        {
            "collector_run_id": run_id,
            "completed_at_utc_ns": 4,
            "completion_version": "raw-journal-run-completion-v2",
            "expected_event_count": 1,
            "expected_segment_count": 1,
            "expected_uncompressed_bytes": 37,
            "first_ingest_seq": 0,
            "git_commit": candidate,
            "hash_algorithm": "sha256",
            "last_ingest_seq": 0,
            "max_uncompressed_bytes_per_run": storage["journal_run_max_uncompressed_bytes"],
            "max_uncompressed_bytes_per_segment": storage["journal_segment_max_uncompressed_bytes"],
            "source_authority_sha256": authority["authority_sha256"],
            "stream": "market",
            "terminal_manifest_sha256": manifest["manifest_sha256"],
            "venue": "polymarket",
            "writer_id": writer_id,
            "writer_software_version": system["code_version"],
        },
        "completion_sha256",
    )
    _write_canonical_record(
        raw / f"run-{run_key}.market.completed.json",
        completion,
    )
    (analytical / "original.marker").write_bytes(b"original analytical fixture\n")
    assert not _git(repository, "status", "--porcelain=v1", "--untracked-files=all")
    evidence = tmp_path / "run-evidence"
    evidence.mkdir()
    (evidence / "git-head.txt").write_text(candidate + "\n", encoding="utf-8")
    (evidence / "design-freeze-commit.txt").write_text(_DESIGN_FREEZE_SHA + "\n", encoding="utf-8")
    (evidence / "soak-worktree.txt").write_text(str(repository) + "\n", encoding="utf-8")
    (evidence / "gamma-market-id.txt").write_text("665374\n", encoding="utf-8")
    (evidence / "input-files.sha256").write_text(
        f"{_sha256(repository / 'uv.lock')}  uv.lock\n"
        f"{_sha256(repository / 'config/phase1.toml')}  config/phase1.toml\n",
        encoding="utf-8",
    )
    shim_directory = tmp_path / "shim"
    shim_directory.mkdir()
    log = _install_uv_shim(shim_directory)
    config_path = repository / "config/phase1.toml"
    summary: dict[str, object] = {
        "actual_duration_nanoseconds": duration_seconds * 1_000_000_000,
        "collector_run_id": run_id,
        "host_runtime_identity": {
            "code_commit": candidate,
            "code_commit_matches_repository": True,
            "config_sha256": _runtime_config_sha256(config_path),
            "imported_package_root": str(repository / "src/multi_market_arbitrage"),
            "lock_sha256": _sha256(repository / "uv.lock"),
            "repository_head_commit": candidate,
            "repository_root": str(repository),
            "schema_version": "gate1-host-runtime-identity-v1",
            "source_binding_valid": True,
            "source_root": str(repository),
            "tracked_source_clean": True,
            "unknown_fields": [],
            "untracked_source_clean": True,
        },
        "requested_duration_seconds": duration_seconds,
        "target": {
            "requested_gamma_market_id": "665374",
            "resolved_gamma_market_id": "665374",
        },
    }
    (evidence / "soak-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    command = _run(
        [
            sys.executable,
            "-I",
            repository / _SEALER_RELATIVE,
            "write-command",
            "--source-worktree",
            repository,
            "--expected-sha",
            candidate,
            "--target-id",
            "665374",
            "--duration-seconds",
            str(duration_seconds),
            "--output",
            evidence / "soak-command.json",
        ]
    )
    assert command.returncode == 0, command.stderr
    (evidence / "soak-exit-code.txt").write_text(f"{exit_code}\n", encoding="utf-8")
    sealed = _run(
        _seal_arguments(
            repository,
            candidate,
            evidence,
            duration_seconds=duration_seconds,
            exit_code=exit_code,
        )
    )
    assert sealed.returncode == 0, sealed.stderr
    environment = _shim_environment(shim_directory, log)
    environment["MMA_TEST_RUN_ID"] = run_id
    return repository, candidate, evidence, tmp_path / "analysis", run_id, environment, log


def _audit_arguments(
    repository: Path,
    candidate: str,
    evidence: Path,
    analysis: Path,
    run_id: str,
) -> list[str | Path]:
    return [
        sys.executable,
        "-I",
        repository / _AUDITOR_RELATIVE,
        "--source-worktree",
        repository,
        "--evidence-dir",
        evidence,
        "--collector-run-id",
        run_id,
        "--config",
        repository / "config/phase1.toml",
        "--analysis-dir",
        analysis,
        "--expected-sha",
        candidate,
    ]


def _mma_calls(log: Path) -> list[list[str]]:
    if not log.exists():
        return []
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    return [call[call.index("mma") + 1 :] for call in calls if "mma" in call]


def test_sealer_rejects_full_child_environment_as_observer_plan(tmp_path: Path) -> None:
    repository, candidate, evidence, _analysis, _run_id, _environment, _log = (
        _prepare_audit_fixture(tmp_path, duration_seconds=60)
    )
    (evidence / "run-completion-seal.json").unlink()
    command_path = evidence / "soak-command.json"
    command = json.loads(command_path.read_text(encoding="utf-8"))
    command["environment"] = {
        **cast(dict[str, str], command["environment"]),
        "LC_ALL": "C",
        "SHLVL": "1",
        "_": "/usr/bin/python3",
    }
    command_path.write_text(
        json.dumps(command, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    rejected = _run(
        _seal_arguments(
            repository,
            candidate,
            evidence,
            duration_seconds=60,
            exit_code=0,
        )
    )
    assert rejected.returncode != 0
    assert "observer command record differs from the exact sealed command" in rejected.stderr


_HOST_IDENTITY_MISMATCHES: tuple[tuple[str, object], ...] = (
    ("code_commit", "0" * 40),
    ("repository_head_commit", "0" * 40),
    ("source_root", "/wrong/source"),
    ("repository_root", "/wrong/repository"),
    ("imported_package_root", "/wrong/package"),
    ("code_commit_matches_repository", False),
    ("source_binding_valid", False),
    ("tracked_source_clean", False),
    ("untracked_source_clean", False),
    ("config_sha256", "0" * 64),
    ("lock_sha256", "0" * 64),
    ("unknown_fields", ["code_commit"]),
)


def _mutate_summary_host_identity(
    summary_path: Path,
    *,
    field: str | None,
    replacement: object = None,
) -> dict[str, object]:
    summary = cast(dict[str, object], json.loads(summary_path.read_text(encoding="utf-8")))
    if field is None:
        summary.pop("host_runtime_identity")
    else:
        host = cast(dict[str, object], summary["host_runtime_identity"])
        host[field] = replacement
    summary_path.write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _rewrite_seal_for_summary_mutation(
    seal_path: Path,
    summary_path: Path,
    summary: dict[str, object],
) -> None:
    seal = cast(dict[str, object], json.loads(seal_path.read_text(encoding="utf-8")))
    summary_fingerprint = cast(dict[str, object], seal["summary"])
    _refresh_fingerprint(summary_fingerprint, summary_path)
    if "host_runtime_identity" in summary:
        seal["host_runtime_identity"] = summary["host_runtime_identity"]
    else:
        seal.pop("host_runtime_identity", None)
    seal_path.write_text(json.dumps(seal, sort_keys=True) + "\n", encoding="utf-8")


def test_operational_fixture_uses_current_reader_valid_v2_identity(tmp_path: Path) -> None:
    from multi_market_arbitrage.journal.models import (
        JOURNAL_RUN_COMPLETION_VERSION,
        JOURNAL_SOURCE_AUTHORITY_VERSION,
    )
    from multi_market_arbitrage.journal.reader import (
        load_run_completion_head,
        load_segment_manifest,
        load_source_authority,
    )

    repository, _, _, _, _, _, _ = _prepare_audit_fixture(tmp_path)
    raw = repository / "data/raw"
    authority = load_source_authority(next(raw.glob("run-*.source-authority.json")))
    manifest = load_segment_manifest(next(raw.glob("run-*.jsonl.zst.manifest.json")))
    completion = load_run_completion_head(next(raw.glob("run-*.completed.json")))

    assert authority.to_record()["authority_version"] == JOURNAL_SOURCE_AUTHORITY_VERSION
    assert completion.completion_version == JOURNAL_RUN_COMPLETION_VERSION
    assert completion.source_authority_sha256 == authority.authority_sha256
    assert completion.terminal_manifest_sha256 == manifest.manifest_sha256


@pytest.mark.parametrize("duration_seconds", [60, 7_200])
def test_audit_helper_sequences_hashes_rebuilds_and_compares(
    tmp_path: Path, duration_seconds: int
) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path, duration_seconds=duration_seconds
    )
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 0, completed.stderr
    assert "NO GATE VERDICT WAS PRODUCED." in completed.stdout
    calls = _mma_calls(log)
    assert [call[0] for call in calls] == [
        "validate-data",
        "replay",
        "replay",
        "rebuild-analytical",
        "validate-data",
        "scan-report",
        "scan-report",
    ]
    assert "--expect-terminal-hash" not in calls[1]
    assert calls[2][calls[2].index("--expect-terminal-hash") + 1] == _TERMINAL_HASH
    assert calls[3][calls[3].index("--expect-terminal-hash") + 1] == _TERMINAL_HASH
    manifest = json.loads((analysis / "audit-manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "PASSED"
    assert manifest["gate_verdict"] is None
    assert manifest["evidence_immutable"] is True
    assert manifest["raw_immutable"] is True
    assert manifest["raw_matches_completion_seal"] is True
    assert manifest["terminal_hash"] == _TERMINAL_HASH
    assert len(manifest["commands"]) == 7
    seal = json.loads((evidence / "run-completion-seal.json").read_text(encoding="utf-8"))
    assert seal["collector_run_id"] == run_id
    assert seal["duration_seconds"] == duration_seconds
    assert seal["source_sha"] == candidate
    assert seal["host_runtime_identity"]["code_commit"] == candidate
    assert seal["config"]["runtime_sha256"] == _runtime_config_sha256(
        repository / "config/phase1.toml"
    )
    assert seal["raw_source_identity"]["expected_git_commit"] == candidate
    assert seal["raw_source_identity"]["schema_version"] == "mma-gate1-raw-source-identity-v2"
    assert len(seal["raw_source_identity"]["segment_manifests"]) == 1
    assert len(seal["raw_source_identity"]["completion_headers"]) == 1
    assert len(seal["raw_source_identity"]["source_authorities"]) == 1
    assert (
        seal["raw_source_identity"]["completion_headers"][0]["source_authority_sha256"]
        == seal["raw_source_identity"]["source_authorities"][0]["authority_sha256"]
    )
    assert seal["raw_inventory"]["sha256"] == manifest["raw_pre_sha256"]
    comparison = json.loads((analysis / "replay-comparison.json").read_text())
    assert comparison["byte_identical"] is True
    scan_comparison = json.loads((analysis / "scan-semantic-comparison.json").read_text())
    assert scan_comparison["semantics_equal"] is True
    before = json.loads((analysis / "raw-inventory.before.json").read_text())
    after = json.loads((analysis / "raw-inventory.after.json").read_text())
    assert before["sha256"] == after["sha256"]
    assert not any(call[:2] == ["observe", "polymarket"] for call in calls)


def _selected_raw_artifacts(repository: Path) -> tuple[Path, Path, str]:
    authority = next(repository.glob("data/raw/run-*.source-authority.json"))
    completion = next(repository.glob("data/raw/run-*.completed.json"))
    run_prefix = authority.name.removesuffix("market.source-authority.json")
    return authority, completion, run_prefix


def _mutate_current_raw_authority_contract(repository: Path, mutation: str) -> None:
    authority_path, completion_path, run_prefix = _selected_raw_artifacts(repository)
    if mutation == "missing-authority":
        authority_path.unlink()
        return
    if mutation == "authority-publication-temp":
        (
            authority_path.parent / f".{run_prefix}market.source-authority.json.fixture.tmp"
        ).write_text("{}\n", encoding="utf-8")
        return
    if mutation == "authority-self-hash":
        authority = cast(dict[str, object], json.loads(authority_path.read_text(encoding="utf-8")))
        authority["authority_sha256"] = "0" * 64
        authority_path.write_text(json.dumps(authority, sort_keys=True) + "\n", encoding="utf-8")
        return
    if mutation in {"authority-config-bound", "authority-git"}:
        authority = cast(dict[str, object], json.loads(authority_path.read_text(encoding="utf-8")))
        if mutation == "authority-config-bound":
            authority["max_events_per_segment"] = cast(int, authority["max_events_per_segment"]) + 1
        else:
            authority["git_commit"] = "0" * 40
        content = dict(authority)
        content.pop("authority_sha256")
        authority["authority_sha256"] = _canonical_record_sha256(content)
        _write_canonical_record(authority_path, authority)
        return
    completion = cast(dict[str, object], json.loads(completion_path.read_text(encoding="utf-8")))
    if mutation == "completion-authority-binding":
        completion["source_authority_sha256"] = "0" * 64
    elif mutation == "legacy-completion":
        completion["completion_version"] = "raw-journal-run-completion-v1"
        completion.pop("source_authority_sha256")
    else:
        raise AssertionError(f"unsupported fixture mutation: {mutation}")
    content = dict(completion)
    content.pop("completion_sha256")
    completion["completion_sha256"] = _canonical_record_sha256(content)
    completion_path.write_text(json.dumps(completion, sort_keys=True) + "\n", encoding="utf-8")


_RAW_AUTHORITY_MUTATIONS = (
    "missing-authority",
    "authority-publication-temp",
    "authority-self-hash",
    "authority-config-bound",
    "authority-git",
    "completion-authority-binding",
    "legacy-completion",
)


@pytest.mark.parametrize("mutation", _RAW_AUTHORITY_MUTATIONS)
def test_sealer_rejects_invalid_current_source_authority_contract(
    tmp_path: Path,
    mutation: str,
) -> None:
    repository, candidate, evidence, _, _, _, _ = _prepare_audit_fixture(tmp_path)
    (evidence / "run-completion-seal.json").unlink()
    _mutate_current_raw_authority_contract(repository, mutation)
    completed = _run(
        _seal_arguments(
            repository,
            candidate,
            evidence,
            duration_seconds=7_200,
            exit_code=0,
        )
    )
    assert completed.returncode == 1
    assert (
        "source-authority" in completed.stderr
        or "source authority" in completed.stderr
        or "completion" in completed.stderr
    )
    assert not (evidence / "run-completion-seal.json").exists()


@pytest.mark.parametrize("mutation", _RAW_AUTHORITY_MUTATIONS)
def test_auditor_rejects_invalid_current_source_authority_contract_before_mma(
    tmp_path: Path,
    mutation: str,
) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    _mutate_current_raw_authority_contract(repository, mutation)
    seal_path = evidence / "run-completion-seal.json"
    seal = cast(dict[str, object], json.loads(seal_path.read_text(encoding="utf-8")))
    seal["raw_inventory"] = _raw_inventory_record(repository / "data/raw")
    seal_path.write_text(json.dumps(seal, sort_keys=True) + "\n", encoding="utf-8")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert (
        "source-authority" in completed.stderr
        or "source authority" in completed.stderr
        or "completion" in completed.stderr
    )
    assert _mma_calls(log) == []
    assert not analysis.exists()


def test_sealer_rejects_omitted_host_runtime_identity(tmp_path: Path) -> None:
    repository, candidate, evidence, _, _, _, _ = _prepare_audit_fixture(tmp_path)
    (evidence / "run-completion-seal.json").unlink()
    _mutate_summary_host_identity(evidence / "soak-summary.json", field=None)
    completed = _run(
        _seal_arguments(
            repository,
            candidate,
            evidence,
            duration_seconds=7_200,
            exit_code=0,
        )
    )
    assert completed.returncode == 1
    assert "omits host_runtime_identity" in completed.stderr
    assert not (evidence / "run-completion-seal.json").exists()


@pytest.mark.parametrize(("field", "replacement"), _HOST_IDENTITY_MISMATCHES)
def test_sealer_rejects_each_host_source_identity_mismatch(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    repository, candidate, evidence, _, _, _, _ = _prepare_audit_fixture(tmp_path)
    (evidence / "run-completion-seal.json").unlink()
    _mutate_summary_host_identity(
        evidence / "soak-summary.json",
        field=field,
        replacement=replacement,
    )
    completed = _run(
        _seal_arguments(
            repository,
            candidate,
            evidence,
            duration_seconds=7_200,
            exit_code=0,
        )
    )
    assert completed.returncode == 1
    assert "host_runtime_identity" in completed.stderr
    assert not (evidence / "run-completion-seal.json").exists()


def test_auditor_rejects_omitted_host_runtime_identity_before_mma(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    summary_path = evidence / "soak-summary.json"
    summary = _mutate_summary_host_identity(summary_path, field=None)
    _rewrite_seal_for_summary_mutation(evidence / "run-completion-seal.json", summary_path, summary)
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "omits host_runtime_identity" in completed.stderr
    assert _mma_calls(log) == []
    assert not analysis.exists()


@pytest.mark.parametrize(("field", "replacement"), _HOST_IDENTITY_MISMATCHES)
def test_auditor_rejects_each_host_source_identity_mismatch_before_mma(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    summary_path = evidence / "soak-summary.json"
    summary = _mutate_summary_host_identity(
        summary_path,
        field=field,
        replacement=replacement,
    )
    _rewrite_seal_for_summary_mutation(evidence / "run-completion-seal.json", summary_path, summary)
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "host_runtime_identity" in completed.stderr
    assert _mma_calls(log) == []
    assert not analysis.exists()


def _mutate_raw_git_commit(
    repository: Path,
    *,
    artifact: str,
    replacement: str | None,
) -> None:
    pattern = (
        "data/raw/run-*.jsonl.zst.manifest.json"
        if artifact == "segment"
        else "data/raw/run-*.completed.json"
    )
    path = next(repository.glob(pattern))
    record = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
    if replacement is None:
        record.pop("git_commit")
    else:
        record["git_commit"] = replacement
    path.write_text(json.dumps(record, sort_keys=True) + "\n", encoding="utf-8")


@pytest.mark.parametrize("artifact", ["segment", "completion"])
@pytest.mark.parametrize("replacement", [None, "0" * 40], ids=["omitted", "mismatched"])
def test_sealer_rejects_missing_or_wrong_raw_git_commit(
    tmp_path: Path,
    artifact: str,
    replacement: str | None,
) -> None:
    repository, candidate, evidence, _, _, _, _ = _prepare_audit_fixture(tmp_path)
    (evidence / "run-completion-seal.json").unlink()
    _mutate_raw_git_commit(repository, artifact=artifact, replacement=replacement)
    completed = _run(
        _seal_arguments(
            repository,
            candidate,
            evidence,
            duration_seconds=7_200,
            exit_code=0,
        )
    )
    assert completed.returncode == 1
    assert "git_commit differs" in completed.stderr
    assert not (evidence / "run-completion-seal.json").exists()


@pytest.mark.parametrize("artifact", ["segment", "completion"])
@pytest.mark.parametrize("replacement", [None, "0" * 40], ids=["omitted", "mismatched"])
def test_auditor_rejects_missing_or_wrong_raw_git_commit_before_mma(
    tmp_path: Path,
    artifact: str,
    replacement: str | None,
) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    _mutate_raw_git_commit(repository, artifact=artifact, replacement=replacement)
    seal_path = evidence / "run-completion-seal.json"
    seal = cast(dict[str, object], json.loads(seal_path.read_text(encoding="utf-8")))
    seal["raw_inventory"] = _raw_inventory_record(repository / "data/raw")
    seal_path.write_text(json.dumps(seal, sort_keys=True) + "\n", encoding="utf-8")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "git_commit differs" in completed.stderr
    assert _mma_calls(log) == []
    assert not analysis.exists()


def test_audit_rejects_raw_mutation_since_runner_seal_before_mma(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    (repository / "data/raw/sealed.raw").write_bytes(b"mutated after completion seal\n")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "raw inventory differs" in completed.stderr
    assert _mma_calls(log) == []
    assert not analysis.exists()


def test_audit_rejects_wrong_collector_run_id_before_mma(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    wrong_run_id = f"{run_id}-different"
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, wrong_run_id), env=environment
    )
    assert completed.returncode == 1
    assert "collector_run_id differs" in completed.stderr
    assert _mma_calls(log) == []


def test_audit_rejects_replay_collector_run_id_mismatch(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    environment["MMA_TEST_REPLAY_RUN_ID"] = "01afffff-2222-7333-8444-555555555555"
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "replay one CollectorRunId differs" in completed.stderr
    assert [call[0] for call in _mma_calls(log)] == ["validate-data", "replay"]
    manifest = json.loads((analysis / "audit-manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "FAILED"


def test_audit_rejects_raw_mutation_during_commands_against_runner_seal(
    tmp_path: Path,
) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    environment["MMA_TEST_MUTATE_RAW_DURING_AUDIT"] = str(repository / "data/raw/sealed.raw")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "raw inventory changed during audit" in completed.stderr
    assert len(_mma_calls(log)) == 7
    manifest = json.loads((analysis / "audit-manifest.json").read_text(encoding="utf-8"))
    assert manifest["raw_immutable"] is False
    assert manifest["raw_matches_completion_seal"] is False


@pytest.mark.parametrize(
    "field",
    [
        "source_sha",
        "design_freeze_sha",
        "source_worktree",
        "evidence_directory",
        "data_root",
        "raw_root",
        "target_id",
        "duration_seconds",
    ],
)
def test_audit_rejects_wrong_sealed_identity_before_mma(tmp_path: Path, field: str) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    seal_path = evidence / "run-completion-seal.json"
    seal = json.loads(seal_path.read_text(encoding="utf-8"))
    replacements: dict[str, object] = {
        "data_root": str(tmp_path / "wrong-data"),
        "design_freeze_sha": "0" * 40,
        "duration_seconds": 60,
        "evidence_directory": str(tmp_path / "wrong-evidence"),
        "raw_root": str(tmp_path / "wrong-raw"),
        "source_sha": "0" * 40,
        "source_worktree": str(tmp_path / "wrong-source"),
        "target_id": "999999",
    }
    seal[field] = replacements[field]
    seal_path.write_text(json.dumps(seal, sort_keys=True) + "\n", encoding="utf-8")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "seal" in completed.stderr or "sealed summary" in completed.stderr
    assert _mma_calls(log) == []


@pytest.mark.parametrize(
    "record_name", ["summary", "config", "lock", "command", "exit_code_artifact"]
)
def test_audit_rejects_wrong_sealed_artifact_hash_before_mma(
    tmp_path: Path, record_name: str
) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    seal_path = evidence / "run-completion-seal.json"
    seal = json.loads(seal_path.read_text(encoding="utf-8"))
    seal[record_name]["sha256"] = "0" * 64
    seal_path.write_text(json.dumps(seal, sort_keys=True) + "\n", encoding="utf-8")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "hash differs" in completed.stderr
    assert _mma_calls(log) == []


def test_audit_rejects_wrong_sealed_raw_hash_before_mma(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    seal_path = evidence / "run-completion-seal.json"
    seal = json.loads(seal_path.read_text(encoding="utf-8"))
    seal["raw_inventory"]["sha256"] = "0" * 64
    seal_path.write_text(json.dumps(seal, sort_keys=True) + "\n", encoding="utf-8")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "raw inventory differs" in completed.stderr
    assert _mma_calls(log) == []


def test_audit_rejects_nonzero_sealed_observer_exit_before_mma(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path, exit_code=23
    )
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "zero observer exit" in completed.stderr
    assert _mma_calls(log) == []


def test_audit_rejects_summary_mutation_since_runner_seal_before_mma(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    summary_path = evidence / "soak-summary.json"
    original_summary = summary_path.read_text(encoding="utf-8")
    summary_path.write_text(original_summary.replace(run_id, "9" + run_id[1:], 1), encoding="utf-8")
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "summary hash differs" in completed.stderr
    assert _mma_calls(log) == []


@pytest.mark.parametrize("overlap", ["evidence", "analysis"])
def test_audit_rejects_path_overlap_before_mma(tmp_path: Path, overlap: str) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    if overlap == "evidence":
        overlapping_evidence = repository / "data" / "evidence"
        shutil.move(str(evidence), overlapping_evidence)
        evidence = overlapping_evidence
    else:
        analysis = repository / "data" / "audit-analysis"
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "overlap" in completed.stderr or "separate" in completed.stderr
    assert _mma_calls(log) == []


def test_audit_rejects_non_exact_uv_version_before_mma(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    _write_executable(
        Path(environment["PATH"].split(":", maxsplit=1)[0]) / "uv",
        """\
#!/usr/bin/env python3
print("uv 0.12.50")
""",
    )
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "uv 0.12.5 is required" in completed.stderr
    assert _mma_calls(log) == []


def test_audit_helper_preserves_partial_artifacts_on_failure(tmp_path: Path) -> None:
    repository, candidate, evidence, analysis, run_id, environment, log = _prepare_audit_fixture(
        tmp_path
    )
    environment["MMA_TEST_FAIL_REPLAY_TWO"] = "1"
    completed = _run(
        _audit_arguments(repository, candidate, evidence, analysis, run_id), env=environment
    )
    assert completed.returncode == 1
    assert "POST-RUN AUDIT FAILED" in completed.stderr
    manifest = json.loads((analysis / "audit-manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "FAILED"
    assert manifest["gate_verdict"] is None
    assert manifest["evidence_immutable"] is True
    assert manifest["raw_immutable"] is True
    assert len(manifest["commands"]) == 3
    assert (analysis / "commands" / "02-replay-one.stdout.json").is_file()
    assert (analysis / "commands" / "03-replay-two.command.json").is_file()
    assert not (analysis / "rebuilt-analytical").exists()
    assert (analysis / "raw-inventory.after.json").is_file()
    assert [call[0] for call in _mma_calls(log)] == ["validate-data", "replay", "replay"]
