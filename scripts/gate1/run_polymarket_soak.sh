#!/usr/bin/env bash
set -Eeuo pipefail

readonly DESIGN_FREEZE_SHA="e7d69aedc328ceda220120377307d5ebcc1a80d9"
readonly REQUIRED_UV_VERSION="0.12.5"
readonly REQUIRED_PYTHON_VERSION="3.14.7"
readonly GATE1_DURATION_SECONDS="21600"
readonly PREFLIGHT_DURATION_SECONDS="60"
readonly PREFLIGHT_PROCESS_TIMEOUT_SECONDS="180"
readonly MINIMUM_FREE_BYTES="$((10 * 1024 * 1024 * 1024))"
readonly RUN_SEAL_RELATIVE="scripts/gate1/seal_polymarket_run.py"

for environment_prefix in GIT_ PYTHON UV_; do
  while IFS= read -r environment_name; do
    unset "$environment_name"
  done < <(compgen -A variable "$environment_prefix")
done
unset BASH_ENV ENV LD_LIBRARY_PATH LD_PRELOAD PYTHON UV VIRTUAL_ENV
export GIT_ATTR_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
export GIT_CONFIG_NOSYSTEM=1 GIT_NO_REPLACE_OBJECTS=1
export GIT_CONFIG_COUNT=4
export GIT_CONFIG_KEY_0=core.hooksPath GIT_CONFIG_VALUE_0=/dev/null
export GIT_CONFIG_KEY_1=core.fsmonitor GIT_CONFIG_VALUE_1=false
export GIT_CONFIG_KEY_2=core.attributesFile GIT_CONFIG_VALUE_2=/dev/null
export GIT_CONFIG_KEY_3=core.excludesFile GIT_CONFIG_VALUE_3=/dev/null
export PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 UV_NO_CONFIG=1

usage() {
  cat <<'EOF'
Usage:
  scripts/gate1/run_polymarket_soak.sh \
    --expected-sha <full-40-character-SHA> \
    --target-id <Gamma-market-ID> [options]

Required:
  --expected-sha SHA      Exact 40-character commit to validate or launch
  --target-id ID          Exact explicit Gamma market ID; no default is permitted

Options:
  --source-repo PATH      Source repository (default: inferred from this script)
  --worktree-parent PATH  Native-Linux detached-worktree parent
  --evidence-root PATH    Native-Linux evidence parent
  --validate-only         Run only offline identity/environment/path/disk validation
  -h, --help              Show this help

Validation-only mode performs no fetch, no public market-data request, no
preflight and no soak launch. It intentionally permits the exact candidate to
differ from origin/main so an unmerged candidate can be qualified safely.

An actual launch requires origin/main to equal --expected-sha exactly. It runs
one explicit-target 60-second preflight and starts exactly 21,600 seconds only
after that preflight succeeds. This script never accepts ancestry as evidence
of exact source identity and never selects a fallback target.
EOF
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

canonical_existing_directory() {
  local path="$1"
  [[ -d "$path" && ! -L "$path" ]] || fail "directory is absent or symbolic: $path"
  (cd -- "$path" && pwd -P)
}

canonical_created_directory() {
  local path="$1"
  [[ ! -L "$path" ]] || fail "directory path cannot be symbolic: $path"
  mkdir -p -- "$path"
  canonical_existing_directory "$path"
}

canonical_future_path() {
  python3 - "$1" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).expanduser().resolve(strict=False))
PY
}

paths_overlap() {
  local first="$1"
  local second="$2"
  [[ "$first" == "$second" || "$first" == "$second"/* || "$second" == "$first"/* ]]
}

require_separate_paths() {
  local first="$1"
  local first_label="$2"
  local second="$3"
  local second_label="$4"
  ! paths_overlap "$first" "$second" \
    || fail "$first_label must not overlap $second_label"
}

filesystem_type() {
  stat -f -c '%T' -- "$1"
}

require_native_linux_filesystem() {
  local path="$1"
  local label="$2"
  local observed
  observed="$(filesystem_type "$path")"
  case "${observed,,}" in
    drvfs|9p|fuseblk|ntfs|ntfs3|msdos|vfat|exfat|cifs|smb2|fuse.sshfs)
      fail "$label must use a native Linux filesystem, observed: $observed"
      ;;
  esac
}

free_bytes() {
  local value
  value="$(df --output=avail -B1 -- "$1" | tail -n 1 | tr -d '[:space:]')"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "could not determine free disk bytes for $1"
  printf '%s\n' "$value"
}

require_disk_headroom() {
  local path="$1"
  local label="$2"
  local available
  available="$(free_bytes "$path")"
  ((available >= MINIMUM_FREE_BYTES)) \
    || fail "$label has less than 10 GiB free: $path"
}

sha256_file() {
  sha256sum -- "$1" | cut -d' ' -f1
}

sha256_git_path() {
  local repository="$1"
  local commit="$2"
  local path="$3"
  git -C "$repository" cat-file blob "${commit}:${path}" | sha256sum | cut -d' ' -f1
}

require_clean_repository() {
  local repository="$1"
  [[ -z "$(git -C "$repository" status --porcelain=v1 --untracked-files=all)" ]] \
    || fail "source repository is not clean: $repository"
}

require_no_replace_refs() {
  local repository="$1"
  [[ -z "$(git -C "$repository" for-each-ref --format='%(refname)' refs/replace)" ]] \
    || fail "source repository contains forbidden Git replacement refs: $repository"
}

verify_python() {
  local output="$1"
  shift
  "$@" - <<'PY' | tee "$output"
import json
import platform
import sys
import sysconfig

if platform.python_implementation() != "CPython":
    raise SystemExit("CPython is required")
if sys.version_info[:3] != (3, 14, 7):
    raise SystemExit(f"CPython 3.14.7 is required, observed {platform.python_version()}")
if sysconfig.get_config_var("Py_GIL_DISABLED") != 0 or not sys._is_gil_enabled():
    raise SystemExit("ordinary GIL-enabled CPython is required")
print(
    json.dumps(
        {
            "executable": sys.executable,
            "implementation": platform.python_implementation(),
            "ordinary_gil": True,
            "version": platform.python_version(),
        },
        sort_keys=True,
    )
)
PY
}

verify_preflight_summary() {
  local summary_path="$1"
  local expected_target="$2"
  local output_path="$3"
  python3 - "$summary_path" "$expected_target" "$output_path" <<'PY'
import json
import math
import pathlib
import sys

summary_path = pathlib.Path(sys.argv[1])
expected_target = sys.argv[2]
output_path = pathlib.Path(sys.argv[3])
try:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"cannot read canonical preflight summary: {error}") from error
if not isinstance(summary, dict):
    raise SystemExit("preflight summary is not a JSON object")
target = summary.get("target")
health = summary.get("health_decision")
evidence = summary.get("health_evidence")
checks = {
    "actual_duration_completed": (
        isinstance(summary.get("actual_duration_nanoseconds"), int)
        and not isinstance(summary.get("actual_duration_nanoseconds"), bool)
        and summary["actual_duration_nanoseconds"] >= 60_000_000_000
    ),
    "capture_healthy": isinstance(health, dict) and health.get("capture_healthy") is True,
    "explicit_selection": summary.get("target_selection_mode") == "EXPLICIT_GATE1",
    "gate1_eligible_so_far": (
        isinstance(health, dict) and health.get("gate1_eligible_so_far") is True
    ),
    "health_evidence_complete": (
        isinstance(evidence, dict)
        and evidence.get("completed") is True
        and evidence.get("gate1_evidence_eligible") is True
    ),
    "requested_duration_exact": (
        isinstance(summary.get("requested_duration_seconds"), (int, float))
        and not isinstance(summary.get("requested_duration_seconds"), bool)
        and math.isfinite(float(summary["requested_duration_seconds"]))
        and float(summary["requested_duration_seconds"]) == 60.0
    ),
    "research_eligible": isinstance(health, dict) and health.get("research_eligible") is True,
    "resolved_target_exact": (
        isinstance(target, dict)
        and target.get("requested_gamma_market_id") == expected_target
        and target.get("resolved_gamma_market_id") == expected_target
    ),
    "status_healthy": summary.get("status") == "HEALTHY",
}
output_path.write_text(
    json.dumps(
        {
            "checks": checks,
            "collector_run_id": summary.get("collector_run_id"),
            "schema_version": "mma-gate1-launcher-preflight-verification-v1",
            "verified": all(checks.values()),
        },
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
if not all(checks.values()):
    failed = ", ".join(key for key, passed in checks.items() if not passed)
    raise SystemExit(f"preflight summary failed: {failed}")
PY
}

EXPECTED_SHA=""
TARGET_ID=""
SOURCE_REPO=""
WORKTREE_PARENT=""
EVIDENCE_ROOT=""
VALIDATE_ONLY=0

while (($#)); do
  case "$1" in
    --expected-sha)
      (($# >= 2)) || fail "--expected-sha requires a value"
      EXPECTED_SHA="$2"
      shift 2
      ;;
    --target-id)
      (($# >= 2)) || fail "--target-id requires a value"
      TARGET_ID="$2"
      shift 2
      ;;
    --source-repo)
      (($# >= 2)) || fail "--source-repo requires a value"
      SOURCE_REPO="$2"
      shift 2
      ;;
    --worktree-parent)
      (($# >= 2)) || fail "--worktree-parent requires a value"
      WORKTREE_PARENT="$2"
      shift 2
      ;;
    --evidence-root)
      (($# >= 2)) || fail "--evidence-root requires a value"
      EVIDENCE_ROOT="$2"
      shift 2
      ;;
    --validate-only)
      VALIDATE_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] \
  || fail "--expected-sha must be a full lowercase 40-character Git SHA"
[[ "$TARGET_ID" =~ ^[1-9][0-9]*$ ]] \
  || fail "--target-id must be an explicit positive numeric Gamma market ID"

require_command git
require_command uv
require_command sha256sum
require_command stat
require_command df
require_command date
require_command python3

SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/$(basename -- "${BASH_SOURCE[0]}")"
if [[ -z "$SOURCE_REPO" ]]; then
  SOURCE_REPO="$(git -C "$(dirname -- "$SCRIPT_PATH")/../.." rev-parse --show-toplevel)"
fi
SOURCE_REPO="$(canonical_existing_directory "$SOURCE_REPO")"
[[ "$(git -C "$SOURCE_REPO" rev-parse --show-toplevel)" == "$SOURCE_REPO" ]] \
  || fail "--source-repo is not a Git worktree root"
require_no_replace_refs "$SOURCE_REPO"
require_clean_repository "$SOURCE_REPO"
require_native_linux_filesystem "$SOURCE_REPO" "source repository"

case "$SCRIPT_PATH" in
  "$SOURCE_REPO"/*) SCRIPT_RELATIVE="${SCRIPT_PATH#"$SOURCE_REPO"/}" ;;
  *) fail "launcher must be invoked from the selected source repository" ;;
esac

if ((VALIDATE_ONLY == 0)); then
  git -C "$SOURCE_REPO" fetch origin --tags --prune \
    || fail "could not refresh origin before the launch identity decision"
  require_clean_repository "$SOURCE_REPO"
fi

RESOLVED_SHA="$(git -C "$SOURCE_REPO" rev-parse --verify "${EXPECTED_SHA}^{commit}")" \
  || fail "--expected-sha does not resolve to a commit"
[[ "$RESOLVED_SHA" == "$EXPECTED_SHA" ]] || fail "expected commit did not resolve exactly"
[[ "$(git -C "$SOURCE_REPO" rev-parse 'design-freeze-v0.1^{commit}')" == "$DESIGN_FREEZE_SHA" ]] \
  || fail "design-freeze-v0.1 does not resolve to the expected immutable commit"

EXPECTED_SCRIPT_SHA="$(sha256_git_path "$SOURCE_REPO" "$EXPECTED_SHA" "$SCRIPT_RELATIVE")" \
  || fail "candidate does not contain this launcher"
[[ "$(sha256_file "$SCRIPT_PATH")" == "$EXPECTED_SCRIPT_SHA" ]] \
  || fail "executed launcher bytes differ from the exact candidate"

ORIGIN_MAIN="$(git -C "$SOURCE_REPO" rev-parse --verify 'refs/remotes/origin/main^{commit}')" \
  || fail "origin/main is unavailable; fetch it explicitly before invoking this script"
if ((VALIDATE_ONLY == 0)) && [[ "$ORIGIN_MAIN" != "$EXPECTED_SHA" ]]; then
  fail "actual launch requires origin/main to equal --expected-sha exactly"
fi

SOURCE_PARENT="$(dirname -- "$SOURCE_REPO")"
[[ -n "$WORKTREE_PARENT" ]] || WORKTREE_PARENT="$SOURCE_PARENT/mma_gate1_worktrees"
[[ -n "$EVIDENCE_ROOT" ]] || EVIDENCE_ROOT="$SOURCE_PARENT/mma_gate1_evidence"
WORKTREE_PARENT="$(canonical_future_path "$WORKTREE_PARENT")"
EVIDENCE_ROOT="$(canonical_future_path "$EVIDENCE_ROOT")"
require_separate_paths "$WORKTREE_PARENT" "worktree parent" "$SOURCE_REPO" "source repository"
require_separate_paths "$EVIDENCE_ROOT" "evidence root" "$SOURCE_REPO" "source repository"
require_separate_paths "$WORKTREE_PARENT" "worktree parent" "$EVIDENCE_ROOT" "evidence root"
WORKTREE_PARENT="$(canonical_created_directory "$WORKTREE_PARENT")"
EVIDENCE_ROOT="$(canonical_created_directory "$EVIDENCE_ROOT")"
require_native_linux_filesystem "$WORKTREE_PARENT" "worktree parent"
require_native_linux_filesystem "$EVIDENCE_ROOT" "evidence root"
require_disk_headroom "$WORKTREE_PARENT" "worktree filesystem"
require_disk_headroom "$EVIDENCE_ROOT" "evidence filesystem"

UV_VERSION="$(uv --version)"
read -r UV_PROGRAM UV_VERSION_NUMBER _ <<< "$UV_VERSION"
[[ "$UV_PROGRAM" == "uv" && "$UV_VERSION_NUMBER" == "$REQUIRED_UV_VERSION" ]] \
  || fail "uv ${REQUIRED_UV_VERSION} is required, observed: $UV_VERSION"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
MODE_LABEL="launch"
((VALIDATE_ONLY == 0)) || MODE_LABEL="validate"
RUN_STEM="${STAMP}_${EXPECTED_SHA:0:12}_${MODE_LABEL}"
SOAK_DIR="$WORKTREE_PARENT/Multi_Market_Arbitrage_gate1_${RUN_STEM}"
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_STEM"
require_separate_paths "$SOAK_DIR" "detached worktree" "$EVIDENCE_DIR" "evidence directory"
[[ ! -e "$SOAK_DIR" ]] || fail "detached worktree already exists: $SOAK_DIR"
[[ ! -e "$EVIDENCE_DIR" ]] || fail "evidence directory already exists: $EVIDENCE_DIR"

umask 077
mkdir -- "$EVIDENCE_DIR"
git -C "$SOURCE_REPO" worktree add --detach "$SOAK_DIR" "$EXPECTED_SHA"
SOAK_DIR="$(canonical_existing_directory "$SOAK_DIR")"
require_native_linux_filesystem "$SOAK_DIR" "detached worktree"
[[ "$(git -C "$SOAK_DIR" rev-parse HEAD)" == "$EXPECTED_SHA" ]] \
  || fail "detached worktree is not at --expected-sha"
require_clean_repository "$SOAK_DIR"
[[ "$(git -C "$SOAK_DIR" rev-parse 'design-freeze-v0.1^{commit}')" == "$DESIGN_FREEZE_SHA" ]] \
  || fail "detached worktree Design Freeze identity differs"

TMPDIR="$EVIDENCE_DIR/tmp"
mkdir -- "$TMPDIR"
export TMPDIR
require_native_linux_filesystem "$TMPDIR" "temporary directory"
require_disk_headroom "$TMPDIR" "temporary filesystem"

cd -- "$SOAK_DIR"
for path in uv.lock config/phase1.toml; do
  [[ -f "$path" && ! -L "$path" ]] || fail "required candidate input is absent or symbolic: $path"
  expected_hash="$(sha256_git_path "$SOURCE_REPO" "$EXPECTED_SHA" "$path")"
  actual_hash="$(sha256_file "$path")"
  [[ "$actual_hash" == "$expected_hash" ]] \
    || fail "$path differs from the exact candidate"
  printf '%s  %s\n' "$actual_hash" "$path" >> "$EVIDENCE_DIR/input-files.sha256"
done
[[ -f "$RUN_SEAL_RELATIVE" && ! -L "$RUN_SEAL_RELATIVE" ]] \
  || fail "candidate run-seal helper is absent or symbolic"
RUN_SEAL_SHA="$(sha256_file "$RUN_SEAL_RELATIVE")"
[[ "$RUN_SEAL_SHA" == "$(sha256_git_path "$SOURCE_REPO" "$EXPECTED_SHA" "$RUN_SEAL_RELATIVE")" ]] \
  || fail "run-seal helper differs from the exact candidate"
printf '%s  %s\n' "$RUN_SEAL_SHA" "$RUN_SEAL_RELATIVE" \
  > "$EVIDENCE_DIR/run-seal-helper.sha256"

uv lock --check --offline
if ((VALIDATE_ONLY)); then
  uv sync --dry-run --offline --frozen --all-groups \
    > "$EVIDENCE_DIR/uv-sync-dry-run.stdout.log" \
    2> "$EVIDENCE_DIR/uv-sync-dry-run.stderr.log"
  PYTHON_EXECUTABLE="$(uv python find --offline --no-python-downloads "$REQUIRED_PYTHON_VERSION")"
  verify_python "$EVIDENCE_DIR/python-runtime.json" "$PYTHON_EXECUTABLE"
else
  require_command nohup
  require_command timeout
  require_command ps
  uv sync --frozen --all-groups
  verify_python "$EVIDENCE_DIR/python-runtime.json" \
    uv run --offline --frozen --no-sync --all-groups python
fi

require_clean_repository "$SOAK_DIR"
for path in uv.lock config/phase1.toml; do
  grep -Fqx "$(sha256_file "$path")  $path" "$EVIDENCE_DIR/input-files.sha256" \
    || fail "$path changed during environment validation"
done

printf '%s\n' "$EXPECTED_SHA" > "$EVIDENCE_DIR/git-head.txt"
printf '%s\n' "$ORIGIN_MAIN" > "$EVIDENCE_DIR/origin-main.txt"
printf '%s\n' "$DESIGN_FREEZE_SHA" > "$EVIDENCE_DIR/design-freeze-commit.txt"
printf '%s\n' "$TARGET_ID" > "$EVIDENCE_DIR/gamma-market-id.txt"
printf '%s\n' "$SOAK_DIR" > "$EVIDENCE_DIR/soak-worktree.txt"
printf '%s\n' "$TMPDIR" > "$EVIDENCE_DIR/tmpdir.txt"
printf '%s\n' "$UV_VERSION" > "$EVIDENCE_DIR/uv-version.txt"
printf '%s\n' "$MODE_LABEL" > "$EVIDENCE_DIR/launcher-mode.txt"
printf '%s\n' "$(filesystem_type "$SOAK_DIR")" > "$EVIDENCE_DIR/worktree-filesystem-type.txt"
printf '%s\n' "$(filesystem_type "$EVIDENCE_DIR")" > "$EVIDENCE_DIR/evidence-filesystem-type.txt"
git status --porcelain=v1 --untracked-files=all > "$EVIDENCE_DIR/git-status-before.txt"
uname -a > "$EVIDENCE_DIR/uname.txt"
hostname > "$EVIDENCE_DIR/hostname.txt"
df -B1 -- "$SOAK_DIR" "$EVIDENCE_DIR" > "$EVIDENCE_DIR/disk-before-bytes.txt"
date -u --iso-8601=seconds > "$EVIDENCE_DIR/validated-at-utc.txt"
sha256sum -- "$SCRIPT_PATH" > "$EVIDENCE_DIR/launcher-script.sha256"

if ((VALIDATE_ONLY)); then
  cat <<EOF
Gate-1 launcher validation completed successfully.
Expected SHA: $EXPECTED_SHA
Origin/main:  $ORIGIN_MAIN
Target ID:    $TARGET_ID
Worktree:     $SOAK_DIR
Evidence:     $EVIDENCE_DIR
NO PUBLIC MARKET-DATA REQUEST WAS PERFORMED.
NO PREFLIGHT WAS RUN.
NO SIX-HOUR SOAK WAS LAUNCHED.
EOF
  exit 0
fi

PRE_DIR="$EVIDENCE_DIR/preflight"
mkdir -- "$PRE_DIR"
date -u --iso-8601=seconds > "$PRE_DIR/started-at-utc.txt"

set +e
PYTHONHASHSEED=0 PYTHONUNBUFFERED=1 \
timeout --signal=TERM --kill-after=30s "${PREFLIGHT_PROCESS_TIMEOUT_SECONDS}s" \
uv run --offline --frozen --no-sync --all-groups mma observe polymarket \
  --gamma-market-id "$TARGET_ID" \
  --duration "$PREFLIGHT_DURATION_SECONDS" \
  --config config/phase1.toml \
  > "$PRE_DIR/summary.json" \
  2> "$PRE_DIR/run.log"
PRE_RC=$?
set -e

printf '%s\n' "$PRE_RC" > "$PRE_DIR/exit-code.txt"
date -u --iso-8601=seconds > "$PRE_DIR/ended-at-utc.txt"
if [[ -d data ]]; then
  mv -- data "$PRE_DIR/data"
fi

if ((PRE_RC != 0)); then
  printf 'Preflight failed with exit %s. No six-hour soak was launched.\n' "$PRE_RC" >&2
  printf 'Preserved evidence: %s\n' "$PRE_DIR" >&2
  exit "$PRE_RC"
fi
[[ -d "$PRE_DIR/data" ]] || fail "preflight exited zero but produced no data directory"
verify_preflight_summary \
  "$PRE_DIR/summary.json" \
  "$TARGET_ID" \
  "$PRE_DIR/verification.json"
uv run --offline --frozen --no-sync --all-groups mma validate-data "$PRE_DIR/data" \
  > "$PRE_DIR/data-validation.json" \
  2> "$PRE_DIR/data-validation.stderr.log" \
  || fail "preflight data validation failed; no six-hour soak was launched"
mkdir -- data

git -C "$SOURCE_REPO" fetch origin --tags --prune \
  > "$EVIDENCE_DIR/post-preflight-fetch.stdout.log" \
  2> "$EVIDENCE_DIR/post-preflight-fetch.stderr.log" \
  || fail "could not refresh origin after preflight; no six-hour soak was launched"
require_clean_repository "$SOURCE_REPO"
[[ "$(git -C "$SOURCE_REPO" rev-parse 'design-freeze-v0.1^{commit}')" == "$DESIGN_FREEZE_SHA" ]] \
  || fail "Design Freeze identity changed after preflight; no six-hour soak was launched"
POST_PREFLIGHT_ORIGIN_MAIN="$(
  git -C "$SOURCE_REPO" rev-parse --verify 'refs/remotes/origin/main^{commit}'
)"
printf '%s\n' "$POST_PREFLIGHT_ORIGIN_MAIN" \
  > "$EVIDENCE_DIR/origin-main-after-preflight.txt"
[[ "$POST_PREFLIGHT_ORIGIN_MAIN" == "$EXPECTED_SHA" ]] \
  || fail "origin/main changed after preflight; no six-hour soak was launched"
[[ "$(git rev-parse HEAD)" == "$EXPECTED_SHA" ]] \
  || fail "detached worktree identity changed after preflight"
require_clean_repository "$SOAK_DIR"

uv run --offline --frozen --no-sync --all-groups python -I "$RUN_SEAL_RELATIVE" \
  write-command \
  --source-worktree "$SOAK_DIR" \
  --expected-sha "$EXPECTED_SHA" \
  --target-id "$TARGET_ID" \
  --duration-seconds "$GATE1_DURATION_SECONDS" \
  --output "$EVIDENCE_DIR/soak-command.json"

RUNNER="$EVIDENCE_DIR/run-soak.sh"
cat > "$RUNNER" <<EOF
#!/usr/bin/env bash
set +e
cd -- $(printf '%q' "$SOAK_DIR") || exit 111
export TMPDIR=$(printf '%q' "$TMPDIR")

printf '%s\n' "\$\$" > $(printf '%q' "$EVIDENCE_DIR/soak-runner.pid")
date -u --iso-8601=seconds > $(printf '%q' "$EVIDENCE_DIR/soak-started-at-utc.txt")

PYTHONHASHSEED=0 PYTHONUNBUFFERED=1 \\
uv run --offline --frozen --no-sync --all-groups mma observe polymarket \\
  --gamma-market-id $(printf '%q' "$TARGET_ID") \\
  --duration $GATE1_DURATION_SECONDS \\
  --config config/phase1.toml \\
  > $(printf '%q' "$EVIDENCE_DIR/soak-summary.json") \\
  2> $(printf '%q' "$EVIDENCE_DIR/soak.log") &
observer_pid=\$!
printf '%s\n' "\$observer_pid" > $(printf '%q' "$EVIDENCE_DIR/observer.pid")
if [[ -r "/proc/\$observer_pid/cmdline" ]]; then
  tr '\0' ' ' < "/proc/\$observer_pid/cmdline" > $(printf '%q' "$EVIDENCE_DIR/observer-command-from-proc.txt")
fi
wait "\$observer_pid"
rc=\$?

printf '%s\n' "\$rc" > $(printf '%q' "$EVIDENCE_DIR/soak-exit-code.txt")
date -u --iso-8601=seconds > $(printf '%q' "$EVIDENCE_DIR/soak-ended-at-utc.txt")
df -B1 -- . > $(printf '%q' "$EVIDENCE_DIR/disk-after-bytes.txt")

uv run --offline --frozen --no-sync --all-groups python -I \
  $(printf '%q' "$RUN_SEAL_RELATIVE") \
  --source-worktree $(printf '%q' "$SOAK_DIR") \
  --evidence-dir $(printf '%q' "$EVIDENCE_DIR") \
  --expected-sha $(printf '%q' "$EXPECTED_SHA") \
  --target-id $(printf '%q' "$TARGET_ID") \
  --duration-seconds $GATE1_DURATION_SECONDS \
  --exit-code "\$rc" \
  --exit-code-file $(printf '%q' "$EVIDENCE_DIR/soak-exit-code.txt") \
  --summary $(printf '%q' "$EVIDENCE_DIR/soak-summary.json") \
  --config $(printf '%q' "$SOAK_DIR/config/phase1.toml") \
  --lock $(printf '%q' "$SOAK_DIR/uv.lock") \
  --command-file $(printf '%q' "$EVIDENCE_DIR/soak-command.json") \
  --raw-root $(printf '%q' "$SOAK_DIR/data/raw") \
  --output $(printf '%q' "$EVIDENCE_DIR/run-completion-seal.json") \
  > $(printf '%q' "$EVIDENCE_DIR/run-seal.stdout.log") \
  2> $(printf '%q' "$EVIDENCE_DIR/run-seal.stderr.log")
seal_rc=\$?
printf '%s\n' "\$seal_rc" > $(printf '%q' "$EVIDENCE_DIR/run-seal-exit-code.txt")

if ((rc != 0)); then
  exit "\$rc"
fi
if ((seal_rc != 0)); then
  exit 112
fi
exit 0
EOF
chmod 700 "$RUNNER"
sha256sum -- "$RUNNER" > "$EVIDENCE_DIR/soak-runner.sha256"

cat > "$EVIDENCE_DIR/soak-command.txt" <<EOF
PYTHONHASHSEED=0 PYTHONUNBUFFERED=1 uv run --offline --frozen --no-sync --all-groups \\
  mma observe polymarket --gamma-market-id $TARGET_ID \\
  --duration $GATE1_DURATION_SECONDS --config config/phase1.toml
EOF

date -u --iso-8601=seconds > "$EVIDENCE_DIR/soak-launched-at-utc.txt"
nohup bash "$RUNNER" > "$EVIDENCE_DIR/nohup.log" 2>&1 &
RUNNER_PID=$!
printf '%s\n' "$RUNNER_PID" > "$EVIDENCE_DIR/nohup-runner.pid"

sleep 3
kill -0 "$RUNNER_PID" 2>/dev/null \
  || fail "soak runner exited during startup; inspect $EVIDENCE_DIR/nohup.log"

cat <<EOF
Gate-1 soak runner started successfully.
Runner PID:   $RUNNER_PID
Worktree:     $SOAK_DIR
Evidence:     $EVIDENCE_DIR
Runtime:      $GATE1_DURATION_SECONDS seconds / 6 hours
Target ID:    $TARGET_ID
Expected SHA: $EXPECTED_SHA

The observer PID is recorded in $EVIDENCE_DIR/observer.pid.
Do not delete or move the worktree, data, logs, summaries or evidence.
EOF
