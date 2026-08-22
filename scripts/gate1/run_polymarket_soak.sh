#!/usr/bin/env bash
set -Eeuo pipefail

readonly ACCEPTED_SHA="a382b93220011428703fe02483f7034f59c1f045"
readonly DESIGN_FREEZE_SHA="e7d69aedc328ceda220120377307d5ebcc1a80d9"
readonly DEFAULT_TARGET_ID="3257332"
readonly GATE1_DURATION_SECONDS="21600"
readonly PREFLIGHT_DURATION_SECONDS="60"
readonly MINIMUM_FREE_BYTES="$((10 * 1024 * 1024 * 1024))"

usage() {
  cat <<'EOF'
Usage:
  scripts/gate1/run_polymarket_soak.sh [options]

Options:
  --target-id ID          Exact Gamma market ID (default: 3257332)
  --source-repo PATH      Source repository (default: inferred from this script)
  --worktree-parent PATH  Parent for detached soak worktree (default: $HOME)
  --evidence-root PATH    Evidence root (default: $HOME/mma_gate1_evidence)
  -h, --help              Show this help

The script:
  1. verifies the accepted implementation SHA and Design Freeze tag;
  2. creates a fresh detached worktree at the accepted SHA;
  3. installs the exact locked environment;
  4. runs a 60-second explicit-target preflight;
  5. starts the 21,600-second Gate-1 soak with nohup;
  6. records PID, command, environment identity, logs, summary, and exit code.
EOF
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

TARGET_ID="$DEFAULT_TARGET_ID"
SOURCE_REPO=""
WORKTREE_PARENT="$HOME"
EVIDENCE_ROOT="$HOME/mma_gate1_evidence"

while (($#)); do
  case "$1" in
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
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

[[ -n "$TARGET_ID" && "$TARGET_ID" != *[[:space:]]* ]] \
  || fail "target ID must be non-empty and contain no whitespace"

require_command git
require_command uv
require_command nohup
require_command sha256sum
require_command stat
require_command df
require_command date
require_command ps

if [[ -z "$SOURCE_REPO" ]]; then
  SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
  SOURCE_REPO="$(git -C "$SCRIPT_DIR/../.." rev-parse --show-toplevel)"
fi
SOURCE_REPO="$(cd -- "$SOURCE_REPO" && pwd -P)"
WORKTREE_PARENT="$(mkdir -p -- "$WORKTREE_PARENT" && cd -- "$WORKTREE_PARENT" && pwd -P)"
EVIDENCE_ROOT="$(mkdir -p -- "$EVIDENCE_ROOT" && cd -- "$EVIDENCE_ROOT" && pwd -P)"

cd -- "$SOURCE_REPO"
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] \
  || fail "source repository is not clean: $SOURCE_REPO"

git fetch origin --tags --prune

[[ "$(git rev-parse 'design-freeze-v0.1^{commit}')" == "$DESIGN_FREEZE_SHA" ]] \
  || fail "design-freeze-v0.1 does not resolve to the expected commit"

git cat-file -e "${ACCEPTED_SHA}^{commit}"
git merge-base --is-ancestor "$ACCEPTED_SHA" origin/main \
  || fail "accepted soak SHA is not an ancestor of origin/main"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SOAK_DIR="$WORKTREE_PARENT/Multi_Market_Arbitrage_gate1_${STAMP}"
EVIDENCE_DIR="$EVIDENCE_ROOT/$STAMP"

[[ ! -e "$SOAK_DIR" ]] || fail "soak worktree already exists: $SOAK_DIR"
[[ ! -e "$EVIDENCE_DIR" ]] || fail "evidence directory already exists: $EVIDENCE_DIR"

umask 077
mkdir -p -- "$EVIDENCE_DIR"
git worktree add --detach "$SOAK_DIR" "$ACCEPTED_SHA"

cd -- "$SOAK_DIR"
[[ "$(git rev-parse HEAD)" == "$ACCEPTED_SHA" ]] \
  || fail "detached worktree is not at the accepted soak SHA"
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] \
  || fail "detached soak worktree is not clean"

FS_TYPE="$(stat -f -c '%T' .)"
case "$FS_TYPE" in
  drvfs|9p|fuseblk|ntfs|msdos|vfat)
    fail "soak worktree must use a native Linux filesystem, observed: $FS_TYPE"
    ;;
esac

FREE_BYTES="$(df --output=avail -B1 . | tail -n 1 | tr -d '[:space:]')"
[[ "$FREE_BYTES" =~ ^[0-9]+$ ]] || fail "could not determine free disk bytes"
((FREE_BYTES >= MINIMUM_FREE_BYTES)) \
  || fail "less than 10 GiB free on soak filesystem"

uv python install
uv lock --check
uv sync --frozen --all-groups

uv --version | tee "$EVIDENCE_DIR/uv-version.txt"
uv run --frozen --all-groups python - <<'PY' \
  | tee "$EVIDENCE_DIR/python-version.txt"
import platform
import sys
import sysconfig

assert platform.python_implementation() == "CPython"
assert sys.version_info[:3] == (3, 14, 7), sys.version
assert sysconfig.get_config_var("Py_GIL_DISABLED") == 0
assert sys._is_gil_enabled()

print(platform.python_version())
print(sys.version)
print(f"executable={sys.executable}")
print("ordinary_gil=true")
PY

git rev-parse HEAD > "$EVIDENCE_DIR/git-head.txt"
git rev-parse 'design-freeze-v0.1^{commit}' > "$EVIDENCE_DIR/design-freeze-commit.txt"
git status --porcelain=v1 --untracked-files=all > "$EVIDENCE_DIR/git-status-before.txt"
sha256sum uv.lock config/phase1.toml > "$EVIDENCE_DIR/input-files.sha256"
uname -a > "$EVIDENCE_DIR/uname.txt"
printf '%s\n' "$FS_TYPE" > "$EVIDENCE_DIR/filesystem-type.txt"
df -h . > "$EVIDENCE_DIR/disk-before.txt"
df -B1 . > "$EVIDENCE_DIR/disk-before-bytes.txt"
date -u --iso-8601=seconds > "$EVIDENCE_DIR/preflight-started-at.txt"
printf '%s\n' "$TARGET_ID" > "$EVIDENCE_DIR/gamma-market-id.txt"
printf '%s\n' "$SOAK_DIR" > "$EVIDENCE_DIR/soak-worktree.txt"

PRE_DIR="$EVIDENCE_DIR/preflight"
mkdir -p -- "$PRE_DIR"

set +e
PYTHONHASHSEED=0 PYTHONUNBUFFERED=1 \
uv run --frozen --all-groups mma observe polymarket \
  --gamma-market-id "$TARGET_ID" \
  --duration "$PREFLIGHT_DURATION_SECONDS" \
  --config config/phase1.toml \
  > "$PRE_DIR/summary.json" \
  2> "$PRE_DIR/run.log"
PRE_RC=$?
set -e

printf '%s\n' "$PRE_RC" > "$PRE_DIR/exit-code.txt"
date -u --iso-8601=seconds > "$PRE_DIR/ended-at.txt"

if ((PRE_RC != 0)); then
  printf 'Preflight failed. Do not start the six-hour soak.\n' >&2
  printf 'Inspect: %s\n' "$PRE_DIR/run.log" >&2
  exit "$PRE_RC"
fi

[[ -d data ]] || fail "preflight succeeded but produced no data directory"
mv -- data "$PRE_DIR/data"
mkdir -p data

RUNNER="$EVIDENCE_DIR/run-soak.sh"
cat > "$RUNNER" <<EOF
#!/usr/bin/env bash
set +e
cd -- $(printf '%q' "$SOAK_DIR") || exit 111

date -u --iso-8601=seconds > $(printf '%q' "$EVIDENCE_DIR/soak-started-at.txt")

PYTHONHASHSEED=0 PYTHONUNBUFFERED=1 \\
uv run --frozen --all-groups mma observe polymarket \\
  --gamma-market-id $(printf '%q' "$TARGET_ID") \\
  --duration $GATE1_DURATION_SECONDS \\
  --config config/phase1.toml \\
  > $(printf '%q' "$EVIDENCE_DIR/soak-summary.json") \\
  2> $(printf '%q' "$EVIDENCE_DIR/soak.log")
rc=\$?

printf '%s\\n' "\$rc" > $(printf '%q' "$EVIDENCE_DIR/soak-exit-code.txt")
date -u --iso-8601=seconds > $(printf '%q' "$EVIDENCE_DIR/soak-ended-at.txt")
df -h . > $(printf '%q' "$EVIDENCE_DIR/disk-after.txt")
df -B1 . > $(printf '%q' "$EVIDENCE_DIR/disk-after-bytes.txt")
exit "\$rc"
EOF
chmod 700 "$RUNNER"
sha256sum "$RUNNER" > "$EVIDENCE_DIR/soak-runner.sha256"

cat > "$EVIDENCE_DIR/soak-command.txt" <<EOF
uv run --frozen --all-groups mma observe polymarket \\
  --gamma-market-id $TARGET_ID \\
  --duration $GATE1_DURATION_SECONDS \\
  --config config/phase1.toml
EOF

date -u --iso-8601=seconds > "$EVIDENCE_DIR/soak-launched-at.txt"
nohup bash "$RUNNER" > "$EVIDENCE_DIR/nohup.log" 2>&1 &
SOAK_PID=$!

printf '%s\n' "$SOAK_PID" | tee "$EVIDENCE_DIR/soak.pid"
sleep 3
kill -0 "$SOAK_PID" 2>/dev/null \
  || fail "soak process exited during startup; inspect $EVIDENCE_DIR/nohup.log"

cat <<EOF

Gate-1 soak started successfully.
PID:          $SOAK_PID
Worktree:     $SOAK_DIR
Evidence:     $EVIDENCE_DIR
Runtime:      $GATE1_DURATION_SECONDS seconds / 6 hours
Target ID:    $TARGET_ID
Filesystem:   $FS_TYPE

Monitor from another terminal:
  ps -p $SOAK_PID -o pid,etime,stat,%cpu,%mem,cmd
  tail -f $(printf '%q' "$EVIDENCE_DIR/soak.log")

Check completion:
  cat $(printf '%q' "$EVIDENCE_DIR/soak-exit-code.txt")
  cat $(printf '%q' "$EVIDENCE_DIR/soak-summary.json")

Do not delete or move the worktree, data directory, logs, summary, or health evidence.
EOF
