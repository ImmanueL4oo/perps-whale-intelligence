#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# Solana Perps Whale Tracker — auto-run wrapper
#
# Usage:
#   ./run.sh                        # run in foreground (default)
#   ./run.sh --threshold 100000     # any flags pass straight through to bot.py
#   ./run.sh --bg                   # run in background, write PID + log
#   ./run.sh stop                   # stop the background bot
#   ./run.sh status                 # show whether the background bot is alive
#   ./run.sh tail                   # tail -f the background log
#   ./run.sh logs                   # cat the full log file
#
# Behaviour:
#   * cd's into the repo dir regardless of where you call it from
#   * activates ./.venv (creates it on first run if missing)
#   * installs/upgrades requirements.txt if it's newer than the venv
#   * single-instance guard: refuses to start a second foreground or background
#     bot — the dashboard port and Mongo collection should only be touched by
#     one process at a time
#   * auto-restart on crash with exponential backoff (capped at 30s); a clean
#     Ctrl+C exits without restarting
#   * rotates logs/bot.log when it crosses 50 MB
# ----------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

VENV_DIR="$REPO_DIR/.venv"
PID_FILE="$REPO_DIR/.run.pid"
LOG_DIR="$REPO_DIR/logs"
LOG_FILE="$LOG_DIR/bot.log"
MAX_LOG_BYTES=$((50 * 1024 * 1024))

mkdir -p "$LOG_DIR"

# ---- helpers ---------------------------------------------------------------
die()   { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }
info()  { printf '\033[36m%s\033[0m %s\n' '›' "$*"; }
ok()    { printf '\033[32m✓\033[0m %s\n' "$*"; }

# Returns 0 if the recorded background PID is alive.
bg_is_alive() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid=$(cat "$PID_FILE" 2>/dev/null || true)
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

# Refuse to start a second instance — independent of how it was launched.
guard_single_instance() {
  if bg_is_alive; then
    die "another bot is already running (pid $(cat "$PID_FILE")). './run.sh stop' first."
  fi
  # Catches the case where someone started a foreground bot from another shell.
  if pgrep -f 'python.*\bbot\.py' >/dev/null 2>&1; then
    die "a python bot.py process is already running. kill it first."
  fi
}

rotate_log_if_huge() {
  [[ -f "$LOG_FILE" ]] || return 0
  local size
  size=$(wc -c <"$LOG_FILE" | tr -d ' ')
  if (( size > MAX_LOG_BYTES )); then
    mv "$LOG_FILE" "$LOG_FILE.$(date +%Y%m%d-%H%M%S)"
    info "rotated log (was $((size / 1024 / 1024)) MB)"
  fi
}

ensure_venv() {
  if [[ ! -d "$VENV_DIR" ]]; then
    info "creating virtualenv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi
  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"

  # Install (or refresh) deps if requirements.txt was touched after the venv.
  local req="$REPO_DIR/requirements.txt"
  local stamp="$VENV_DIR/.requirements.stamp"
  if [[ -f "$req" && ( ! -f "$stamp" || "$req" -nt "$stamp" ) ]]; then
    info "installing/refreshing requirements"
    pip install --quiet --upgrade pip
    pip install --quiet -r "$req"
    touch "$stamp"
    ok "deps up to date"
  fi
}

ensure_env_file() {
  if [[ ! -f "$REPO_DIR/.env" && -f "$REPO_DIR/.env.example" ]]; then
    info ".env not found — copying from .env.example (edit it for telegram/mongo creds)"
    cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
  fi
}

# Run bot.py in a supervisor loop. Exit code 130 = SIGINT (clean Ctrl+C) →
# don't restart. Anything else → exponential backoff and try again.
supervise() {
  local backoff=2
  while :; do
    rotate_log_if_huge
    if (( $# > 0 )); then
      info "starting bot.py $*"
    else
      info "starting bot.py"
    fi
    set +e
    python -u bot.py "$@"
    local rc=$?
    set -e
    if (( rc == 130 || rc == 0 )); then
      ok "bot exited cleanly (code $rc)"
      return 0
    fi
    info "bot crashed (code $rc) — restarting in ${backoff}s"
    sleep "$backoff"
    backoff=$(( backoff * 2 ))
    (( backoff > 30 )) && backoff=30
  done
}

# ---- subcommands -----------------------------------------------------------
cmd_status() {
  if bg_is_alive; then
    local pid
    pid=$(cat "$PID_FILE")
    ok "background bot is running (pid $pid)"
    ps -p "$pid" -o pid,etime,command 2>/dev/null || true
  else
    info "no background bot running"
    if pgrep -f 'python.*\bbot\.py' >/dev/null 2>&1; then
      info "but a foreground bot.py is alive:"
      pgrep -fl 'python.*\bbot\.py' || true
    fi
  fi
}

cmd_stop() {
  if ! bg_is_alive; then
    info "no background bot to stop"
    rm -f "$PID_FILE"
    return 0
  fi
  local wrapper_pid
  wrapper_pid=$(cat "$PID_FILE")
  info "sending shutdown signal to bot (pid $wrapper_pid)"
  # The wrapper has a trap that forwards a single SIGINT to its python child
  # and then exits without restarting — so the bot drains its WebSockets
  # through bot.py's normal Ctrl+C handler.
  kill -TERM "$wrapper_pid" 2>/dev/null || true
  for _ in 1 2 3 4 5 6 7; do
    sleep 1
    kill -0 "$wrapper_pid" 2>/dev/null || { ok "stopped"; rm -f "$PID_FILE"; return 0; }
  done
  info "still alive after 7s — force-killing"
  local py_pids
  py_pids=$(pgrep -P "$wrapper_pid" 2>/dev/null || true)
  [[ -n "$py_pids" ]] && \
    while IFS= read -r pid; do kill -KILL "$pid" 2>/dev/null || true; done <<<"$py_pids"
  kill -KILL "$wrapper_pid" 2>/dev/null || true
  rm -f "$PID_FILE"
  ok "killed"
}

cmd_tail() {
  [[ -f "$LOG_FILE" ]] || die "no log file at $LOG_FILE — start the bot in --bg mode first"
  exec tail -F "$LOG_FILE"
}

cmd_logs() {
  [[ -f "$LOG_FILE" ]] || die "no log file at $LOG_FILE"
  exec less +G "$LOG_FILE"
}

# ---- argument routing ------------------------------------------------------
case "${1:-run}" in
  status) cmd_status; exit 0 ;;
  stop)   cmd_stop;   exit 0 ;;
  tail)   cmd_tail ;;
  logs)   cmd_logs ;;
esac

# Background mode: --bg can appear anywhere in the args.
BG=0
PASSTHROUGH=()
for arg in "$@"; do
  if [[ "$arg" == "--bg" || "$arg" == "-b" ]]; then
    BG=1
  elif [[ "$arg" == "run" ]]; then
    : # 'run' is just an explicit verb — drop it
  else
    PASSTHROUGH+=("$arg")
  fi
done

guard_single_instance
ensure_env_file
ensure_venv

if (( BG == 1 )); then
  rotate_log_if_huge
  info "starting in background — log: $LOG_FILE"
  # Build the python command outside the inner shell so we don't inject a
  # literal `""` (empty arg) when there are no passthrough flags. Each arg is
  # printf %q-escaped so quoting / spaces / globs survive. The
  # ${arr[@]+"${arr[@]}"} dance is bash-3.2-safe (macOS default bash) when
  # the array is empty under `set -u`.
  PY_ARGS=""
  if (( ${#PASSTHROUGH[@]} > 0 )); then
    for a in "${PASSTHROUGH[@]}"; do
      PY_ARGS+=" $(printf '%q' "$a")"
    done
  fi
  # nohup + setsid so the bot survives terminal close.
  nohup bash -c '
    cd "'"$REPO_DIR"'"
    source "'"$VENV_DIR"'/bin/activate"
    child=""
    stopping=0
    forward() {
      stopping=1
      [[ -n "$child" ]] && kill -INT "$child" 2>/dev/null || true
    }
    trap forward INT TERM
    backoff=2
    while :; do
      python -u bot.py'"$PY_ARGS"' >>"'"$LOG_FILE"'" 2>&1 &
      child=$!
      wait "$child" 2>/dev/null
      rc=$?
      child=""
      if (( stopping == 1 || rc == 130 || rc == 0 )); then
        echo "[runner] bot exited (code $rc) — not restarting" >>"'"$LOG_FILE"'"
        exit 0
      fi
      echo "[runner] bot crashed (code $rc) — restarting in ${backoff}s" >>"'"$LOG_FILE"'"
      sleep "$backoff"
      backoff=$(( backoff * 2 ))
      (( backoff > 30 )) && backoff=30
    done
  ' >/dev/null 2>&1 &
  bg_pid=$!
  echo "$bg_pid" >"$PID_FILE"
  sleep 1
  if bg_is_alive; then
    ok "running in background (pid $bg_pid)"
    info "tail logs:   ./run.sh tail"
    info "stop:        ./run.sh stop"
    info "dashboard:   http://localhost:8787"
  else
    rm -f "$PID_FILE"
    die "background bot failed to start — check $LOG_FILE"
  fi
  exit 0
fi

# Foreground mode (default): supervised, but Ctrl+C exits cleanly.
trap 'echo; info "received Ctrl+C — exiting"; exit 130' INT
# IMPORTANT: ${arr[@]+"${arr[@]}"} is bash-3.2 safe when the array is empty
# under `set -u`. We can't just use "${arr[@]:-}" because `:-` would expand
# to a single empty string, which argparse rejects as an unrecognized arg.
supervise ${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}
