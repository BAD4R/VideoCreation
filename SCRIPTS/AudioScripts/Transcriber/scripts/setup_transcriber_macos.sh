#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  setup_transcriber_macos.sh
    [--project-dir DIR]
    [--venv-dir DIR]
    [--python-bin PATH]
    [--device cpu]
    [--no-brew-install]

Notes:
  - This script installs missing dependencies for transcriber on macOS.
  - Dependencies are installed into a local virtualenv (default: .venv).
EOF
}

log() {
  echo "[setup] $*"
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

PROJECT_DIR=""
VENV_DIR=".venv"
PYTHON_BIN=""
DEVICE="cpu"
NO_BREW_INSTALL="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir) PROJECT_DIR="${2:-}"; shift 2 ;;
    --venv-dir) VENV_DIR="${2:-}"; shift 2 ;;
    --python-bin) PYTHON_BIN="${2:-}"; shift 2 ;;
    --device) DEVICE="${2:-}"; shift 2 ;;
    --no-brew-install) NO_BREW_INSTALL="1"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ "$DEVICE" != "cpu" ]]; then
  echo "--device supports only 'cpu' on macOS in this installer." >&2
  exit 2
fi

if [[ -z "$PROJECT_DIR" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
else
  PROJECT_DIR="$(cd "$PROJECT_DIR" && pwd)"
fi

if [[ "$VENV_DIR" = /* ]]; then
  VENV_PATH="$VENV_DIR"
else
  VENV_PATH="$PROJECT_DIR/$VENV_DIR"
fi

REQUIREMENTS_PATH="$PROJECT_DIR/requirements.txt"
if [[ ! -f "$REQUIREMENTS_PATH" ]]; then
  echo "requirements.txt not found: $REQUIREMENTS_PATH" >&2
  exit 1
fi

ensure_brew_env() {
  if [[ -x /opt/homebrew/bin/brew ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
  elif [[ -x /usr/local/bin/brew ]]; then
    eval "$(/usr/local/bin/brew shellenv)"
  fi
}

ensure_brew() {
  if have_cmd brew; then
    return
  fi
  if [[ "$NO_BREW_INSTALL" == "1" ]]; then
    echo "Homebrew is required but not found (--no-brew-install was set)." >&2
    exit 1
  fi
  log "Installing Homebrew..."
  NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  ensure_brew_env
  if ! have_cmd brew; then
    echo "Homebrew install finished but brew is still unavailable in PATH." >&2
    exit 1
  fi
}

ensure_formula() {
  local formula="$1"
  if brew list --formula "$formula" >/dev/null 2>&1; then
    log "$formula already installed."
  else
    log "Installing $formula..."
    brew install "$formula"
  fi
}

resolve_bootstrap_python() {
  if [[ -n "$PYTHON_BIN" ]]; then
    if [[ ! -x "$PYTHON_BIN" ]]; then
      echo "Provided --python-bin is not executable: $PYTHON_BIN" >&2
      exit 1
    fi
    echo "$PYTHON_BIN"
    return
  fi

  if [[ -x "$VENV_PATH/bin/python3" ]]; then
    echo "$VENV_PATH/bin/python3"
    return
  fi

  if have_cmd python3; then
    echo "$(command -v python3)"
    return
  fi

  ensure_formula python@3.11
  local py311
  py311="$(brew --prefix python@3.11)/bin/python3.11"
  if [[ ! -x "$py311" ]]; then
    echo "python@3.11 was installed but executable not found at $py311" >&2
    exit 1
  fi
  echo "$py311"
}

ensure_python_version() {
  local py="$1"
  if "$py" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)'; then
    return
  fi
  ensure_formula python@3.11
}

log "ProjectDir=$PROJECT_DIR"
log "VenvDir=$VENV_PATH"
log "Device=$DEVICE"

ensure_brew_env
ensure_brew
ensure_brew_env
ensure_formula ffmpeg

BOOTSTRAP_PY="$(resolve_bootstrap_python)"
ensure_python_version "$BOOTSTRAP_PY"

if [[ ! -x "$VENV_PATH/bin/python3" ]]; then
  log "Creating virtual environment..."
  "$BOOTSTRAP_PY" -m venv "$VENV_PATH"
else
  log "Virtual environment already exists."
fi

VENV_PY="$VENV_PATH/bin/python3"
if [[ ! -x "$VENV_PY" ]]; then
  echo "venv python not found: $VENV_PY" >&2
  exit 1
fi

log "Upgrading pip/setuptools/wheel..."
"$VENV_PY" -m pip install --upgrade pip setuptools wheel

log "Installing Python requirements..."
"$VENV_PY" -m pip install --upgrade -r "$REQUIREMENTS_PATH"

log "Verifying runtime..."
"$VENV_PY" -c "import whisperx, rapidfuzz, unidecode, torch; print('torch=' + torch.__version__)"
ffmpeg -version >/dev/null
ffprobe -version >/dev/null

echo
echo "READY"
echo "python: $VENV_PY"
echo "example:"
echo "  \"$VENV_PY\" \"$PROJECT_DIR/transcribe.py\" --help"

