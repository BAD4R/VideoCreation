#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  invoke_remote_transcriber.sh
    --host HOST
    --user USER
    --main-folder-path PATH
    --channel-name NAME
    --video-folder-name NAME
    [--project-dir WIN_PATH]
    [--jobs-root WIN_PATH]
    [--device DEVICE]
    [--language LANG]
    [--mode MODE]
    [--text-parts-path PATH]
    [--text-file-name NAME]
    [--chunk-limit N]
    [--min-index-chars N]
    [--min-index-tokens N]
    [--asr-workers N]
    [--min-free-vram-gb N]
    [--asr-prompt 0|1]
    [--asr-prompt-max-chars N]
    [--job-id ID]
    [--poll-seconds N]
    [--out-dir DIR]
    [--identity-file PATH]
    [--python-exe WIN_PATH]
    [--sync-source auto|local|remote]
    [--local-video-dir DIR]
    [--extra-arg ARG]...

Modes:
  sync-source=local  : main-folder-path is local mac path, source video is uploaded to Windows.
  sync-source=remote : main-folder-path is Windows path, no source upload.
  sync-source=auto   : local when main-folder-path starts with '/', otherwise remote.

Output on Mac:
  out-dir/<video>_transcript.json
  out-dir/<video>_transcriptProgress.json
  out-dir/<video>_status.json
  out-dir/<video>_run.log
  out-dir/<video>_remote-console.log
EOF
}

required() {
  local name="$1"
  local value="$2"
  if [[ -z "${value}" ]]; then
    echo "Missing required argument: ${name}" >&2
    usage
    exit 2
  fi
}

ps_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

win_to_scp_path() {
  local p="$1"
  p="${p//\\//}"
  if [[ "${p}" =~ ^[A-Za-z]:/ ]]; then
    p="/${p}"
  fi
  printf "%s" "${p}"
}

HOST=""
USER_NAME=""
MAIN_FOLDER_PATH=""
CHANNEL_NAME=""
VIDEO_FOLDER_NAME=""
PROJECT_DIR='C:\Users\V\Desktop\Channels\SCRIPTS\AudioScripts\Transcriber'
JOBS_ROOT=""
DEVICE="cpu"
LANGUAGE=""
MODE=""
TEXT_PARTS_PATH=""
TEXT_FILE_NAME=""
CHUNK_LIMIT=""
MIN_INDEX_CHARS="18"
MIN_INDEX_TOKENS="0"
ASR_WORKERS="1"
MIN_FREE_VRAM_GB="0.0"
ASR_PROMPT=""
ASR_PROMPT_MAX_CHARS="800"
JOB_ID=""
POLL_SECONDS="3"
OUT_DIR="./remote_results"
OUT_DIR_SET="0"
IDENTITY_FILE=""
PYTHON_EXE=""
SYNC_SOURCE="auto"
LOCAL_VIDEO_DIR=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="${2:-}"; shift 2 ;;
    --user) USER_NAME="${2:-}"; shift 2 ;;
    --main-folder-path) MAIN_FOLDER_PATH="${2:-}"; shift 2 ;;
    --channel-name) CHANNEL_NAME="${2:-}"; shift 2 ;;
    --video-folder-name) VIDEO_FOLDER_NAME="${2:-}"; shift 2 ;;
    --project-dir) PROJECT_DIR="${2:-}"; shift 2 ;;
    --jobs-root) JOBS_ROOT="${2:-}"; shift 2 ;;
    --device) DEVICE="${2:-}"; shift 2 ;;
    --language) LANGUAGE="${2:-}"; shift 2 ;;
    --mode) MODE="${2:-}"; shift 2 ;;
    --text-parts-path) TEXT_PARTS_PATH="${2:-}"; shift 2 ;;
    --text-file-name) TEXT_FILE_NAME="${2:-}"; shift 2 ;;
    --chunk-limit) CHUNK_LIMIT="${2:-}"; shift 2 ;;
    --min-index-chars) MIN_INDEX_CHARS="${2:-}"; shift 2 ;;
    --min-index-tokens) MIN_INDEX_TOKENS="${2:-}"; shift 2 ;;
    --asr-workers) ASR_WORKERS="${2:-}"; shift 2 ;;
    --min-free-vram-gb) MIN_FREE_VRAM_GB="${2:-}"; shift 2 ;;
    --asr-prompt) ASR_PROMPT="${2:-}"; shift 2 ;;
    --asr-prompt-max-chars) ASR_PROMPT_MAX_CHARS="${2:-}"; shift 2 ;;
    --job-id) JOB_ID="${2:-}"; shift 2 ;;
    --poll-seconds) POLL_SECONDS="${2:-}"; shift 2 ;;
    --out-dir) OUT_DIR="${2:-}"; OUT_DIR_SET="1"; shift 2 ;;
    --identity-file) IDENTITY_FILE="${2:-}"; shift 2 ;;
    --python-exe) PYTHON_EXE="${2:-}"; shift 2 ;;
    --sync-source) SYNC_SOURCE="${2:-}"; shift 2 ;;
    --local-video-dir) LOCAL_VIDEO_DIR="${2:-}"; shift 2 ;;
    --extra-arg) EXTRA_ARGS+=("${2:-}"); shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

required "--host" "${HOST}"
required "--user" "${USER_NAME}"
required "--main-folder-path" "${MAIN_FOLDER_PATH}"
required "--channel-name" "${CHANNEL_NAME}"
required "--video-folder-name" "${VIDEO_FOLDER_NAME}"

if [[ -z "${JOB_ID}" ]]; then
  JOB_ID="$(date -u +"%Y%m%d-%H%M%S")-$RANDOM"
fi

if [[ "${SYNC_SOURCE}" == "auto" ]]; then
  if [[ "${MAIN_FOLDER_PATH}" == /* ]]; then
    SYNC_SOURCE="local"
  else
    SYNC_SOURCE="remote"
  fi
fi
if [[ "${SYNC_SOURCE}" != "local" && "${SYNC_SOURCE}" != "remote" ]]; then
  echo "Invalid --sync-source value: ${SYNC_SOURCE}" >&2
  exit 2
fi

SSH_TARGET="${USER_NAME}@${HOST}"
SSH_OPTS=()
if [[ -n "${IDENTITY_FILE}" ]]; then
  SSH_OPTS+=(-i "${IDENTITY_FILE}")
fi

if [[ "${PROJECT_DIR}" == /* ]]; then
  echo "--project-dir must be a Windows path on remote host, got mac path: ${PROJECT_DIR}" >&2
  exit 2
fi
if [[ -n "${JOBS_ROOT}" && "${JOBS_ROOT}" == /* ]]; then
  echo "--jobs-root must be a Windows path on remote host, got mac path: ${JOBS_ROOT}" >&2
  exit 2
fi

REMOTE_SCRIPT_PATH="${PROJECT_DIR}\\scripts\\run_transcriber_job.ps1"
REMOTE_JOBS_ROOT="${JOBS_ROOT:-${PROJECT_DIR}\\remote_jobs}"
REMOTE_JOB_DIR="${REMOTE_JOBS_ROOT}\\${JOB_ID}"
REMOTE_STATUS="${REMOTE_JOB_DIR}\\status.json"
REMOTE_RUNLOG="${REMOTE_JOB_DIR}\\run.log"

REMOTE_MAIN_FOLDER="${MAIN_FOLDER_PATH}"
REMOTE_TEXT_PARTS_PATH="${TEXT_PARTS_PATH}"
REMOTE_OUT_FILE_PATH=""

if [[ "${SYNC_SOURCE}" == "local" ]]; then
  if [[ -z "${LOCAL_VIDEO_DIR}" ]]; then
    c1="${MAIN_FOLDER_PATH}/${CHANNEL_NAME}/VIDEOS/${VIDEO_FOLDER_NAME}"
    c2="${MAIN_FOLDER_PATH}/${CHANNEL_NAME}/${VIDEO_FOLDER_NAME}"
    if [[ -d "${c1}" ]]; then
      LOCAL_VIDEO_DIR="${c1}"
    elif [[ -d "${c2}" ]]; then
      LOCAL_VIDEO_DIR="${c2}"
    else
      echo "Local video dir not found. Checked:" >&2
      echo "  ${c1}" >&2
      echo "  ${c2}" >&2
      echo "Pass --local-video-dir explicitly." >&2
      exit 1
    fi
  fi

  REMOTE_MAIN_FOLDER="${REMOTE_JOBS_ROOT}\\${JOB_ID}\\source_main"
  REMOTE_SOURCE_PARENT="${REMOTE_MAIN_FOLDER}\\${CHANNEL_NAME}\\VIDEOS"
  REMOTE_SOURCE_PARENT_SCP="$(win_to_scp_path "${REMOTE_SOURCE_PARENT}")"

  echo "[local] uploading source video dir to Windows..."
  ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" "powershell -NoProfile -ExecutionPolicy Bypass -Command \"New-Item -ItemType Directory -Path '$(ps_escape "${REMOTE_SOURCE_PARENT}")' -Force | Out-Null\""
  scp -r "${SSH_OPTS[@]}" "${LOCAL_VIDEO_DIR}" "${SSH_TARGET}:${REMOTE_SOURCE_PARENT_SCP}"

  if [[ -n "${TEXT_PARTS_PATH}" && -f "${TEXT_PARTS_PATH}" ]]; then
    REMOTE_TEXT_PARTS_PATH="${REMOTE_JOBS_ROOT}\\${JOB_ID}\\input\\$(basename "${TEXT_PARTS_PATH}")"
    REMOTE_TEXT_PARTS_SCP="$(win_to_scp_path "${REMOTE_TEXT_PARTS_PATH}")"
    ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" "powershell -NoProfile -ExecutionPolicy Bypass -Command \"New-Item -ItemType Directory -Path '$(ps_escape "${REMOTE_JOBS_ROOT}\\${JOB_ID}\\input")' -Force | Out-Null\""
    scp "${SSH_OPTS[@]}" "${TEXT_PARTS_PATH}" "${SSH_TARGET}:${REMOTE_TEXT_PARTS_SCP}"
  fi

  REMOTE_OUT_FILE_PATH="${REMOTE_MAIN_FOLDER}\\${CHANNEL_NAME}\\VIDEOS\\${VIDEO_FOLDER_NAME}\\VOICE\\transcript\\${VIDEO_FOLDER_NAME}_transcript.json"
else
  REMOTE_OUT_FILE_PATH="${REMOTE_JOB_DIR}\\transcript.json"
fi

if [[ "${OUT_DIR_SET}" == "0" && "${SYNC_SOURCE}" == "local" ]]; then
  OUT_DIR="${LOCAL_VIDEO_DIR}/VOICE/transcript"
fi
mkdir -p "${OUT_DIR}"

LOCAL_TRANSCRIPT_PATH="${OUT_DIR}/${VIDEO_FOLDER_NAME}_transcript.json"
LOCAL_PROGRESS_PATH="${OUT_DIR}/${VIDEO_FOLDER_NAME}_transcriptProgress.json"
LOCAL_STATUS_PATH="${OUT_DIR}/${VIDEO_FOLDER_NAME}_status.json"
LOCAL_RUNLOG_PATH="${OUT_DIR}/${VIDEO_FOLDER_NAME}_run.log"
LOCAL_REMOTE_CONSOLE_PATH="${OUT_DIR}/${VIDEO_FOLDER_NAME}_remote-console.log"

REMOTE_PROGRESS="${REMOTE_OUT_FILE_PATH%.*}Progress.${REMOTE_OUT_FILE_PATH##*.}"
REMOTE_TRANSCRIPT="${REMOTE_OUT_FILE_PATH}"

REMOTE_TRANSCRIPT_SCP="$(win_to_scp_path "${REMOTE_TRANSCRIPT}")"
REMOTE_PROGRESS_SCP="$(win_to_scp_path "${REMOTE_PROGRESS}")"
REMOTE_STATUS_SCP="$(win_to_scp_path "${REMOTE_STATUS}")"
REMOTE_RUNLOG_SCP="$(win_to_scp_path "${REMOTE_RUNLOG}")"

PS_EXTRA_ARGS='@()'
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  PS_EXTRA_ARGS='@('
  for arg in "${EXTRA_ARGS[@]}"; do
    esc="$(ps_escape "$arg")"
    PS_EXTRA_ARGS+="'${esc}',"
  done
  PS_EXTRA_ARGS="${PS_EXTRA_ARGS%,})"
fi

PS_CMD="\$ErrorActionPreference = 'Stop'; "
PS_CMD+="& '$(ps_escape "${REMOTE_SCRIPT_PATH}")' "
PS_CMD+="-MainFolderPath '$(ps_escape "${REMOTE_MAIN_FOLDER}")' "
PS_CMD+="-ChannelName '$(ps_escape "${CHANNEL_NAME}")' "
PS_CMD+="-VideoFolderName '$(ps_escape "${VIDEO_FOLDER_NAME}")' "
PS_CMD+="-Device '$(ps_escape "${DEVICE}")' "
PS_CMD+="-Language '$(ps_escape "${LANGUAGE}")' "
PS_CMD+="-Mode '$(ps_escape "${MODE}")' "
PS_CMD+="-TextPartsPath '$(ps_escape "${REMOTE_TEXT_PARTS_PATH}")' "
PS_CMD+="-TextFileName '$(ps_escape "${TEXT_FILE_NAME}")' "
PS_CMD+="-MinIndexChars ${MIN_INDEX_CHARS} "
PS_CMD+="-MinIndexTokens ${MIN_INDEX_TOKENS} "
PS_CMD+="-AsrWorkers ${ASR_WORKERS} "
PS_CMD+="-MinFreeVramGb ${MIN_FREE_VRAM_GB} "
PS_CMD+="-AsrPrompt '$(ps_escape "${ASR_PROMPT}")' "
PS_CMD+="-AsrPromptMaxChars ${ASR_PROMPT_MAX_CHARS} "
PS_CMD+="-JobId '$(ps_escape "${JOB_ID}")' "
PS_CMD+="-ProjectDir '$(ps_escape "${PROJECT_DIR}")' "
PS_CMD+="-OutFilePath '$(ps_escape "${REMOTE_OUT_FILE_PATH}")' "
PS_CMD+="-ExtraArgs ${PS_EXTRA_ARGS} "

if [[ -n "${JOBS_ROOT}" ]]; then
  PS_CMD+="-JobsRoot '$(ps_escape "${JOBS_ROOT}")' "
fi
if [[ -n "${PYTHON_EXE}" ]]; then
  PS_CMD+="-PythonExe '$(ps_escape "${PYTHON_EXE}")' "
fi
if [[ -n "${CHUNK_LIMIT}" ]]; then
  PS_CMD+="-ChunkLimit ${CHUNK_LIMIT} "
fi

echo "[local] job_id=${JOB_ID}"
echo "[local] mode=${SYNC_SOURCE}"
echo "[local] out_dir=${OUT_DIR}"
echo "[local] local transcript path=${LOCAL_TRANSCRIPT_PATH}"
echo "[local] local progress path=${LOCAL_PROGRESS_PATH}"

ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" "powershell -NoProfile -ExecutionPolicy Bypass -Command \"$PS_CMD\"" \
  > "${LOCAL_REMOTE_CONSOLE_PATH}" 2>&1 &
SSH_PID=$!

sync_during_run() {
  scp "${SSH_OPTS[@]}" "${SSH_TARGET}:${REMOTE_PROGRESS_SCP}" "${LOCAL_PROGRESS_PATH}" >/dev/null 2>&1 || true
  scp "${SSH_OPTS[@]}" "${SSH_TARGET}:${REMOTE_STATUS_SCP}" "${LOCAL_STATUS_PATH}" >/dev/null 2>&1 || true
  scp "${SSH_OPTS[@]}" "${SSH_TARGET}:${REMOTE_RUNLOG_SCP}" "${LOCAL_RUNLOG_PATH}" >/dev/null 2>&1 || true
}

sync_transcript_final() {
  local tries=0
  while [[ ${tries} -lt 12 ]]; do
    if scp "${SSH_OPTS[@]}" "${SSH_TARGET}:${REMOTE_TRANSCRIPT_SCP}" "${LOCAL_TRANSCRIPT_PATH}" >/dev/null 2>&1; then
      return 0
    fi
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

while kill -0 "${SSH_PID}" >/dev/null 2>&1; do
  sync_during_run
  sleep "${POLL_SECONDS}"
done

wait "${SSH_PID}" || true
sync_during_run
if ! sync_transcript_final; then
  echo "[warn] transcript.json was not fetched after remote completion." >&2
fi

echo "[local] remote console: ${LOCAL_REMOTE_CONSOLE_PATH}"
echo "[local] job status: ${LOCAL_STATUS_PATH}"
echo "[local] transcript: ${LOCAL_TRANSCRIPT_PATH}"
echo "[local] progress: ${LOCAL_PROGRESS_PATH}"

if [[ ! -f "${LOCAL_STATUS_PATH}" ]]; then
  echo "Remote run finished, but status.json was not fetched." >&2
  exit 1
fi

if command -v jq >/dev/null 2>&1; then
  OK_VALUE="$(jq -r '.ok // false' "${LOCAL_STATUS_PATH}" 2>/dev/null || echo "false")"
else
  OK_VALUE="$(grep -E '"ok"\s*:\s*(true|false)' "${LOCAL_STATUS_PATH}" | head -n 1 | sed -E 's/.*:\s*(true|false).*/\1/' || echo "false")"
fi

if [[ "${OK_VALUE}" != "true" ]]; then
  echo "Remote transcriber job failed. See ${LOCAL_REMOTE_CONSOLE_PATH} and ${LOCAL_RUNLOG_PATH}" >&2
  exit 1
fi

echo "Remote transcriber job completed successfully."
