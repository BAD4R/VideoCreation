#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental transcription + alignment with strictly monotonic timestamps.
Resumable. Silent third‑party logs. Strong per‑file boundary checks.

CLI (same as before):
  --mainFolderPath
  --channelName
  --videoFolderName
  --device
  --language
Optional:
  --outFilePath    (explicit JSON path; default -> .../VOICE/transcript/<video>_transcript.json)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
import contextlib
import io
import logging
import threading
import warnings
import gc
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import urllib.request
import urllib.parse

from rapidfuzz import fuzz
from unidecode import unidecode

try:
    import torch  # type: ignore
except Exception:
    torch = None
try:
    import whisperx  # type: ignore
except Exception:
    whisperx = None

AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac"}
SENT_SPLIT_RE = re.compile(r"(?<=[.!?…])(?:\s+|(?=[«\"'“”A-Za-zА-ЯЁ]))")
MIN_SENT_CHARS = 50  # минимальная длина предложения для вывода
TEXT_PART_RETRY_THRESHOLD = 5  # ??????????? ???????? ??? ???????????? ???????????
MIN_MATCH_MS = 200  # защита от нулевых/почти нулевых совпадений (200 мс)
RECOVER_LOOKAHEAD_SENTENCES = 8  # fallback: try to resume alignment a few sentences ahead
MAX_EXTRA_TOKENS = 6  # ограничение на расширение окна матчинга

WORD_RE = re.compile(r"[\w\-']+", re.UNICODE)
_num_chunk = re.compile(r"(\d+)")
_COMPACT_RE = re.compile(r"[^0-9a-z]+")

# Optional log redirection: default stdout; set TRANSCRIBE_LOG_TARGET=stderr or path.
_LOG_TARGET = (os.environ.get("TRANSCRIBE_LOG_TARGET") or "stdout").strip()
_LOG_FILE_HANDLE = None

def _log_stream():
    global _LOG_FILE_HANDLE
    target_norm = _LOG_TARGET.lower()
    if target_norm == "stdout":
        return sys.stdout
    if target_norm in ("", "stderr"):
        return sys.stderr
    if _LOG_FILE_HANDLE is None:
        try:
            _LOG_FILE_HANDLE = open(_LOG_TARGET, "a", encoding="utf-8")
        except Exception:
            _LOG_FILE_HANDLE = sys.stderr
    return _LOG_FILE_HANDLE

# --- small stderr logger ---
def eprint(*a, **k):
    msg = " ".join(str(x) for x in a)
    stream = _log_stream()
    try:
        print(msg, file=stream, **k)
    except Exception:
        try:
            stream.write(msg + "\n")
        except Exception:
            sys.stderr.write(msg + "\n")

# --- strict final line ---
def final_log(success: bool) -> None:
    """Print only 'true' or 'false' as the very last line and exit immediately."""
    msg = "true\n" if success else "false\n"
    try:
        sys.stdout.write(msg)
        sys.stdout.flush()
    except Exception:
        pass
    try:
        if sys.stderr is not sys.stdout:
            sys.stderr.write(msg)
            sys.stderr.flush()
    except Exception:
        pass
    os._exit(0)

_SCRIPT_START = time.time()
_TIME_SPENT_BASE = 0.0
_SAVE_ITEMS_CACHE: Dict[str, List[Dict[str, Any]]] = {}

def _load_existing_items(out_path: Path) -> Optional[List[Dict[str, Any]]]:
    key = str(out_path)
    if key in _SAVE_ITEMS_CACHE:
        return _SAVE_ITEMS_CACHE[key]
    if not out_path.exists():
        return None
    try:
        with out_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items")
        if isinstance(items, list):
            _SAVE_ITEMS_CACHE[key] = items
            return items
    except Exception:
        return None
    return None

def finish(success: bool) -> None:
    elapsed = max(0.0, time.time() - _SCRIPT_START)
    eprint(f"[time] elapsed {elapsed:.1f}s")
    final_log(success)

# --- text utils ---
def norm_text(s: str) -> str:
    s = unidecode(s.lower().strip())
    return re.sub(r"\s+", " ", s)

def tokenize_words(s: str) -> List[str]:
    return [m.group(0).lower() for m in WORD_RE.finditer(unidecode(s))]

def _compact_for_match(s: str) -> str:
    if not s:
        return ""
    return _COMPACT_RE.sub("", s.lower())

def _compact_token_eq(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return _compact_for_match(a) == _compact_for_match(b)

# -------- case-insensitive dir helpers --------
def _find_subdir_case_insensitive(parent: Path, name: str) -> Optional[Path]:
    if not parent.exists() or not parent.is_dir():
        return None
    lname = name.lower()
    for child in parent.iterdir():
        if child.is_dir() and child.name.lower() == lname:
            return child
    return None

def _video_roots(main: Path, channel: str, video: str) -> List[Path]:
    return [main / channel / "VIDEOS" / video, main / channel / video]

# -------- natural sort --------
def natural_key(s: str):
    return [int(t) if t.isdigit() else t.lower() for t in _num_chunk.split(s)]

# -------- input discovery --------
@dataclass
class AudioItem:
    path: Path

def scan_audio(main: Path, channel: str, video: str) -> List[AudioItem]:
    files: List[Path] = []
    for base in _video_roots(main, channel, video):
        ad = _find_subdir_case_insensitive(base, "VOICE")
        if not ad: continue
        for p in ad.iterdir():
            if p.is_file() and p.suffix.lower() in AUDIO_EXTS:
                files.append(p)
    files.sort(key=lambda q: natural_key(q.name))
    return [AudioItem(p) for p in files]

# -------- TEXTS --------
def scan_texts(mainFolderPath: Path,
               channelName: str,
               videoFolderName: str,
               textFileName: Optional[str] = None) -> List[str]:
    """
    Ищет тексты в обоих вариантах путей и папок:
      <main>/<channel>/<video>/TEXTS/           и <main>/<channel>/<video>/TEXTS/used/
      <main>/<channel>/VIDEOS/<video>/TEXTS/    и <main>/<channel>/VIDEOS/<video>/TEXTS/used/
    Имя 'TEXTS' ищется без учёта регистра. Расширения: .txt, .srt, .vtt.
    Если один и тот же файл есть и в TEXTS/ и в TEXTS/used/, приоритет за версией из used/.
    Возвращает список сырых текстовых блоков (строки).
    """
    ALLOWED = {".txt", ".srt", ".vtt"}

    def _collect_files(d: Optional[Path]) -> List[Path]:
        if not d or not d.exists():
            return []
        files = []
        for p in d.iterdir():
            if not p.is_file() or p.suffix.lower() not in ALLOWED:
                continue
            if p.stem.lower().endswith("_snapshot"):
                continue
            files.append(p)
        return sorted(files, key=lambda p: natural_key(p.name))

    copy_suffix_re = re.compile(r"[\s._-]*(copy|копия)(?:\s*\(\d+\)|\s*\d+)?$", re.IGNORECASE)

    def _canonical_stem(stem: str) -> str:
        s = stem.strip().lower()
        s = copy_suffix_re.sub("", s).strip()
        return s

    def _has_copy_marker(stem: str) -> bool:
        return copy_suffix_re.search(stem.strip().lower()) is not None

    def _pick_preferred(candidates: List[Path]) -> Path:
        ext_rank = {".txt": 3, ".srt": 2, ".vtt": 1}
        return sorted(
            candidates,
            key=lambda p: (
                int(p.stat().st_mtime_ns) if p.exists() else 0,
                int(p.stat().st_size) if p.exists() else 0,
                ext_rank.get(p.suffix.lower(), 0),
                p.name.lower(),
            ),
            reverse=True,
        )[0]

    roots: List[Path] = [
        Path(mainFolderPath) / channelName / videoFolderName,
        Path(mainFolderPath) / channelName / "VIDEOS" / videoFolderName,
    ]

    collected: Dict[str, Path] = {}
    scanned_dirs: List[str] = []

    for root in roots:
        texts_dir = _find_subdir_case_insensitive(root, "TEXTS")
        if not texts_dir:
            continue
        used_dir = _find_subdir_case_insensitive(texts_dir, "used")
        for p in _collect_files(texts_dir):
            collected[p.name] = p
        scanned_dirs.append(str(texts_dir))
        if used_dir:
            for p in _collect_files(used_dir):
                collected[p.name] = p
            scanned_dirs.append(str(used_dir))

    paths = [collected[name] for name in sorted(collected.keys(), key=natural_key)]
    if not paths:
        eprint(f"[texts] searched {len(scanned_dirs)} dir(s), found 0 files")
        for d in scanned_dirs:
            eprint(f"[texts] scanned: {d}")
        return []

    selected_paths = list(paths)

    if textFileName:
        name_l = textFileName.strip().lower()
        exact = [p for p in paths if p.name.lower() == name_l]
        if not exact:
            exact = [p for p in paths if p.stem.lower() == name_l]
        if exact:
            selected_paths = [exact[0]]
            eprint(f"[texts] selected via --textFileName: {selected_paths[0].name}")
        else:
            eprint(f"[texts] warn: --textFileName '{textFileName}' not found; using auto selection")

    if not textFileName:
        groups: Dict[str, List[Path]] = {}
        for p in selected_paths:
            key = _canonical_stem(p.stem)
            groups.setdefault(key, []).append(p)

        filtered: List[Path] = []
        for key in sorted(groups.keys(), key=natural_key):
            group = groups[key]
            if len(group) == 1:
                filtered.append(group[0])
                continue

            has_copy_variant = any(_has_copy_marker(p.stem) for p in group)
            if not has_copy_variant:
                filtered.extend(sorted(group, key=lambda p: natural_key(p.name)))
                continue

            picked = _pick_preferred(group)
            skipped = [p.name for p in group if p != picked]
            eprint(f"[texts] variant group '{key}': using {picked.name}; skipped {', '.join(skipped)}")
            filtered.append(picked)

        selected_paths = filtered

    eprint(f"[texts] found {len(selected_paths)} file(s): " + ", ".join(p.name for p in selected_paths))

    blocks: List[str] = []
    for p in selected_paths:
        try:
            blocks.append(p.read_text(encoding="utf-8"))
        except Exception:
            blocks.append(p.read_text(encoding="cp1251", errors="ignore"))
    return blocks

# ---------- План-сегментация "как в JS": splitBalanced ----------

_RX_SENT_END = re.compile(r'([.!?…]+[)"»\]]*\s)')
_TAG_START = "["
_TAG_END = "]"

def _iter_tag_spans(text: str) -> List[Tuple[int, int]]:
    """Return list of [start, end) spans for bracket tags like [tag].
    If a tag is not closed, treat it as spanning to end of string.
    """
    spans: List[Tuple[int, int]] = []
    if _TAG_START not in text:
        return spans
    i = 0
    n = len(text)
    while i < n:
        start = text.find(_TAG_START, i)
        if start < 0:
            break
        end = text.find(_TAG_END, start + 1)
        if end < 0:
            spans.append((start, n))
            break
        spans.append((start, end + 1))
        i = end + 1
    return spans

def _mask_tags(text: str) -> str:
    """Replace characters inside [tag] spans with a safe placeholder to
    prevent punctuation/space based splitting inside tags.
    """
    if _TAG_START not in text:
        return text
    spans = _iter_tag_spans(text)
    if not spans:
        return text
    chars = list(text)
    for start, end in spans:
        for i in range(start, end):
            chars[i] = "a"
    return "".join(chars)

def _adjust_break_index(idx: int, spans: List[Tuple[int, int]]) -> int:
    """Move break index out of a tag span if it falls inside one."""
    for start, end in spans:
        if start < idx < end:
            if start > 0:
                return start
            return end
    return idx

def _find_break(remaining: str, limit: int) -> int:
    window = remaining[:limit]
    masked = _mask_tags(window)
    spans = _iter_tag_spans(remaining)
    best = -1
    for m in _RX_SENT_END.finditer(masked):
        best = m.end()
    if best >= int(limit * 0.6):
        idx = _adjust_break_index(best, spans)
        if idx <= 0:
            idx = max(1, best)
        return _safe_break_index(remaining, idx)
    last_space = max(masked.rfind(' '), masked.rfind('\n'), masked.rfind('\t'))
    if last_space > 0:
        idx = _adjust_break_index(last_space + 1, spans)
        if idx <= 0:
            idx = max(1, last_space + 1)
        return _safe_break_index(remaining, idx)
    idx = _adjust_break_index(limit, spans)
    if idx <= 0:
        if spans and spans[0][0] == 0:
            idx = spans[0][1]
        if idx <= 0:
            idx = 1
    return _safe_break_index(remaining, idx)

def _split_into_sentences_with_paras(source: str) -> List[str]:
    out: List[str] = []
    paras = re.split(r'\n\s*\n', source)
    for p_idx, p in enumerate(paras):
        p = (p or '').strip()
        if not p:
            continue
        i = 0
        rx = re.compile(r'([.!?…]+[)"»\]]*)(\s+|$)')
        masked = _mask_tags(p)
        for m in rx.finditer(masked):
            end = m.end()
            sent = p[i:end]
            if sent.strip():
                out.append(sent)
            i = end
        if i < len(p):
            tail = p[i:]
            if tail.strip():
                out.append(tail)
        if p_idx < len(paras) - 1:
            out.append('\n\n')
    # схлопываем подряд идущие \n\n
    cleaned: List[str] = []
    for s in out:
        if s == '\n\n' and cleaned and cleaned[-1] == '\n\n':
            continue
        cleaned.append(s)
    return cleaned

def split_balanced(source: str, limit: int) -> List[str]:
    # 1) разрезаем в "предложения" c маркерами параграфов
    sents = _split_into_sentences_with_paras(source)

    # 1a) режем сверхдлинные предложения (> limit) на допустимые куски
    normalized: List[str] = []
    for s in sents:
        if s == '\n\n' or len(s) <= limit:
            normalized.append(s)
            continue
        rest = s.strip()
        while len(rest) > limit:
            bp = _find_break(rest, limit)
            chunk = rest[:bp].strip()
            if chunk:
                normalized.append(chunk)
            rest = rest[bp:].strip()
        if rest:
            normalized.append(rest)
    sents = normalized

    # 2) префиксные длины
    n = len(sents)
    pref = [0] * (n + 1)
    for i in range(n):
        pref[i + 1] = pref[i] + len(sents[i])

    def chunk_cost(i: int, j: int) -> float:
        length = pref[j] - pref[i]
        if length > limit:
            return float('inf')
        slack = limit - length
        return float(slack * slack)

    # 4) DP
    best = [float('inf')] * (n + 1)
    next_break = [-1] * (n + 1)
    best[n] = 0.0

    for i in range(n - 1, -1, -1):
        if sents[i] == '\n\n':
            best[i] = best[i + 1]
            next_break[i] = i + 1
            continue
        j = i + 1
        while j <= n and (pref[j] - pref[i]) <= limit:
            cost = chunk_cost(i, j) + best[j]
            if cost < best[i]:
                best[i] = cost
                next_break[i] = j
            j += 1

    # 5) восстановление
    chunks: List[str] = []
    i = 0
    while i < n:
        j = next_break[i]
        if j == -1 or j <= i:
            j = min(n, i + 1)
        text = "".join(sents[i:j]).trim() if hasattr(str, "trim") else "".join(sents[i:j]).strip()
        if text:
            chunks.append(text)
        i = j
    return chunks


# -------- ASR & words --------
@dataclass
class Word:
    text: str
    start: float
    end: float

# Silence third-party spam during ASR (thread-safe, re-entrant).
_ASR_SUPPRESS_LOCK = threading.RLock()
_ASR_SUPPRESS_STATE = {
    "depth": 0,
    "old_filters": None,
    "old_levels": None,
    "old_stdout": None,
    "old_stderr": None,
    "devnull": None,
}
_ASR_SUPPRESS_TARGETS = [
    "lightning", "pytorch_lightning", "pytorch_lightning.utilities.rank_zero",
    "pyannote", "pyannote.audio", "pyannote.audio.core", "pyannote.audio.core.io",
    "whisperx", "whisperx.vads", "whisperx.vads.pyannote",
    "transformers", "urllib3", "numba", "torch"
]
_ASR_SUPPRESS_WARNING_PATTERNS = [
    "Lightning automatically upgraded your loaded checkpoint",
    "Model was trained with pyannote.audio",
    "Model was trained with torch",
]


def _wait_for_min_vram(min_bytes: int, poll_interval: float = 0.5, timeout: float = 60.0):
    if min_bytes <= 0 or torch is None or not torch.cuda.is_available():
        return
    start = time.time()
    warned = False
    while True:
        try:
            free, total = torch.cuda.mem_get_info()
        except Exception:
            return
        if free >= min_bytes:
            return
        if not warned:
            need_gb = min_bytes / (1024 ** 3)
            free_gb = free / (1024 ** 3)
            eprint(f"[vram] waiting for {need_gb:.1f}GB free (currently {free_gb:.1f}GB)")
            warned = True
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass
        if timeout and (time.time() - start) > timeout:
            return
        time.sleep(poll_interval)


def _clear_cuda_cache():
    """Release cached CUDA allocations after GC to avoid gradual VRAM growth."""
    if torch is None:
        return
    try:
        gc.collect()
    except Exception:
        pass
    if torch.cuda.is_available():
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


@contextlib.contextmanager
def _suppress_asr_noise():
    with _ASR_SUPPRESS_LOCK:
        if _ASR_SUPPRESS_STATE["depth"] == 0:
            _ASR_SUPPRESS_STATE["old_filters"] = warnings.filters[:]
            _ASR_SUPPRESS_STATE["old_levels"] = {
                name: logging.getLogger(name).level for name in _ASR_SUPPRESS_TARGETS
            }
            _ASR_SUPPRESS_STATE["old_stdout"] = sys.stdout
            _ASR_SUPPRESS_STATE["old_stderr"] = sys.stderr
            warnings.simplefilter("ignore")
            for pattern in _ASR_SUPPRESS_WARNING_PATTERNS:
                warnings.filterwarnings("ignore", message=pattern)
            for name in _ASR_SUPPRESS_TARGETS:
                logging.getLogger(name).setLevel(logging.ERROR)
            try:
                if _ASR_SUPPRESS_STATE["devnull"] is None:
                    _ASR_SUPPRESS_STATE["devnull"] = open(os.devnull, "w")
                sys.stdout = _ASR_SUPPRESS_STATE["devnull"]  # type: ignore
                sys.stderr = _ASR_SUPPRESS_STATE["devnull"]  # type: ignore
            except Exception:
                sys.stdout = _ASR_SUPPRESS_STATE["old_stdout"]  # type: ignore
                sys.stderr = _ASR_SUPPRESS_STATE["old_stderr"]  # type: ignore
        _ASR_SUPPRESS_STATE["depth"] += 1
    try:
        yield
    finally:
        with _ASR_SUPPRESS_LOCK:
            _ASR_SUPPRESS_STATE["depth"] -= 1
            if _ASR_SUPPRESS_STATE["depth"] == 0:
                warnings.filters[:] = _ASR_SUPPRESS_STATE["old_filters"]
                for name, level in (_ASR_SUPPRESS_STATE["old_levels"] or {}).items():
                    logging.getLogger(name).setLevel(level)
                try:
                    if _ASR_SUPPRESS_STATE["old_stdout"] is not None:
                        sys.stdout = _ASR_SUPPRESS_STATE["old_stdout"]  # type: ignore
                    if _ASR_SUPPRESS_STATE["old_stderr"] is not None:
                        sys.stderr = _ASR_SUPPRESS_STATE["old_stderr"]  # type: ignore
                except Exception:
                    pass
                if _ASR_SUPPRESS_STATE.get("devnull") is not None:
                    try:
                        _ASR_SUPPRESS_STATE["devnull"].close()
                    except Exception:
                        pass
                    _ASR_SUPPRESS_STATE["devnull"] = None


def _tokens_within_segment(seg_text: str, start: float, end: float) -> List[Word]:
    toks = tokenize_words(seg_text)
    if not toks: return [Word(seg_text.strip() or "<unk>", start, end)]
    dur = max(0.0, end - start); n = len(toks)
    if dur <= 0.0:
        step = 0.01
        return [Word(t, start + i*step, start + (i+1)*step) for i,t in enumerate(toks)]
    step = dur / n; out=[]; cur=start
    for t in toks:
        nxt = cur + step
        out.append(Word(t, cur, nxt)); cur = nxt
    out[-1] = Word(out[-1].text, out[-1].start, end)
    return out


def _try_whisperx(audio_path: Path, device: str, language: Optional[str],
                  model_name: str, compute_type: str, batch_size: int,
                  prompt: Optional[str] = None):
    with _suppress_asr_noise():
        model = whisperx.load_model(model_name, device, compute_type=compute_type, language=language)
        audio = whisperx.load_audio(str(audio_path))
        kwargs = {"batch_size": batch_size, "language": language}
        if prompt:
            kwargs["initial_prompt"] = prompt
        try:
            result = model.transcribe(audio, **kwargs)
        except TypeError:
            if "initial_prompt" in kwargs:
                kwargs.pop("initial_prompt", None)
                result = model.transcribe(audio, **kwargs)
            else:
                raise

    duration = float(result.get("duration", 0.0))
    if duration == 0.0:
        segs = result.get("segments") or []
        if segs: duration = float(segs[-1].get("end", 0.0))

    words: List[Word] = []
    try:
        with _suppress_asr_noise():
            model_a, metadata = whisperx.load_align_model(language_code=result["language"], device=device)
            aligned = whisperx.align(result["segments"], model_a, metadata, audio, device=device, return_char_alignments=False)
        for w in aligned.get("word_segments", []):
            txt = (w.get("text") or "").strip()
            if txt: words.append(Word(txt, float(w["start"]), float(w["end"])))
    except Exception:
        pass

    if not words:
        segs = result.get("segments") or []
        for s in segs:
            s_text = (s.get("text") or "").strip()
            s_start = float(s.get("start", 0.0))
            s_end = float(s.get("end", s_start))
            words.extend(_tokens_within_segment(s_text, s_start, s_end))

    lang = result.get("language", language or "unk")
    return words, lang, duration


def transcribe_words(audio_path: Path, device: str, language: Optional[str],
                     prompt: Optional[str] = None):
    if whisperx is None:
        eprint(f"[asr] skip {audio_path.name}")
        return [], (language or "unk"), 0.0

    if device == "cuda":
        attempts = [
            # ("large-v2","float16",8),
            # ("large-v2","float16",4),
            # ("large-v2","float16",2),
            # ("large-v2","float16",1),
            ("medium","float16",8),
            # ("medium","float16",4),
            # ("medium","float16",2),
            # ("medium","float16",1),
            # ("small","float16",2),
            # ("small","float16",1),
            # ("base","float16",1),
        ]
    else:
        attempts = [("small","float32",1),("base","float32",1)]

    last_err=None
    for model_name,ctype,bs in attempts:
        try:
            eprint(f"[asr] {audio_path.name} {model_name}/{ctype} bs={bs} dev={device}")
            return _try_whisperx(audio_path, device, language, model_name, ctype, bs, prompt=prompt)
        except RuntimeError as e:
            msg = str(e).lower(); last_err=e
            if ("out of memory" in msg or "cuda" in msg) and torch is not None and device=="cuda":
                eprint("[oom] retry smaller")
                try: torch.cuda.empty_cache()
                except Exception: pass
                continue
            break
        except Exception as e:
            last_err=e; continue

    if device == "cuda":
        try:
            eprint("[fallback] cpu/small")
            return _try_whisperx(audio_path, "cpu", language, "small", "float32", 1)
        except Exception as e:
            last_err=e

    eprint(f"[asr!] {audio_path.name}")
    return [], (language or "unk"), 0.0


def _summarize_words(words: List[Word], limit: int = 9999999999) -> str:
    if not words:
        return "<empty>"
    sample = " ".join(w.text for w in words[:limit])
    if len(words) > limit:
        sample += " ..."
    return sample

# --- sanitize ---
SANITIZE_MAP = {
    '«': "'", '»': "'", '‹': "'", '›': "'",
    '“': '"', '”': '"', '„': '"', '‟': '"', '〝': '"', '〞': '"',
    '’': "'", '‘': "'", '‚': "'", '′': "'", '‵': "'", '＇': "'",
    '—': '-', '–': '-', '−': '-', '‒': '-', '―': '-',
    '…': '...',
    '·': '*', '•': '*', '∙': '*',
    '\u00A0': ' ', '\u1680': ' ', '\u2000': ' ', '\u2001': ' ', '\u2002': ' ',
    '\u2003': ' ', '\u2004': ' ', '\u2005': ' ', '\u2006': ' ', '\u2007': ' ',
    '\u2008': ' ', '\u2009': ' ', '\u200A': ' ', '\u202F': ' ', '\u205F': ' ',
    '\u3000': ' ', '\u200B': '', '\u200C': '', '\u200D': '', '\uFEFF': '',
}
SANITIZE_TR = str.maketrans(SANITIZE_MAP)

def sanitize_text(s: str, for_split: bool = False) -> str:
    if not s:
        return s
    if for_split:
        s = s.replace("\\n", "\n").replace("\\r", "\r")
        local_map = dict(SANITIZE_MAP); local_map.pop('…', None)
        s = s.translate(str.maketrans(local_map))
        s = re.sub(r"[ \t\f\v]+", " ", s)
    else:
        s = s.translate(SANITIZE_TR)
        s = re.sub(r"\.{4,}", "...", s)
        s = re.sub(r"\s+", " ", s)
    return s.strip()

# -------- sentences & alignment --------
@dataclass
class Sentence:
    text: str
    norm: str
    tokens: List[str]

def _is_weak_opener_text(txt: str) -> bool:
    t = txt.strip()
    return t in {"—", "-", "–"} or len(t) <= 2


def split_into_sentences(text: str) -> List[str]:
    if not text:
        return []
    text = text.replace("\\n", "\n").replace("\\r", "\r")
    has_arabic = bool(re.search(r"[\u0600-\u06FF]", text))
    min_len = 20 if has_arabic else MIN_SENT_CHARS

    def _is_sentence_starter(ch: str) -> bool:
        return (ch.isalpha() or ch.isdigit() or ch in "«\"'“”(")

    line_blocks = [blk for blk in re.split(r"(?:\r?\n)+", text) if blk.strip()]
    all_parts: List[str] = []

    for block in line_blocks:
        para = block.strip()
        if not para:
            continue
        parts: List[str] = []
        start = 0; i = 0; n = len(para)
        while i < n:
            ch = para[i]
            if ch in ".!?…؟":
                j = i + 1
                while j < n and para[j].isspace():
                    j += 1
                if j >= n:
                    frag = para[start:].strip()
                    if frag: parts.append(frag)
                    start = n; break
                if _is_sentence_starter(para[j]):
                    frag = para[start:i + 1].strip()
                    if frag: parts.append(frag)
                    start = j; i = j; continue
            i += 1
        if start < n:
            tail = para[start:].strip()
            if tail: parts.append(tail)

        # merge weak openers
        merged: List[str] = []
        k = 0
        while k < len(parts):
            cur = parts[k]
            if _is_weak_opener_text(cur) and (k + 1) < len(parts):
                merged.append((cur.rstrip() + " " + parts[k + 1].lstrip()).strip())
                k += 2
            else:
                merged.append(cur); k += 1

        # glue short
        result_block: List[str] = []
        carry: Optional[str] = None
        for frag in merged:
            if carry is not None:
                frag = (carry + " " + frag).strip(); carry = None
            if len(frag) < min_len:
                if result_block:
                    result_block[-1] = (result_block[-1] + " " + frag).strip()
                else:
                    carry = frag
            else:
                result_block.append(frag)
        if carry is not None:
            if result_block:
                result_block[-1] = (result_block[-1] + " " + carry).strip()
            else:
                result_block.append(carry)

        # dedupe consecutive
        deduped: List[str] = []
        prev = None
        for it in result_block:
            if it != prev:
                deduped.append(it); prev = it
        all_parts.extend(deduped)

    # final dedupe
    final: List[str] = []
    prev = None
    for it in all_parts:
        if it != prev:
            final.append(it); prev = it
    return final

# --- TTS-style balanced split helpers (JS parity) ---
def _safe_break_index(s: str, idx: int) -> int:
    # Match JS safeBreakIndex: avoid splitting UTF-16 surrogate pairs.
    if 0 < idx < len(s):
        prev = ord(s[idx - 1])
        curr = ord(s[idx])
        if 0xD800 <= prev <= 0xDBFF and 0xDC00 <= curr <= 0xDFFF:
            idx -= 1
    return max(0, min(len(s), idx))


def _find_break(remaining: str, limit: int) -> int:
    window = remaining[:limit]
    masked = _mask_tags(window)
    spans = _iter_tag_spans(remaining)
    best = -1
    rx = re.compile(r'([.!?…]+[)"»\]]?\s)')
    for m in rx.finditer(masked):
        best = m.start() + len(m.group(0))
    if best >= int(limit * 0.6):
        idx = _adjust_break_index(best, spans)
        if idx <= 0:
            idx = max(1, best)
        return _safe_break_index(remaining, idx)
    last_space = max(masked.rfind(" "), masked.rfind("\n"), masked.rfind("\t"))
    if last_space > 0:
        idx = _adjust_break_index(last_space + 1, spans)
        if idx <= 0:
            idx = max(1, last_space + 1)
        return _safe_break_index(remaining, idx)
    idx = _adjust_break_index(limit, spans)
    if idx <= 0:
        if spans and spans[0][0] == 0:
            idx = spans[0][1]
        if idx <= 0:
            idx = 1
    return _safe_break_index(remaining, idx)


def _split_into_sentences_with_paras(source: str) -> List[str]:
    result: List[str] = []
    paras = re.split(r"\n\s*\n", source)
    for p_idx, p in enumerate(paras):
        p = (p or "").strip()
        if not p:
            continue
        i = 0
        rx = re.compile(r'([.!?…]+[)"»\]]*)(\s+|$)')
        masked = _mask_tags(p)
        for m in rx.finditer(masked):
            end = m.start() + len(m.group(0))
            sent = p[i:end]
            if sent.strip():
                result.append(sent)
            i = end
        if i < len(p):
            tail = p[i:]
            if tail.strip():
                result.append(tail)
        if p_idx < len(paras) - 1:
            result.append("\n\n")
    # dedupe repeated paragraph separators
    compact: List[str] = []
    for cur in result:
        if cur == "\n\n" and compact and compact[-1] == "\n\n":
            continue
        compact.append(cur)
    return compact


def split_balanced_tts(source: str, limit: int) -> List[str]:
    """JS-parity splitBalanced: balanced by sentences/paras, DP over chunks."""
    if limit is None or limit <= 0:
        return [source] if source.strip() else []
    sents = _split_into_sentences_with_paras(source)

    # Normalize overlong sentences
    normalized: List[str] = []
    for s in sents:
        if s == "\n\n":
            normalized.append(s)
            continue
        if len(s) <= limit:
            normalized.append(s)
            continue
        rest = s
        while len(rest) > limit:
            bp = _find_break(rest, limit)
            chunk = rest[:bp].strip()
            if chunk:
                normalized.append(chunk)
            rest = rest[bp:].strip()
        if rest.strip():
            normalized.append(rest.strip())
    sents = normalized

    n = len(sents)
    pref = [0] * (n + 1)
    for i in range(n):
        pref[i + 1] = pref[i] + len(sents[i])

    def chunk_cost(i: int, j: int) -> float:
        length = pref[j] - pref[i]
        if length > limit:
            return float("inf")
        slack = limit - length
        return slack * slack

    best = [float("inf")] * (n + 1)
    next_break = [-1] * (n + 1)
    best[n] = 0.0

    for i in range(n - 1, -1, -1):
        if sents[i] == "\n\n":
            best[i] = best[i + 1]
            next_break[i] = i + 1
            continue
        j = i + 1
        while j <= n and (pref[j] - pref[i]) <= limit:
            cost = chunk_cost(i, j) + best[j]
            if cost < best[i]:
                best[i] = cost
                next_break[i] = j
            j += 1

    chunks: List[str] = []
    i = 0
    while i < n:
        j = next_break[i]
        if j == -1 or j <= i:
            j = min(n, i + 1)
        text = "".join(sents[i:j]).strip()
        if text:
            chunks.append(text)
        i = j
    return chunks


def _tokens_from_text_for_order(text: str) -> List[str]:
    if not text:
        return []
    text = re.sub(r"\[[^][]*\]", "", text)
    vis = sanitize_text(text, for_split=False)
    nrm = norm_text(vis)
    return tokenize_words(nrm)


def _find_token_subsequence(stream: List[str],
                            seq: List[str],
                            start: int,
                            max_seek: int = 5000) -> Optional[int]:
    if not seq:
        return start
    if start < 0:
        start = 0
    max_i = len(stream) - len(seq)
    if max_i < start:
        return None
    if max_seek is not None and max_seek > 0:
        max_i = min(max_i, start + max_seek)
    first = seq[0]
    i = start
    while i <= max_i:
        try:
            i = stream.index(first, i, max_i + 1)
        except ValueError:
            return None
        if stream[i:i + len(seq)] == seq:
            return i
        i += 1
    return None


def _dominant_order(orders: List[Any]) -> Optional[Any]:
    counts: Dict[Any, int] = {}
    first_idx: Dict[Any, int] = {}
    for i, order in enumerate(orders):
        if order is None:
            continue
        counts[order] = counts.get(order, 0) + 1
        if order not in first_idx:
            first_idx[order] = i
    if not counts:
        return None
    return max(counts.keys(), key=lambda k: (counts[k], -first_idx.get(k, 0)))


def _assign_orders_to_sentences(sentences: List[Sentence],
                                parts: List[Dict[str, Any]]) -> None:
    flat_tokens: List[str] = []
    flat_orders: List[Any] = []
    for p in parts:
        if not isinstance(p, dict):
            continue
        order = p.get("order")
        tokens = _tokens_from_text_for_order(p.get("text", ""))
        for tok in tokens:
            flat_tokens.append(tok)
            flat_orders.append(order)

    if not flat_tokens:
        eprint("[manga] warn: empty token stream for order mapping")
        return

    cursor = 0
    misses = 0
    spans = 0
    for s in sentences:
        meta = getattr(s, "meta", None)
        if not isinstance(meta, dict):
            meta = {}
            s.meta = meta
        if not s.tokens:
            meta["order"] = None
            continue
        pos = _find_token_subsequence(flat_tokens, s.tokens, cursor)
        if pos is None:
            meta["order"] = None
            misses += 1
            continue
        span_orders = flat_orders[pos:pos + len(s.tokens)]
        meta["order"] = _dominant_order(span_orders)
        if len({o for o in span_orders if o is not None}) > 1:
            spans += 1
        cursor = pos + len(s.tokens)

    if misses:
        eprint(f"[manga] warn: order map misses={misses}")
    if spans:
        eprint(f"[manga] warn: sentence spans multiple orders={spans}")


def _assign_suborders(sentences: List[Sentence]) -> None:
    counters: Dict[Any, int] = {}
    for s in sentences:
        meta = getattr(s, "meta", None)
        if not isinstance(meta, dict):
            meta = {}
            s.meta = meta
        order = meta.get("order")
        if order is None:
            meta["suborder"] = None
            continue
        sub = counters.get(order, 0)
        meta["suborder"] = sub
        counters[order] = sub + 1


def _rebuild_chunk_ranges(sentences: List[Sentence]) -> List[Tuple[int, int]]:
    if not sentences:
        return []
    ranges: List[Tuple[int, int]] = []
    start = 0
    prev_chunk = getattr(sentences[0], "meta", {}).get("chunk")
    for i, s in enumerate(sentences):
        chunk = getattr(s, "meta", {}).get("chunk")
        if chunk != prev_chunk:
            ranges.append((start, i))
            start = i
            prev_chunk = chunk
    ranges.append((start, len(sentences)))
    return ranges


def _merge_sentences_with_meta(left: Sentence,
                               right: Sentence,
                               keep_meta: Dict[str, Any]) -> Sentence:
    merged_text = (left.text.rstrip() + " " + right.text.lstrip()).strip()
    merged = make_sentence(merged_text)
    merged.meta = dict(keep_meta) if isinstance(keep_meta, dict) else {}
    return merged


def _coalesce_short_sentences_with_meta(items: List[Sentence],
                                        min_chars: int = 0,
                                        min_tokens: int = 0) -> None:
    if (min_chars or 0) <= 0 and (min_tokens or 0) <= 0:
        return

    def _too_short(s: Sentence) -> bool:
        if min_chars and len(s.text.strip()) < min_chars:
            return True
        if min_tokens and len(s.tokens) < min_tokens:
            return True
        return False

    def _same_bucket(a: Sentence, b: Sentence) -> bool:
        ma = getattr(a, "meta", {}) if isinstance(getattr(a, "meta", None), dict) else {}
        mb = getattr(b, "meta", {}) if isinstance(getattr(b, "meta", None), dict) else {}
        return ma.get("chunk") == mb.get("chunk") and ma.get("order") == mb.get("order")

    i = 0
    while i < len(items):
        s = items[i]
        if not _too_short(s):
            i += 1
            continue
        if i > 0 and _same_bucket(items[i - 1], s):
            merged = _merge_sentences_with_meta(items[i - 1], s, items[i - 1].meta)
            items[i - 1] = merged
            del items[i]
            continue
        if i + 1 < len(items) and _same_bucket(s, items[i + 1]):
            merged = _merge_sentences_with_meta(s, items[i + 1], items[i + 1].meta)
            items[i + 1] = merged
            del items[i]
            continue
        i += 1


def _build_manga_sentences_from_texts(text_blocks: List[str],
                                      chunk_limit: int) -> Tuple[List[Sentence], List[Tuple[int, int]], List[str]]:
    chunk_texts: List[str] = []
    for block in text_blocks:
        block = block or ""
        if not block.strip():
            continue
        if chunk_limit and chunk_limit > 0:
            chunk_texts.extend(split_balanced_tts(block, chunk_limit))
        else:
            chunk_texts.append(block)

    sentences: List[Sentence] = []
    chunk_ranges: List[Tuple[int, int]] = []
    for chunk_idx, chunk in enumerate(chunk_texts):
        part_sents = build_sentence_stream([chunk])
        split_multi_sentences_inplace(part_sents)
        for s in part_sents:
            s.meta = {"chunk": chunk_idx}
        start = len(sentences)
        sentences.extend(part_sents)
        end = len(sentences)
        chunk_ranges.append((start, end))
    return sentences, chunk_ranges, chunk_texts


def build_sentence_stream(texts: List[str]) -> List[Sentence]:
    out = []
    for txt in texts:
        for s in split_into_sentences(txt):
            sent = make_sentence(s)
            if sent.tokens:
                out.append(sent)

    return out

# --- helpers for building Sentence objects and pre-splitting ---
SPLIT_HARD_RE = re.compile(r'(?<=[\.\!\?…])\s+(?=[«"\“”„(]*[A-ZА-ЯЁ0-9])')

def make_sentence(text: str) -> Sentence:
    text = re.sub(r"\[[^][]*\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    vis = sanitize_text(text, for_split=False)

    nrm = norm_text(vis)
    toks = tokenize_words(nrm)
    return Sentence(vis, nrm, toks)

def coalesce_short_sentences_inplace(items: List[Sentence],
                                     min_chars: int = 0,
                                     min_tokens: int = 0) -> None:
    """Склеивает слишком короткие предложения с соседними.
    Предпочтение — слить в предыдущее; если это первый элемент — в следующее.
    """
    if (min_chars or 0) <= 0 and (min_tokens or 0) <= 0:
        return
    i = 0
    while i < len(items):
        s = items[i]
        too_short = False
        if min_chars and len(s.text.strip()) < min_chars:
            too_short = True
        if min_tokens and len(s.tokens) < min_tokens:
            too_short = True
        if not too_short:
            i += 1
            continue

        if i > 0:
            merged = (items[i-1].text.rstrip() + " " + s.text.lstrip()).strip()
            items[i-1] = make_sentence(merged)
            del items[i]
            # остаёмся на том же i, т.к. сдвинулась лента
        elif i + 1 < len(items):
            merged = (s.text.rstrip() + " " + items[i+1].text.lstrip()).strip()
            items[i+1] = make_sentence(merged)
            del items[i]
        else:
            # единственный элемент — оставляем как есть
            i += 1

def split_multi_sentences_inplace(items: List[Sentence]) -> None:
    i = 0
    while i < len(items):
        txt = items[i].text
        parts = [p.strip() for p in SPLIT_HARD_RE.split(txt) if p.strip()]
        if len(parts) > 1:
            # заменить текущий и вставить остальные как полноценные Sentence
            items[i] = make_sentence(parts[0])
            j = 1
            while j < len(parts):
                items.insert(i + j, make_sentence(parts[j]))
                j += 1
            i += len(parts)
        else:
            i += 1


def _best_score(a: str, b: str) -> int:
    return max(
        int(fuzz.ratio(a, b)),
        int(fuzz.partial_ratio(a, b)),
        int(fuzz.token_set_ratio(a, b)),
        int(fuzz.token_sort_ratio(a, b)),
    )


class IncrementalAligner:
    def __init__(self, sentences: List[Sentence],
                 min_score: int = 75, max_checks: int = 4000, dynamic_factor: int = 20,
                 text_parts_manager: Optional[TextPartsManager] = None):
        self.sentences = sentences
        self.min_score = min_score
        self.max_checks = max_checks
        self.dynamic_factor = dynamic_factor
        self.word_texts: List[str] = []
        self.words: List[Word] = []
        self.word_srcs: List[Optional[str]] = []
        self.cursor = 0
        self.sent_idx = 0
        self.last_start_ms = -1
        self.last_end_ms = -1  # NEW: enforce monotonicity by previous END
        self.stop_idx: Optional[int] = None
        self.forced_src: Optional[str] = None
        # (Sentence, start_ms, end_ms, audio_file)
        self.results: List[Tuple[Sentence, Optional[int], Optional[int], Optional[str]]] = []
        self.text_parts_manager = text_parts_manager

    def _window_str(self, i: int, j: int) -> str:
        return " ".join(self.word_texts[i:j])

    def _dominant_src(self, i: int, j: int, st_ms: int, en_ms: int) -> Optional[str]:
        """Выбираем источник по МАКСИМАЛЬНОМУ ПЕРЕКРЫТИЮ ОТРЕЗКА [st,en] по времени."""
        if st_ms is None or en_ms is None:
            return None
        st_s = st_ms / 1000.0
        en_s = en_ms / 1000.0
        overlap: Dict[str, float] = {}
        for w, sname in zip(self.words[i:j], self.word_srcs[i:j]):
            if not sname:
                continue
            a = max(w.start, st_s)
            b = min(w.end, en_s)
            if b > a:
                overlap[sname] = overlap.get(sname, 0.0) + (b - a)
        if overlap:
            return max(overlap.items(), key=lambda kv: kv[1])[0]
        # Фоллбек: берём источник слова, пересекающего начало интервала
        for k in range(i, j):
            if self.words[k].end >= st_s:
                return self.word_srcs[k]
        # Финальный фоллбек: голосование по числу слов (как было)
        freq: Dict[str, int] = {}
        for sname in self.word_srcs[i:j]:
            if not sname: continue
            freq[sname] = freq.get(sname, 0) + 1
        return max(freq.items(), key=lambda kv: kv[1])[0] if freq else None


    def _ptr_for_time(self, t_ms: int) -> int:
        """Find first word index whose end >= t_ms (milliseconds)."""
        for k, w in enumerate(self.words):
            if int(round(w.end * 1000)) >= t_ms:
                return k
        return len(self.words)

    def _try_match_sentence_core(self, s: Sentence, aggressive: bool) -> Optional[Tuple[int,int,Optional[str],int]]:
        L = max(1, len(s.tokens))
        base_lens = [L, max(1, int(L * 0.8)), max(1, int(L * 1.2))]
        expansions = ([0.6, 0.8, 1.0, 1.3, 1.6, 2.0, 2.5] if aggressive
                      else [1.0, 1.3, 1.6, 2.0])
        max_extra = max(MAX_EXTRA_TOKENS, int(L * (0.6 if aggressive else 0.2)))
        max_len = L + max_extra
        dyn_limit = max(self.max_checks, self.dynamic_factor * max(0, len(self.words) - self.cursor))
        if aggressive:
            dyn_limit = max(dyn_limit, 200_000)
        score_threshold = 62 if aggressive else self.min_score
        s_compact = _compact_for_match(s.norm)
        last_tok = s.tokens[-1] if s.tokens else ""
        last_compact = _compact_for_match(last_tok)
        require_tail = (len(s.tokens) >= 8 and len(last_compact) >= 3)
        tail_positions: Optional[List[int]] = None
        if require_tail:
            tail_positions = [
                idx for idx in range(self.cursor, len(self.word_texts))
                if _compact_token_eq(self.word_texts[idx], last_tok)
            ]

        checks = 0
        def _end_idx_from_matches(win_tokens: List[str], start_idx: int, fallback_end_idx: int) -> int:
            if not s.tokens:
                return fallback_end_idx
            sent_comp = [_compact_for_match(t) for t in s.tokens]
            win_comp = [_compact_for_match(t) for t in win_tokens]
            sent_i = 0
            w_i = 0
            last_match = None
            while sent_i < len(sent_comp) and w_i < len(win_comp):
                if sent_comp[sent_i] and sent_comp[sent_i] == win_comp[w_i]:
                    last_match = w_i
                    sent_i += 1
                    w_i += 1
                    continue
                matched = False
                # try merging 2-3 sentence tokens to match one ASR token (e.g. "jin"+"gu" -> "jingu")
                for j in (2, 3):
                    if sent_i + j <= len(sent_comp):
                        merged = "".join(sent_comp[sent_i:sent_i + j])
                        if merged and merged == win_comp[w_i]:
                            last_match = w_i
                            sent_i += j
                            w_i += 1
                            matched = True
                            break
                if matched:
                    continue
                # try merging 2-3 ASR tokens to match one sentence token (e.g. ASR split)
                for j in (2, 3):
                    if w_i + j <= len(win_comp):
                        merged = "".join(win_comp[w_i:w_i + j])
                        if merged and merged == sent_comp[sent_i]:
                            last_match = w_i + j - 1
                            w_i += j
                            sent_i += 1
                            matched = True
                            break
                if matched:
                    continue
                # skip unmatched ASR token and keep scanning
                w_i += 1
            if last_match is None:
                return fallback_end_idx
            return start_idx + last_match + 1

        for exp in expansions:
            cand: List[int] = []
            seen = set()
            for k0 in base_lens:
                k = max(1, int(k0 * exp))
                if k not in seen and k <= (len(self.words) - self.cursor):
                    cand.append(k); seen.add(k)
            if not cand:
                continue
            cand = [k for k in cand if k <= max_len]
            if not cand:
                continue
            if require_tail and tail_positions:
                for end_idx in tail_positions:
                    best = None  # (score, i, k)
                    for k in cand:
                        i = end_idx - (k - 1)
                        if i < self.cursor or (i + k) > len(self.words):
                            continue
                        if checks > dyn_limit:
                            break
                        win_tokens = self.word_texts[i:i + k]
                        win = " ".join(win_tokens)
                        score = _best_score(s.norm, win)
                        checks += 1
                        if score < score_threshold and s_compact:
                            win_compact = _compact_for_match(win)
                            if win_compact:
                                score = max(score, _best_score(s_compact, win_compact))
                        if best is None or score > best[0]:
                            best = (score, i, k)
                    if checks > dyn_limit:
                        break
                    if best and best[0] >= score_threshold:
                        _, i, k = best
                        win_tokens = self.word_texts[i:i + k]
                        end_idx = _end_idx_from_matches(win_tokens, i, i + k)
                        st0 = int(round(self.words[i].start * 1000))
                        en0 = int(round(self.words[end_idx - 1].end * 1000))
                        st = max(st0, self.last_end_ms + 1)
                        en = max(en0, st)
                        if (en - st) < MIN_MATCH_MS:
                            en = st + MIN_MATCH_MS
                        src_win = None
                        return st, en, src_win, end_idx
                if checks > dyn_limit:
                    break
                # Strict tail anchor may fail on ASR typos in the final word.
                # Fall back to regular scanning instead of hard-failing.

            for k in cand:
                i = self.cursor
                step = max(1, k // (10 if aggressive else 6))
                while i + k <= len(self.words):
                    if checks > dyn_limit:
                        break
                    win_tokens = self.word_texts[i:i + k]
                    win = " ".join(win_tokens)
                    score = _best_score(s.norm, win)
                    checks += 1
                    if score < score_threshold and s_compact:
                        win_compact = _compact_for_match(win)
                        if win_compact:
                            score = max(score, _best_score(s_compact, win_compact))
                    if score >= score_threshold:
                        end_idx = _end_idx_from_matches(win_tokens, i, i + k)
                        st0 = int(round(self.words[i].start * 1000))
                        en0 = int(round(self.words[end_idx - 1].end * 1000))
                        st = max(st0, self.last_end_ms + 1)
                        en = max(en0, st)
                        if (en - st) < MIN_MATCH_MS:
                            en = st + MIN_MATCH_MS
                        # src_win остаётся как было (или будет форсирован выше)
                        src_win = None
                        return st, en, src_win, end_idx

                    i += step
        return None

    def _try_match_sentence(self, s: Sentence, aggressive: bool) -> bool:
        hit = self._try_match_sentence_core(s, aggressive)
        if hit is None:
            return False
        st, en, src_win, end_idx = hit
        if self.forced_src:
            src_win = self.forced_src
        self.results.append((s, st, en, src_win))

        self.last_start_ms = st
        self.last_end_ms = en  # NEW: track previous end for monotonicity
        self.cursor = min(len(self.words), max(self.cursor, end_idx))
        self.sent_idx += 1
        return True

    def extend_words_and_align(self, new_words: List[Word], src: Optional[str] = None,
                               aggressive: bool = False) -> List[Tuple[Sentence, Optional[int], Optional[int], Optional[str]]]:
        start_len = len(self.results)
        # NEW: replace words for this src instead of blindly appending duplicates
        if src is not None and self.word_srcs:
            keep_w: List[Word] = []
            keep_t: List[str] = []
            keep_s: List[Optional[str]] = []
            for w, t, sname in zip(self.words, self.word_texts, self.word_srcs):
                if sname != src:
                    keep_w.append(w); keep_t.append(t); keep_s.append(sname)
            if len(keep_w) != len(self.words):
                self.words = keep_w
                self.word_texts = keep_t
                self.word_srcs = keep_s
                # after removal, ensure cursor is not past the new pointer for last_end_ms
                self.cursor = min(len(self.words), self._ptr_for_time(self.last_end_ms + 1))

        before = len(self.words)
        for w in new_words:
            self.words.append(w)
            self.word_texts.append(unidecode(w.text.lower()))
            self.word_srcs.append(src)
        eprint(f"[+w] +{len(self.words) - before} T={len(self.words)}")

        while self.sent_idx < len(self.sentences):
            if self.stop_idx is not None and self.sent_idx >= self.stop_idx:
                break
            s = self.sentences[self.sent_idx]
            meta = getattr(s, "meta", None)
            if meta and meta.get("placeholder"):
                self.results.append((s, None, None, None))
                self.sent_idx += 1
                continue
            matched = self._try_match_sentence(s, aggressive=aggressive)
            if not matched:
                break

        return self.results[start_len:]

# -------- JSON writing (items pretty + tokens inline) --------

def _normalize_text(s: str) -> str:
    """Возвращает ИМЕННО нормализованный вариант: sanitize -> norm_text (lower+unidecode+spaces)."""
    vis = sanitize_text(s, for_split=False)  # чистим кавычки/тире/пробелы, но без принудительного lowercase
    return norm_text(vis)                    # делаем lower + unidecode + схлопываем пробелы



def _build_item(idx: int, sentence_obj: Sentence, start_ms, end_ms, src):
    text = getattr(sentence_obj, "text", str(sentence_obj))
    tokens = getattr(sentence_obj, "tokens", None)
    if tokens is None:
        tokens = text.split()
    meta = getattr(sentence_obj, "meta", {}) if sentence_obj is not None else {}
    merged_with = None
    pre_merged_text = None
    order = None
    suborder = None
    chunk = None
    if isinstance(meta, dict):
        merged_with = meta.get("merged_into")
        pre_merged_text = meta.get("pre_merged_text")
        if "order" in meta:
            order = meta.get("order")
        if "suborder" in meta:
            suborder = meta.get("suborder")
        if "chunk" in meta:
            chunk = meta.get("chunk")
    if pre_merged_text is None:
        pre_merged_text = None  # explicitly null when not merged
    item = {
        "idx": idx,
        "text": text,
        "normalized": _normalize_text(text),
        "tokens": tokens,  # ????? ??????? ?? inline JSON ??????
        "start_ms": int(start_ms) if start_ms is not None else None,
        "end_ms": int(end_ms) if end_ms is not None else None,
        "audio_file": src,
        "merged_with": merged_with,
        "preMergedText": pre_merged_text
    }
    # always present, default null
    item["order"] = order
    item["suborder"] = suborder
    item["chunk"] = chunk
    return item


def _sync_progress_meta(meta: Dict[str, Any],
                        results: List[Tuple[Sentence, Optional[int], Optional[int], Optional[str]]],
                        sentences_all: List[Sentence],
                        status: Optional[str] = None) -> None:
    progress = meta.get("progress")
    if not isinstance(progress, dict):
        progress = {}
        meta["progress"] = progress
    if status:
        progress["status"] = status
    else:
        prev_status = progress.get("status")
        if prev_status == "done":
            progress["status"] = "success"
        elif prev_status not in ("processing", "success", "error"):
            progress["status"] = "processing"
    progress["total"] = len(sentences_all)
    progress["current"] = len(results)
    elapsed = max(0.0, time.time() - _SCRIPT_START)
    progress["time_spent"] = round(_TIME_SPENT_BASE + elapsed, 2)


def _reset_progress_on_start(meta: Dict[str, Any]) -> None:
    progress = meta.get("progress")
    if not isinstance(progress, dict):
        progress = {}
        meta["progress"] = progress
    if progress.get("status") == "error":
        progress["status"] = "processing"
    progress["problem_audio"] = None
    progress["unrecognized_text"] = None


def _fail_alignment_and_exit(out_path: Path,
                             aligner: IncrementalAligner,
                             sentences: List[Sentence],
                             meta: Dict[str, Any],
                             audio_name: Optional[str],
                             audio_idx: Optional[int],
                             problem_text: Optional[str] = None) -> None:
    progress = meta.get("progress")
    if not isinstance(progress, dict):
        progress = {}
        meta["progress"] = progress
    if audio_name:
        progress["last_audio"] = audio_name
    if audio_idx is not None:
        progress["problem_audio"] = str(audio_idx)
    elif audio_name is not None:
        progress["problem_audio"] = audio_name
    else:
        progress["problem_audio"] = None
    if problem_text is None:
        if aligner.sent_idx < len(sentences):
            problem_text = sentences[aligner.sent_idx].text
        else:
            problem_text = ""
    progress["unrecognized_text"] = problem_text
    _sync_progress_meta(meta, aligner.results, sentences, status="error")
    save_progress_json(out_path, aligner.results, meta, sentences)
    finish(False)



def _progress_sidecar_path(out_path: Path) -> Path:
    return out_path.with_name(out_path.stem + "Progress" + out_path.suffix)

def _prepare_meta_for_write(meta: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    meta_out: Dict[str, Any] = dict(meta) if isinstance(meta, dict) else {}
    placeholders: Dict[str, str] = {}
    meta_out.pop("processed_audio_totals", None)
    processed = meta_out.get("processed_audio")
    if isinstance(processed, list):
        compact: List[Any] = []
        for i, entry in enumerate(processed):
            if isinstance(entry, (list, tuple)):
                ph = f"__PROC_AUDIO_INLINE_{i}__"
                placeholders[ph] = json.dumps(list(entry), ensure_ascii=False)
                compact.append(ph)
            else:
                compact.append(entry)
        meta_out["processed_audio"] = compact
    return meta_out, placeholders


def _write_progress_meta(out_path: Path, meta: Dict[str, Any]) -> None:
    progress_path = _progress_sidecar_path(out_path)
    meta_out, placeholders = _prepare_meta_for_write(meta)
    payload = {"meta": meta_out}
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    for ph, arr in placeholders.items():
        text = text.replace(f"\"{ph}\"", arr)
    tmp = progress_path.with_suffix(progress_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
    _atomic_replace_with_retry(tmp, progress_path)


def _atomic_replace_with_retry(src: Path, dst: Path, retries: int = 40, base_sleep: float = 0.05) -> None:
    """
    Windows-safe atomic replace:
    when another process reads dst (scp/sftp), os.replace may temporarily fail
    with sharing/access errors. Retry briefly instead of failing whole run.
    """
    last_exc: Optional[BaseException] = None
    for i in range(max(1, retries)):
        try:
            os.replace(src, dst)
            return
        except PermissionError as exc:
            last_exc = exc
        except OSError as exc:
            # 5 = access denied, 32 = sharing violation (Windows)
            if getattr(exc, "winerror", None) in (5, 32):
                last_exc = exc
            else:
                raise
        time.sleep(min(0.25, base_sleep * (1.0 + 0.15 * i)))
    if last_exc is not None:
        raise last_exc
    os.replace(src, dst)


def save_progress_json(out_path: Path,
                       results: List[Tuple[Sentence, Optional[int], Optional[int], Optional[str]]],
                       meta: Dict[str, Any],
                       sentences_all: List[Sentence]) -> None:
    """
    Пишем {"meta": {...}, "items": [...]} с pretty-print (indent=2),
    при этом МАССИВ tokens — всегда в одну строку.
    В items кладём ВСЕ предложения из исходного текста, даже те, у которых ещё нет таймингов.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _sync_progress_meta(meta, results, sentences_all)
    _write_progress_meta(out_path, meta)

    meta_info, meta_placeholders = _prepare_meta_for_write(meta)
    existing_items = _load_existing_items(out_path)

    # Построим индекс таймингов по позициям предложений
    times: Dict[int, Tuple[Optional[int], Optional[int], Optional[str]]] = {}
    # results идут в порядке предложений, поэтому их индекс = позиция
    for i, (sent, st, en, src) in enumerate(results):
        times[i] = (st, en, src)

    items: List[Dict[str, Any]] = []
    last_end_ms: Optional[int] = None
    last_audio_file: Optional[str] = None
    for i, sent in enumerate(sentences_all):
        st, en, src = times.get(i, (None, None, None))
        if st is None and existing_items and i < len(existing_items):
            prev = existing_items[i]
            if isinstance(prev, dict):
                prev_text = (prev.get("text") or "").strip()
                if prev_text and prev_text == sent.text.strip():
                    prev_st = prev.get("start_ms")
                    if prev_st is not None:
                        st = prev_st
                        en = prev.get("end_ms")
                        src = prev.get("audio_file")
        sent_meta = getattr(sent, "meta", None)
        is_placeholder = isinstance(sent_meta, dict) and sent_meta.get("placeholder") is True
        if st is None and is_placeholder and last_end_ms is not None:
            st = last_end_ms
            en = last_end_ms
            if src is None:
                src = last_audio_file
        items.append(_build_item(i, sent, st, en, src))
        if items[-1].get("end_ms") is not None:
            last_end_ms = items[-1]["end_ms"]
            if items[-1].get("audio_file"):
                last_audio_file = items[-1]["audio_file"]

    # Плейсхолдеры для компактных tokens
    placeholders: Dict[str, str] = {}
    placeholders.update(meta_placeholders)
    for i, it in enumerate(items):
        ph = f"__TOKENS_INLINE_{i}__"
        tokens_inline = json.dumps(it["tokens"], ensure_ascii=False, separators=(',', ':'))
        placeholders[ph] = tokens_inline
        it["tokens"] = ph

    payload = {"meta": meta_info, "items": items}

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    for ph, arr in placeholders.items():
        text = text.replace(f"\"{ph}\"", arr)

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
    _atomic_replace_with_retry(tmp, out_path)
    _SAVE_ITEMS_CACHE[str(out_path)] = items

# -------- resume helpers --------

def _load_existing(out_path: Path):
    if not out_path.exists() or out_path.stat().st_size == 0:
        return None
    try:
        with out_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    if not isinstance(data, dict) or "meta" not in data:
        return None

    unified: List[Tuple[Any, Optional[int], Optional[int], Optional[str]]] = []
    if isinstance(data.get("items"), list):
        class _Sent:
            __slots__ = ("text", "tokens")
            def __init__(self, t, toks=None): self.text = t; self.tokens = toks or tokenize_words(norm_text(t))
        for it in data["items"]:
            text = it.get("text", "")
            st = it.get("start_ms", None)
            en = it.get("end_ms", None)
            src = it.get("audio_file", None)
            unified.append((_Sent(text), st, en, src))
    elif isinstance(data.get("results"), list):
        unified = data["results"]

    data["__results_unified__"] = unified
    return data


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _processed_audio_name(entry: Any) -> Optional[str]:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, (list, tuple)) and entry and isinstance(entry[0], str):
        return entry[0]
    return None


def _processed_audio_names(entries: List[Any]) -> List[str]:
    out: List[str] = []
    for entry in entries:
        name = _processed_audio_name(entry)
        if name:
            out.append(name)
    return out


def _load_processed_audio_totals(meta: Dict[str, Any]) -> Dict[str, float]:
    raw = meta.get("processed_audio_totals")
    if isinstance(raw, dict):
        return {str(k): _as_float(v, 0.0) for k, v in raw.items()}
    return {}


def _normalize_processed_audio_list(meta: Dict[str, Any],
                                    processed_raw: List[Any]) -> List[List[Any]]:
    totals = _load_processed_audio_totals(meta)
    out: List[List[Any]] = []
    for entry in processed_raw:
        name = _processed_audio_name(entry)
        if not name:
            continue
        last = 0.0
        total = totals.get(name, 0.0)
        if isinstance(entry, (list, tuple)):
            if len(entry) > 1:
                last = _as_float(entry[1], 0.0)
            if len(entry) > 2:
                total = _as_float(entry[2], total)
            elif len(entry) == 2:
                total = max(total, last)
        if total < last:
            total = last
        totals[name] = max(totals.get(name, 0.0), total)
        out.append([name, round(last, 2), round(total, 2)])
    meta["processed_audio_totals"] = totals
    return out


def _record_processed_audio(meta: Dict[str, Any], name: str, attempt_sec: float) -> None:
    if not isinstance(meta.get("processed_audio"), list):
        meta["processed_audio"] = []
    totals = _load_processed_audio_totals(meta)
    prev_total = totals.get(name, 0.0)
    attempt_sec = max(0.0, float(attempt_sec))
    total = prev_total + attempt_sec
    meta["processed_audio"].append([name, round(attempt_sec, 2), round(total, 2)])
    totals[name] = total
    meta["processed_audio_totals"] = totals


def _sanitize_processed_audio(existing: Dict[str, Any]) -> None:
    meta = existing.get("meta")
    items = existing.get("items")
    if not isinstance(meta, dict) or not isinstance(items, list):
        return
    processed = _normalize_processed_audio_list(meta, list(meta.get("processed_audio") or []))
    meta["processed_audio"] = processed
    if not processed:
        # Rebuild from items when meta is missing/empty.
        seen = set()
        ordered = []
        totals = _load_processed_audio_totals(meta)
        for it in items:
            name = it.get("audio_file")
            if name and it.get("end_ms") is not None and name not in seen:
                ordered.append([name, 0.0, round(totals.get(name, 0.0), 2)])
                seen.add(name)
        if ordered:
            meta["processed_audio"] = ordered
            progress = meta.get("progress")
            if isinstance(progress, dict):
                progress["last_audio"] = ordered[-1][0]
                progress["audios_done"] = len(ordered)
        return

    last_audio = None
    for it in items:
        name = it.get("audio_file")
        if name and it.get("end_ms") is not None:
            last_audio = name

    processed_names = _processed_audio_names(processed)
    if last_audio is None:
        meta["processed_audio"] = []
    elif last_audio in processed_names:
        meta["processed_audio"] = processed[:processed_names.index(last_audio) + 1]
    else:
        seen = set()
        ordered = []
        totals = _load_processed_audio_totals(meta)
        for it in items:
            name = it.get("audio_file")
            if name and name not in seen:
                ordered.append([name, 0.0, round(totals.get(name, 0.0), 2)])
                seen.add(name)
        meta["processed_audio"] = ordered

    progress = meta.get("progress")
    if isinstance(progress, dict):
        last_name = _processed_audio_name(meta["processed_audio"][-1]) if meta["processed_audio"] else None
        progress["last_audio"] = last_name
        progress["audios_done"] = len(meta["processed_audio"])


def _rewind_last_audio(existing: Dict[str, Any]) -> Optional[str]:
    meta = existing.get("meta")
    if not isinstance(meta, dict):
        return None
    processed_audio = _normalize_processed_audio_list(meta, list(meta.get("processed_audio") or []))
    if not processed_audio:
        return None
    last_entry = processed_audio.pop()
    last_audio = _processed_audio_name(last_entry)
    meta["processed_audio"] = processed_audio
    items = existing.get("items")
    if last_audio and isinstance(items, list):
        for it in items:
            if it.get("audio_file") == last_audio:
                it["start_ms"] = None
                it["end_ms"] = None
                it["audio_file"] = None
    return last_audio


def _append_null_and_exit(out_path: Path, aligner, sentences, meta):
    idx = aligner.sent_idx
    if idx < len(sentences):
        problem_text = sentences[idx].text
        aligner.results.append((sentences[idx], None, None, None))
        aligner.sent_idx += 1
        audio_name = None
        progress = meta.get("progress")
        if isinstance(progress, dict):
            audio_name = progress.get("last_audio")
        _fail_alignment_and_exit(out_path, aligner, sentences, meta, audio_name, None, problem_text)
    finish(False)


def _prime_from_existing(existing: Dict[str, Any],
                         sentences: List[Sentence],
                         aligner: IncrementalAligner
                         ) -> Tuple[int, List[List[Any]], float]:
    items = existing.get("items") or []
    meta = existing.get("meta", {})
    if not isinstance(meta, dict):
        meta = {}
    processed_audio = _normalize_processed_audio_list(
        meta,
        list(meta.get("processed_audio") or [])
    )
    progress = meta.get("progress") or {}
    running_offset = float(progress.get("total_duration_sec") or 0.0)

    k = 0
    for k in range(min(len(items), len(sentences))):
        if (items[k].get("text") or "").strip() != sentences[k].text.strip():
            break
        st = items[k].get("start_ms")
        en = items[k].get("end_ms")
        src = items[k].get("audio_file")
        if st is None:
            break
        en_eff = int(en) if en is not None else int(st)
        aligner.results.append((sentences[k], int(st), en_eff, src))
        aligner.last_start_ms = int(st)
        aligner.last_end_ms = en_eff
    else:
        k = min(len(items), len(sentences))
    aligner.sent_idx = k
    # ensure cursor is aligned to last_end boundary
    aligner.cursor = aligner._ptr_for_time(aligner.last_end_ms + 1)

    eprint(f"[resume] {k}/{len(sentences)}")
    return k, processed_audio, running_offset

# -------- path to transcript --------

def _build_output_path(mainFolderPath: Path, channelName: str, videoFolderName: str) -> Path:
    video_root_candidates = [
        Path(mainFolderPath) / channelName / videoFolderName,
        Path(mainFolderPath) / channelName / "VIDEOS" / videoFolderName,
    ]
    video_root = None
    for cand in video_root_candidates:
        if cand.exists():
            video_root = cand; break
    if video_root is None:
        video_root = video_root_candidates[0]
        video_root.mkdir(parents=True, exist_ok=True)

    audio_dir = _find_subdir_case_insensitive(video_root, "VOICE")
    if audio_dir is None:
        audio_dir = video_root / "VOICE"
        audio_dir.mkdir(parents=True, exist_ok=True)

    transcript_dir = _find_subdir_case_insensitive(audio_dir, "transcript")
    if transcript_dir is None:
        transcript_dir = audio_dir / "transcript"
        transcript_dir.mkdir(parents=True, exist_ok=True)

    out_path = transcript_dir / f"{videoFolderName}_transcript.json"
    eprint(f"[path] transcript -> {out_path}")
    return out_path


# -------- textParts helpers --------
_BRACKET_TAG_RE = re.compile(r"\[[^][]*\]")

def _clean_asr_prompt(text: str, max_chars: int) -> str:
    if not text:
        return ""
    text = _BRACKET_TAG_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    if max_chars and max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text
_TEXT_PART_FIELD = "textForVoiceover"

def _read_json_from_path_or_url(path_or_url: str) -> Any:
    try:
        if isinstance(path_or_url, (Path, )):
            with open(str(path_or_url), "r", encoding="utf-8") as fh:
                return json.load(fh)
        url = str(path_or_url)
        from urllib.parse import urlparse
        if urlparse(url).scheme in ("http", "https"):
            from urllib.request import urlopen
            with urlopen(url) as resp:
                data = resp.read()
            try:
                return json.loads(data.decode("utf-8"))
            except Exception:
                return json.loads(data.decode("cp1251", errors="ignore"))
        else:
            with open(url, "r", encoding="utf-8") as fh:
                return json.load(fh)
    except Exception as exc:
        eprint(f"[textParts] load failed: {exc}")
        return None

def _iter_textparts(obj: Any):
    """Deep traversal: gather strings from 'textForVoiceover' fields located inside arrays of dicts."""
    if isinstance(obj, dict):
        if _TEXT_PART_FIELD in obj and isinstance(obj[_TEXT_PART_FIELD], str):
            yield obj[_TEXT_PART_FIELD]
        for v in obj.values():
            yield from _iter_textparts(v)
    elif isinstance(obj, list):
        for el in obj:
            if isinstance(el, dict) and _TEXT_PART_FIELD in el and isinstance(el[_TEXT_PART_FIELD], str):
                yield el[_TEXT_PART_FIELD]
            else:
                yield from _iter_textparts(el)

def _load_text_parts(textPartsPath: str) -> list[str]:
    data = _read_json_from_path_or_url(textPartsPath)
    if data is None:
        return []
    parts = [s for s in _iter_textparts(data) if isinstance(s, str)]
    cleaned = []
    for s in parts:
        s = _BRACKET_TAG_RE.sub("", s)
        s = re.sub(r"\s+", " ", s).strip()
        cleaned.append(s)
    return cleaned

def _load_manga_text_parts(path: str) -> List[Dict[str, Any]]:
    """Collect voiceover texts in display order defined by boxData.order/textForVoiceover."""
    data = _read_json_from_path_or_url(path)
    if data is None:
        return []
    tree = None
    if isinstance(data, dict) and isinstance(data.get("mangaTree"), list):
        tree = data.get("mangaTree")
    elif isinstance(data, list):
        tree = data
    if tree is None:
        eprint(f"[manga] no 'mangaTree' array in: {path}")
        return []

    collected: List[Tuple[int, int, str]] = []
    seq = 0
    for idx, item in enumerate(tree):
        if not isinstance(item, dict):
            continue
        box = item.get("boxData") or {}
        orders = box.get("order") or []
        texts = box.get("textForVoiceover") or []
        if not isinstance(orders, list) or not isinstance(texts, list):
            eprint(f"[manga] skip box idx={idx}: order/textForVoiceover not list")
            continue
        if len(orders) != len(texts) and orders:
            eprint(f"[manga] warn box idx={idx}: order len {len(orders)} != texts len {len(texts)}")
        for ord_raw, txt in zip(orders, texts):
            try:
                ord_val = int(ord_raw)
            except Exception:
                continue
            if not isinstance(txt, str):
                continue
            t_clean = re.sub(r"\s+", " ", txt).strip()
            if not t_clean:
                continue
            collected.append((ord_val, seq, t_clean))
            seq += 1

    if not collected:
        return []

    collected.sort(key=lambda t: (t[0], t[1]))
    ordered = [{"text": txt, "order": ord_val} for ord_val, _seq, txt in collected]
    eprint(f"[manga] ordered parts: {len(ordered)}")
    return ordered


def _split_manga_parts_by_files(parts: List[Dict[str, Any]], text_blocks: List[str], chunk_limit: int
                               ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, int]]]:
    """
    Split manga parts according to the TTS chunking applied per TEXT file.
    Each TEXT block is split with split_balanced_tts(limit), then we map those chunk boundaries
    onto the manga parts stream, slicing parts if a boundary falls inside.
    suborder increments for multiple slices of the same order.
    Returns (parts_with_meta, chunk_ranges) where chunk_ranges are (start,end) indices in the parts list.
    """
    if chunk_limit is None or chunk_limit <= 0 or not text_blocks:
        normalized = [{"text": p.get("text", ""), "order": p.get("order"), "suborder": None, "chunk": None}
                      for p in parts]
        return normalized, []

    chunk_texts: List[str] = []
    for block in text_blocks:
        block = block or ""
        chunk_texts.extend(split_balanced_tts(block, chunk_limit))


    # Stream through original parts
    stream: List[Tuple[str, Optional[int]]] = []
    for p in parts:
        txt = p.get("text", "") if isinstance(p, dict) else str(p or "")
        ord_val = p.get("order") if isinstance(p, dict) else None
        if not txt:
            continue
        if stream:
            stream.append((" ", None))  # separator between parts
        stream.append((txt, ord_val))

    out: List[Dict[str, Any]] = []
    chunk_ranges: List[Tuple[int, int]] = []
    seg_idx = 0
    seg_pos = 0
    order_counters: Dict[int, int] = {}

    for chunk_idx, chunk_txt in enumerate(chunk_texts):
        remaining = len(chunk_txt)
        start_idx = len(out)
        while remaining > 0 and seg_idx < len(stream):
            seg_text, seg_order = stream[seg_idx]
            available = len(seg_text) - seg_pos
            take = min(available, remaining)
            slice_text = seg_text[seg_pos:seg_pos + take]
            # пропускаем чистые пробелы, чтобы не появлялись пустые предложения
            if slice_text.strip():
                if seg_order is not None:
                    sub = order_counters.get(seg_order, 0)
                    order_counters[seg_order] = sub + 1
                else:
                    sub = None
                out.append({
                    "text": slice_text,
                    "order": seg_order,
                    "suborder": sub,
                    "chunk": chunk_idx,
                })
            remaining -= take
            seg_pos += take
            if seg_pos >= len(seg_text):
                seg_idx += 1
                seg_pos = 0
        end_idx = len(out)
        chunk_ranges.append((start_idx, end_idx))

    if seg_idx < len(stream):
        eprint(f"[manga] warn: {len(stream) - seg_idx} segments not consumed by chunking")  # pragma: no cover
    return out, chunk_ranges


class TextPartsManager:
    """Track relationships between original text parts so we can merge/split dynamically."""

    def __init__(self, parts: List[Any], min_chars: int):
        self.parts: List[str] = []
        self.part_meta: List[Dict[str, Any]] = []
        for p in parts:
            if isinstance(p, dict):
                txt = p.get("text", "")
                order = p.get("order")
                suborder = p.get("suborder")
                chunk = p.get("chunk")
            elif isinstance(p, (tuple, list)) and len(p) >= 3:
                txt, order, suborder = p[0], p[1], p[2]
                chunk = None
            else:
                txt, order, suborder = (p or ""), None, None
                chunk = None
            self.parts.append(txt)
            self.part_meta.append({"order": order, "suborder": suborder, "chunk": chunk})
        self.min_chars = max(0, int(min_chars or 0))
        self.prefix_children: List[List[int]] = [[] for _ in parts]
        self.suffix_children: List[List[int]] = [[] for _ in parts]
        self.attached_to: List[Optional[int]] = [None] * len(parts)
        self.merged_into: List[Optional[int]] = [None] * len(parts)
        self.attach_mode: List[Optional[str]] = [None] * len(parts)
        self._initial_merge()

    def _initial_merge(self) -> None:
        n = len(self.parts)
        idx = 0
        while idx < n:
            if self.attached_to[idx] is not None:
                idx += 1
                continue
            carrier = idx
            total_len = len((self.parts[idx] or "").strip())
            idx += 1
            while idx < n:
                if self.part_meta[idx].get("chunk") != self.part_meta[carrier].get("chunk"):
                    break
                next_len = len((self.parts[idx] or "").strip())
                # Основной критерий: наращиваем, пока суммарно не дотянули до min_chars
                if total_len < self.min_chars:
                    self._attach_suffix(idx, carrier)
                    total_len += next_len
                    idx += 1
                    continue
                # Дополнительный критерий: если следующий кусок сам по себе короткий (< min_chars),
                # приклеиваем его к уже достаточно длинному, чтобы не оставлять крошечные хвосты.
                if next_len < self.min_chars:
                    self._attach_suffix(idx, carrier)
                    total_len += next_len
                    idx += 1
                    continue
                # Иначе остановка на границе нормального размера
                break

    def _find_previous_carrier(self, idx: int) -> Optional[int]:
        for j in range(idx - 1, -1, -1):
            if self.attached_to[j] is None:
                return j
        return None

    def _find_next_carrier(self, idx: int) -> Optional[int]:
        for j in range(idx + 1, len(self.parts)):
            if self.attached_to[j] is None:
                return j
        return None

    def _attach_suffix(self, child_idx: int, target_idx: int) -> None:
        self.suffix_children[target_idx].append(child_idx)
        self.attached_to[child_idx] = target_idx
        self.attach_mode[child_idx] = "suffix"
        self.merged_into[child_idx] = target_idx

    def _attach_prefix(self, child_idx: int, target_idx: int, front: bool = False) -> None:
        lst = self.prefix_children[target_idx]
        if front:
            lst.insert(0, child_idx)
        else:
            lst.append(child_idx)
        self.attached_to[child_idx] = target_idx
        self.attach_mode[child_idx] = "prefix"
        self.merged_into[child_idx] = target_idx

    def _compose_text(self, idx: int) -> str:
        parts: List[str] = []
        for child in self.prefix_children[idx]:
            parts.append(self.parts[child])
        parts.append(self.parts[idx])
        for child in self.suffix_children[idx]:
            parts.append(self.parts[child])
        return " ".join(p for p in parts if p).strip()

    def _make_real_sentence(self, idx: int) -> Sentence:
        text = self._compose_text(idx)
        sent = make_sentence(text) if text else Sentence("", "", [])
        sent.meta = {
            "placeholder": False,
            "text_part_index": idx,
            "prefix_children": list(self.prefix_children[idx]),
            "suffix_children": list(self.suffix_children[idx]),
            "merged_into": None,
            "pre_merged_text": self.parts[idx],
            "order": self.part_meta[idx].get("order"),
            "suborder": self.part_meta[idx].get("suborder"),
            "chunk": self.part_meta[idx].get("chunk"),
        }
        return sent

    def _make_placeholder_sentence(self, idx: int) -> Sentence:
        sent = Sentence("", "", [])
        sent.meta = {
            "placeholder": True,
            "text_part_index": idx,
            "attached_to": self.attached_to[idx],
            "attachment_mode": self.attach_mode[idx],
            "original_text": self.parts[idx],
            "merged_into": self.merged_into[idx],
            "pre_merged_text": self.parts[idx],
            "order": self.part_meta[idx].get("order"),
            "suborder": self.part_meta[idx].get("suborder"),
            "chunk": self.part_meta[idx].get("chunk"),
        }
        return sent

    def build_sentences(self) -> List[Sentence]:
        result: List[Sentence] = []
        for idx in range(len(self.parts)):
            if self.attached_to[idx] is None:
                result.append(self._make_real_sentence(idx))
            else:
                result.append(self._make_placeholder_sentence(idx))
        return result

    def refresh_sentence(self, idx: int, sentences: List[Sentence]) -> None:
        if idx < 0 or idx >= len(self.parts):
            return
        if self.attached_to[idx] is None:
            sentences[idx] = self._make_real_sentence(idx)
        else:
            sentences[idx] = self._make_placeholder_sentence(idx)

    def has_suffix_attachment(self, idx: int) -> bool:
        return 0 <= idx < len(self.parts) and bool(self.suffix_children[idx])

    def shift_suffix_to_next(self, idx: int, sentences: List[Sentence]) -> Optional[int]:
        if not self.has_suffix_attachment(idx):
            return None
        child_idx = self.suffix_children[idx].pop(0)
        self.attached_to[child_idx] = None
        self.attach_mode[child_idx] = None
        self.merged_into[child_idx] = None
        target = self._find_next_carrier(child_idx)
        if target is None:
            # revert to previous state
            self.suffix_children[idx].insert(0, child_idx)
            self.attached_to[child_idx] = idx
            self.attach_mode[child_idx] = "suffix"
            self.merged_into[child_idx] = idx
            return None
        self._attach_prefix(child_idx, target, front=True)
        self.refresh_sentence(idx, sentences)
        self.refresh_sentence(target, sentences)
        self.refresh_sentence(child_idx, sentences)
        return child_idx

# -------- main --------

def main():
    global _TIME_SPENT_BASE
    ap = argparse.ArgumentParser(description="Incremental alignment with monotonic timestamps (resumable).")
    ap.add_argument("--mainFolderPath", required=True, type=Path)
    ap.add_argument("--channelName", required=True)
    ap.add_argument("--videoFolderName", required=True)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--language", default=None)
    ap.add_argument("--outFilePath", default=None)
    ap.add_argument("--mode", default="default",
                    help="Режим обработки текста: default|manga (manga читает порядок из mangaTree.boxData.order).")
    ap.add_argument("--chunkLimit", type=int, default=0,
                    help="Длина частей (символов) для разбиения текста под озвучку. 0 = не использовать.")
    ap.add_argument("--minIndexChars", type=int, default=18,
                help="Минимальная длина текста под одним индексом; 0 = выключено")
    ap.add_argument("--minIndexTokens", type=int, default=0,
                    help="Минимальное число токенов под индексом; 0 = выключено")
    ap.add_argument("--textPartsPath", default=None,
                help="Путь или URL к JSON с подготовленными частями текста (поле 'textForVoiceover').")
    ap.add_argument("--textFileName", default=None,
                help="Явно выбрать один файл из TEXTS (имя файла или stem).")
    ap.add_argument("--asrWorkers", type=int, default=1,
                    help="���������� ������� ASR (>=1). 1 = �������� ������, >1 = ����������� WhisperX.")
    ap.add_argument("--minFreeVramGb", type=float, default=0.0,
                    help="����������� �������� VRAM (� ����) ����� ������������ ����� ASR. 0 = �� ��������.")

    ap.add_argument("--asrPrompt", type=int, default=None,
                    help="Use expected chunk text as ASR prompt (1/0). Default: on in manga mode.")
    ap.add_argument("--asrPromptMaxChars", type=int, default=800,
                    help="Max chars of prompt text per audio file.")

    args = ap.parse_args()
    _TIME_SPENT_BASE = 0.0

    try:
        audios = scan_audio(args.mainFolderPath, args.channelName, args.videoFolderName)
        texts  = scan_texts(args.mainFolderPath, args.channelName, args.videoFolderName, args.textFileName)

        out_path = Path(args.outFilePath) if args.outFilePath else _build_output_path(
            args.mainFolderPath, args.channelName, args.videoFolderName
        )

        chunk_ranges: List[Tuple[int, int]] = []
        chunk_texts: List[str] = []
        text_parts_manager: Optional[TextPartsManager] = None
        mode = (getattr(args, "mode", "default") or "default").strip().lower()

        # === ????? ??????? ?????? ===
        if mode == "manga":
            if not args.textPartsPath:
                eprint("[manga] require --textPartsPath pointing to mangaTree JSON")
                sentences = []
            else:
                parts = _load_manga_text_parts(args.textPartsPath)
                if not parts:
                    eprint(f"[manga] no ordered parts from: {args.textPartsPath}")
                    sentences = []
                else:
                    if not texts:
                        eprint("[manga] no TEXTS found for chunking")
                        sentences = []
                    else:
                        sentences, chunk_ranges, chunk_texts = _build_manga_sentences_from_texts(texts, args.chunkLimit)
                        eprint(f"[manga] chunks from TEXTS: {len(chunk_ranges)}, sentences={len(sentences)}")
                        _assign_orders_to_sentences(sentences, parts)
                        manga_min_chars = max(0, int(args.minIndexChars or 0))
                        manga_min_tokens = max(0, int(args.minIndexTokens or 0))
                        _coalesce_short_sentences_with_meta(sentences, manga_min_chars, manga_min_tokens)
                        _assign_suborders(sentences)
                        chunk_ranges = _rebuild_chunk_ranges(sentences)
                        eprint(f"[manga] min_chars={manga_min_chars}")
        elif args.textPartsPath:
            parts = _load_text_parts(args.textPartsPath)
            if not parts:
                eprint(f"[textParts] no usable '{_TEXT_PART_FIELD}' found in: {args.textPartsPath}")
                sentences = []
            else:
                eprint(f"[textParts] loaded {len(parts)} parts")
                text_parts_manager = TextPartsManager(parts, MIN_SENT_CHARS)
                sentences = text_parts_manager.build_sentences()
        # === АВТО-РАЗБИВКА (как раньше) ===
        elif args.chunkLimit and args.chunkLimit > 0:
            if not texts:
                sentences = []
            else:
                sentences, chunk_ranges, chunk_texts = _build_manga_sentences_from_texts(texts, args.chunkLimit)
                eprint(f"[chunks] from TEXTS: {len(chunk_ranges)}, sentences={len(sentences)}")
                min_chars = max(0, int(args.minIndexChars or 0))
                min_tokens = max(0, int(args.minIndexTokens or 0))
                _coalesce_short_sentences_with_meta(sentences, min_chars, min_tokens)
                chunk_ranges = _rebuild_chunk_ranges(sentences)
        else:
            sentences = build_sentence_stream(texts)
            split_multi_sentences_inplace(sentences)
            coalesce_short_sentences_inplace(sentences, args.minIndexChars, args.minIndexTokens)




        meta: Dict[str, Any] = {
            "channel": args.channelName,
            "video": args.videoFolderName,
            "language": args.language or "",
            "num_sentences_total": len(sentences),
            "progress": {
                "status": "processing",
                "total": len(sentences),
                "current": 0,
                "audios_total": len(audios),
                "audios_done": 0,
                "last_audio": None,
                "total_duration_sec": 0.0
            },
            "processed_audio": [],
            "processed_audio_totals": {}
        }
        aligner = IncrementalAligner(sentences, min_score=75, max_checks=4000, dynamic_factor=20,
                                     text_parts_manager=text_parts_manager)

        # helper точной длительности
        def _actual_duration_seconds(p: Path, fallback: float) -> float:
            import subprocess, wave
            try:
                cmd = [
                    "ffprobe",
                    "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "default=nw=1:nk=1",
                    str(p),
                ]
                out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
                dur = float(out.strip())
                if dur > 0: return dur
            except Exception:
                pass
            try:
                if p.suffix.lower() == ".wav":
                    with wave.open(str(p), "rb") as wf:
                        frames = wf.getnframes(); rate = wf.getframerate()
                        if rate > 0:
                            dur = frames / float(rate)
                            if dur > 0: return dur
            except Exception:
                pass
            return float(fallback or 0.0)

        running_offset = 0.0
        existing = _load_existing(out_path)
        skip_audio_names: set[str] = set()
        processed_audio: List[List[Any]] = []
        if existing:
            _sanitize_processed_audio(existing)
            rewound_audio = _rewind_last_audio(existing)
            if rewound_audio:
                eprint(f"[resume] rewind last audio: {rewound_audio}")
            skipped, processed_audio, running_offset = _prime_from_existing(existing, sentences, aligner)
            meta.update(existing.get("meta", {}))
            meta["num_sentences_total"] = len(sentences)
            meta["processed_audio"] = processed_audio
            _reset_progress_on_start(meta)
            try:
                _TIME_SPENT_BASE = float(meta.get("progress", {}).get("time_spent") or 0.0)
            except Exception:
                _TIME_SPENT_BASE = 0.0
            _sync_progress_meta(meta, aligner.results, sentences, status="processing")
            meta["progress"]["audios_total"] = len(audios)
            running_offset = 0.0
            processed_audio_names = _processed_audio_names(processed_audio)
            if processed_audio_names:
                audio_by_name = {a.path.name: a.path for a in audios}
                for name in processed_audio_names:
                    p = audio_by_name.get(name)
                    if p:
                        running_offset += _actual_duration_seconds(p, 0.0)
            meta["progress"]["total_duration_sec"] = running_offset
            meta["progress"]["audios_done"] = len(processed_audio)
            meta["progress"]["last_audio"] = processed_audio_names[-1] if processed_audio_names else None
            skip_audio_names = set(processed_audio_names)
            eprint(f"[init:resume] {out_path} (resume, {len(skip_audio_names)} files already processed)")
            save_progress_json(out_path, aligner.results, meta, sentences)
        else:
            # Пишем каркас JSON с ВСЕМИ предложениями (тайминги None)
            _reset_progress_on_start(meta)
            save_progress_json(out_path, aligner.results, meta, sentences)
            eprint(f"[init:new] {out_path} (created)")

        # Нет аудио/текста — ошибка
        if not audios or not sentences:
            if not audios: eprint("[warn] no audio")
            if not sentences: eprint("[warn] no text")
            _sync_progress_meta(meta, aligner.results, sentences, status="error")
            save_progress_json(out_path, aligner.results, meta, sentences)
            finish(False)

        # ---- Основной проход по аудиофайлам с удержанием границ файла ----
        MARGIN_MS = 1500  # 6 секунд допуск на паузы

        def _last_end_ms_for_file(results_list, fname: str, file_start_ms: int) -> int:
            last = file_start_ms
            for _s, st, en, src in results_list:
                if src == fname and en is not None:
                    if en > last: last = en
            return last
        def _avg_ms_per_char(results_list) -> Optional[float]:
            total_ms = 0
            total_chars = 0
            for _s, st, en, _src in results_list:
                if st is None or en is None:
                    continue
                txt = (_s.text or "").strip() if _s is not None else ""
                if not txt:
                    continue
                dur = max(0, int(en) - int(st))
                if dur <= 0:
                    continue
                total_ms += dur
                total_chars += len(txt)
            if total_chars <= 0:
                return None
            return total_ms / total_chars

        def _weighted_durations(total_ms: int, weights: List[int], min_each: int) -> List[int]:
            n = len(weights)
            if n == 0:
                return []
            if total_ms <= 0:
                return [0] * n
            min_each_f = float(min_each)
            if total_ms < min_each_f * n:
                min_each_f = total_ms / n
            weight_sum = float(sum(weights)) or 1.0
            remaining = max(0.0, total_ms - (min_each_f * n))
            raw = [min_each_f + (remaining * (w / weight_sum)) for w in weights]
            out = [int(round(x)) for x in raw]
            # fix rounding drift
            diff = int(round(total_ms - sum(out)))
            if diff != 0:
                step = 1 if diff > 0 else -1
                i = 0
                while diff != 0 and i < n * 2:
                    idx = i % n
                    if out[idx] + step >= 0:
                        out[idx] += step
                        diff -= step
                    i += 1
            if total_ms > 0:
                for i in range(n):
                    if out[i] < 1:
                        out[i] = 1
                total = sum(out)
                if total != total_ms:
                    out[-1] += (total_ms - total)
            return out

        def _approximate_missing_chunk(aligner_obj: IncrementalAligner,
                                       sentences_list: List[Sentence],
                                       chunk_end_idx: Optional[int],
                                       audio_name: str,
                                       file_start_ms: int,
                                       file_end_ms: int) -> bool:
            if chunk_end_idx is None:
                return False
            start_idx = aligner_obj.sent_idx
            if start_idx >= chunk_end_idx:
                return False
            last_end = _last_end_ms_for_file(aligner_obj.results, audio_name, file_start_ms)
            base_start = max(file_start_ms, last_end + 1)
            if base_start > file_end_ms:
                base_start = file_end_ms
            remaining_ms = max(0, file_end_ms - base_start)
            pending = sentences_list[start_idx:chunk_end_idx]
            if not pending:
                return False
            weights = [max(1, len((s.text or "").strip())) for s in pending]
            avg = _avg_ms_per_char(aligner_obj.results)
            if avg is None or avg <= 0:
                avg = remaining_ms / max(1, sum(weights))
            expected = avg * sum(weights)
            if expected > 0 and abs(expected - remaining_ms) > max(1000, expected * 0.5):
                eprint(f"[fallback] warn: expected {expected:.0f}ms, remaining {remaining_ms}ms")
            durs = _weighted_durations(remaining_ms, weights, MIN_MATCH_MS)
            cur = base_start
            for s, dur in zip(pending, durs):
                st = cur
                en = cur + dur
                aligner_obj.results.append((s, st, en, audio_name))
                cur = en
            # force last end to file end
            if aligner_obj.results:
                s_last, st_last, _en_last, src_last = aligner_obj.results[-1]
                aligner_obj.results[-1] = (s_last, st_last, file_end_ms, src_last)
                cur = file_end_ms
            aligner_obj.sent_idx = chunk_end_idx
            if aligner_obj.results:
                aligner_obj.last_start_ms = aligner_obj.results[-1][1]
                aligner_obj.last_end_ms = aligner_obj.results[-1][2]
                aligner_obj.cursor = aligner_obj._ptr_for_time(aligner_obj.last_end_ms + 1)
            eprint(f"[fallback] approx timings for idx {start_idx}-{chunk_end_idx - 1} in {audio_name}")
            return True

        def _recover_with_anchor(aligner_obj: IncrementalAligner,
                                 sentences_list: List[Sentence],
                                 chunk_end_idx: Optional[int],
                                 audio_name: str,
                                 file_start_ms: int,
                                 file_end_ms: int,
                                 lookahead: int = RECOVER_LOOKAHEAD_SENTENCES) -> bool:
            if chunk_end_idx is None:
                return False
            start_idx = aligner_obj.sent_idx
            if start_idx >= chunk_end_idx:
                return False
            max_idx = min(chunk_end_idx, start_idx + max(1, lookahead) + 1)
            anchor_hit = None  # (idx, (st,en,src,end_idx))
            for j in range(start_idx + 1, max_idx):
                hit = aligner_obj._try_match_sentence_core(sentences_list[j], aggressive=True)
                if hit is not None:
                    anchor_hit = (j, hit)
                    break
            if anchor_hit is None:
                return False
            anchor_idx, (st, en, _src_win, end_idx) = anchor_hit
            if st is None or en is None:
                return False
            last_end = _last_end_ms_for_file(aligner_obj.results, audio_name, file_start_ms)
            base_start = max(file_start_ms, last_end + 1)
            anchor_start = max(base_start, st)
            pending = sentences_list[start_idx:anchor_idx]
            if pending:
                min_total = MIN_MATCH_MS * len(pending)
                if (anchor_start - base_start) < min_total:
                    anchor_start = min(file_end_ms, base_start + min_total)
                gap_ms = max(0, anchor_start - base_start)
                weights = [max(1, len((s.text or "").strip())) for s in pending]
                durs = _weighted_durations(gap_ms, weights, MIN_MATCH_MS)
                cur = base_start
                for s, dur in zip(pending, durs):
                    st_i = cur
                    en_i = cur + dur
                    aligner_obj.results.append((s, st_i, en_i, audio_name))
                    cur = en_i
            else:
                cur = base_start
            # commit anchor using ASR match, but keep monotonicity
            st = max(anchor_start, st, cur)
            en = max(en, st + MIN_MATCH_MS)
            if en > file_end_ms:
                en = file_end_ms
                if en < st:
                    st = max(file_start_ms, en - MIN_MATCH_MS)
            aligner_obj.results.append((sentences_list[anchor_idx], st, en, audio_name))
            aligner_obj.sent_idx = anchor_idx + 1
            aligner_obj.last_start_ms = st
            aligner_obj.last_end_ms = en
            aligner_obj.cursor = min(len(aligner_obj.words), max(aligner_obj.cursor, end_idx))
            eprint(f"[recover] anchor idx={anchor_idx} after gap {start_idx}-{anchor_idx - 1} in {audio_name}")
            return True

        asr_workers_requested = max(1, int(args.asrWorkers or 1))
        jobs_pending_total = sum(1 for a in audios if a.path.name not in skip_audio_names)
        worker_slots = max(1, min(asr_workers_requested, jobs_pending_total or 1))
        if worker_slots > 1:
            if worker_slots != asr_workers_requested:
                eprint(f"[asr-workers] {worker_slots}/{asr_workers_requested}")
            else:
                eprint(f"[asr-workers] {worker_slots}")

        min_free_vram_bytes = int(max(0.0, args.minFreeVramGb or 0.0) * (1024 ** 3))

        use_prompt = bool(args.asrPrompt) if args.asrPrompt is not None else (mode == "manga")
        prompt_max_chars = max(0, int(args.asrPromptMaxChars or 0))
        if use_prompt and chunk_texts and len(chunk_texts) != len(audios):
            eprint(f"[asr-prompt] warn: chunks={len(chunk_texts)} audios={len(audios)}; disabling prompts")
            use_prompt = False

        def _prompt_for_index(idx0: int) -> Optional[str]:
            if not use_prompt or not chunk_texts or idx0 < 0 or idx0 >= len(chunk_texts):
                return None
            prompt = _clean_asr_prompt(chunk_texts[idx0], prompt_max_chars)
            return prompt if prompt else None

        def _run_asr_job(audio_item: AudioItem, prompt: Optional[str]):
            if min_free_vram_bytes > 0 and args.device == "cuda":
                _wait_for_min_vram(min_free_vram_bytes)
            start_ts = time.time()
            words, lang, asr_dur = transcribe_words(audio_item.path, args.device, args.language, prompt=prompt)
            asr_time = max(0.0, time.time() - start_ts)
            real_dur = _actual_duration_seconds(audio_item.path, asr_dur)
            return {"words": words, "language": lang, "real_duration": real_dur, "asr_time": asr_time}

        with ThreadPoolExecutor(max_workers=worker_slots) as executor:
            pending: Dict[str, Tuple[AudioItem, Any]] = {}
            next_submit_idx = 0

            def _fill_pending():
                nonlocal next_submit_idx
                if worker_slots <= 0:
                    return
                while len(pending) < worker_slots and next_submit_idx < len(audios):
                    idx0 = next_submit_idx
                    candidate = audios[idx0]
                    next_submit_idx += 1
                    if candidate.path.name in skip_audio_names:
                        continue
                    prompt = _prompt_for_index(idx0)
                    pending[candidate.path.name] = (candidate, executor.submit(_run_asr_job, candidate, prompt))

            _fill_pending()
            for idx, a in enumerate(audios, 1):
                _fill_pending()
                # �᫨ �ᯮ��㥬 ����: i-� 䠩� ����砥� ᢮� �������� �।�������
                if chunk_ranges and (idx - 1) < len(chunk_ranges):
                    chunk_start, chunk_end = chunk_ranges[idx - 1]
                else:
                    chunk_start, chunk_end = None, None

                if a.path.name in skip_audio_names:
                    meta["progress"]["audios_done"] = idx
                    meta["progress"]["last_audio"] = a.path.name
                    meta["progress"]["total_duration_sec"] = running_offset
                    _sync_progress_meta(meta, aligner.results, sentences, status="processing")
                    _write_progress_meta(out_path, meta)
                    continue

                job_pair = pending.pop(a.path.name, None)
                if job_pair is None:
                    prompt = _prompt_for_index(idx - 1)
                    job_pair = (a, executor.submit(_run_asr_job, a, prompt))
                file_start_sent_idx = aligner.sent_idx

                # �ਢ�뢠�� ⠩����� � ⥪�饬� 䠩�� (��᫥ �஢�ન ᪨��)
                aligner.forced_src = a.path.name
                aligner.stop_idx  = chunk_end

                eprint(f"[run] {idx}/{len(audios)} {a.path.name}")
                job_result = job_pair[1].result()
                words = job_result["words"]
                lang = job_result["language"]
                real_dur = job_result["real_duration"]
                attempt_time = _as_float(job_result.get("asr_time"), 0.0)
                if lang:
                    meta["language"] = lang
                # heard_preview = _summarize_words(words, limit=40)
                heard_preview = _summarize_words(words)
                eprint(f"[heard] {a.path.name}: {heard_preview}")

                file_start_sec = running_offset
                file_start_ms = int(round(file_start_sec * 1000))
                file_end_ms = int(round((file_start_sec + real_dur) * 1000))

                # ��ࢠ� ����� ᫮�
                shifted = [Word(w.text, w.start + file_start_sec, w.end + file_start_sec) for w in words]
                # Use last ASR word time to tolerate trailing silence at file end.
                last_word_end_ms = file_start_ms
                if shifted:
                    last_word_end_ms = int(round(shifted[-1].end * 1000))
                prev_sent_idx = aligner.sent_idx
                added = aligner.extend_words_and_align(shifted, src=a.path.name, aggressive=False)
                eprint(f"[+s] +{len(added)} T={len(aligner.results)}/{len(sentences)}")
                next_idx_msg = f"{aligner.sent_idx}/{len(sentences)}" if sentences else "0/0"
                eprint(f"[idx] next_sentence={next_idx_msg} chunk_limit={chunk_end if chunk_end is not None else '-'}")
                save_progress_json(out_path, aligner.results, meta, sentences)

                # ??????: ???? ??????????? ?? ??????? ? ??????? ????? ? ?????? ? ?????????.
                used_fallback = False
                while True:
                    if aligner.sent_idx >= len(sentences):
                        break
                    reached_chunk_end = (chunk_end is not None and aligner.sent_idx >= chunk_end)
                    if reached_chunk_end:
                        break
                    last_end_ms = _last_end_ms_for_file(aligner.results, a.path.name, file_start_ms)
                    still_in_this_file = (last_end_ms + MARGIN_MS) < last_word_end_ms
                    expected_more_in_chunk = (chunk_end is not None and aligner.sent_idx < chunk_end)
                    progressed = (aligner.sent_idx > prev_sent_idx)
                    if not progressed:
                        if expected_more_in_chunk:
                            if _recover_with_anchor(aligner, sentences, chunk_end, a.path.name, file_start_ms, file_end_ms):
                                used_fallback = True
                                prev_sent_idx = max(-1, aligner.sent_idx - 1)
                                continue
                            if _approximate_missing_chunk(aligner, sentences, chunk_end, a.path.name, file_start_ms, file_end_ms):
                                used_fallback = True
                                break
                        if expected_more_in_chunk and still_in_this_file:
                            _fail_alignment_and_exit(out_path, aligner, sentences, meta, a.path.name, idx)
                        if expected_more_in_chunk and not still_in_this_file:
                            eprint(f"[warn] chunk underflow in {a.path.name}: expected < {chunk_end}, got {aligner.sent_idx}")
                        break
                    if not still_in_this_file:
                        if expected_more_in_chunk:
                            if not used_fallback:
                                if _recover_with_anchor(aligner, sentences, chunk_end, a.path.name, file_start_ms, file_end_ms):
                                    used_fallback = True
                                    prev_sent_idx = max(-1, aligner.sent_idx - 1)
                                    continue
                                _approximate_missing_chunk(aligner, sentences, chunk_end, a.path.name, file_start_ms, file_end_ms)
                                used_fallback = True
                            eprint(f"[warn] chunk underflow in {a.path.name}: expected < {chunk_end}, got {aligner.sent_idx}")
                        break
                    eprint(f"[advance] file={a.path.name} -> idx={aligner.sent_idx}/{len(sentences)}")
                    prev_sent_idx = aligner.sent_idx
                    _ = aligner.extend_words_and_align([], src=None, aggressive=False)
                    save_progress_json(out_path, aligner.results, meta, sentences)

                _ = aligner.extend_words_and_align([], src=None, aggressive=False)

                # ��� ����� ������ �����
                aligner.forced_src = None
                aligner.stop_idx  = None

                running_offset += real_dur
                meta["progress"]["audios_done"] = idx
                meta["progress"]["last_audio"] = a.path.name
                meta["progress"]["total_duration_sec"] = running_offset
                if aligner.sent_idx > file_start_sent_idx:
                    _record_processed_audio(meta, a.path.name, attempt_time)
                else:
                    eprint(f"[warn] no progress in {a.path.name}; not marking as processed")
                save_progress_json(out_path, aligner.results, meta, sentences)
                eprint("[save]")
                if args.device == "cuda":
                    _clear_cuda_cache()


        # Если ещё осталось ненайденное предложение — ставим null и выходим с false
        if aligner.sent_idx < len(sentences):
            _append_null_and_exit(out_path, aligner, sentences, meta)

        # Всё найдено
        _sync_progress_meta(meta, aligner.results, sentences, status="success")
        save_progress_json(out_path, aligner.results, meta, sentences)
        eprint("[done]")
        finish(True)

    except KeyboardInterrupt:
        eprint("[int] saving…")
        try:
            out_path = Path(args.outFilePath) if args.outFilePath else _build_output_path(
                args.mainFolderPath, args.channelName, args.videoFolderName
            )
            if 'meta' in locals() and 'aligner' in locals() and 'sentences' in locals():
                _sync_progress_meta(meta, aligner.results, sentences, status="error")
                save_progress_json(out_path, aligner.results, meta, sentences)
                eprint("[saved]")
        except Exception:
            pass
        finish(False)
    except Exception as e:
        eprint(f"[fatal] {e}")
        try:
            out_path = Path(args.outFilePath) if args.outFilePath else _build_output_path(
                args.mainFolderPath, args.channelName, args.videoFolderName
            )
            if 'meta' in locals() and 'aligner' in locals() and 'sentences' in locals():
                _sync_progress_meta(meta, aligner.results, sentences, status="error")
                save_progress_json(out_path, aligner.results, meta, sentences)
        except Exception:
            pass
        finish(False)


if __name__ == "__main__":
    main()
