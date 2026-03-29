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
  --outFilePath    (explicit JSON path; default -> .../AUDIO/transcript/<video>_transcript.json)
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
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

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
MIN_MATCH_MS = 200  # защита от нулевых/почти нулевых совпадений (200 мс)

WORD_RE = re.compile(r"[\w\-']+", re.UNICODE)
_num_chunk = re.compile(r"(\d+)")

# --- small stderr logger ---
def eprint(*a, **k):
    msg = " ".join(str(x) for x in a)
    try: print(msg, file=sys.stderr, **k)
    except Exception: sys.stderr.write(msg + "\n")

# --- strict final line ---
def final_log(success: bool) -> None:
    """Print only 'true' or 'false' as the very last line and exit immediately."""
    try:
        sys.stdout.write(("true\n" if success else "false\n"))
        sys.stdout.flush()
    finally:
        os._exit(0)

# --- text utils ---
def norm_text(s: str) -> str:
    s = unidecode(s.lower().strip())
    return re.sub(r"\s+", " ", s)

def tokenize_words(s: str) -> List[str]:
    return [m.group(0).lower() for m in WORD_RE.finditer(unidecode(s))]

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
        ad = _find_subdir_case_insensitive(base, "AUDIO")
        if not ad: continue
        for p in ad.iterdir():
            if p.is_file() and p.suffix.lower() in AUDIO_EXTS:
                files.append(p)
    files.sort(key=lambda q: natural_key(q.name))
    return [AudioItem(p) for p in files]

# -------- TEXTS --------
def scan_texts(mainFolderPath: Path, channelName: str, videoFolderName: str) -> List[str]:
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
        files = [p for p in d.iterdir() if p.is_file() and p.suffix.lower() in ALLOWED]
        return sorted(files, key=lambda p: natural_key(p.name))

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

    eprint(f"[texts] found {len(paths)} file(s): " + ", ".join(p.name for p in paths))

    blocks: List[str] = []
    for p in paths:
        try:
            blocks.append(p.read_text(encoding="utf-8"))
        except Exception:
            blocks.append(p.read_text(encoding="cp1251", errors="ignore"))
    return blocks

# ---------- План-сегментация "как в JS": splitBalanced ----------

_RX_SENT_END = re.compile(r'([.!?…]+[)"»\]]*\s)')

def _find_break(remaining: str, limit: int) -> int:
    window = remaining[:limit]
    best = -1
    for m in _RX_SENT_END.finditer(window):
        best = m.end()
    if best >= int(limit * 0.6):
        return best
    last_space = max(window.rfind(' '), window.rfind('\n'), window.rfind('\t'))
    if last_space > 0:
        return last_space + 1
    return limit

def _split_into_sentences_with_paras(source: str) -> List[str]:
    out: List[str] = []
    paras = re.split(r'\n\s*\n', source)
    for p_idx, p in enumerate(paras):
        p = (p or '').strip()
        if not p:
            continue
        i = 0
        rx = re.compile(r'([.!?…]+[)"»\]]*)(\s+|$)')
        for m in rx.finditer(p):
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

# Silence third-party spam during ASR
@contextlib.contextmanager
def _suppress_asr_noise():
    targets = [
        "lightning", "pytorch_lightning", "pyannote", "pyannote.audio", "whisperx",
        "transformers", "urllib3", "numba", "torch"
    ]
    old_levels = {}
    for name in targets:
        lg = logging.getLogger(name)
        old_levels[name] = lg.level
        lg.setLevel(logging.ERROR)
    old_filters = warnings.filters[:]
    warnings.simplefilter("ignore")
    devnull = open(os.devnull, "w")
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout = devnull
        sys.stderr = devnull
        yield
    finally:
        sys.stdout = old_out
        sys.stderr = old_err
        try: devnull.close()
        except Exception: pass
        warnings.filters[:] = old_filters
        for name in targets:
            logging.getLogger(name).setLevel(old_levels[name])


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
                  model_name: str, compute_type: str, batch_size: int):
    with _suppress_asr_noise():
        model = whisperx.load_model(model_name, device, compute_type=compute_type, language=language)
        audio = whisperx.load_audio(str(audio_path))
        result = model.transcribe(audio, batch_size=batch_size, language=language)

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


def transcribe_words(audio_path: Path, device: str, language: Optional[str]):
    if whisperx is None:
        eprint(f"[asr] skip {audio_path.name}")
        return [], (language or "unk"), 0.0

    if device == "cuda":
        attempts = [
            ("large-v2","float16",8),("large-v2","float16",4),("large-v2","float16",2),("large-v2","float16",1),
            ("medium","float16",4),("medium","float16",2),("medium","float16",1),
            ("small","float16",2),("small","float16",1),
            ("base","float16",1),
        ]
    else:
        attempts = [("small","float32",1),("base","float32",1)]

    last_err=None
    for model_name,ctype,bs in attempts:
        try:
            eprint(f"[asr] {audio_path.name} {model_name}/{ctype} bs={bs} dev={device}")
            return _try_whisperx(audio_path, device, language, model_name, ctype, bs)
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
                 min_score: int = 75, max_checks: int = 4000, dynamic_factor: int = 20):
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

    def _try_match_sentence_core(self, s: Sentence, aggressive: bool) -> Optional[Tuple[int,int,Optional[str]]]:
        L = max(1, len(s.tokens))
        base_lens = [L, max(1, int(L * 0.8)), max(1, int(L * 1.2))]
        expansions = ([0.6, 0.8, 1.0, 1.3, 1.6, 2.0, 2.5] if aggressive
                      else [1.0, 1.3, 1.6, 2.0])
        dyn_limit = max(self.max_checks, self.dynamic_factor * max(0, len(self.words) - self.cursor))
        if aggressive:
            dyn_limit = max(dyn_limit, 200_000)

        checks = 0
        for exp in expansions:
            cand: List[int] = []
            seen = set()
            for k0 in base_lens:
                k = max(1, int(k0 * exp))
                if k not in seen and k <= (len(self.words) - self.cursor):
                    cand.append(k); seen.add(k)
            if not cand:
                continue
            for k in cand:
                i = self.cursor
                step = max(1, k // (10 if aggressive else 6))
                while i + k <= len(self.words):
                    if checks > dyn_limit:
                        break
                    score = _best_score(s.norm, self._window_str(i, i + k))
                    checks += 1
                    if score >= (62 if aggressive else self.min_score):
                        st0 = int(round(self.words[i].start * 1000))
                        en0 = int(round(self.words[i + k - 1].end * 1000))
                        st = max(st0, self.last_end_ms + 1)
                        en = max(en0, st)
                        if (en - st) < MIN_MATCH_MS:
                            i += step
                            continue
                        # src_win остаётся как было (или будет форсирован выше)
                        src_win = None
                        return st, en, src_win


                    i += step
        return None

    def _try_match_sentence(self, s: Sentence, aggressive: bool) -> bool:
        hit = self._try_match_sentence_core(s, aggressive)
        if hit is None:
            return False
        st, en, src_win = hit
        if self.forced_src:
            src_win = self.forced_src
        self.results.append((s, st, en, src_win))

        self.last_start_ms = st
        self.last_end_ms = en  # NEW: track previous end for monotonicity
        self.cursor = min(len(self.words), self._ptr_for_time(self.last_end_ms + 1))
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
    return {
        "idx": idx,
        "text": text,
        "normalized": _normalize_text(text),
        "tokens": tokens,  # позже заменим на inline JSON массив
        "start_ms": int(start_ms) if start_ms is not None else None,
        "end_ms": int(end_ms) if end_ms is not None else None,
        "audio_file": src
    }


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

    # Построим индекс таймингов по позициям предложений
    times: Dict[int, Tuple[Optional[int], Optional[int], Optional[str]]] = {}
    # results идут в порядке предложений, поэтому их индекс = позиция
    for i, (sent, st, en, src) in enumerate(results):
        times[i] = (st, en, src)

    items: List[Dict[str, Any]] = []
    for i, sent in enumerate(sentences_all):
        st, en, src = times.get(i, (None, None, None))
        items.append(_build_item(i, sent, st, en, src))

    # Плейсхолдеры для компактных tokens
    placeholders: Dict[str, str] = {}
    for i, it in enumerate(items):
        ph = f"__TOKENS_INLINE_{i}__"
        tokens_inline = json.dumps(it["tokens"], ensure_ascii=False, separators=(',', ':'))
        placeholders[ph] = tokens_inline
        it["tokens"] = ph

    payload = {"meta": meta, "items": items}

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    for ph, arr in placeholders.items():
        text = text.replace(f"\"{ph}\"", arr)

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, out_path)

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


def _append_null_and_exit(out_path: Path, aligner, sentences, meta):
    idx = aligner.sent_idx
    if idx < len(sentences):
        aligner.results.append((sentences[idx], None, None, None))
        aligner.sent_idx += 1
        save_progress_json(out_path, aligner.results, meta, sentences)
    final_log(False)


def _prime_from_existing(existing: Dict[str, Any],
                         sentences: List[Sentence],
                         aligner: IncrementalAligner
                         ) -> Tuple[int, List[str], float]:
    items = existing.get("items") or []
    processed_audio = list(existing.get("meta", {}).get("processed_audio") or [])
    progress = existing.get("meta", {}).get("progress") or {}
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

    audio_dir = _find_subdir_case_insensitive(video_root, "AUDIO")
    if audio_dir is None:
        audio_dir = video_root / "AUDIO"
        audio_dir.mkdir(parents=True, exist_ok=True)

    transcript_dir = _find_subdir_case_insensitive(audio_dir, "transcript")
    if transcript_dir is None:
        transcript_dir = audio_dir / "transcript"
        transcript_dir.mkdir(parents=True, exist_ok=True)

    out_path = transcript_dir / f"{videoFolderName}_transcript.json"
    eprint(f"[path] transcript -> {out_path}")
    return out_path

# -------- main --------

def main():
    ap = argparse.ArgumentParser(description="Incremental alignment with monotonic timestamps (resumable).")
    ap.add_argument("--mainFolderPath", required=True, type=Path)
    ap.add_argument("--channelName", required=True)
    ap.add_argument("--videoFolderName", required=True)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--language", default=None)
    ap.add_argument("--outFilePath", default=None)
    ap.add_argument("--chunkLimit", type=int, default=0,
                    help="Длина частей (символов) для разбиения текста под озвучку. 0 = не использовать.")
    ap.add_argument("--minIndexChars", type=int, default=18,
                help="Минимальная длина текста под одним индексом; 0 = выключено")
    ap.add_argument("--minIndexTokens", type=int, default=0,
                    help="Минимальное число токенов под индексом; 0 = выключено")

    args = ap.parse_args()

    try:
        audios = scan_audio(args.mainFolderPath, args.channelName, args.videoFolderName)
        texts  = scan_texts(args.mainFolderPath, args.channelName, args.videoFolderName)

        out_path = Path(args.outFilePath) if args.outFilePath else _build_output_path(
            args.mainFolderPath, args.channelName, args.videoFolderName
        )

        chunk_ranges: List[Tuple[int, int]] = []
        if args.chunkLimit and args.chunkLimit > 0:
            sentences: List[Sentence] = []
            total_chunks = 0
            for raw in texts:
                parts = split_balanced(raw, args.chunkLimit)
                total_chunks += len(parts)
                for part in parts:
                    part_sents = build_sentence_stream([part])
                    split_multi_sentences_inplace(part_sents)
                    coalesce_short_sentences_inplace(part_sents, args.minIndexChars, args.minIndexTokens)
                    start = len(sentences)
                    sentences.extend(part_sents)
                    end = len(sentences)
                    chunk_ranges.append((start, end))
            if total_chunks != len(chunk_ranges):
                eprint(f"[plan] warn: chunks={total_chunks}, ranges={len(chunk_ranges)}")
        else:
            sentences = build_sentence_stream(texts)
            split_multi_sentences_inplace(sentences)
            coalesce_short_sentences_inplace(sentences, args.minIndexChars, args.minIndexTokens)




        meta: Dict[str, Any] = {
            "channel": args.channelName,
            "video": args.videoFolderName,
            "language": args.language or "",
            "num_sentences_total": len(sentences),
            "progress": {"audios_total": len(audios), "audios_done": 0, "last_audio": None, "total_duration_sec": 0.0},
            "processed_audio": []
        }
        aligner = IncrementalAligner(sentences, min_score=75, max_checks=4000, dynamic_factor=20)

        running_offset = 0.0
        existing = _load_existing(out_path)
        skip_audio_names: set[str] = set()
        if existing:
            primed_results = existing.get("__results_unified__", [])
            skipped, processed_audio, running_offset = _prime_from_existing(
                {"results": primed_results, "meta": existing.get("meta", {})},
                sentences, aligner
            )
            meta.update(existing.get("meta", {}))
            meta["num_sentences_total"] = len(sentences)
            meta["processed_audio"] = processed_audio
            skip_audio_names = set(processed_audio)
            eprint(f"[init:resume] {out_path} (resume, {len(skip_audio_names)} files already processed)")
        else:
            # Пишем каркас JSON с ВСЕМИ предложениями (тайминги None)
            save_progress_json(out_path, aligner.results, meta, sentences)
            eprint(f"[init:new] {out_path} (created)")

        # Нет аудио/текста — ошибка
        if not audios or not sentences:
            if not audios: eprint("[warn] no audio")
            if not sentences: eprint("[warn] no text")
            save_progress_json(out_path, aligner.results, meta, sentences)
            final_log(False)

        # helper точной длительности
        import subprocess, shlex, wave
        def _actual_duration_seconds(p: Path, fallback: float) -> float:
            try:
                cmd = f'ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 {shlex.quote(str(p))}'
                out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
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

        # ---- Основной проход по аудиофайлам с удержанием границ файла ----
        MARGIN_MS = 1500  # 6 секунд допуск на паузы
        MAX_RETRIES = 20  # максимум повторных попыток внутри одного файла

        def _last_end_ms_for_file(results_list, fname: str, file_start_ms: int) -> int:
            last = file_start_ms
            for _s, st, en, src in results_list:
                if src == fname and en is not None:
                    if en > last: last = en
            return last

        def _sentence_variant_drop_prefix(s: Sentence, drop_n: int) -> Sentence:
            if drop_n <= 0 or drop_n >= len(s.tokens):
                return s
            new_tokens = s.tokens[drop_n:]
            new_text = " ".join(new_tokens)
            return Sentence(new_text, norm_text(new_text), new_tokens)

        for idx, a in enumerate(audios, 1):
            # Если используем план: i-й файл получает свой диапазон предложений
            if chunk_ranges and (idx - 1) < len(chunk_ranges):
                chunk_start, chunk_end = chunk_ranges[idx - 1]
            else:
                chunk_start, chunk_end = None, None

            if a.path.name in skip_audio_names:
                meta["progress"]["audios_done"] = idx
                meta["progress"]["last_audio"] = a.path.name
                meta["progress"]["total_duration_sec"] = running_offset
                save_progress_json(out_path, aligner.results, meta, sentences)
                continue

            # Привязываем таймкоды к текущему файлу (после проверки скипа)
            aligner.forced_src = a.path.name
            aligner.stop_idx  = chunk_end


            if a.path.name in skip_audio_names:
                meta["progress"]["audios_done"] = idx
                meta["progress"]["last_audio"] = a.path.name
                meta["progress"]["total_duration_sec"] = running_offset
                save_progress_json(out_path, aligner.results, meta, sentences)
                continue

            eprint(f"[run] {idx}/{len(audios)} {a.path.name}")
            words, lang, asr_dur = transcribe_words(a.path, args.device, args.language)
            if lang: meta["language"] = lang

            file_start_sec = running_offset
            real_dur = _actual_duration_seconds(a.path, asr_dur)
            file_start_ms = int(round(file_start_sec * 1000))
            file_end_ms = int(round((file_start_sec + real_dur) * 1000))

            # первая подача слов
            shifted = [Word(w.text, w.start + file_start_sec, w.end + file_start_sec) for w in words]
            prev_sent_idx = aligner.sent_idx
            added = aligner.extend_words_and_align(shifted, src=a.path.name, aggressive=False)
            eprint(f"[+s] +{len(added)} T={len(aligner.results)}/{len(sentences)}")
            save_progress_json(out_path, aligner.results, meta, sentences)

            # если не продвинулись, но по времени явно ещё внутри файла — повторяем попытки на ЭТОМ ЖЕ файле
            attempts = 0
            while True:
                last_end_ms = _last_end_ms_for_file(aligner.results, a.path.name, file_start_ms)
                still_in_this_file = (last_end_ms + MARGIN_MS) < file_end_ms
                progressed = (aligner.sent_idx > prev_sent_idx)
                reached_chunk_end = (chunk_end is not None and aligner.sent_idx >= chunk_end)
                if not still_in_this_file or reached_chunk_end:
                    break


                if progressed:
                    # Был прогресс в ЭТОМ ЖЕ файле — продолжаем «осушать» его,
                    # а не прыгаем к следующему файлу.
                    prev_sent_idx = aligner.sent_idx
                    attempts = 0
                    # Попробуем добрать ещё предложения на тех же словах без нового ASR
                    _ = aligner.extend_words_and_align([], src=None, aggressive=False)
                    save_progress_json(out_path, aligner.results, meta, sentences)
                    continue


                attempts += 1
                if attempts > MAX_RETRIES:
                    # Автосплит: распиливаем текущий индекс на несколько предложений (Sentence)
                    cur_item: Sentence = sentences[aligner.sent_idx]
                    parts = [p.strip() for p in SPLIT_HARD_RE.split(cur_item.text) if p.strip()]

                    if len(parts) > 1:
                        # заменить текущий и вставить остальные как Sentence
                        sentences[aligner.sent_idx] = make_sentence(parts[0])
                        insert_at = aligner.sent_idx + 1
                        for ptxt in parts[1:]:
                            sentences.insert(insert_at, make_sentence(ptxt))
                            insert_at += 1

                        # обновить счётчик в мета (полезно для UI/логов)
                        try:
                            meta["num_sentences_total"] = len(sentences)
                        except Exception:
                            pass

                        # Сброс попыток и попробовать добрать без нового ASR
                        attempts = 0
                        _ = aligner.extend_words_and_align([], src=None, aggressive=False)
                        save_progress_json(out_path, aligner.results, meta, sentences)
                        continue  # продолжить матчинг после распила

                    # Если распилить не получилось — НЕ валим прогон.
                    # Просто выходим из цикла попыток для ЭТОГО файла и идём к следующему аудио.
                    eprint(f"[pass] {a.path.name} give up matching idx={aligner.sent_idx} in this file; move to next audio")
                    break



                # Ещё раз снимаем ASR по этому же файлу и пробуем агрессивное выравнивание
                words_try, lang_try, _ = transcribe_words(a.path, args.device, args.language)
                if lang_try: meta["language"] = lang_try
                shifted_try = [Word(w.text, w.start + file_start_sec, w.end + file_start_sec) for w in words_try]
                _ = aligner.extend_words_and_align(shifted_try, src=a.path.name, aggressive=True)
                save_progress_json(out_path, aligner.results, meta, sentences)

                # Доп. эвристика: попробовать матчить следующее предложение без первых 1–2 токенов
                if aligner.sent_idx < len(sentences):
                    s = sentences[aligner.sent_idx]
                    for drop in (1, 2):
                        s_var = _sentence_variant_drop_prefix(s, drop)
                        hit = aligner._try_match_sentence_core(s_var, aggressive=True)
                        if hit is not None:
                            st, en, src_win = hit
                            aligner.results.append((s, st, en, src_win or a.path.name))
                            aligner.last_start_ms = st
                            aligner.last_end_ms = en
                            aligner.cursor = min(len(aligner.words), aligner._ptr_for_time(aligner.last_end_ms + 1))
                            aligner.sent_idx += 1
                            save_progress_json(out_path, aligner.results, meta, sentences)
                            break

            # Перед выходом к следующему файлу — финальная попытка без нового ASR
            _ = aligner.extend_words_and_align([], src=None, aggressive=False)

            # ← СБРОС ФОРСОВ ЗДЕСЬ
            aligner.forced_src = None
            aligner.stop_idx  = None

            running_offset += real_dur
            meta["progress"]["audios_done"] = idx
            meta["progress"]["last_audio"] = a.path.name
            meta["progress"]["total_duration_sec"] = running_offset
            meta["processed_audio"].append(a.path.name)
            save_progress_json(out_path, aligner.results, meta, sentences)
            eprint("[save]")

        # Финальный агрессивный проход
        if aligner.sent_idx < len(sentences):
            eprint("[deep-scan]")
            old_min, old_max = aligner.min_score, aligner.max_checks
            try:
                aligner.min_score = min(old_min, 65)
                aligner.max_checks = max(old_max, 200000)
                added = aligner.extend_words_and_align([], src=None, aggressive=True)
                eprint(f"[deep] +{len(added)} (now {len(aligner.results)}/{len(sentences)})")
            finally:
                aligner.min_score = old_min
                aligner.max_checks = old_max

        # Если ещё осталось ненайденное предложение — ставим null и выходим с false
        if aligner.sent_idx < len(sentences):
            _append_null_and_exit(out_path, aligner, sentences, meta)

        # Всё найдено
        save_progress_json(out_path, aligner.results, meta, sentences)
        eprint("[done]")
        final_log(True)

    except KeyboardInterrupt:
        eprint("[int] saving…")
        try:
            out_path = Path(args.outFilePath) if args.outFilePath else _build_output_path(
                args.mainFolderPath, args.channelName, args.videoFolderName
            )
            if 'meta' in locals() and 'aligner' in locals() and 'sentences' in locals():
                save_progress_json(out_path, aligner.results, meta, sentences)
                eprint("[saved]")
        except Exception:
            pass
        final_log(False)
    except Exception as e:
        eprint(f"[fatal] {e}")
        try:
            out_path = Path(args.outFilePath) if args.outFilePath else _build_output_path(
                args.mainFolderPath, args.channelName, args.videoFolderName
            )
            if 'meta' in locals() and 'aligner' in locals() and 'sentences' in locals():
                save_progress_json(out_path, aligner.results, meta, sentences)
        except Exception:
            pass
        final_log(False)


if __name__ == "__main__":
    main()
