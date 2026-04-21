# -*- coding: utf-8 -*-
r"""
GrausamesErbe_script_v17_FAST_TemplateFraming_SHORTLOG_AudioChain.py

Р’Р°СЂРёР°РЅС‚ СЃ РїРѕРІС‹С€РµРЅРЅРѕР№ СЃРєРѕСЂРѕСЃС‚СЊСЋ СѓРєР»Р°РґРєРё РЅР° С‚Р°Р№РјР»Р°Р№РЅ:
  вЂў РЎС‚Р°СЂС‚С‹ СЃС‡РёС‚Р°СЋС‚СЃСЏ РёР· С„Р°РєС‚РёС‡РµСЃРєРёС… РґР»РёС‚РµР»СЊРЅРѕСЃС‚РµР№ (С„РёРєСЃ РґС‹СЂ 1вЂ“2 РєР°РґСЂР°).
  вЂў РЈРєР»Р°РґРєР° V1 РґРµР»Р°РµС‚СЃСЏ РћР”РќРРњ batch-РІС‹Р·РѕРІРѕРј AppendToTimeline() СЃ source-range (startFrame/endFrame) вЂ” Р±РµР·
    РїРѕСЃС‚-С‚СЂРёРјРѕРІ, Р±Р»РµР№РґР°, РїРѕРёСЃРєР° РґРѕР±Р°РІР»РµРЅРЅС‹С… СЌР»РµРјРµРЅС‚РѕРІ Рё Р·Р°РґРµСЂР¶РµРє.
  вЂў РљР°СЂС‚С‹ Р±РёРЅРѕРІ РєРµС€РёСЂСѓСЋС‚СЃСЏ (Р±РµР· РїРѕРІС‚РѕСЂРЅРѕРіРѕ СЃРєР°РЅРёСЂРѕРІР°РЅРёСЏ РЅР° РєР°Р¶РґС‹Р№ СЌР»РµРјРµРЅС‚).
  вЂў РђСѓРґРёРѕ РЅР° A1 С‚РѕР¶Рµ СѓРєР»Р°РґС‹РІР°РµС‚СЃСЏ Р±Р°С‚С‡РµРј.

Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅРѕ РІ СЌС‚РѕР№ РІРµСЂСЃРёРё:
  вЂў РЎС‚СЂРѕРіР°СЏ РѕС‡РёСЃС‚РєР° РґРѕСЂРѕР¶РµРє V1, V4, A1 РїРµСЂРµРґ СѓРєР»Р°РґРєРѕР№.
  вЂў РЈСЃС‚РѕР№С‡РёРІС‹Р№ РїРѕРёСЃРє/РёРјРїРѕСЂС‚ MPI РґР»СЏ Р°СѓРґРёРѕ/РІРёРґРµРѕ (РёСЃРєР»СЋС‡Р°РµС‚ В«РїСЂРѕРїСѓСЃРєРёВ»).
  вЂў Р”Р»РёС‚РµР»СЊРЅРѕСЃС‚СЊ Р°СѓРґРёРѕ Р±РµСЂС‘Рј РёР· СЃРІРѕР№СЃС‚РІ РєР»РёРїР° (Frames/Duration), Р° РЅРµ С‡РµСЂРµР· ffprobe.
  вЂў РџРµСЂРµС…РѕРґС‹ РЅР° V4 РєРѕСЂСЂРµРєС‚РЅРѕ СЃС‚Р°РІСЏС‚СЃСЏ РЅР° РєР°Р¶РґС‹Р№ СЃС‚С‹Рє Рё РїРѕРґСЂРµР·Р°СЋС‚СЃСЏ РїРѕРґ СЃРѕСЃРµРґРЅРёРµ СЌР»РµРјРµРЅС‚С‹.
  вЂў true РїРµС‡Р°С‚Р°РµС‚СЃСЏ С‚РѕР»СЊРєРѕ РїСЂРё СѓСЃРїРµС€РЅРѕРј Р°РІС‚Рѕ-СЂРµРЅРґРµСЂРµ; Р±РµР· --auto-render РїРµС‡Р°С‚Р°РµС‚СЃСЏ false.
"""

import sys, re, argparse, subprocess, json, time, shutil, traceback, os, random, hashlib, math
from pathlib import Path
from typing import Optional, List, Tuple, Dict
from PIL import Image, ImageFile, ImageOps
ImageFile.LOAD_TRUNCATED_IMAGES = True

# ---------- constants ----------
AUDIO_EXTS = (".wav", ".mp3", ".flac", ".aif", ".aiff", ".m4a", ".ogg")
IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff", ".bmp")
VIDEO_EXT = ".mp4"

MIN_IMAGE_SEC = 3.0            # РјРёРЅРёРјСѓРј РґР»СЏ РєР°СЂС‚РёРЅРєРё РїСЂРё РЅР°Р»РёС‡РёРё РєР»РёРїР°
DUR_TOL = 0.10                 # РґРѕРїСѓСЃРє РїСЂРѕРІРµСЂРєРё РіРѕС‚РѕРІС‹С… С„Р°Р№Р»РѕРІ (СЃРµРє)
SAFE_PAD_FRAMES = 2            # РіРµРЅРµСЂРёРј +2 РєР°РґСЂР° Р·Р°РїР°СЃР°
FFMPEG_TIMEOUT_SEC = 600       # С‚Р°Р№РјР°СѓС‚ ffmpeg
COPY_CHUNK_BYTES = 8 * 1024 * 1024
COPY_LOG_EVERY_BYTES = 300 * 1024 * 1024
NORM_CACHE_DIR = "_normalized"
NORM_TARGET_I = -16.0          # LUFS
NORM_TARGET_TP = -1.5          # dBTP
NORM_TARGET_LRA = 11.0         # LU
TRANSCRIPT_PART_MIN_MS_DEFAULT = 5000
TRANSCRIPT_PART_MAX_MS_DEFAULT = 10000
# ---------- logging ----------
MAX_LINE = 140
VERBOSE = False

def _now_ts():
    t = time.localtime()
    ms = int((time.time() - int(time.time())) * 1000)
    return f"{t.tm_hour:02d}:{t.tm_min:02d}:{t.tm_sec:02d}.{ms:03d}"

def slog(cat: str, msg: str, force: bool=False):
    if not VERBOSE and not force and cat in {"DBG","FFMPEG","PLANX"}:
        return
    line = f"[{_now_ts()}][{cat}] {msg}"
    if len(line) > MAX_LINE:
        line = line[:MAX_LINE-1] + "вЂ¦"
    print(line)

# ---------- Resolve bootstrap ----------
def acquire_resolve():
    try:
        programdata = os.environ.get("PROGRAMDATA", r"C:\ProgramData")
        candidates = [
            os.path.join(programdata, "Blackmagic Design", "DaVinci Resolve", "Support", "Developer", "Scripting", "Modules"),
            os.path.join(programdata, "Blackmagic Design", "DaVinci Resolve", "Support", "Developer", "Scripting", "Examples", "Fusion", "Modules"),
            r"C:\Program Files\Blackmagic Design\DaVinci Resolve\Developer\Scripting\Modules",
        ]
        for p in candidates:
            if os.path.isdir(p) and p not in sys.path:
                sys.path.append(p)
    except Exception:
        pass
    try:
        import DaVinciResolveScript as bmd  # type: ignore
        if hasattr(bmd, "scriptapp"):
            r = bmd.scriptapp("Resolve")
            if r: return r
    except Exception:
        pass
    try:
        import fusionscript as bmd  # type: ignore
        if hasattr(bmd, "scriptapp"):
            fusion = bmd.scriptapp("Fusion")
            if fusion and hasattr(fusion, "GetResolve"):
                r = fusion.GetResolve()
                if r: return r
    except Exception:
        pass
    raise SystemExit("Resolve API not available: cannot get scriptapp('Resolve').")

# ---------- utils ----------
# ---------- image sanitizing (PNG cleaner) ----------
CLEAN_SUBDIR = "_clean"       # РїР°РїРєР° РґР»СЏ РєСЌС€РёСЂРѕРІР°РЅРЅС‹С… В«С‡РёСЃС‚С‹С…В» РєР°СЂС‚РёРЅРѕРє
MAX_SAFE_W = 3840             # РјРѕР¶РЅРѕ СѓРІРµР»РёС‡РёС‚СЊ/СѓРјРµРЅСЊС€РёС‚СЊ РїРѕ Р¶РµР»Р°РЅРёСЋ
MAX_SAFE_H = 2160

def _clean_dst_path(src: Path) -> Path:
    # РєР»Р°РґС‘Рј В«С‡РёСЃС‚С‹РµВ» РєРѕРїРёРё СЂСЏРґРѕРј СЃ РёСЃС…РѕРґРЅС‹РјРё, РІ РїРѕРґРїР°РїРєСѓ _clean
    return src.parent / CLEAN_SUBDIR / (src.stem + "_clean.png")

def ensure_clean_image(src: Path) -> Path:
    """
    Р”Р»СЏ PNG: СѓРґР°Р»СЏРµРј РЅРµСЃС‚Р°РЅРґР°СЂС‚РЅС‹Рµ С‡Р°РЅРєРё, РїСЂРёРІРѕРґРёРј Рє RGB Рё, РїСЂРё РЅРµРѕР±С…РѕРґРёРјРѕСЃС‚Рё,
    РѕРіСЂР°РЅРёС‡РёРІР°РµРј СЂР°Р·РјРµСЂ (<=4K). Р’РѕР·РІСЂР°С‰Р°РµРј РїСѓС‚СЊ Рє В«С‡РёСЃС‚РѕР№В» PNG.
    Р”Р»СЏ РѕСЃС‚Р°Р»СЊРЅС‹С… С„РѕСЂРјР°С‚РѕРІ вЂ” РїСЂРѕСЃС‚Рѕ РІРѕР·РІСЂР°С‰Р°РµРј РёСЃС…РѕРґРЅРёРє.
    """
    try:
        ext = src.suffix.lower()
        if ext != ".png":
            return src  # С‡РёСЃС‚РёРј С‚РѕР»СЊРєРѕ PNG; JPEG/WEBP/TIF РјРѕР¶РЅРѕ С‚СЂРѕРіР°С‚СЊ РїСЂРё Р¶РµР»Р°РЅРёРё

        dst = _clean_dst_path(src)
        dst.parent.mkdir(parents=True, exist_ok=True)

        # РєСЌС€ РїРѕ РІСЂРµРјРµРЅРё Рё СЂР°Р·РјРµСЂСѓ РёСЃС…РѕРґРЅРёРєР°: РµСЃР»Рё РЅРµ СЃС‚Р°СЂРµРµ вЂ” РёСЃРїРѕР»СЊР·СѓРµРј РіРѕС‚РѕРІРѕРµ
        try:
            if dst.exists():
                if dst.stat().st_mtime >= src.stat().st_mtime and dst.stat().st_size > 0:
                    return dst
        except Exception:
            pass

        # Р·Р°РіСЂСѓР·РєР° Рё РЅРѕСЂРјР°Р»РёР·Р°С†РёСЏ
        with Image.open(src) as im:
            # РїСЂРёРІРѕРґРёРј Рє RGB (СЃРЅРёРјР°РµС‚ РїР°Р»РёС‚СЂС‹ P/LA/Р°Р»СЊС„Сѓ Рё С‚.Рї., СѓР±РёСЂР°РµС‚ В«СЃС‚СЂР°РЅРЅРѕСЃС‚РёВ»)
            if im.mode not in ("RGB", "RGBA"):
                im = im.convert("RGBA" if "A" in im.getbands() else "RGB")

            # РјСЏРіРєРёР№ РґР°СѓРЅСЃРєРµР№Р», РµСЃР»Рё РєР°СЂС‚РёРЅРєР° СЃР»РёС€РєРѕРј Р±РѕР»СЊС€Р°СЏ (NVENC Р»СЋР±РёС‚ <=4K)
            w, h = im.size
            if w > MAX_SAFE_W or h > MAX_SAFE_H:
                # РїСЂРѕРїРѕСЂС†РёРѕРЅР°Р»СЊРЅРѕ РІРїРёСЃС‹РІР°РµРј РІ СЂР°РјРєСѓ MAX_SAFE_W x MAX_SAFE_H
                im.thumbnail((MAX_SAFE_W, MAX_SAFE_H), Image.Resampling.LANCZOS)

            # РµСЃР»Рё Р±С‹Р»Р° Р°Р»СЊС„Р° вЂ” РЅР° С‡С‘СЂРЅС‹Р№ С„РѕРЅ (РёР»Рё РјРѕР¶РЅРѕ РЅР° Р±РµР»С‹Р№; СЃРµР№С‡Р°СЃ вЂ” С‡С‘СЂРЅС‹Р№)
            if im.mode == "RGBA":
                bg = Image.new("RGB", im.size, (0, 0, 0))
                bg.paste(im, mask=im.split()[-1])
                im = bg

            # СЃРѕС…СЂР°РЅСЏРµРј РєР°Рє В«С‡РёСЃС‚СѓСЋВ» PNG вЂ” Р±РµР· СЌРєР·РѕС‚РёС‡РµСЃРєРёС… РјРµС‚Р°РґР°РЅРЅС‹С…/С‡Р°РЅРєРѕРІ
            im.save(dst, format="PNG", optimize=True)

        return dst
    except Exception:
        # РІ СЃР»СѓС‡Р°Рµ Р»СЋР±РѕР№ РѕС€РёР±РєРё РІРѕР·РІСЂР°С‰Р°РµРј РёСЃС…РѕРґРЅРёРє, РЅРµ Р»РѕРјР°СЏ РїР°Р№РїР»Р°Р№РЅ
        return src

# ---------- dedupe clips (new) ----------
def _md5_of_file(path: Path, chunk_size: int = 16 * 1024 * 1024) -> str:
    """
    Р‘С‹СЃС‚СЂС‹Р№ Рё РЅР°РґС‘Р¶РЅС‹Р№ MD5: С‡РёС‚Р°РµРј С„Р°Р№Р» РєСѓСЃРєР°РјРё (РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ 16 РњР‘).
    Р”Р»СЏ РёРґРµРЅС‚РёС‡РЅС‹С… .mp4 С…РµС€ СЃРѕРІРїР°РґС‘С‚, РґР°Р¶Рµ РµСЃР»Рё РёРјРµРЅР° СЂР°Р·РЅС‹Рµ.
    """
    h = hashlib.md5()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def dedupe_duplicate_clips(images_dir: Path) -> int:
    """
    РС‰РµС‚ РґСѓР±Р»РёРєР°С‚С‹ .mp4 РєР»РёРїРѕРІ РІ РїР°РїРєРµ РєР°СЂС‚РёРЅРѕРє (IMAGES/images) Рё СѓРґР°Р»СЏРµС‚ РєРѕРїРёРё,
    РѕСЃС‚Р°РІР»СЏСЏ С‚РѕР»СЊРєРѕ РїРµСЂРІС‹Р№ С„Р°Р№Р» РїРѕ natural-СЃРѕСЂС‚РёСЂРѕРІРєРµ. Р”СѓР±Р»РёРєР°С‚С‹ РѕРїСЂРµРґРµР»СЏСЋС‚СЃСЏ РїРѕ
    (СЂР°Р·РјРµСЂ С„Р°Р№Р»Р°, md5 СЃРѕРґРµСЂР¶РёРјРѕРіРѕ).
    Р’РѕР·РІСЂР°С‰Р°РµС‚ РєРѕР»РёС‡РµСЃС‚РІРѕ СѓРґР°Р»С‘РЅРЅС‹С… С„Р°Р№Р»РѕРІ.
    """
    # Р±РµСЂС‘Рј С‚РѕР»СЊРєРѕ mp4 РІ РєРѕСЂРЅРµ images_dir (РєР°Рє РІ РїСЂРёРјРµСЂРµ: 0lig_clip_0002.mp4 Рё С‚.Рї.)
    clips = sorted(
        [p for p in images_dir.iterdir() if p.is_file() and p.suffix.lower() == ".mp4"],
        key=lambda p: natural_key(p.name),
    )
    if not clips:
        return 0

    seen: Dict[Tuple[int, str], Path] = {}
    removed = 0

    slog("SCAN", f"dedupe: scanning {len(clips)} mp4", force=True)
    for p in clips:
        try:
            size = p.stat().st_size
        except Exception:
            # РµСЃР»Рё РЅРµ С‡РёС‚Р°РµС‚СЃСЏ stat вЂ” РїСЂРѕРїСѓСЃРєР°РµРј
            continue
        # СЃРЅР°С‡Р°Р»Р° Р±С‹СЃС‚СЂС‹Р№ РєР»СЋС‡ С‚РѕР»СЊРєРѕ РїРѕ СЂР°Р·РјРµСЂСѓ
        same_size = [k for k in seen.keys() if k[0] == size]
        if not same_size:
            # РЅРёРєРѕРіРѕ С‚Р°РєРѕРіРѕ СЂР°Р·РјРµСЂР° РµС‰С‘ РЅРµС‚: СЃСЂР°Р·Сѓ СЃС‡РёС‚Р°РµРј md5 Рё Р·Р°РЅРѕСЃРёРј
            try:
                md5 = _md5_of_file(p)
            except Exception as e:
                slog("DEDUP", f"hash fail {p.name}: {e}", force=True)
                continue
            key = (size, md5)
            if key not in seen:
                seen[key] = p
            else:
                # С‚РµРѕСЂРµС‚РёС‡РµСЃРєРё РЅРµ РґРѕР№РґС‘Рј СЃСЋРґР°, РЅРѕ РїСѓСЃС‚СЊ Р±СѓРґРµС‚
                try:
                    p.unlink()
                    removed += 1
                    slog("DEDUP", f"rm {p.name} (dup of {seen[key].name})")
                except Exception as e:
                    slog("DEDUP", f"rm fail {p.name}: {e}", force=True)
            continue

        # РµСЃС‚СЊ С„Р°Р№Р»С‹ С‚Р°РєРѕРіРѕ Р¶Рµ СЂР°Р·РјРµСЂР° вЂ” СЃСЂР°РІРЅРёРј С‚РѕС‡РЅС‹Рј md5
        try:
            md5 = _md5_of_file(p)
        except Exception as e:
            slog("DEDUP", f"hash fail {p.name}: {e}", force=True)
            continue
        key = (size, md5)

        if key in seen:
            # РґСѓР±Р»РёРєР°С‚ РЅР°Р№РґРµРЅ вЂ” СѓРґР°Р»СЏРµРј С‚РµРєСѓС‰РёР№ (С‚Р°Рє РєР°Рє РѕРЅ РќР• РїРµСЂРІС‹Р№ РїРѕ natural-СЃРѕСЂС‚РёСЂРѕРІРєРµ)
            try:
                p.unlink()
                removed += 1
                slog("DEDUP", f"rm {p.name} (dup of {seen[key].name})")
            except Exception as e:
                slog("DEDUP", f"rm fail {p.name}: {e}", force=True)
        else:
            # РїРµСЂРІС‹Р№ СЌРєР·РµРјРїР»СЏСЂ С‚Р°РєРѕРіРѕ СЃРѕРґРµСЂР¶РёРјРѕРіРѕ вЂ” Р·Р°РїРѕРјРёРЅР°РµРј
            seen[key] = p

    if removed:
        slog("SCAN", f"dedupe: removed {removed} duplicates", force=True)
    else:
        slog("SCAN", "dedupe: no duplicates", force=True)
    return removed

def is_image(p: Path) -> bool: return p.suffix.lower() in IMAGE_EXTS
def is_audio(p: Path) -> bool: return p.suffix.lower() in AUDIO_EXTS

def natural_key(s: str):
    parts = re.split(r"(\d+)", s)
    return [int(p) if p.isdigit() else p.lower() for p in parts]

def require_cmd(name: str) -> bool:
    if not shutil.which(name):
        slog("ERR", f"'{name}' not in PATH. Need ffmpeg/ffprobe.", force=True)
        return False
    return True

def ffprobe_duration(path: Path) -> float | None:
    try:
        out = subprocess.check_output(
            ["ffprobe","-v","error","-print_format","json","-show_entries","format=duration",str(path)],
            stderr=subprocess.STDOUT
        )
        dur = float(json.loads(out.decode("utf-8","ignore")).get("format",{}).get("duration",0.0))
        return dur if dur>0 else None
    except Exception:
        return None

def _parse_rate(rate: str) -> Optional[float]:
    try:
        if not rate:
            return None
        if "/" in str(rate):
            num, den = str(rate).split("/", 1)
            den_f = float(den)
            if den_f == 0:
                return None
            return float(num) / den_f
        return float(rate)
    except Exception:
        return None

def ffprobe_video_info(path: Path) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    try:
        out = subprocess.check_output(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=avg_frame_rate,r_frame_rate,duration,nb_frames:format=duration",
                "-of", "json",
                str(path),
            ],
            stderr=subprocess.STDOUT,
        )
        data = json.loads(out.decode("utf-8", "ignore"))
        streams = data.get("streams") or []
        stream = streams[0] if streams else {}
        fps_val = _parse_rate(stream.get("avg_frame_rate")) or _parse_rate(stream.get("r_frame_rate"))
        dur = None
        for raw in (stream.get("duration"), (data.get("format") or {}).get("duration")):
            try:
                if raw is not None:
                    dur = float(raw)
                    break
            except Exception:
                pass
        frames = None
        try:
            if stream.get("nb_frames") is not None:
                frames = int(float(stream.get("nb_frames")))
        except Exception:
            frames = None
        return dur, fps_val, frames
    except Exception:
        return None, None, None

def pick_encoder():
    enc_list = ""
    try:
        enc_list = subprocess.check_output(["ffmpeg","-hide_banner","-loglevel","error","-encoders"], stderr=subprocess.STDOUT).decode("utf-8","ignore")
    except Exception:
        pass
    if "h264_nvenc" in enc_list: return ("h264_nvenc", ["-preset","p5"])
    if "h264_qsv"   in enc_list: return ("h264_qsv",   ["-preset","veryfast"])
    if "h264_amf"   in enc_list: return ("h264_amf",   [])
    return ("libx264", ["-preset","ultrafast","-crf","18"])

def run_ffmpeg(cmd: List[str], cat: str):
    t0 = time.time()
    if VERBOSE:
        slog("FFMPEG", f"{cat} | {' '.join(cmd)}")
    else:
        slog("FFMPEG", f"{cat}", force=True)
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=FFMPEG_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        slog("FFMPEG", f"{cat} | TIMEOUT {FFMPEG_TIMEOUT_SEC}s", force=True)
        return False
    ok = (res.returncode == 0)
    dt = time.time() - t0
    if ok:
        slog("FFMPEG", f"{cat} | OK {dt:.1f}s")
    else:
        out = (res.stdout or b"").decode("utf-8","ignore")
        if not VERBOSE:
            out = "\n".join(out.splitlines()[-6:])
        slog("FFMPEG", f"{cat} | FAIL rc={res.returncode} {dt:.1f}s | OUT:\n{out}", force=True)
    return ok

def make_video_from_image(img: Path, out_mp4: Path, duration_s: float, fps: float, encoder: tuple[str, list[str]]) -> bool:
    out_mp4.parent.mkdir(parents=True, exist_ok=True)
    fps_i = int(round(fps)) or 24
    vcodec, extra = encoder
    duration_s = max(0.01, float(duration_s))
    cmd = [
        "ffmpeg","-nostdin","-y","-loglevel","error",
        "-loop","1","-t", f"{duration_s:.6f}","-framerate", str(fps_i),
        "-i", str(img),
        "-c:v", vcodec, *extra,
        "-pix_fmt","yuv420p",
        "-vf","scale=trunc(iw/2)*2:trunc(ih/2)*2",
        "-r", str(fps_i),
        str(out_mp4)
    ]
    return run_ffmpeg(cmd, f"STILL->{out_mp4.name} ({duration_s:.3f}s)")

def make_trimmed_clip(src: Path, out_mp4: Path, duration_s: float, fps: float, encoder: tuple[str, list[str]]) -> bool:
    out_mp4.parent.mkdir(parents=True, exist_ok=True)
    fps_i = int(round(fps)) or 24
    vcodec, extra = encoder
    duration_s = max(0.0, float(duration_s))
    if duration_s <= 0.01:
        try:
            if out_mp4.exists(): out_mp4.unlink()
        except Exception: pass
        slog("BUILD", f"skip trim {src.name}: {duration_s:.3f}s")
        return False
    cmd = [
        "ffmpeg","-nostdin","-y","-loglevel","error",
        "-t", f"{duration_s:.6f}",
        "-i", str(src),
        "-c:v", vcodec, *extra,
        "-pix_fmt","yuv420p",
        "-r", str(fps_i),
        "-an",
        str(out_mp4)
    ]
    return run_ffmpeg(cmd, f"TRIM {src.name} -> {out_mp4.name} ({duration_s:.3f}s)")

def copy_file_with_progress(src: Path, dst: Path) -> bool:
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        size = src.stat().st_size
        slog("COPY", f"{src.name} -> {dst.name} {size/1024/1024:.1f}MB")
        try:
            if dst.exists():
                dst.unlink()
            os.link(src, dst)
            slog("COPY", f"hardlink ok")
            return True
        except Exception:
            pass
        copied = 0
        last_log = 0
        with src.open("rb") as fsrc, dst.open("wb") as fdst:
            while True:
                buf = fsrc.read(COPY_CHUNK_BYTES)
                if not buf: break
                fdst.write(buf)
                copied += len(buf)
                if copied - last_log >= COPY_LOG_EVERY_BYTES:
                    slog("COPY", f"{dst.name}: {copied/1024/1024:.0f}/{size/1024/1024:.0f}MB")
                    last_log = copied
        try: shutil.copystat(src, dst)
        except Exception: pass
        slog("COPY", "done")
        return True
    except Exception as e:
        slog("COPY", f"FAIL {e}", force=True)
        return False

def _cache_is_fresh(src: Path, dst: Path) -> bool:
    try:
        return dst.exists() and dst.stat().st_size > 0 and dst.stat().st_mtime >= src.stat().st_mtime
    except Exception:
        return False

def media_has_audio_stream(path: Path) -> bool:
    if not shutil.which("ffprobe"):
        return True
    try:
        out = subprocess.check_output(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "stream=index",
                "-of", "csv=p=0",
                str(path),
            ],
            stderr=subprocess.STDOUT,
        )
        return bool((out or b"").strip())
    except Exception:
        return False

def normalize_video_clip_audio(src_mp4: Path, dst_mp4: Path) -> bool:
    """
    Нормализация встроенного аудио клипа до целевого loudness.
    Видео поток копируется без перекодирования.
    """
    try:
        dst_mp4.parent.mkdir(parents=True, exist_ok=True)
        if _cache_is_fresh(src_mp4, dst_mp4):
            return True
        filt = f"loudnorm=I={NORM_TARGET_I}:TP={NORM_TARGET_TP}:LRA={NORM_TARGET_LRA}"
        cmd = [
            "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
            "-i", str(src_mp4),
            "-map", "0:v:0",
            "-map", "0:a:0?",
            "-c:v", "copy",
            "-af", filt,
            "-c:a", "aac",
            "-b:a", "192k",
            "-movflags", "+faststart",
            str(dst_mp4),
        ]
        ok = run_ffmpeg(cmd, f"NORM CLIP {src_mp4.name}")
        if not ok:
            try:
                if dst_mp4.exists():
                    dst_mp4.unlink()
            except Exception:
                pass
        return ok
    except Exception as e:
        slog("NORM", f"clip fail {src_mp4.name}: {e}", force=True)
        return False

def normalize_voice_audio(src_audio: Path, dst_audio: Path) -> bool:
    """
    Нормализация voice-файла до целевого loudness.
    Выход сохраняем в WAV для стабильного импорта.
    """
    try:
        dst_audio.parent.mkdir(parents=True, exist_ok=True)
        if _cache_is_fresh(src_audio, dst_audio):
            return True
        filt = f"loudnorm=I={NORM_TARGET_I}:TP={NORM_TARGET_TP}:LRA={NORM_TARGET_LRA}"
        cmd = [
            "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
            "-i", str(src_audio),
            "-vn",
            "-af", filt,
            "-ar", "48000",
            "-ac", "2",
            "-c:a", "pcm_s16le",
            str(dst_audio),
        ]
        ok = run_ffmpeg(cmd, f"NORM VOICE {src_audio.name}")
        if not ok:
            try:
                if dst_audio.exists():
                    dst_audio.unlink()
            except Exception:
                pass
        return ok
    except Exception as e:
        slog("NORM", f"voice fail {src_audio.name}: {e}", force=True)
        return False

def normalize_media_before_import(roll: Path, clip_files: List[Path], voice_files: List[Path]) -> Tuple[List[Path], List[Path]]:
    norm_root = roll / NORM_CACHE_DIR
    norm_clips_dir = norm_root / "videoClips"
    norm_voice_dir = norm_root / "voice"

    out_clips: List[Path] = []
    out_voice: List[Path] = []

    if clip_files:
        slog("NORM", f"clips: {len(clip_files)} to {NORM_TARGET_I:.1f} LUFS")
    for src in clip_files:
        if not media_has_audio_stream(src):
            out_clips.append(src)
            continue
        dst = norm_clips_dir / src.name
        if normalize_video_clip_audio(src, dst):
            out_clips.append(dst)
        else:
            slog("NORM", f"clip fallback source: {src.name}", force=True)
            out_clips.append(src)

    if voice_files:
        slog("NORM", f"voice: {len(voice_files)} to {NORM_TARGET_I:.1f} LUFS")
    for src in voice_files:
        suffix_tag = src.suffix.lower().replace(".", "_")
        dst = norm_voice_dir / f"{src.stem}{suffix_tag}_norm.wav"
        if normalize_voice_audio(src, dst):
            out_voice.append(dst)
        else:
            slog("NORM", f"voice fallback source: {src.name}", force=True)
            out_voice.append(src)

    return out_clips, out_voice

def tc_from_frames(frames:int, fps:float)->str:
    fps_i=int(round(fps)) or 24
    hh=frames//(fps_i*3600); rem=frames%(fps_i*3600)
    mm=rem//(fps_i*60); rem%=fps_i*60
    ss=rem//fps_i; ff=rem%fps_i
    return f"{hh:02d}:{mm:02d}:{ss:02d}:{ff:02d}"

def parse_tc_to_frames(tc: str, fps: float) -> Optional[int]:
    try:
        if ":" in tc:
            hh,mm,ss,ff = [int(x) for x in tc.split(":")]
            fps_i = int(round(fps)) or 24
            return hh*3600*fps_i + mm*60*fps_i + ss*fps_i + ff
        else:
            val = float(tc)
            fps_i = int(round(fps)) or 24
            return int(round(val * fps_i))
    except Exception:
        return None

def fps_of_timeline(tl) -> float:
    try:
        v = tl.GetSetting("timelineFrameRate")
        if v: return float(v)
    except Exception: pass
    return 24.0

def get_or_create_timeline(project, media_pool, name: str):
    wanted = (name or "").strip().lower()
    timelines = []
    try:
        raw = project.GetTimelines() or []
        timelines = list(raw.values()) if isinstance(raw, dict) else list(raw)
    except Exception:
        timelines = []
    if not timelines:
        i = 1
        while True:
            try:
                t = project.GetTimelineByIndex(i)
            except Exception:
                t = None
            if not t:
                break
            timelines.append(t)
            i += 1
    for t in timelines:
        try:
            if (t.GetName() or "").strip().lower() == wanted:
                return t
        except Exception:
            pass

    create_project_tl = getattr(project, "CreateTimeline", None)
    if callable(create_project_tl):
        try:
            tl = create_project_tl(name)
            if tl:
                return tl
        except Exception:
            pass

    for meth in ("CreateEmptyTimeline", "CreateTimeline"):
        try:
            fn = getattr(media_pool, meth, None)
        except Exception:
            fn = None
        if callable(fn):
            try:
                tl = fn(name)
                if tl:
                    return tl
            except Exception:
                pass

    try:
        return project.GetCurrentTimeline()
    except Exception:
        return timelines[0] if timelines else None

def fps_of_project_media(project, fallback: float) -> float:
    try:
        v = project.GetSetting("timelineFrameRate")
        if v:
            parsed = float(v)
            if parsed > 0:
                return parsed
    except Exception:
        pass
    return float(fallback) if fallback and fallback > 0 else 24.0

def ensure_track_exists(tl, track_type: str, index: int):
    def _count() -> int:
        try:
            return int(tl.GetTrackCount(track_type) or 0)
        except Exception:
            return 0

    def _try_add_one() -> bool:
        before = _count()
        old_name = "AddVideoTrack" if track_type == "video" else "AddAudioTrack"
        try:
            fn = getattr(tl, old_name, None)
        except Exception:
            fn = None
        if callable(fn):
            try:
                fn()
            except Exception:
                pass
            if _count() > before:
                return True

        try:
            add_track = getattr(tl, "AddTrack", None)
        except Exception:
            add_track = None
        if callable(add_track):
            variants = []
            if track_type == "audio":
                variants = [("audio", "stereo"), ("audio", "mono"), ("audio",)]
            else:
                variants = [("video",)]
            for args in variants:
                try:
                    add_track(*args)
                except Exception:
                    continue
                if _count() > before:
                    return True
        return _count() > before

    cnt = _count()
    while cnt < index:
        if not _try_add_one():
            break
        cnt = _count()

def _track_items_list(tl, track_type: str, idx: int):
    try:
        items = tl.GetItemsInTrack(track_type, idx)
        return list(items.values()) if isinstance(items, dict) else list(items or [])
    except Exception:
        return []

def clear_track_items_strict(tl, track_type: str, idx: int):
    items = _track_items_list(tl, track_type, idx)
    if not items:
        slog("TL", f"clear {track_type}#{idx} (empty)")
        return
    # bulk
    for m in ("DeleteItems", "DeleteClips"):
        if hasattr(tl, m):
            try:
                getattr(tl, m)(items)
                slog("TL", f"clear {track_type}#{idx} ok (bulk {m})")
                return
            except Exception:
                pass
    # fallback per-item
    removed = 0
    for it in items:
        for m in ("DeleteItem", "RemoveItem"):
            if hasattr(tl, m):
                try:
                    getattr(tl, m)(it); removed += 1; break
                except Exception:
                    pass
        else:
            try:
                it.SetSelected(True)
                if hasattr(tl, "DeleteSelectedClips"):
                    tl.DeleteSelectedClips(); removed += 1
            except Exception:
                pass
            finally:
                try: it.SetSelected(False)
                except Exception: pass
    slog("TL", f"clear {track_type}#{idx} del={removed}/{len(items)}")

# ---------- bins/maps ----------
def clear_media_bin(folder, mp=None):
    if not folder: 
        return
    try:
        clips = list(folder.GetClipList() or [])
    except Exception:
        clips = []
    if not clips:
        try:
            slog("BIN", f"clear '{folder.GetName()}' (empty)")
        except Exception:
            slog("BIN", "clear (empty)")
        return

    ok = False
    delete_targets = []
    if mp is not None:
        delete_targets.append(mp)
    delete_targets.append(folder)

    for target in delete_targets:
        for meth in ("DeleteClips", "DeleteItems"):
            if not hasattr(target, meth):
                continue
            try:
                r = getattr(target, meth)(clips)
                ok = (r is True) or (r is None)
                if ok:
                    break
            except Exception:
                pass
        if ok:
            break

    if not ok:
        removed = 0
        for c in clips:
            for target in delete_targets:
                done = False
                for meth in ("DeleteClips", "DeleteItems"):
                    if not hasattr(target, meth):
                        continue
                    try:
                        r = getattr(target, meth)([c])
                        if (r is True) or (r is None):
                            removed += 1
                            done = True
                            break
                    except Exception:
                        pass
                if done:
                    break
        try:
            slog("BIN", f"clear '{folder.GetName()}' rm={removed}/{len(clips)}")
        except Exception:
            slog("BIN", f"clear rm={removed}/{len(clips)}")
    else:
        try:
            slog("BIN", f"clear '{folder.GetName()}' {len(clips)}")
        except Exception:
            slog("BIN", f"clear {len(clips)}")

def _key_of_clip(c) -> Tuple[str,str,str]:
    nm_file = ""; nm_clip=""; nm_name=""
    try: nm_file = (c.GetClipProperty("File Name") or "").strip()
    except Exception: pass
    try: nm_clip = (c.GetClipProperty("Clip Name") or "").strip()
    except Exception: pass
    try: nm_name = (c.GetName() or "").strip()
    except Exception: pass
    return nm_file, nm_clip, nm_name

def _add_keys_to_map(m: dict, c) -> None:
    keys = set()
    # РёРјРµРЅР°
    for raw in _key_of_clip(c):
        if not raw: 
            continue
        base = Path(raw).name.lower()
        if base: 
            keys.add(base)
    # РїРѕР»РЅС‹Р№ РїСѓС‚СЊ
    try:
        fp = (c.GetClipProperty("File Path") or "").strip()
        if fp:
            fp_key = Path(fp).resolve().as_posix().lower()
            keys.add(fp_key)
    except Exception:
        pass
    for k in keys:
        m.setdefault(k, c)

def map_video_by_filename(folder) -> Dict[str, object]:
    m={}
    try:
        for c in list(folder.GetClipList() or []):
            _add_keys_to_map(m, c)
    except Exception:
        pass
    if VERBOSE: 
        slog("BIN", f"video map size={len(m)}")
    return m

def map_video_by_filename_recursive(folder) -> Dict[str, object]:
    m = {}
    stack = [folder] if folder else []
    while stack:
        f = stack.pop()
        try:
            for c in list(f.GetClipList() or []):
                _add_keys_to_map(m, c)
        except Exception:
            pass
        try:
            stack.extend(list(f.GetSubFolderList() or []))
        except Exception:
            pass
    if VERBOSE:
        slog("BIN", f"video recursive map size={len(m)}")
    return m

def map_audio_by_filename(folder) -> Dict[str, object]:
    m={}
    try:
        for c in list(folder.GetClipList() or []):
            _add_keys_to_map(m, c)
    except Exception:
        pass
    if VERBOSE: 
        slog("BIN", f"audio map size={len(m)}")
    return m

def _rebuild_video_map(mp, project, bin_images, video_map):
    video_map.clear()
    video_map.update(map_video_by_filename_recursive(bin_images))
    return video_map

def _rebuild_audio_map(mp, project, bin_parts, audio_map):
    audio_map.clear()
    audio_map.update(map_audio_by_filename(bin_parts))
    return audio_map

# ---------- domain: images & clips ----------
def normalize_clip_to_image_stem(clip_stem: str) -> str:
    s = clip_stem.lower()
    s = s.replace("_clip_", "_").replace("-clip-", "-")
    s = s.replace("_clip", "").replace("-clip", "")
    s = s.replace("clip_", "").replace(" clip ", " ")
    s = s.replace("clip", "")
    s = re.sub(r"__+","_", s); s = re.sub(r"--+","-", s)
    return s.strip("_- ")

def find_clips_for_images(images_dir: Path, images_by_stem: dict) -> dict:
    clip_map={}
    for p in images_dir.iterdir():
        if p.is_file() and p.suffix.lower()==".mp4" and "clip" in p.stem.lower():
            cand = normalize_clip_to_image_stem(p.stem)
            if cand in images_by_stem:
                exist = clip_map.get(cand)
                if exist is None or natural_key(p.name) < natural_key(exist.name):
                    clip_map[cand] = p
    if clip_map:
        slog("SCAN", f"clips: {len(clip_map)}")
    return clip_map

def load_timestamps_by_roll(roll_path: Path) -> List[int]:
    ts_path = roll_path / "IMAGES" / "timestamps.json"
    if not ts_path.exists():
        slog("ERR", f"timestamps.json not found", force=True)
        return []
    try:
        data = json.loads(ts_path.read_text(encoding="utf-8"))
        arr = data.get("time") or []
        return [int(x) for x in arr]
    except Exception as e:
        slog("ERR", f"timestamps.json parse: {e}", force=True)
        return []

def _first_int_from_stem(path: Path) -> Optional[int]:
    m = re.search(r"(\d+)", path.stem)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _indexed_files(files: List[Path]) -> Dict[int, Path]:
    out: Dict[int, Path] = {}
    for p in sorted(files, key=lambda x: natural_key(x.name)):
        idx = _first_int_from_stem(p)
        if idx is None:
            continue
        out.setdefault(idx, p)
    return out

def _get_nested(data: dict, keys: List[str], default=None):
    cur = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default

def _to_int_or_none(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except Exception:
        return None

def load_visual_segment_specs(roll: Path) -> List[dict]:
    seg_dir = roll / "VISUALS" / "VISUAL_SEGMENTS"
    if not seg_dir.exists() or not seg_dir.is_dir():
        slog("PLAN", "VISUALS/VISUAL_SEGMENTS not found -> video-only fallback", force=True)
        return []

    specs: List[dict] = []
    files = sorted(seg_dir.glob("*_visualSegment.json"), key=lambda p: natural_key(p.name))
    if not files:
        files = sorted(seg_dir.glob("*.json"), key=lambda p: natural_key(p.name))

    for fallback_idx, path in enumerate(files):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            raise RuntimeError(f"visual segment parse failed: {path.name}: {e}")

        fmt = str(_get_nested(data, ["segment_plan", "selected_base_format"], "") or "").strip().lower()
        idx = _first_int_from_stem(path)
        start_ms = _to_int_or_none(_get_nested(data, ["segment_meta", "start_ms"]))
        end_ms = _to_int_or_none(_get_nested(data, ["segment_meta", "end_ms"]))
        dur_ms = _to_int_or_none(_get_nested(data, ["segment_meta", "durMs"]))
        if dur_ms is None:
            dur_ms = _to_int_or_none(_get_nested(data, ["segment_meta", "duration_ms"]))
        if start_ms is None:
            start_ms = _to_int_or_none(_get_nested(data, ["segment_meta", "start"]))
        if end_ms is None:
            end_ms = _to_int_or_none(_get_nested(data, ["segment_meta", "end"]))
        specs.append({
            "index": int(idx if idx is not None else fallback_idx),
            "path": path,
            "format": fmt,
            "kind": "auto",
            "start_ms": start_ms,
            "end_ms": end_ms,
            "duration_ms": dur_ms,
        })

    return specs

def build_visual_assets(roll: Path, video_files: List[Path], still_files: List[Path]) -> List[dict]:
    specs = load_visual_segment_specs(roll)
    video_files = sorted(video_files, key=lambda p: natural_key(p.name))
    still_files = sorted(still_files, key=lambda p: natural_key(p.name))
    video_by_idx = _indexed_files(video_files)
    still_by_idx = _indexed_files(still_files)
    video_by_stem = {p.stem.lower(): p for p in reversed(video_files)}

    if not specs:
        if not still_files and not video_files:
            raise RuntimeError("no start frames in CLIPS/startFrames and no video clips in CLIPS/videoClips")
        if still_files:
            assets = []
            for i, still_path in enumerate(still_files):
                idx = _first_int_from_stem(still_path)
                video_path = None
                if idx is not None:
                    video_path = video_by_idx.get(idx)
                if video_path is None:
                    video_path = video_by_stem.get(still_path.stem.lower())
                if video_path:
                    assets.append({
                        "index": int(idx if idx is not None else i),
                        "kind": "video",
                        "format": "auto_by_start_frame",
                        "path": video_path,
                        "fill_still_path": still_path,
                        "segment_json": None,
                    })
                else:
                    assets.append({
                        "index": int(idx if idx is not None else i),
                        "kind": "still",
                        "format": "auto_by_start_frame",
                        "path": still_path,
                        "fill_still_path": still_path,
                        "segment_json": None,
                    })
            return assets
        return [
            {
                "index": i,
                "kind": "video",
                "format": "video_fallback_no_start_frame",
                "path": p,
                "fill_still_path": None,
                "segment_json": None,
            }
            for i, p in enumerate(video_files)
        ]

    assets: List[dict] = []
    still_cursor = 0
    used_stills = set()
    all_spec_indices = [int(s["index"]) for s in specs]
    use_direct_still_index = bool(all_spec_indices) and all(i in still_by_idx for i in all_spec_indices)

    def pick_for_segment(files: List[Path], by_idx: Dict[int, Path], used: set, cursor: int, seg_idx: int, prefer_direct: bool) -> Tuple[Optional[Path], int]:
        direct = by_idx.get(seg_idx) if prefer_direct else None
        if direct and direct not in used:
            used.add(direct)
            return direct, cursor
        while cursor < len(files) and files[cursor] in used:
            cursor += 1
        if cursor < len(files):
            p = files[cursor]
            used.add(p)
            return p, cursor + 1
        return None, cursor

    for spec in specs:
        seg_idx = int(spec["index"])
        still_path, still_cursor = pick_for_segment(still_files, still_by_idx, used_stills, still_cursor, seg_idx, use_direct_still_index)
        if not still_path:
            raise RuntimeError(f"missing start frame for visual segment {seg_idx:05d} ({spec['path'].name}) in CLIPS/startFrames")

        still_idx = _first_int_from_stem(still_path)
        video_path = None
        if still_idx is not None:
            video_path = video_by_idx.get(still_idx)
        if video_path is None:
            video_path = video_by_stem.get(still_path.stem.lower())

        if video_path:
            assets.append({
                **spec,
                "kind": "video",
                "format": spec.get("format") or "auto_by_start_frame",
                "path": video_path,
                "fill_still_path": still_path,
                "segment_json": spec["path"],
            })
        else:
            assets.append({
                **spec,
                "kind": "still",
                "format": spec.get("format") or "auto_by_start_frame",
                "path": still_path,
                "fill_still_path": still_path,
                "segment_json": spec["path"],
            })

    return assets

def build_slots_from_visual_assets(assets: List[dict], fps: float) -> Tuple[List[int], List[int], List[int]]:
    fps_f = float(fps) if fps and fps > 0 else 24.0
    starts: List[int] = []
    ends: List[int] = []
    frames: List[int] = []
    durations_ms: List[int] = []

    for asset in assets:
        start_ms = _to_int_or_none(asset.get("start_ms"))
        end_ms = _to_int_or_none(asset.get("end_ms"))
        dur_ms = _to_int_or_none(asset.get("duration_ms"))
        if start_ms is None:
            return [], [], []
        if end_ms is None and dur_ms is not None:
            end_ms = start_ms + dur_ms
        if end_ms is None or end_ms <= start_ms:
            return [], [], []

        start_f = max(0, int(round((start_ms / 1000.0) * fps_f)))
        end_f = max(start_f + 1, int(round((end_ms / 1000.0) * fps_f)))
        starts.append(start_f)
        ends.append(end_f)

    for i, start_f in enumerate(starts):
        target_end = starts[i + 1] if i + 1 < len(starts) else ends[i]
        if target_end <= start_f:
            target_end = max(start_f + 1, ends[i])
        frame_count = max(1, int(target_end - start_f))
        frames.append(frame_count)
        durations_ms.append(max(1, int(round((frame_count / fps_f) * 1000.0))))

    return starts, frames, durations_ms

def alpha_token(num: int) -> str:
    n = max(0, int(num))
    chars = []
    while True:
        chars.append(chr(ord("a") + (n % 26)))
        n = n // 26
        if n == 0:
            break
    return "".join(reversed(chars)).rjust(4, "a")

def prepared_png_name(kind: str, idx: int, src: Path, suffix: Optional[str] = None, extra: str = "") -> str:
    raw = f"{kind}|{idx}|{src.resolve() if src.exists() else src}"
    if extra:
        raw += f"|{extra}"
    digest = hashlib.md5(raw.encode("utf-8", "ignore")).hexdigest()[:8]
    ext = suffix or src.suffix or ".png"
    ext = str(ext).lower()
    if not ext.startswith("."):
        ext = "." + ext
    if ext not in IMAGE_EXTS:
        ext = ".png"
    return f"{kind}_{alpha_token(idx)}_{digest}_image{ext}"

def _prepared_still_is_current(src: Path, dst: Path) -> bool:
    try:
        if not dst.exists() or dst.stat().st_size <= 0 or dst.stat().st_mtime < src.stat().st_mtime:
            return False
        with Image.open(dst) as im:
            return im.format == "PNG"
    except Exception:
        return False

def copy_prepared_still(src: Path, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if _prepared_still_is_current(src, dst):
        return dst
    with Image.open(src) as im:
        im = ImageOps.exif_transpose(im)
        im.load()
        has_alpha = im.mode in ("RGBA", "LA") or "transparency" in im.info
        out = im.convert("RGBA" if has_alpha else "RGB")
        out.save(dst, format="PNG", optimize=False, compress_level=6)
    return dst

def extract_freeze_frame(src_video: Path, dst_png: Path, actual_sec: Optional[float], source_fps: Optional[float], freeze_sec: Optional[float] = None) -> bool:
    dst_png.parent.mkdir(parents=True, exist_ok=True)
    try:
        if dst_png.exists() and dst_png.stat().st_size > 0 and dst_png.stat().st_mtime >= src_video.stat().st_mtime:
            return True
    except Exception:
        pass
    backoff = 0.08
    if source_fps and source_fps > 0:
        backoff = max(backoff, 1.5 / float(source_fps))
    seek_end = None
    if freeze_sec and freeze_sec > 0:
        seek_end = float(freeze_sec)
        if actual_sec and actual_sec > 0:
            seek_end = min(seek_end, float(actual_sec))
    use_tail_seek = seek_end is None or (actual_sec and abs(float(actual_sec) - float(seek_end)) <= backoff * 2.0)
    if not use_tail_seek and seek_end is not None:
        seek = max(0.0, float(seek_end) - backoff)
        cmd = [
            "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
            "-ss", f"{seek:.6f}",
            "-i", str(src_video),
            "-frames:v", "1",
            str(dst_png),
        ]
        if run_ffmpeg(cmd, f"FREEZE SEEK {src_video.name}"):
            return True
    cmd = [
        "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
        "-sseof", f"-{backoff:.6f}",
        "-i", str(src_video),
        "-frames:v", "1",
        str(dst_png),
    ]
    if run_ffmpeg(cmd, f"FREEZE {src_video.name}"):
        return True
    fallback_end = seek_end if seek_end is not None else actual_sec
    if fallback_end and fallback_end > 0:
        seek = max(0.0, float(fallback_end) - backoff)
        cmd = [
            "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
            "-ss", f"{seek:.6f}",
            "-i", str(src_video),
            "-frames:v", "1",
            str(dst_png),
        ]
        return run_ffmpeg(cmd, f"FREEZE SEEK {src_video.name}")
    return False

def prepare_visual_media_for_resolve(roll: Path, assets: List[dict], slot_frames: List[int], slot_ms: List[int], fps: float) -> List[Path]:
    fps_f = float(fps) if fps and fps > 0 else 24.0
    half_tl_frame = 0.5 / fps_f
    prep_root = roll / "_resolve_visuals"
    still_dir = prep_root / "stills"
    freeze_dir = prep_root / "freezes"
    imports: List[Path] = []

    def add_import(path: Path):
        if path and path.exists() and path not in imports:
            imports.append(path)

    for idx, asset in enumerate(assets):
        seg_idx = int(asset.get("index", idx))
        kind = asset.get("kind")
        if kind == "still":
            src = Path(asset["path"])
            if not src.exists():
                raise RuntimeError(f"still source missing for segment {seg_idx:05d}: {src}")
            dst = still_dir / prepared_png_name("still", seg_idx, src, suffix=".png", extra="cleanpng-v1")
            copy_prepared_still(src, dst)
            asset["import_path"] = dst
            add_import(dst)
            continue

        src_video = Path(asset.get("import_path") or asset.get("path"))
        raw_video = Path(asset.get("path") or src_video)
        if not src_video.exists():
            raise RuntimeError(f"video source missing for segment {seg_idx:05d}: {src_video}")
        add_import(src_video)

        target_f = int(max(1, slot_frames[idx])) if idx < len(slot_frames) else 0
        target_ms = int(slot_ms[idx]) if idx < len(slot_ms) and slot_ms[idx] else int(round(target_f * (1000.0 / fps_f))) if target_f > 0 else 0
        if target_f <= 0 or target_ms <= 0:
            continue

        target_sec = target_f / fps_f
        actual_sec, source_fps, source_frames = ffprobe_video_info(raw_video)
        if actual_sec is None:
            actual_sec = ffprobe_duration(raw_video)
        if not source_fps or source_fps <= 0:
            source_fps = fps_f
        if actual_sec is None:
            actual_sec = target_sec

        take_sec = max(0.001, min(float(actual_sec), target_sec))
        source_take_f = int(max(1, math.ceil(take_sec * float(source_fps))))
        if source_frames:
            source_take_f = min(source_take_f, int(source_frames))
        place_timeline_f = int(max(1, min(target_f, round(take_sec * fps_f))))
        fill_frames = int(max(0, target_f - place_timeline_f))

        asset["actual_sec"] = float(actual_sec)
        asset["source_fps"] = float(source_fps)
        asset["source_frames"] = int(source_frames) if source_frames else None
        asset["source_take_f"] = source_take_f
        asset["place_timeline_f"] = place_timeline_f
        asset["fill_frames"] = fill_frames

        freeze_png = freeze_dir / prepared_png_name("freeze", seg_idx, raw_video, suffix=".png", extra=f"{take_sec:.6f}")
        if not extract_freeze_frame(raw_video, freeze_png, actual_sec, source_fps, freeze_sec=take_sec):
            if fill_frames > 0 and float(actual_sec) < target_sec - half_tl_frame:
                raise RuntimeError(f"failed to create freeze frame for segment {seg_idx:05d}: {raw_video}")
        elif freeze_png.exists():
            asset["freeze_path"] = freeze_png
            add_import(freeze_png)

    return imports

def audit_planned_video_items(items: List[dict]) -> Tuple[int, int]:
    if not items:
        return 0, 0
    ordered = sorted(items, key=lambda x: (int(x["start"]), int(x["end"])))
    gaps = 0
    overlaps = 0
    prev_end = 0
    for item in ordered:
        st = int(item["start"])
        en = int(item["end"])
        if st > prev_end:
            gaps += 1
            slog("AUDIT", f"gap {prev_end}->{st} ({st - prev_end}f) before {item.get('label')}", force=True)
        elif st < prev_end:
            overlaps += 1
            slog("AUDIT", f"overlap {st}->{prev_end} ({prev_end - st}f) at {item.get('label')}", force=True)
        prev_end = max(prev_end, en)
    slog("AUDIT", f"planned V1 items={len(ordered)} gaps={gaps} overlaps={overlaps} end={prev_end}", force=True)
    return gaps, overlaps

def collect_timeline_video_gaps(tl, fps: float, expected_end: Optional[int] = None) -> Tuple[List[Tuple[int, int]], int, int, int]:
    try:
        items = tl.GetItemsInTrack("video", 1)
        items = list(items.values()) if isinstance(items, dict) else list(items or [])
    except Exception:
        items = []

    bounds: List[Tuple[int, int, str]] = []
    for it in items:
        try:
            st, en = get_item_bounds_frames(it, fps)
        except Exception:
            continue
        if en <= st:
            continue
        try:
            nm = (it.GetName() or "").strip()
        except Exception:
            nm = ""
        bounds.append((int(st), int(en), nm))

    bounds.sort(key=lambda x: (x[0], x[1], x[2]))
    gaps: List[Tuple[int, int]] = []
    overlaps = 0
    prev_end = 0
    for st, en, nm in bounds:
        if st > prev_end:
            gaps.append((prev_end, st))
            slog("AUDIT", f"actual gap {prev_end}->{st} ({st - prev_end}f) before {nm}", force=True)
        elif st < prev_end:
            overlaps += 1
            slog("AUDIT", f"actual overlap {st}->{prev_end} ({prev_end - st}f) at {nm}", force=True)
        prev_end = max(prev_end, en)

    if expected_end is not None and expected_end > prev_end:
        gaps.append((prev_end, int(expected_end)))
        slog("AUDIT", f"actual tail gap {prev_end}->{int(expected_end)} ({int(expected_end) - prev_end}f)", force=True)

    slog("AUDIT", f"actual V1 items={len(bounds)} gaps={len(gaps)} overlaps={overlaps} end={prev_end}", force=True)
    return gaps, len(gaps), overlaps, prev_end

# ---------- frame planning (С€Р°Р±Р»РѕРЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ, Р‘Р•Р— РґС‹СЂ) ----------
def plan_frames_like_template(times_ms: List[int], fps: float = 24.0) -> Tuple[List[int], List[int], Dict[str, list]]:
    fps_i = int(round(fps)) or 24
    frame_ms = 1000.0 / fps_i

    duration_frames = [max(1, int(round(float(ms) / frame_ms))) for ms in times_ms]

    acc_ms=[]; s=0.0
    for ms in times_ms:
        s += float(ms); acc_ms.append(int(s))

    y = [int(round(ms / frame_ms)) for ms in acc_ms]

    diffs=[]; s2=0
    for i, df in enumerate(duration_frames):
        s2 += df
        diffs.append(y[i] - s2)

    final=[]
    for i, df in enumerate(duration_frames):
        fi = max(1, df - diffs[i])
        final.append(fi)

    # СЃС‚Р°СЂС‚С‹ СЃС‡РёС‚Р°РµРј РёР· real final, С‡С‚РѕР±С‹ СЂРѕРІРЅРѕ СЃС‚С‹РєРѕРІР°С‚СЊ РїРѕРґСЂСЏРґ
    start_frames = [0]
    for fi in final[:-1]:
        start_frames.append(start_frames[-1] + fi)

    debug = {
        "duration_ms": [int(x) for x in times_ms],
        "duration_frames": duration_frames,
        "start_time_ms": acc_ms,
        "start_time_frames": y,
        "difference": diffs,
        "final_dur_frames": final
    }
    return start_frames, final, debug

# ---------- transcript-aligned placement ----------
_END_CHARS = {".", "!", "?", "…", "。", "！", "？"}
_CLOSERS = {'"', "”", "’", "'", ")", "]", "}", "»", "›", "）", "】", "』", "」"}
_ABBREV = {
    # EN
    "mr","mrs","ms","mx","dr","prof","sr","jr","st","mt","ft","vs",
    "etc","eg","ie","fig","eq","ref","refs","no","nos","dept","inc","ltd","corp","co","bros",
    "est","misc","approx","appt","avg","min","max","temp","vol","ch","sec","pp","p",
    "jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec",
    "am","pm","a.m","p.m","u.s","u.k","e.u","u.n",

    # RU
    "г","гг","ул","д","кв","стр","рис","пр","просп","наб","бул","пер","пл","пос","обл","р-н",
    "им","акад","доц","проф","см","т.д","т.п","и т.д","и т.п","др",
    "мин","сек","ч","мес","гл","ст","пп","п","млн","млрд","тыс","руб","коп",

    # PL
    "np","itp","itd","m.in","tzn","tj","wg","ok","nr","ul","al","pl","r",
    "godz","min","sek","str","rys","tab","rozdz","pkt","ppkt",
    "dr","hab","mgr","inz","prof",

    # CZ
    "např","napr","tzv","atd","apod","tj","tzn","cl","odst","str","obr","tab","kap",
    "ing","mgr","mudr","judr","phdr","bc","dis",

    # SP (ES)
    "sr","sra","srta","dr","dra","ud","uds","pag","pags","num","nro",
    "aprox","etc","av","pto","dpto","art","cap","eeuu",

    # IT
    "sig","sigg","sigra","sigre","dr","dott","dssa","ing","avv","prof",
    "pag","pagg","num","art","cap","ecc","ca","circa","vle","pza","pzza","cso",

    # FR
    "m","mm","mme","mlle","mlles","dr","pr","etc","env","av","bd","st","ste",
    "no","num","art","chap","pag","tel","tva",

    # GE (DE)
    "hr","fr","dr","prof","bzw","z.b","zb","bsp","bspw","usw","vgl","ca",
    "d.h","dh","u.a","ua","u.ä","ue","str","nr","s","abb","ggf","dipl","ing","dipl-ing",

    # SW
    "bl.a","bla","t.ex","tex","dvs","m.fl","mfl","m.m","mm","osv",
    "ca","nr","sid","kap","fig","kl","dr","prof","ing","fru","herr",

    # HE
    "דר","פרופ","גב","מר","מס","עמ","וכו","לדוג","רח","טל","סע","פרק",
}

def _clean_text(s) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\r\n", " ").replace("\n", " ")).strip()

def _pick_first(obj: dict, keys: List[str]):
    for k in keys:
        if isinstance(obj, dict) and obj.get(k) is not None:
            return obj.get(k)
    return None

def _to_number(x) -> Optional[float]:
    try:
        n = float(x)
        if math.isfinite(n):
            return n
    except Exception:
        pass
    return None

def _to_int_safe(x, default: int = 0) -> int:
    n = _to_number(x)
    if n is None:
        return int(default)
    try:
        return int(round(n))
    except Exception:
        return int(default)

def _normalize_start_end_ms(raw_seg: dict, time_unit: str = "auto") -> Tuple[int, int]:
    s_ms = _to_number(raw_seg.get("start_ms"))
    e_ms = _to_number(raw_seg.get("end_ms"))
    if s_ms is not None and e_ms is not None:
        start = max(0, int(round(s_ms)))
        end = max(start, int(round(e_ms)))
        return start, end

    s = _to_number(raw_seg.get("start"))
    e = _to_number(raw_seg.get("end"))
    if s is None or e is None:
        return 0, 0

    factor = 1
    if time_unit == "s":
        factor = 1000
    elif time_unit == "ms":
        factor = 1
    else:
        diff = e - s
        looks_like_seconds = (diff > 0 and diff <= 120 and e <= 20000)
        factor = 1000 if looks_like_seconds else 1

    start = max(0, int(round(s * factor)))
    end = max(start, int(round(e * factor)))
    return start, end

def _fill_gaps_in_place(segs: List[dict], total_dur_ms: Optional[int]) -> int:
    if not segs:
        return int(total_dur_ms or 0)

    segs.sort(key=lambda x: (int(x["startMs"]), int(x["_origIndex"])))

    if segs[0]["startMs"] > 0:
        segs[0]["startMs"] = 0
    if segs[0]["startMs"] < 0:
        segs[0]["startMs"] = 0
    if segs[0]["endMs"] < segs[0]["startMs"]:
        segs[0]["endMs"] = segs[0]["startMs"]

    if not (isinstance(total_dur_ms, int) and total_dur_ms > 0):
        total_dur_ms = max(int(s["endMs"]) for s in segs)
    else:
        total_dur_ms = max(0, int(total_dur_ms))

    for i in range(len(segs) - 1):
        cur = segs[i]
        nxt = segs[i + 1]

        if cur["startMs"] < 0:
            cur["startMs"] = 0
        if nxt["startMs"] < 0:
            nxt["startMs"] = 0
        if cur["endMs"] < cur["startMs"]:
            cur["endMs"] = cur["startMs"]
        if nxt["endMs"] < nxt["startMs"]:
            nxt["endMs"] = nxt["startMs"]

        desired_end = int(nxt["startMs"]) - 1
        if desired_end < int(cur["startMs"]):
            desired_end = int(cur["startMs"])
        cur["endMs"] = desired_end
        if cur["endMs"] < cur["startMs"]:
            cur["endMs"] = cur["startMs"]

    last = segs[-1]
    if last["startMs"] < 0:
        last["startMs"] = 0
    if last["endMs"] < last["startMs"]:
        last["endMs"] = last["startMs"]
    if last["endMs"] < total_dur_ms:
        last["endMs"] = total_dur_ms
    if last["endMs"] < last["startMs"]:
        last["endMs"] = last["startMs"]

    return int(total_dur_ms)

def _is_letter_or_digit(ch: str) -> bool:
    return re.match(r"[0-9A-Za-zÀ-ÖØ-öø-ÿА-Яа-яЁё]", ch or "") is not None

def _is_abbrev_dot(text: str, dot_pos: int) -> bool:
    i = dot_pos - 1
    while i >= 0 and text[i] == " ":
        i -= 1

    end = i + 1
    while i >= 0 and (_is_letter_or_digit(text[i]) or text[i] == "-"):
        i -= 1
    start = i + 1

    word = text[start:end].lower()
    if not word:
        return False
    if len(word) == 1:
        return True
    if word in _ABBREV:
        return True

    lookback = text[max(0, start - 4): dot_pos + 1].lower()
    if "." in lookback and len(lookback) <= 6:
        return True
    return False

def _find_sentence_boundaries(text: str) -> List[int]:
    out: List[int] = []
    n = len(text)
    i = 0
    while i < n:
        ch = text[i]
        if ch not in _END_CHARS:
            i += 1
            continue

        j = i
        while j + 1 < n and text[j + 1] in _END_CHARS:
            j += 1

        if text[j] == "." and _is_abbrev_dot(text, j):
            i = j + 1
            continue

        k = j + 1
        while k < n and text[k] in _CLOSERS:
            k += 1

        if k == n or (k < n and text[k].isspace()):
            out.append(k)
        i = j + 1

    return out

def _last_whitespace_before(text: str, idx: int) -> int:
    if not text:
        return -1
    i = min(idx, len(text) - 1)
    while i >= 0:
        if text[i].isspace():
            return i
        i -= 1
    return -1

def _extract_total_duration_ms(transcript_data: dict) -> Optional[int]:
    cands: List[float] = []
    for k in ("total_dur_ms", "totalDurMs", "total_duration_ms"):
        v = transcript_data.get(k)
        try:
            if v is not None:
                cands.append(float(v))
        except Exception:
            pass

    for v in (
        transcript_data.get("total_duration_sec"),
        transcript_data.get("meta", {}).get("progress", {}).get("total_duration_sec"),
        transcript_data.get("meta", {}).get("total_duration_sec"),
    ):
        try:
            if v is not None:
                cands.append(float(v) * 1000.0)
        except Exception:
            pass

    for c in cands:
        if math.isfinite(c) and c > 0:
            return int(round(c))
    return None

def _split_transcript_parts(raw_segments: List[dict], min_ms: int, max_ms: int, total_dur_ms: Optional[int]) -> Tuple[List[dict], int]:
    if not (isinstance(min_ms, int) and min_ms > 0):
        min_ms = TRANSCRIPT_PART_MIN_MS_DEFAULT
    if not (isinstance(max_ms, int) and max_ms > 0):
        max_ms = TRANSCRIPT_PART_MAX_MS_DEFAULT
    if min_ms > max_ms:
        min_ms, max_ms = max_ms, min_ms

    segs: List[dict] = []
    for i, seg in enumerate(raw_segments if isinstance(raw_segments, list) else []):
        if not isinstance(seg, dict):
            continue
        text = _clean_text(seg.get("text") or seg.get("transcript") or seg.get("caption") or "")
        s, e = _normalize_start_end_ms({
            "start_ms": seg.get("start_ms", _pick_first(seg, ["start_ms", "start"])),
            "end_ms": seg.get("end_ms", _pick_first(seg, ["end_ms", "end"])),
            "start": seg.get("start", _pick_first(seg, ["start_ms", "start"])),
            "end": seg.get("end", _pick_first(seg, ["end_ms", "end"])),
        })
        if text:
            segs.append({
                "_origIndex": i,
                "text": text,
                "startMs": int(s),
                "endMs": int(e),
            })

    total_ms = _fill_gaps_in_place(segs, total_dur_ms)
    segments = [{"text": s["text"], "durMs": max(0, int(s["endMs"]) - int(s["startMs"]))} for s in segs]

    parts: List[dict] = []

    def push_part(txt: str, dur: int):
        t = _clean_text(txt)
        if not t:
            return
        d = max(0, _to_int_safe(dur, 0))
        if d <= 0:
            return
        parts.append({
            "partIndex": len(parts),
            "text": t,
            "meta": {"durMs": d, "charCount": len(t)},
        })

    buf_text = ""
    buf_dur_ms = 0

    def cut_buffer_strict():
        nonlocal buf_text, buf_dur_ms
        t = buf_text
        total_chars = len(t)

        if total_chars == 0 or buf_dur_ms <= 0:
            push_part(t, buf_dur_ms)
            buf_text = ""
            buf_dur_ms = 0
            return

        denom = max(1, buf_dur_ms)

        max_char = int(math.floor(total_chars * (max_ms / denom)))
        max_char = max(1, min(total_chars - 1, max_char))

        min_char = int(math.floor(total_chars * (min_ms / denom)))
        min_char = max(0, min(max_char, min_char))

        boundaries = _find_sentence_boundaries(t)
        cut_idx = -1

        for b in reversed(boundaries):
            if min_char <= b <= max_char:
                cut_idx = b
                break

        if cut_idx < 0:
            for b in reversed(boundaries):
                if b <= max_char:
                    cut_idx = b
                    break

        if cut_idx < 0:
            ws = _last_whitespace_before(t, max_char)
            if ws > 0:
                cut_idx = ws + 1

        if cut_idx < 1 or cut_idx >= total_chars:
            cut_idx = max_char

        left_raw = t[:cut_idx]
        right_raw = t[cut_idx:]
        left_text = _clean_text(left_raw)
        right_text = _clean_text(right_raw)

        left_ratio = cut_idx / max(1, total_chars)
        left_dur = int(math.floor(buf_dur_ms * left_ratio))
        if left_dur > max_ms:
            left_dur = max_ms

        right_dur = max(0, buf_dur_ms - left_dur)

        push_part(left_text, left_dur)
        buf_text = right_text
        buf_dur_ms = right_dur

    for seg in segments:
        buf_text = (buf_text + " " + seg["text"]).strip() if buf_text else seg["text"]
        buf_dur_ms += int(seg["durMs"])

        while buf_text and buf_dur_ms > max_ms:
            cut_buffer_strict()
            if buf_dur_ms < min_ms:
                break

    if buf_text:
        while buf_text and buf_dur_ms > max_ms:
            cut_buffer_strict()
            if buf_dur_ms < min_ms:
                break
        if buf_text:
            push_part(buf_text, buf_dur_ms)

    # Санитизация: durMs всегда положительный int, partIndex/charCount консистентны
    clean_parts: List[dict] = []
    for p in parts:
        if not isinstance(p, dict):
            continue
        txt = _clean_text(p.get("text", ""))
        if not txt:
            continue
        d = max(0, _to_int_safe((p.get("meta") or {}).get("durMs"), 0))
        if d <= 0:
            continue
        clean_parts.append({
            "partIndex": len(clean_parts),
            "text": txt,
            "meta": {"durMs": d, "charCount": len(txt)},
        })

    return clean_parts, total_ms

def _find_transcript_path(roll: Path) -> Optional[Path]:
    t_dir = roll / "VOICE" / "transcript"
    if not t_dir.exists() or not t_dir.is_dir():
        return None

    exact = t_dir / f"{roll.name}_transcript.json"
    if exact.exists() and exact.is_file():
        return exact

    cands = sorted([p for p in t_dir.glob("*_transcript.json") if p.is_file()], key=lambda p: natural_key(p.name))
    return cands[0] if cands else None

def build_slots_from_transcript(roll: Path, fps: float, min_ms: int, max_ms: int) -> Tuple[List[int], List[int], List[int], Optional[Path]]:
    tp = _find_transcript_path(roll)
    if not tp:
        return [], [], [], None

    try:
        data = json.loads(tp.read_text(encoding="utf-8"))
    except Exception as e:
        slog("PLAN", f"transcript parse fail: {e}", force=True)
        return [], [], [], tp

    raw = data.get("items") or data.get("segments") or data.get("result", {}).get("items") or []
    if not isinstance(raw, list) or not raw:
        slog("PLAN", "transcript has no items", force=True)
        return [], [], [], tp

    total_ms = _extract_total_duration_ms(data)
    parts, _ = _split_transcript_parts(raw, int(min_ms), int(max_ms), total_ms)
    if not parts:
        return [], [], [], tp

    fps_i = int(round(fps)) or 24
    part_ms = [max(0, _to_int_safe((p.get("meta") or {}).get("durMs"), 0)) for p in parts if isinstance(p, dict)]
    part_ms = [ms for ms in part_ms if ms > 0]
    if not part_ms:
        slog("PLAN", "transcript parts have no valid durMs", force=True)
        return [], [], [], tp
    slot_frames = [max(1, int(math.ceil((ms / 1000.0) * fps_i))) for ms in part_ms]

    starts = [0]
    for f in slot_frames[:-1]:
        starts.append(starts[-1] + int(f))

    return starts, slot_frames, part_ms, tp

def _fit_slot_frames_to_count(slot_frames: List[int], target_count: int) -> List[int]:
    src = [max(1, int(f)) for f in (slot_frames or []) if int(f) > 0]
    n = int(target_count)
    if n <= 0:
        return []
    if not src:
        return []

    out = list(src)

    # Shrink by merging shortest local chunks into a neighbor.
    while len(out) > n:
        i = min(range(len(out)), key=lambda k: out[k])
        if len(out) == 1:
            break
        if i == 0:
            out[1] += out[0]
            del out[0]
        elif i == len(out) - 1:
            out[-2] += out[-1]
            out.pop()
        else:
            if out[i - 1] <= out[i + 1]:
                out[i - 1] += out[i]
                del out[i]
            else:
                out[i + 1] += out[i]
                del out[i]

    # Grow by splitting the longest chunks in half.
    while len(out) < n:
        i = max(range(len(out)), key=lambda k: out[k])
        cur = out[i]
        left = max(1, cur // 2)
        right = max(1, cur - left)
        out[i] = left
        out.insert(i + 1, right)

    return out

# ---------- helpers: fast payloads ----------
def make_video_payload(mpi, track_idx: int, record_frame: int, length_frames: int):
    return {
        "mediaPoolItem": mpi,
        "trackType": "video",
        "trackIndex": int(track_idx),
        "recordFrame": int(record_frame),
        "startFrame": 0,
        "endFrame": int(max(1, length_frames)),
    }

def make_audio_payload(mpi, track_idx: int, record_frame: int, length_frames: int, fps: float):
    """
    РџСЂРѕСЃС‚РѕР№ РїСЌР№Р»РѕР°Рґ: Р±РµСЂС‘Рј 1-Р№ Р°СѓРґРёРѕРїРѕС‚РѕРє Рё РєР»Р°РґС‘Рј РїРѕРґСЂСЏРґ.
    """
    L = int(max(1, length_frames))
    return {
        "mediaPoolItem": mpi,
        "mediaType": 2,
        "trackType": "audio",
        "trackIndex": int(track_idx),
        "recordFrame": int(record_frame),
        "sourceIndex": 1,   # РїРµСЂРІС‹Р№ Р°СѓРґРёРѕРїРѕС‚РѕРє/РґРѕСЂРѕР¶РєР° РєР»РёРїР°
        "sourceIn": 0,
        "sourceOut": L,
    }


def set_default_still_duration(project, frames: int):
    try:
        project.SetSetting({"timelineDefaultStillDuration": int(max(1, frames))})
    except Exception:
        pass

def set_still_out_point(mpi, length_frames: int):
    if not mpi or length_frames is None:
        return
    try:
        clip_type = (mpi.GetClipProperty("Type") or "").strip().lower()
    except Exception:
        clip_type = ""
    if clip_type and "still" not in clip_type and "image" not in clip_type:
        return
    try:
        mpi.SetClipProperty("Out", str(int(max(1, length_frames))))
    except Exception:
        pass

def adjust_still_item_bounds(tl, still_bounds: List[dict], fps: float):
    if not tl or not still_bounds:
        return
    try:
        items = tl.GetItemsInTrack("video", 1)
        items = list(items.values()) if isinstance(items, dict) else list(items or [])
    except Exception:
        items = []
    if not items:
        return

    used = set()
    for entry in still_bounds:
        path = Path(entry["path"])
        fname = path.name.lower()
        stem = path.stem.lower()
        target_start = int(entry["start"])
        target_end = int(entry["end"])
        best = None
        best_score = None
        fallback = None
        fallback_score = None

        for it in items:
            if id(it) in used:
                continue
            try:
                st, _ = get_item_bounds_frames(it, fps)
            except Exception:
                st = 0
            delta = abs(int(st) - target_start)
            try:
                nm = (it.GetName() or "").strip().lower()
            except Exception:
                nm = ""
            try:
                fp = (it.GetClipProperty("File Path") or "").strip().lower()
                fp_name = Path(fp).name.lower() if fp else ""
                fp_stem = Path(fp).stem.lower() if fp else ""
            except Exception:
                fp = ""
                fp_name = ""
                fp_stem = ""

            matched = (
                nm in {fname, stem}
                or fp_name == fname
                or fp_stem == stem
                or (fp and fp.endswith(fname))
            )
            if matched and (best is None or delta < best_score):
                best = it
                best_score = delta
            if fallback is None or delta < fallback_score:
                fallback = it
                fallback_score = delta

        target = best or fallback
        if not target:
            continue
        used.add(id(target))
        try:
            target.SetStart(int(target_start))
            target.SetEnd(int(max(target_start + 1, target_end)))
        except Exception:
            pass


def get_media_duration_frames_from_props(mpi, fps: float, default_seconds: float = 3.0) -> int:
    """
    РџСЂРѕСЃС‚РѕР№ СЃРїРѕСЃРѕР±: Р±РµСЂС‘Рј Duration (СЃРµРєСѓРЅРґС‹ РёР»Рё TC) Рё РїРµСЂРµРІРѕРґРёРј РІ РєР°РґСЂС‹ С‚Р°Р№РјР»Р°Р№РЅР°.
    Р•СЃР»Рё РЅРµС‚ вЂ” РґР°С‘Рј С„РёРєСЃ РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ.
    """
    fps_i = int(round(fps)) or 24
    try:
        props = mpi.GetClipProperty() or {}
    except Exception:
        props = {}

    dur = props.get("Duration")
    if dur is not None:
        try:
            if isinstance(dur, (int, float)):
                seconds = float(dur)
                return max(1, int(round(seconds * fps_i)))
            tc_val = parse_tc_to_frames(str(dur), fps)
            if tc_val and tc_val > 0:
                return int(tc_val)
            seconds = float(str(dur))
            return max(1, int(round(seconds * fps_i)))
        except Exception:
            pass

    return max(1, int(round(default_seconds * fps_i)))

# ---------- transitions helpers (FAST & ROBUST) ----------

def list_bin_items_by_prefix_recursive(folder, prefix_ci: str):
    """Р РµРєСѓСЂСЃРёРІРЅРѕ СЃРѕР±РёСЂР°РµРј РєР»РёРїС‹ РёР· Р±РёРЅР° Рё РІСЃРµС… РїРѕРґРїР°РїРѕРє РїРѕ РїСЂРµС„РёРєСЃСѓ РёРјРµРЅРё РєР»РёРїР°."""
    items=[]
    if not folder:
        return items
    stack=[folder]
    while stack:
        f=stack.pop()
        try:
            for c in list(f.GetClipList() or []):
                nm=""
                try:
                    nm=(c.GetClipProperty("Clip Name") or c.GetName() or "").strip()
                except Exception:
                    try: nm=(c.GetName() or "").strip()
                    except Exception: nm=""
                if nm.lower().startswith(prefix_ci.lower()):
                    items.append((nm, c))
        except Exception:
            pass
        try:
            subs=list(f.GetSubFolderList() or [])
            for sf in subs: stack.append(sf)
        except Exception:
            pass
    items.sort(key=lambda t:natural_key(t[0]))
    return [c for (_nm,c) in items]


def get_mpi_duration_frames(mpi, fps: float, default_seconds: float) -> int:
    fps_i=int(round(fps)) or 24
    try:
        props = mpi.GetClipProperty() or {}
    except Exception:
        props = {}
    try:
        fr = props.get("Frames") or props.get("Video Frames") or props.get("Nb Frames") or props.get("Audio Frames")
        if fr is not None:
            val = int(str(fr).split('.')[0])
            if val > 0: return val
    except Exception:
        pass
    try:
        dur = props.get("Duration")
        if dur:
            if isinstance(dur, (int, float)):
                val = int(round(float(dur) * fps_i))
                if val > 0: return val
            else:
                val = parse_tc_to_frames(str(dur), fps)
                if val and val > 0: return val
    except Exception:
        pass
    return int(round(default_seconds * fps_i))


def get_item_bounds_frames(ti, fps: float) -> tuple[int,int]:
    """РќР°РґС‘Р¶РЅРѕ РґРѕСЃС‚Р°С‘Рј СЃС‚Р°СЂС‚/СЌРЅРґ; РїРѕСЃР»Рµ batch-Append РїСЂСЏРјС‹Рµ РјРµС‚РѕРґС‹ РёРЅРѕРіРґР° РІРѕР·РІСЂР°С‰Р°СЋС‚ 0."""
    s=e=None
    for m in ("GetStartFrame","GetStart"):
        if hasattr(ti,m):
            try:
                v=getattr(ti,m)()
                if isinstance(v,int): s=v; break
            except Exception: pass
    for m in ("GetEndFrame","GetEnd"):
        if hasattr(ti,m):
            try:
                v=getattr(ti,m)()
                if isinstance(v,int): e=v; break
            except Exception: pass
    if s is None: s=0
    if e is None: e=s
    return int(s), int(e)


def place_transitions_on_v4(project, tl, mp, fps: float, _v1_items_unused, fusion_folder):
    """
    Р‘С‹СЃС‚СЂР°СЏ РєРѕСЂСЂРµРєС‚РЅР°СЏ СЂР°СЃСЃС‚Р°РЅРѕРІРєР°:
      вЂў С‡РёСЃС‚РёРј V4;
      вЂў РёС‰РµРј transition_* СЂРµРєСѓСЂСЃРёРІРЅРѕ РІ fusion (РёР»Рё transitions) Р±РёРЅРµ;
      вЂў СЃС‡РёС‚Р°РµРј СЃС‚С‹РєРё V1 РїРѕ С„Р°РєС‚РёС‡РµСЃРєРёРј РіСЂР°РЅРёС†Р°Рј;
      вЂў СЃС‚Р°РІРёРј РѕРґРЅРёРј batch AppendToTimeline Р±РµР· РїРµСЂРµСЃРµС‡РµРЅРёР№.
    """
    ensure_track_exists(tl, "video", 4)
    # РѕС‡РёС‰Р°РµРј V4
    try:
        items_v4 = tl.GetItemsInTrack("video", 4)
        if isinstance(items_v4, dict): items_v4 = list(items_v4.values())
        if items_v4:
            for m in ("DeleteItems","DeleteClips"):
                if hasattr(tl,m):
                    try:
                        getattr(tl,m)(items_v4); break
                    except Exception: pass
    except Exception: pass

    # СЃРѕР±РёСЂР°РµРј РїРµСЂРµС…РѕРґС‹
    transitions = list_bin_items_by_prefix_recursive(fusion_folder, "transition_")
    if not transitions:
        slog("FX", "no transitions")
        return

    # Р°РєС‚СѓР°Р»СЊРЅС‹Рµ V1-СЌР»РµРјРµРЅС‚С‹
    try:
        v1_items = tl.GetItemsInTrack("video", 1)
        v1_items = list(v1_items.values()) if isinstance(v1_items, dict) else list(v1_items or [])
    except Exception:
        v1_items = []
    if len(v1_items) < 2:
        slog("FX", "no cuts")
        return

    items_sorted = sorted(v1_items, key=lambda it: get_item_bounds_frames(it, fps)[0])
    cut_frames = [get_item_bounds_frames(items_sorted[i], fps)[0] for i in range(1,len(items_sorted))]
    if not cut_frames:
        slog("FX", "no cuts")
        return

    # РґРµС‚РµСЂРјРёРЅРёСЂРѕРІР°РЅРЅС‹Р№ РІС‹Р±РѕСЂ СЌС„С„РµРєС‚РѕРІ
    seed_int = int.from_bytes(hashlib.md5(("transitions"+project.GetName()).encode("utf-8")).digest(), "big")
    rnd = random.Random(seed_int)
    pool = transitions[:]
    order=[]
    for _ in range(len(cut_frames)):
        if not pool: pool = transitions[:]
        order.append(pool.pop(rnd.randrange(len(pool))))

    # Р±Р°С‚С‡-РїСЌР№Р»РѕР°РґС‹ Р±РµР· РїРµСЂРµСЃРµС‡РµРЅРёР№
    payloads=[]; last_end=-1; placed=0
    for cut, mpi_tr in zip(cut_frames, order):
        L = get_mpi_duration_frames(mpi_tr, fps, default_seconds=3.0)
        if L<1: continue
        start = max(0, cut - max(1, L//2))
        end = start + L
        if start < last_end:
            continue
        payloads.append({
            "mediaPoolItem": mpi_tr,
            "trackType": "video",
            "trackIndex": 4,
            "recordFrame": int(start),
            "startFrame": 0,
            "endFrame": int(L),
        })
        last_end = end
        placed += 1

    if payloads:
        try:
            mp.AppendToTimeline(payloads)
        except Exception as e:
            slog("FX", f"append FAIL: {e}", force=True)
            placed = 0

    slog("FX", f"placed: {placed}")

def place_motions_fill_v3(project, tl, mp, fps: float, fusion_folder, out_frame: int):
    """
    РЎРѕР±РёСЂР°РµРј РІСЃРµ motion_* РёР· fusion/transitions Р±РёРЅР° Рё РєР»Р°РґС‘Рј РёС… РџРћР”Р РЇР” РЅР° V3
    РІ СЂР°РЅРґРѕРјРЅРѕРј РїРѕСЂСЏРґРєРµ, РїРѕРєР° СЃСѓРјРјР°СЂРЅР°СЏ РґР»РёРЅР° РЅРµ СЃС‚Р°РЅРµС‚ >= out_frame.
    """
    ensure_track_exists(tl, "video", 3)

    # РѕС‡РёСЃС‚РєР° V3
    try:
        items_v3 = tl.GetItemsInTrack("video", 3)
        if isinstance(items_v3, dict): items_v3 = list(items_v3.values())
        if items_v3:
            for m in ("DeleteItems", "DeleteClips"):
                if hasattr(tl, m):
                    try:
                        getattr(tl, m)(items_v3); break
                    except Exception:
                        pass
    except Exception:
        pass

    motions = list_bin_items_by_prefix_recursive(fusion_folder, "motion_")
    if not motions:
        slog("FX", "no motions")
        return

    # СЂР°РЅРґРѕРјРЅС‹Р№ РїРѕСЂСЏРґРѕРє Рё С†РёРєР»РёС‡РµСЃРєР°СЏ РІС‹РґР°С‡Р°, РїРѕРєР° РЅРµ РґРѕР±СЊС‘Рј out_frame
    import random, hashlib
    # РЅР°СЃС‚РѕСЏС‰РёР№ СЂР°РЅРґРѕРј РЅР° РєР°Р¶РґС‹Р№ Р·Р°РїСѓСЃРє:
    random.shuffle(motions)

    fps_i = int(round(fps)) or 24
    cur = 0
    payloads = []
    placed = 0

    def dur_frames(mpi):
        # РµСЃС‚СЊ helper get_mpi_duration_frames РІ СЃРєСЂРёРїС‚Рµ вЂ” РёСЃРїРѕР»СЊР·СѓРµРј РµРіРѕ
        return max(1, int(get_mpi_duration_frames(mpi, fps, default_seconds=3.0)))

    # С†РёРєР»: РїРѕРІС‚РѕСЂСЏРµРј РїР°С‡РєР°РјРё (РїРµСЂРµС‚Р°СЃРѕРІС‹РІР°СЏ РјРµР¶РґСѓ С†РёРєР»Р°РјРё), РїРѕРєР° РЅРµ РїРµСЂРµРєСЂРѕРµРј out_frame
    while cur < int(out_frame):
        pool = motions[:]
        random.shuffle(pool)
        for mpi_fx in pool:
            L = dur_frames(mpi_fx)
            payloads.append({
                "mediaPoolItem": mpi_fx,
                "trackType": "video",
                "trackIndex": 3,
                "recordFrame": int(cur),
                "startFrame": 0,
                "endFrame": int(L),
            })
            cur += L
            placed += 1
            if cur >= int(out_frame):
                break

    if payloads:
        try:
            mp.AppendToTimeline(payloads)
        except Exception as e:
            slog("FX", f"motions append FAIL: {e}", force=True)
            return

    slog("FX", f"motions V3 placed: {placed} items, end={tc_from_frames(cur, fps)} (>= out={tc_from_frames(out_frame, fps)})")


# ---------- main ----------
def main():
    global VERBOSE
    ap = argparse.ArgumentParser(description="Import ready video clips to timeline: V1(video) + A1(clip audio) + A2(voice).")
    ap.add_argument("roll_folder", help=".../Channels/<Channel>/VIDEOS/<RollFolder>")
    ap.add_argument("--project", dest="project_name", default=None)
    ap.add_argument("--auto-render", dest="auto_render", action="store_true", default=False)
    ap.add_argument("--verbose", dest="verbose", action="store_true", default=False, help="detailed logs")
    ap.add_argument("--timeline-fps", dest="timeline_fps", type=float, default=None, help="override timeline FPS if Resolve API reports the wrong value")
    ap.add_argument("--part-min-ms", dest="part_min_ms", type=int, default=TRANSCRIPT_PART_MIN_MS_DEFAULT, help="min part duration for transcript splitter")
    ap.add_argument("--part-max-ms", dest="part_max_ms", type=int, default=TRANSCRIPT_PART_MAX_MS_DEFAULT, help="max part duration for transcript splitter")
    ap.add_argument("--no-transcript-slots", dest="no_transcript_slots", action="store_true", default=False, help="place clips sequentially, ignore transcript-based slots")
    args = ap.parse_args()
    VERBOSE = bool(args.verbose)

    slog("START", "begin", force=True)

    roll = Path(args.roll_folder).resolve()
    if not roll.exists():
        print("false"); raise SystemExit(f"roll not found: {roll}")

    clips_root = roll / "CLIPS"
    clips_dir = clips_root / "videoClips"
    start_frames_dir = roll / "CLIPS" / "startFrames"

    # ---------- scan inputs ----------
    video_candidates: Dict[object, Path] = {}

    def add_video_candidates(folder: Path):
        if not folder.exists() or not folder.is_dir():
            return
        for p in sorted([x for x in folder.iterdir() if x.is_file() and x.suffix.lower() == ".mp4"], key=lambda x: natural_key(x.name)):
            idx = _first_int_from_stem(p)
            key = ("idx", idx) if idx is not None else ("stem", p.stem.lower())
            video_candidates.setdefault(key, p)

    add_video_candidates(clips_dir)
    add_video_candidates(clips_root)
    raw_video_files = sorted(list(video_candidates.values()), key=lambda p:natural_key(p.name))
    raw_still_files = sorted(
        [p for p in (list(start_frames_dir.iterdir()) if start_frames_dir.exists() and start_frames_dir.is_dir() else []) if p.is_file() and is_image(p)],
        key=lambda p:natural_key(p.name),
    )
    try:
        visual_assets = build_visual_assets(roll, raw_video_files, raw_still_files)
    except Exception as e:
        print("false"); raise SystemExit(str(e))

    raw_clip_files = []
    for asset in visual_assets:
        if asset.get("kind") == "video":
            p = Path(asset["path"])
            if p not in raw_clip_files:
                raw_clip_files.append(p)

    # VOICE files (chain on A1)
    audio_dir = roll / "VOICE"
    audio_files = sorted([p for p in (list(audio_dir.iterdir()) if audio_dir.exists() else []) if p.is_file() and is_audio(p)], key=lambda p:natural_key(p.name))
    slog("SCAN", f"visual segments: {len(visual_assets)}")
    slog("SCAN", f"video clips: {len(raw_clip_files)}")
    slog("SCAN", f"start frames: {len(raw_still_files)}")
    slog("SCAN", f"voice: {len(audio_files)}")

    if not require_cmd("ffmpeg"):
        print("false"); raise SystemExit("need ffmpeg")

    # Нормализация громкости до импорта в Resolve.
    clip_files, audio_files = normalize_media_before_import(roll, raw_clip_files, audio_files)
    norm_by_raw = {}
    for raw, norm in zip(raw_clip_files, clip_files):
        try:
            norm_by_raw[raw.resolve().as_posix().lower()] = norm
        except Exception:
            norm_by_raw[raw.as_posix().lower()] = norm
    for asset in visual_assets:
        if asset.get("kind") == "video":
            raw_path = Path(asset["path"])
            asset["has_audio"] = media_has_audio_stream(raw_path)
            try:
                key = raw_path.resolve().as_posix().lower()
            except Exception:
                key = raw_path.as_posix().lower()
            asset["import_path"] = norm_by_raw.get(key, raw_path)
        else:
            asset["import_path"] = Path(asset["path"])

    slog("SCAN", f"video clips normalized: {len(clip_files)}")
    slog("SCAN", f"voice normalized: {len(audio_files)}")

    # ---------- Resolve / TL ----------
    resolve = acquire_resolve()
    pm = resolve.GetProjectManager()
    project = pm.LoadProject(args.project_name.strip()) if args.project_name else pm.GetCurrentProject()
    if not project and args.project_name:
        try:
            if hasattr(pm, "CreateProject"):
                project = pm.CreateProject(args.project_name.strip())
        except Exception:
            project = None
    if not project:
        print("false"); raise SystemExit("project open/create failed")

    mp = project.GetMediaPool()
    TIMELINE_NAME = "Timeline"
    tl = get_or_create_timeline(project, mp, TIMELINE_NAME)
    if not tl:
        print("false"); raise SystemExit("timeline fail")
    project.SetCurrentTimeline(tl)
    try: resolve.OpenPage("edit")
    except Exception: pass
    api_fps = fps_of_timeline(tl)
    fps = float(args.timeline_fps) if args.timeline_fps and args.timeline_fps > 0 else api_fps
    fps_i = int(round(fps)) or 24
    fps_f = float(fps) if fps and fps > 0 else 24.0
    image_source_fps = fps_of_project_media(project, fps_f)
    frame_ms = 1000.0 / fps_f
    if args.timeline_fps and args.timeline_fps > 0:
        slog("TL", f"fps={fps} (override; api={api_fps})")
    else:
        slog("TL", f"fps={fps}")
    if abs(float(image_source_fps) - fps_f) > 0.001:
        slog("TL", f"still/freeze source fps={image_source_fps} -> timeline fps={fps_f}")
    slog("PLAN", f"visuals={len(visual_assets)}")
    set_default_still_duration(project, 1)

    slot_starts_f: List[int] = []
    slot_frames_f: List[int] = []
    slot_ms: List[int] = []
    slot_source: Optional[Path] = None
    use_transcript_slot_lengths = False
    use_transcript_slot_starts = False
    slot_starts_f, slot_frames_f, slot_ms = build_slots_from_visual_assets(visual_assets, fps)
    if slot_frames_f:
        slog("PLAN", f"visual segment slots: {len(slot_frames_f)}")
        use_transcript_slot_lengths = True
        use_transcript_slot_starts = True
    elif not args.no_transcript_slots:
        slot_starts_f, slot_frames_f, slot_ms, slot_source = build_slots_from_transcript(
            roll=roll,
            fps=fps,
            min_ms=int(args.part_min_ms),
            max_ms=int(args.part_max_ms),
        )
        if slot_frames_f:
            src_name = slot_source.name if slot_source else "transcript"
            slog("PLAN", f"transcript slots: {len(slot_frames_f)} ({src_name})")
            use_transcript_slot_lengths = True
            use_transcript_slot_starts = True
        else:
            slog("PLAN", "transcript slots unavailable -> sequential placement")

    if use_transcript_slot_lengths and len(slot_frames_f) != len(visual_assets):
        old_slots = len(slot_frames_f)
        fitted = _fit_slot_frames_to_count(slot_frames_f, len(visual_assets))
        if fitted and len(fitted) == len(visual_assets):
            slot_frames_f = [int(max(1, f)) for f in fitted]
            slot_starts_f = [0]
            for f in slot_frames_f[:-1]:
                slot_starts_f.append(int(slot_starts_f[-1]) + int(f))
            slot_ms = [int(round(int(f) * frame_ms)) for f in slot_frames_f]
            slog("PLAN", f"slot/visual mismatch fixed: {old_slots} -> {len(slot_frames_f)} to match visuals={len(visual_assets)}", force=True)
        else:
            slog("PLAN", f"slot/visual mismatch unresolved: slots={old_slots} visuals={len(visual_assets)}; using available prefix only", force=True)

    try:
        visual_import_files = prepare_visual_media_for_resolve(roll, visual_assets, slot_frames_f, slot_ms, fps)
    except Exception as e:
        print("false"); raise SystemExit(str(e))
    freeze_count = sum(1 for a in visual_assets if a.get("freeze_path"))
    prepared_still_count = sum(1 for a in visual_assets if a.get("kind") == "still")
    slog("SCAN", f"visual imports prepared: {len(visual_import_files)} (stills={prepared_still_count}, freezes={freeze_count})")

    # ---------- Импорт в проект (очистка бинов) ----------
    mp_root = mp.GetRootFolder()
    def find_or_create_child_bin(parent, name: str):
        wanted = name.strip().lower()
        try:
            for sub in list(parent.GetSubFolderList() or []):
                try:
                    if (sub.GetName() or "").strip().lower() == wanted:
                        return sub
                except Exception:
                    pass
        except Exception:
            pass
        try:
            made = parent.AddSubFolder(name)
            if made:
                return made
        except Exception:
            pass
        try:
            made = mp.AddSubFolder(parent, name)
            if made:
                return made
        except Exception:
            pass
        try:
            for sub in list(parent.GetSubFolderList() or []):
                try:
                    if (sub.GetName() or "").strip().lower() == wanted:
                        return sub
                except Exception:
                    pass
        except Exception:
            pass
        raise RuntimeError("cannot create/find bin: %s" % name)

    def find_or_create_bin_path(root, names: List[str]):
        cur = root
        for name in names:
            cur = find_or_create_child_bin(cur, name)
        return cur

    bin_images = find_or_create_bin_path(mp_root, ["clipsAndImages", "visuals"])
    bin_parts  = find_or_create_bin_path(mp_root, ["clipsAndImages", "audio"])

    clear_media_bin(bin_parts, mp)
    clear_media_bin(bin_images, mp)

    def _bin_contains_all_by_path(folder, paths_abs_lower: List[str]) -> bool:
        try:
            clips = list(folder.GetClipList() or [])
        except Exception:
            clips = []
        seen = set()
        for c in clips:
            try:
                fp = (c.GetClipProperty("File Path") or "").strip()
                if fp:
                    key = Path(fp).resolve().as_posix().lower()
                    if key in paths_abs_lower:
                        seen.add(key)
            except Exception:
                pass
        return len(seen) == len(paths_abs_lower)

    def import_media_and_wait(mp, project, folder, files, timeout_sec: float = 45.0):
        """Импорт и блокирующее ожидание, пока Resolve проиндексирует все файлы в бине."""
        if not files:
            return
        abs_paths = [Path(p).resolve() for p in files]
        abs_keys  = [p.as_posix().lower() for p in abs_paths]

        try:
            pool = project.GetMediaPool() or mp
            pool.SetCurrentFolder(folder)
        except Exception:
            pass

        str_paths = [str(p) for p in abs_paths]
        slog("BIN", f"import {len(str_paths)} -> {folder.GetName()}")
        try:
            mp.ImportMedia(str_paths)
        except Exception as e:
            slog("BIN", f"batch fail: {e}; 1by1")
            for p in abs_paths:
                try:
                    mp.ImportMedia([str(p)])
                except Exception as e2:
                    slog("BIN", f"imp fail: {p.name}: {e2}")

        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            if _bin_contains_all_by_path(folder, abs_keys):
                return
            time.sleep(0.15)

        if not _bin_contains_all_by_path(folder, abs_keys):
            slog("BIN", f"indexing timeout after {timeout_sec:.1f}s (not all files visible)", force=True)

    import_media_and_wait(mp, project, bin_images, visual_import_files, timeout_sec=5.0)
    if audio_files:
        import_media_and_wait(mp, project, bin_parts, audio_files, timeout_sec=5.0)

    # --- КЕШ-КАРТЫ один раз ---
    video_map = map_video_by_filename_recursive(bin_images)
    audio_map = map_audio_by_filename(bin_parts)

    def get_mpi_video(filename: str, src_path: Path):
        key_name = Path(filename).name.lower()
        mpi = video_map.get(key_name)
        if mpi:
            return mpi
        full = Path(src_path).resolve()
        key_full = full.as_posix().lower()
        mpi = video_map.get(key_full)
        if mpi:
            return mpi
        if full.exists():
            import_media_and_wait(mp, project, bin_images, [full])
            _rebuild_video_map(mp, project, bin_images, video_map)
            return video_map.get(key_full) or video_map.get(key_name)
        try:
            for c in list(bin_images.GetClipList() or []):
                nmf, nmc, nmn = _key_of_clip(c)
                for raw in (nmf, nmc, nmn):
                    if Path(raw).name.lower() == key_name:
                        return c
                fp = (c.GetClipProperty("File Path") or "").strip()
                if fp and Path(fp).resolve().as_posix().lower() == key_full:
                    return c
        except Exception:
            pass
        return None

    def get_mpi_audio(filename: str, src_path: Path):
        key_name = Path(filename).name.lower()
        mpi = audio_map.get(key_name)
        if mpi:
            return mpi
        full = Path(src_path).resolve()
        key_full = full.as_posix().lower()
        mpi = audio_map.get(key_full)
        if mpi:
            return mpi
        if full.exists():
            import_media_and_wait(mp, project, bin_parts, [full])
            _rebuild_audio_map(mp, project, bin_parts, audio_map)
            return audio_map.get(key_full) or audio_map.get(key_name)
        try:
            for c in list(bin_parts.GetClipList() or []):
                nmf, nmc, nmn = _key_of_clip(c)
                for raw in (nmf, nmc, nmn):
                    if Path(raw).name.lower() == key_name:
                        return c
                fp = (c.GetClipProperty("File Path") or "").strip()
                if fp and Path(fp).resolve().as_posix().lower() == key_full:
                    return c
        except Exception:
            pass
        return None

    def get_mpi_video_retry(src_path: Path, retries: int = 6, delay_sec: float = 0.2):
        for attempt in range(retries):
            mpi = get_mpi_video(src_path.name, src_path)
            if mpi:
                return mpi
            time.sleep(delay_sec * (attempt + 1))
        return None

    def get_mpi_audio_retry(src_path: Path, retries: int = 6, delay_sec: float = 0.2):
        for attempt in range(retries):
            mpi = get_mpi_audio(src_path.name, src_path)
            if mpi:
                return mpi
            time.sleep(delay_sec * (attempt + 1))
        return None

    def append_payloads_resilient(payloads: List[dict], tag: str, chunk_size: int = 40) -> int:
        if not payloads:
            return 0
        placed = 0
        for i in range(0, len(payloads), max(1, int(chunk_size))):
            chunk = payloads[i:i + max(1, int(chunk_size))]
            ok = False
            for attempt in range(3):
                try:
                    r = mp.AppendToTimeline(chunk)
                    ok = (r is not False)
                except Exception:
                    ok = False
                if ok:
                    placed += len(chunk)
                    break
                time.sleep(0.15 * (attempt + 1))
            if ok:
                continue

            slog(tag, f"batch append fallback: {i}..{i + len(chunk) - 1}", force=True)
            for p in chunk:
                try:
                    r = mp.AppendToTimeline([p])
                    if r is not False:
                        placed += 1
                except Exception as e:
                    slog(tag, f"item append fail: {e}", force=True)
        return placed

    def _record_frames_on_track(track_type: str, track_idx: int) -> List[int]:
        try:
            items = tl.GetItemsInTrack(track_type, int(track_idx))
            items = list(items.values()) if isinstance(items, dict) else list(items or [])
        except Exception:
            items = []
        starts: List[int] = []
        for it in items:
            try:
                s, _ = get_item_bounds_frames(it, fps)
                starts.append(int(s))
            except Exception:
                pass
        return starts

    def _missing_payloads_by_record(payloads: List[dict], track_type: str, track_idx: int, tolerance_frames: int = 1) -> List[dict]:
        existing = _record_frames_on_track(track_type, track_idx)
        missing: List[dict] = []
        tol = max(0, int(tolerance_frames))
        for p in payloads:
            rf = int(p.get("recordFrame", 0))
            ok = any(abs(int(s) - rf) <= tol for s in existing)
            if not ok:
                missing.append(p)
        return missing

    def verify_and_repair_payloads(payloads: List[dict], track_type: str, track_idx: int, tag: str) -> int:
        if not payloads:
            return 0

        last_missing = payloads
        for wait_try in range(4):
            time.sleep(0.2 * (wait_try + 1))
            missing = _missing_payloads_by_record(payloads, track_type, track_idx, tolerance_frames=1)
            if not missing:
                return len(payloads)
            last_missing = missing

        slog(tag, f"verify missing={len(last_missing)} -> repairing individually", force=True)
        for p in last_missing:
            try:
                mp.AppendToTimeline([p])
            except Exception as e:
                slog(tag, f"repair append fail: {e}", force=True)

        time.sleep(0.8)
        final_missing = _missing_payloads_by_record(payloads, track_type, track_idx, tolerance_frames=1)
        if final_missing:
            slog(tag, f"still missing after repair: {len(final_missing)}", force=True)
        return len(payloads) - len(final_missing)

    def _audio_items_on_track(track_idx: int):
        try:
            items = tl.GetItemsInTrack("audio", int(track_idx))
            return list(items.values()) if isinstance(items, dict) else list(items or [])
        except Exception:
            return []

    def _mpi_file_key(mpi) -> str:
        if not mpi:
            return ""
        try:
            fp = (mpi.GetClipProperty("File Path") or "").strip()
            if fp:
                return Path(fp).resolve().as_posix().lower()
        except Exception:
            pass
        return ""

    def _timeline_item_file_key(item) -> str:
        try:
            return _mpi_file_key(item.GetMediaPoolItem())
        except Exception:
            return ""

    def _audio_item_exists(track_idx: int, src_path: Path, start_f: int, tolerance_frames: int = 1) -> bool:
        try:
            src_key = src_path.resolve().as_posix().lower()
        except Exception:
            src_key = src_path.as_posix().lower()
        src_name = src_path.name.lower()
        tol = max(0, int(tolerance_frames))
        for it in _audio_items_on_track(track_idx):
            try:
                st, _ = get_item_bounds_frames(it, fps)
            except Exception:
                continue
            if abs(int(st) - int(start_f)) > tol:
                continue
            key = _timeline_item_file_key(it)
            if key and key == src_key:
                return True
            try:
                nm = (it.GetName() or "").strip().lower()
            except Exception:
                nm = ""
            if nm == src_name or nm == src_path.stem.lower():
                return True
        return False

    def append_audio_payloads_strict(entries: List[dict], track_idx: int, tag: str) -> int:
        if not entries:
            return 0
        placed = 0
        for i, entry in enumerate(entries):
            payload = entry["payload"]
            src_path = Path(entry["path"])
            rec_f = int(payload.get("recordFrame", 0))
            if _audio_item_exists(track_idx, src_path, rec_f, tolerance_frames=1):
                placed += 1
                continue

            ok = False
            for attempt in range(6):
                try:
                    r = mp.AppendToTimeline([payload])
                    if r is False:
                        time.sleep(0.15 * (attempt + 1))
                        continue
                except Exception as e:
                    if attempt == 5:
                        slog(tag, f"append fail {src_path.name}: {e}", force=True)
                    time.sleep(0.15 * (attempt + 1))
                    continue

                for wait_try in range(5):
                    time.sleep(0.12 * (wait_try + 1))
                    if _audio_item_exists(track_idx, src_path, rec_f, tolerance_frames=1):
                        ok = True
                        break
                if ok:
                    break

            if ok:
                placed += 1
            else:
                slog(tag, f"missing after strict append: #{i} {src_path.name} at {rec_f}", force=True)

        return placed

    def audit_audio_track_contiguous(track_idx: int, expected_entries: List[dict], expected_end: int, tag: str) -> Tuple[int, int, int]:
        missing = 0
        for entry in expected_entries:
            payload = entry["payload"]
            if not _audio_item_exists(track_idx, Path(entry["path"]), int(payload.get("recordFrame", 0)), tolerance_frames=1):
                missing += 1

        items = sorted(_audio_items_on_track(track_idx), key=lambda it: get_item_bounds_frames(it, fps)[0])
        gaps = 0
        overlaps = 0
        prev_end = 0
        for it in items:
            st, en = get_item_bounds_frames(it, fps)
            if st > prev_end:
                gaps += 1
                try:
                    nm = it.GetName()
                except Exception:
                    nm = ""
                slog(tag, f"gap {prev_end}->{st} ({st - prev_end}f) before {nm}", force=True)
            elif st < prev_end:
                overlaps += 1
            prev_end = max(prev_end, en)
        if expected_end > prev_end:
            gaps += 1
            slog(tag, f"tail gap {prev_end}->{expected_end} ({expected_end - prev_end}f)", force=True)
        slog(tag, f"audit audio#{track_idx}: expected={len(expected_entries)} actual={len(items)} missing={missing} gaps={gaps} overlaps={overlaps} end={prev_end}", force=True)
        return missing, gaps, overlaps

    # ---------- Чистим дорожки ----------
    ensure_track_exists(tl, "video", 1)
    ensure_track_exists(tl, "audio", 1)
    ensure_track_exists(tl, "audio", 2)
    clear_track_items_strict(tl, "video", 1)
    clear_track_items_strict(tl, "audio", 1)
    clear_track_items_strict(tl, "audio", 2)

    # ---------- V1 + A1: mixed visual segments ----------
    v_payloads = []
    a1_payloads = []
    a1_entries = []
    still_bounds = []
    planned_v_items = []
    planned_ms = []
    cur_v = 0
    clipped_to_target = 0
    shorter_than_target = 0
    used_slot_count = 0
    short_fill_frames = 0
    missing_fill_frames = 0
    still_segments = 0
    video_segments = 0

    target_video_frames = 0
    strict_target_enabled = False
    if use_transcript_slot_lengths and use_transcript_slot_starts and len(slot_frames_f) >= len(visual_assets) and len(slot_starts_f) >= len(visual_assets):
        target_video_frames = int(max(int(slot_starts_f[i]) + int(max(1, slot_frames_f[i])) for i in range(len(visual_assets))))
        strict_target_enabled = True

    def still_source_frames(timeline_frames: int) -> int:
        tl_frames = int(max(1, timeline_frames))
        if fps_f <= 0:
            return tl_frames
        return int(max(1, math.ceil(tl_frames * float(image_source_fps) / fps_f)))

    for idx, asset in enumerate(visual_assets):
        has_slot = use_transcript_slot_lengths and idx < len(slot_frames_f)
        if has_slot:
            target_f = int(max(1, slot_frames_f[idx]))
            used_slot_count += 1
        else:
            target_f = 0
        target_ms = int(slot_ms[idx]) if idx < len(slot_ms) and slot_ms[idx] else int(round(target_f * frame_ms)) if target_f > 0 else 0
        target_sec = (target_ms / 1000.0) if target_ms > 0 else 0.0

        if use_transcript_slot_starts and idx < len(slot_starts_f):
            rec_f = int(max(0, slot_starts_f[idx]))
        else:
            rec_f = int(cur_v)

        kind = asset.get("kind")
        src_path = Path(asset.get("import_path") or asset.get("path"))
        seg_idx = int(asset.get("index", idx))

        if kind == "still":
            still_segments += 1
            if target_f <= 0:
                target_f = int(max(1, round(MIN_IMAGE_SEC * fps_f)))
            mpi_still = get_mpi_video_retry(src_path)
            if not mpi_still:
                slog("V1", f"skip still (mpi miss): {src_path.name}", force=True)
                continue
            src_len_f = still_source_frames(target_f)
            set_still_out_point(mpi_still, src_len_f)
            v_payloads.append(make_video_payload(mpi_still, 1, rec_f, src_len_f))
            still_bounds.append({"path": src_path, "start": rec_f, "end": rec_f + target_f})
            planned_v_items.append({"start": rec_f, "end": rec_f + target_f, "label": f"still {seg_idx}", "repair_path": src_path})
            planned_ms.append(int(round(target_f * frame_ms)))
            cur_v = max(cur_v, rec_f + target_f)
            continue

        video_segments += 1
        mpi_v = get_mpi_video_retry(src_path)
        if not mpi_v:
            slog("V1", f"skip clip (mpi miss): {src_path.name}", force=True)
            continue

        if target_f <= 0:
            actual_f = get_media_duration_frames_from_props(mpi_v, fps)
            if actual_f < 1:
                slog("V1", f"skip clip (zero dur): {src_path.name}", force=True)
                continue
            target_f = int(max(1, actual_f))
            target_sec = target_f / fps_f
            target_ms = int(round(target_sec * 1000.0))
        else:
            target_sec = target_f / fps_f

        raw_path = Path(asset.get("path") or src_path)
        actual_sec = asset.get("actual_sec")
        source_fps = asset.get("source_fps")
        source_frames = asset.get("source_frames")
        if actual_sec is None:
            actual_sec, source_fps_probe, source_frames_probe = ffprobe_video_info(raw_path)
            if source_fps is None:
                source_fps = source_fps_probe
            if source_frames is None:
                source_frames = source_frames_probe
        if actual_sec is None:
            actual_f = get_media_duration_frames_from_props(mpi_v, fps)
            actual_sec = actual_f / fps_f if actual_f > 0 else target_sec
        if not source_fps or source_fps <= 0:
            source_fps = fps_f

        source_take_f = asset.get("source_take_f")
        place_timeline_f = asset.get("place_timeline_f")
        fill_frames = asset.get("fill_frames")
        if not source_take_f or not place_timeline_f or fill_frames is None:
            take_sec = max(0.001, min(float(actual_sec), target_sec if target_sec > 0 else float(actual_sec)))
            source_take_f = int(max(1, math.ceil(take_sec * float(source_fps))))
            if source_frames:
                source_take_f = min(source_take_f, int(source_frames))
            place_timeline_f = int(max(1, min(target_f, round(take_sec * fps_f))))
            fill_frames = int(max(0, target_f - place_timeline_f))
        source_take_f = int(max(1, source_take_f))
        place_timeline_f = int(max(1, min(target_f, place_timeline_f)))
        fill_frames = int(max(0, target_f - place_timeline_f))

        half_tl_frame = 0.5 / fps_f
        if actual_sec > target_sec + half_tl_frame:
            clipped_to_target += 1
        elif actual_sec < target_sec - half_tl_frame:
            shorter_than_target += 1

        v_payloads.append(make_video_payload(mpi_v, 1, rec_f, source_take_f))
        planned_v_items.append({
            "start": rec_f,
            "end": rec_f + place_timeline_f,
            "label": f"video {seg_idx}",
            "repair_path": asset.get("freeze_path"),
        })
        if asset.get("has_audio", True):
            a1_payload = make_audio_payload(mpi_v, 1, rec_f, source_take_f, fps)
            a1_payloads.append(a1_payload)
            a1_entries.append({"payload": a1_payload, "path": src_path})
        if VERBOSE:
            slog(
                "PLANX",
                f"seg={seg_idx} clip={src_path.name} target={target_sec:.3f}s "
                f"actual={actual_sec:.3f}s src_fps={source_fps:.3f} src_take={source_take_f} "
                f"tl_place={place_timeline_f}/{target_f}",
            )

        if fill_frames > 0:
            fill_path = asset.get("freeze_path")
            if fill_path and Path(fill_path).exists():
                fill_src = Path(fill_path)
                mpi_fill = get_mpi_video_retry(fill_src)
                if mpi_fill:
                    fill_start = rec_f + place_timeline_f
                    fill_src_len_f = still_source_frames(fill_frames)
                    set_still_out_point(mpi_fill, fill_src_len_f)
                    v_payloads.append(make_video_payload(mpi_fill, 1, fill_start, fill_src_len_f))
                    still_bounds.append({"path": fill_src, "start": fill_start, "end": fill_start + fill_frames})
                    planned_v_items.append({"start": fill_start, "end": fill_start + fill_frames, "label": f"freeze {seg_idx}", "repair_path": fill_src})
                    short_fill_frames += fill_frames
                else:
                    missing_fill_frames += fill_frames
                    slog("V1", f"short clip freeze mpi miss: {fill_src.name}", force=True)
            else:
                missing_fill_frames += fill_frames
                slog("V1", f"short clip has no freeze fill: segment={seg_idx} clip={src_path.name}", force=True)

        planned_ms.append(int(round(target_f * frame_ms)))
        cur_v = max(cur_v, rec_f + target_f)

    if use_transcript_slot_lengths:
        slog("PLAN", f"slot-strict mode: used={used_slot_count}, videos={video_segments}, stills={still_segments}, longer={clipped_to_target}, shorter={shorter_than_target}, fill_frames={short_fill_frames}, missing_fill_frames={missing_fill_frames}")

    planned_gaps, planned_overlaps = audit_planned_video_items(planned_v_items)
    if planned_gaps or planned_overlaps or missing_fill_frames:
        print("false"); raise SystemExit(f"planned V1 is not continuous: gaps={planned_gaps}, overlaps={planned_overlaps}, missing_fill_frames={missing_fill_frames}")

    def repair_actual_v1_gaps(expected_end: Optional[int]) -> int:
        gaps, gap_count, _overlaps, _actual_end = collect_timeline_video_gaps(tl, fps, expected_end)
        if not gap_count:
            return 0

        def pick_repair_path(gap_start: int) -> Optional[Path]:
            covering = [
                it for it in planned_v_items
                if it.get("repair_path") and int(it["start"]) <= int(gap_start) < int(it["end"])
            ]
            if covering:
                covering.sort(key=lambda it: (int(it["start"]), int(it["end"])))
                return Path(covering[-1]["repair_path"])
            previous = [
                it for it in planned_v_items
                if it.get("repair_path") and int(it["end"]) <= int(gap_start)
            ]
            if previous:
                previous.sort(key=lambda it: (int(it["end"]), int(it["start"])))
                return Path(previous[-1]["repair_path"])
            return None

        repair_payloads = []
        repair_bounds = []
        for gap_start, gap_end in gaps:
            length = int(gap_end) - int(gap_start)
            if length <= 0:
                continue
            repair_path = pick_repair_path(int(gap_start))
            if not repair_path or not repair_path.exists():
                slog("AUDIT", f"cannot repair gap {gap_start}->{gap_end}: no freeze/still source", force=True)
                continue
            mpi_repair = get_mpi_video_retry(repair_path)
            if not mpi_repair:
                slog("AUDIT", f"cannot repair gap {gap_start}->{gap_end}: mpi miss {repair_path.name}", force=True)
                continue
            repair_src_len_f = still_source_frames(length)
            set_still_out_point(mpi_repair, repair_src_len_f)
            repair_payloads.append(make_video_payload(mpi_repair, 1, int(gap_start), repair_src_len_f))
            repair_bounds.append({"path": repair_path, "start": int(gap_start), "end": int(gap_end)})

        if not repair_payloads:
            return 0
        placed = append_payloads_resilient(repair_payloads, "V1GAP", chunk_size=20)
        time.sleep(0.3)
        adjust_still_item_bounds(tl, repair_bounds, fps)
        slog("AUDIT", f"actual gap repair placed: {placed}/{len(repair_payloads)}", force=True)
        return placed

    placed_v = append_payloads_resilient(v_payloads, "V1")
    placed_a1 = append_audio_payloads_strict(a1_entries, 1, "A1")
    verified_v = verify_and_repair_payloads(v_payloads, "video", 1, "V1")
    verified_a1 = placed_a1
    adjust_still_item_bounds(tl, still_bounds, fps)
    expected_v_end = int(target_video_frames) if strict_target_enabled and target_video_frames > 0 else int(cur_v)
    actual_gaps, actual_gap_count, actual_overlaps, actual_end = collect_timeline_video_gaps(tl, fps, expected_v_end)
    if actual_gap_count:
        repair_actual_v1_gaps(expected_v_end)
        time.sleep(0.5)
        actual_gaps, actual_gap_count, actual_overlaps, actual_end = collect_timeline_video_gaps(tl, fps, expected_v_end)
    if actual_gap_count or actual_overlaps:
        print("false"); raise SystemExit(f"actual V1 is not continuous: gaps={actual_gap_count}, overlaps={actual_overlaps}")
    slog("TL", f"V1 placed FAST: {verified_v}/{len(v_payloads)} items (raw {placed_v}), end={tc_from_frames(cur_v, fps)}")
    slog("A1", f"clip audio placed: {verified_a1}/{len(a1_entries)} items")

    # ---------- A2: кладём VOICE строго друг за другом ----------
    cur_a = 0
    a2_payloads = []
    a2_entries = []
    if audio_files:
        for af in audio_files:
            mpi_a = get_mpi_audio_retry(af)
            if not mpi_a:
                slog("A2", f"miss {af.name}", force=True)
                continue

            dur_f = get_media_duration_frames_from_props(mpi_a, fps)
            if dur_f < 1:
                continue

            payload = make_audio_payload(mpi_a, 2, cur_a, dur_f, fps)
            a2_payloads.append(payload)
            a2_entries.append({"payload": payload, "path": af})

            cur_a += dur_f

        placed_a2 = append_audio_payloads_strict(a2_entries, 2, "A2")
        missing_a2, gaps_a2, overlaps_a2 = audit_audio_track_contiguous(2, a2_entries, cur_a, "A2")
        if missing_a2:
            retry_placed_a2 = append_audio_payloads_strict(a2_entries, 2, "A2")
            placed_a2 = max(placed_a2, retry_placed_a2)
            missing_a2, gaps_a2, overlaps_a2 = audit_audio_track_contiguous(2, a2_entries, cur_a, "A2")
        verified_a2 = len(a2_entries) - missing_a2
        if missing_a2 or gaps_a2 or overlaps_a2:
            print("false"); raise SystemExit(f"A2 audio is not continuous: missing={missing_a2}, gaps={gaps_a2}, overlaps={overlaps_a2}")
        slog("A2", f"placed STRICT: {verified_a2}/{len(a2_entries)} items (raw {placed_a2}), end={tc_from_frames(cur_a, fps)}")
    else:
        slog("A2", "no voice")

    # ---------- Mark In/Out ----------
    out_video = cur_v
    if strict_target_enabled and target_video_frames > 0 and out_video > target_video_frames:
        out_video = int(target_video_frames)
    out_frame = max(out_video, cur_a)
    try:
        if hasattr(tl,"ClearInOutRange"): tl.ClearInOutRange()
        if hasattr(tl,"SetMarkInOut"): tl.SetMarkInOut(0, int(out_frame))
        else:
            if hasattr(tl,"SetInPoint"): tl.SetInPoint(0)
            if hasattr(tl,"SetOutPoint"): tl.SetOutPoint(int(out_frame))
    except Exception: pass
    try:
        project.SetRenderSettings({"UseMarkInOut":True,"RenderRange":"InOut","MarkIn":0,"MarkOut":int(out_frame)})
    except Exception: pass
    slog("TL", f"InOut: 0..{out_frame} ({tc_from_frames(out_frame,fps)})")
    # ---------- Transitions / Motions ----------
    # Отключено по запросу: не добавляем ничего на V3/V4.
    slog("FX", "transitions/motions disabled")




    # ---------- Render queue ----------
    try: resolve.OpenPage("deliver")
    except Exception: pass
    try:
        if hasattr(project,"IsRenderingInProgress") and project.IsRenderingInProgress():
            try: project.StopRendering()
            except Exception: pass
    except Exception: pass
    try:
        jobs=project.GetRenderJobList() or []
        for j in list(jobs):
            jid=None
            if isinstance(j,dict):
                for k in ("JobId","jobId","RenderJobId","RenderID","Id","ID"):
                    v=j.get(k)
                    if v: jid=v; break
            elif isinstance(j,(str,int)):
                jid=j
            if jid is None: continue
            try: project.DeleteRenderJob(jid)
            except Exception: pass
    except Exception: pass

    td=roll.resolve(); td.mkdir(parents=True, exist_ok=True)
    safe_dir=td.as_posix()
    settings={"TargetDir":safe_dir,"CustomName":roll.name,"MarkIn":0,"MarkOut":int(out_frame),
              "UseMarkInOut":True,"RenderRange":"InOut","RenderMode":"InOut",
              "ExportVideo":True,"ExportAudio":True,"VideoFormat":"mp4","VideoCodec":"H265",
              "VideoBitrate":"3200","VideoQuality":"3200","EncodingProfile":"High","UseTimelineLocation":False}
    try:
        project.SetRenderSettings({"TargetDir":safe_dir,"UseTimelineLocation":False})
        project.SetRenderSettings(settings)
    except Exception: pass
    jid = project.AddRenderJob()
    slog("REND", f"job={jid}")

    ok = True
    if args.auto_render:
        try:
            started = False
            if hasattr(project, "IsRenderingInProgress") and project.IsRenderingInProgress():
                started = True
            else:
                if hasattr(project,"StartRendering"): started = bool(project.StartRendering())
            if not started:
                ok = False
                slog("REND", "start FAIL", force=True)
            else:
                slog("REND", "renderingвЂ¦")
                while True:
                    time.sleep(0.4)
                    try:
                        if not project.IsRenderingInProgress():
                            break
                    except Exception:
                        break
                status = "unknown"
                try:
                    jobs = list(project.GetRenderJobList() or [])
                    if jobs:
                        st = project.GetRenderJobStatus(jobs[-1].get("JobId") if isinstance(jobs[-1], dict) else jobs[-1])
                        if isinstance(st, dict):
                            status = (st.get("JobStatus") or "").lower()
                except Exception:
                    pass
                ok = (status == "complete")
                slog("REND", f"done={status}")
        except Exception as e:
            slog("REND", f"ERR {e}", force=True)
            ok = False

    # true РѕР·РЅР°С‡Р°РµС‚, С‡С‚Рѕ СЃР±РѕСЂРєР° РїСЂРѕС€Р»Р°; СЃ --auto-render РµС‰Рµ Рё СЂРµРЅРґРµСЂ РґРѕР»Р¶РµРЅ Р·Р°РІРµСЂС€РёС‚СЊСЃСЏ.
    if ok:
        slog("END", "true", force=True)
        print("true")
    else:
        slog("END", "false", force=True)
        print("false")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except Exception:
        try:
            slog("FATAL", "script error", force=True)
            traceback.print_exc()
        except Exception:
            pass
        print("false")
        sys.exit(1)

