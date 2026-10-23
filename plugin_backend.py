"""SDCodex-GalleryDL slim JSON API (Stitch frontend, no OldCode templates).

Task storage mirrors OldCode artillery layout so existing task folders keep
working: <TASKS>/<slug>/{urls.txt, command.txt, status.json, stdout.log}.
"""

import json
import io
import heapq
import mimetypes
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
import shlex
from pathlib import Path
from urllib import request as urlrequest
from urllib import error as urlerror
import http.client

from flask import Blueprint, Response, jsonify, request, send_file, send_from_directory

gdl_api = Blueprint("gdl_api", __name__, url_prefix="/gdl/api")

_procs: dict = {}          # slug -> Popen (task runs)
_quick: dict = {}          # job_id -> {state, log file, proc, ...}
_lock = threading.RLock()  # reentrant: status helpers call each other
_scan_lock = threading.Lock()
_scan_cache: dict = {"ts": 0.0, "data": None}
_TELEMETRY_TTL = 10.0
# Task log totals/counters change slowly and the walk is expensive; keep them
# out of the 10s media scan cycle.
_aux_cache: dict = {"ts": 0.0, "tasks_bytes": 0, "filtered": 0, "errors": 0}
_AUX_TTL = 60.0
_scheduler = None
_settings_loaded = False
_slots = threading.BoundedSemaphore(max(1, int(os.environ.get("TASK_CONCURRENT_MAX", "5") or "5")))
_max_concurrent = max(1, int(os.environ.get("TASK_CONCURRENT_MAX", "5") or "5"))
_queued_runs = 0
_paused_tasks: set[str] = set()
_one_time_proc = None
_one_time_lock = threading.Lock()
_oauth_proc = None
_oauth_task = ""
_oauth_site = ""
_oauth_lock = threading.Lock()
_OAUTH_SITES = {"deviantart": "DeviantArt", "reddit": "Reddit", "tumblr": "Tumblr", "flickr": "Flickr", "mastodon": "Mastodon", "pixiv": "Pixiv"}
_OAUTH_STDIN_SITES = {"pixiv"}
_MEDIA_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".webm", ".mkv"}
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
_DEFAULT_CONFIG_URL = os.environ.get("GALLERYDL_DEFAULT_CONFIG_URL", "https://raw.githubusercontent.com/mikf/gallery-dl/master/docs/gallery-dl.conf")
_DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "defaults", "gallery-dl.conf")
_TOOL_CACHE: dict[str, str] = {}


def _init_scheduler():
    """Start cron jobs declared by task folders, preserving the task layout."""
    global _scheduler, _settings_loaded, _slots, _max_concurrent
    if not _settings_loaded:
        try:
            saved = int(_read_file_strip(os.path.join(config_dir(), "task_concurrent_max.txt")) or _max_concurrent)
            _max_concurrent = max(1, min(64, saved))
            _slots = threading.BoundedSemaphore(_max_concurrent)
        except (TypeError, ValueError, OSError):
            pass
        _settings_loaded = True
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        return False
    if _scheduler is None:
        _scheduler = BackgroundScheduler(daemon=True, timezone="UTC")
        _scheduler.start()
    for slug in sorted(os.listdir(tasks_dir())):
        base = os.path.join(tasks_dir(), slug)
        expr = _read_file_strip(os.path.join(base, "cron.txt")) if os.path.isdir(base) else ""
        if not expr:
            continue
        try:
            _scheduler.add_job(_run_task, CronTrigger.from_crontab(expr, timezone="UTC"),
                               args=[slug], id=f"task_{slug}", replace_existing=True,
                               max_instances=1, coalesce=True)
        except Exception:
            continue
    return True


def _set_schedule(slug: str, expr: str):
    if not expr:
        if _scheduler:
            try:
                _scheduler.remove_job(f"task_{slug}")
            except Exception:
                pass
        try:
            os.remove(os.path.join(tasks_dir(), slug, "cron.txt"))
        except FileNotFoundError:
            pass
        return
    if not _init_scheduler():
        raise RuntimeError("Scheduled tasks require APScheduler; install plugin requirements")
    from apscheduler.triggers.cron import CronTrigger
    trigger = CronTrigger.from_crontab(expr, timezone="UTC")
    _scheduler.add_job(_run_task, trigger, args=[slug], id=f"task_{slug}",
                       replace_existing=True, max_instances=1, coalesce=True)
    with open(os.path.join(tasks_dir(), slug, "cron.txt"), "w", encoding="utf-8") as f:
        f.write(expr + "\n")


# ------------------------------------------------------------- helpers ---

def _authed():
    try:
        from sdcodex import auth as core_auth
    except Exception:
        # Standalone deployment: there is no core session to check.
        return True
    try:
        u = core_auth.current_user()
        return u is not None
    except Exception:
        return False


def _deny():
    return jsonify({"error": "unauthorized"}), 401


# Kiosk display endpoints are intentionally public: they are opened on a TV or
# wall tablet that never signs in. Everything else under /gdl/api requires a
# core session, including reads of task logs and settings.
_PUBLIC_GDL_PREFIXES = ("/gdl/api/kiosk/",)


@gdl_api.before_request
def _gdl_auth_guard():
    if any(request.path.startswith(prefix) for prefix in _PUBLIC_GDL_PREFIXES):
        return None
    if not _authed():
        return _deny()
    return None


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def tasks_dir() -> str:
    # Old GalleryDL releases used *_DIR. Keep honoring those variables so an
    # upgrade continues reading the existing mounted task directory. The
    # original application also defaulted to /tasks before the plugin volume
    # moved to /data/tasks; prefer that populated location during migration.
    configured = os.environ.get("TASKS") or os.environ.get("TASKS_DIR")
    d = configured or "/data/tasks"
    legacy_candidates = [os.environ.get("TASKS_DIR"), "/tasks"]
    for legacy in legacy_candidates:
        if legacy and os.path.realpath(legacy) != os.path.realpath(d) and _has_saved_tasks(legacy):
            d = legacy
            break
    os.makedirs(d, exist_ok=True)
    return d


def config_dir() -> str:
    d = _env("CONFIG", os.environ.get("CONFIG_DIR") or "/data/config")
    os.makedirs(d, exist_ok=True)
    return d


def downloads_dir() -> str:
    return _env("IMAGES", os.environ.get("DOWNLOADS_DIR") or "/data/downloads")


def _scan_downloads(limit: int = 12) -> dict:
    """One cached walk of the downloads tree for telemetry + the filmstrip.

    /data/downloads is often a large mounted library. Walking it three times
    per page poll (total size, today's count, newest media) kept every waitress
    thread busy and exhausted the connection pool. Scan once, reuse the result
    for a short TTL, and let concurrent callers read the previous snapshot
    instead of queuing another full walk.
    """
    now = time.time()
    data = _scan_cache.get("data")
    if data is not None and now - _scan_cache.get("ts", 0) < _TELEMETRY_TTL:
        return data
    if not _scan_lock.acquire(blocking=False):
        return data or {"bytes": 0, "today_items": 0, "today_bytes": 0, "media": []}
    try:
        now = time.time()
        data = _scan_cache.get("data")
        if data is not None and now - _scan_cache.get("ts", 0) < _TELEMETRY_TTL:
            return data
        root = downloads_dir()
        today = datetime.now().astimezone().date()
        total = 0
        today_items = 0
        today_bytes = 0
        newest: list = []
        try:
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                for filename in filenames:
                    path = os.path.join(dirpath, filename)
                    try:
                        st = os.stat(path)
                    except OSError:
                        continue
                    total += st.st_size
                    if datetime.fromtimestamp(st.st_mtime).date() == today:
                        today_items += 1
                        today_bytes += st.st_size
                    if filename.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".webm")):
                        item = (st.st_mtime, filename, os.path.relpath(path, root), st.st_size)
                        if len(newest) < limit:
                            heapq.heappush(newest, item)
                        elif item > newest[0]:
                            heapq.heapreplace(newest, item)
        except OSError:
            pass
        media = [
            {"name": name, "rel": rel, "size": size, "mtime": mtime}
            for mtime, name, rel, size in sorted(newest, key=lambda x: x[0], reverse=True)
        ]
        data = {"bytes": total, "today_items": today_items, "today_bytes": today_bytes, "media": media}
        _scan_cache["data"] = data
        _scan_cache["ts"] = now
        return data
    finally:
        _scan_lock.release()


def _has_saved_tasks(path: str) -> bool:
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                if not entry.is_dir(follow_symlinks=False):
                    continue
                if any(os.path.isfile(os.path.join(entry.path, marker)) for marker in (
                    "urls.txt", "name.txt", "command.txt", "status.json", "cron.txt",
                )):
                    return True
    except OSError:
        return False
    return False


def _slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (name or "task").lower()).strip("-")
    return s or f"task-{int(time.time())}"


def _valid_slug(value: str) -> bool:
    return bool(value and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,119}", value))


def _task_base(slug: str):
    if not _valid_slug(slug):
        return None
    root = os.path.realpath(tasks_dir())
    base = os.path.realpath(os.path.join(root, slug))
    return base if os.path.commonpath((root, base)) == root else None


def _save_task(data, slug: str | None = None):
    name = str(data.get("name") or "").strip()
    if not name:
        raise ValueError("task name required")
    old_slug = str(data.get("original_slug") or slug or "").strip()
    target_slug = _slug(name)
    if old_slug and old_slug != target_slug:
        old_base = _task_base(old_slug)
        new_base = _task_base(target_slug)
        if not old_base or not os.path.isdir(old_base):
            raise FileNotFoundError("unknown task")
        if os.path.exists(new_base):
            raise FileExistsError("a task with that name already exists")
        os.rename(old_base, new_base)
        _set_schedule(old_slug, "")
    else:
        new_base = _task_base(target_slug)
        if new_base is None:
            raise ValueError("invalid task name")
        if not os.path.isdir(new_base):
            os.makedirs(new_base, exist_ok=False)
        elif not old_slug:
            raise FileExistsError("a task with that name already exists")
    urls = data.get("urls") or []
    if isinstance(urls, str):
        urls = urls.splitlines()
    urls = [str(u).strip() for u in urls if str(u).strip()]
    if not urls and not data.get("keep_existing_urls"):
        existing = _read_file_strip(os.path.join(new_base, "urls.txt"))
        if not existing:
            raise ValueError("at least one URL required")
    elif urls:
        with open(os.path.join(new_base, "urls.txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(urls) + "\n")
    command = str(data.get("command") or "").strip() or f"gallery-dl --input-file urls.txt --config {os.path.join(config_dir(), 'gallery-dl.conf')} --destination {downloads_dir()}"
    try:
        shlex.split(command)
    except ValueError as e:
        raise ValueError(f"invalid command: {e}")
    for filename, value in (("name.txt", name), ("command.txt", command), ("oauth_site.txt", str(data.get("oauth_site") or "").strip())):
        path = os.path.join(new_base, filename)
        if value:
            Path(path).write_text(value + "\n", encoding="utf-8")
        elif os.path.exists(path):
            os.remove(path)
    st = _read_json(os.path.join(new_base, "status.json"), {})
    st.update({"name": name, "command": command})
    _write_json(os.path.join(new_base, "status.json"), st)
    schedule = str(data.get("schedule") or "").strip()
    _set_schedule(target_slug, schedule)
    if "timeout" in data:
        timeout = str(data.get("timeout") or "").strip()
        if timeout and not timeout.isdigit(): raise ValueError("timeout must be seconds")
        timeout_path = os.path.join(new_base, "timeout.txt")
        if timeout: Path(timeout_path).write_text(timeout + "\n", encoding="utf-8")
        elif os.path.exists(timeout_path): os.remove(timeout_path)
    cookie = data.get("cookie_file") or data.get("cookie_path")
    if cookie:
        # Existing cookie profile paths are constrained to the plugin config directory.
        candidate = os.path.realpath(os.path.join(config_dir(), os.path.basename(str(cookie))))
        if os.path.isfile(candidate):
            shutil.copy2(candidate, os.path.join(new_base, "cookies.txt"))
    Path(os.path.join(new_base, "stdout.log")).touch(exist_ok=True)
    Path(os.path.join(new_base, "logs.txt")).touch(exist_ok=True)
    return target_slug


def _read_json(path: str, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, data) -> None:
    # Task runners and the stop route can write status.json concurrently;
    # a shared ".tmp" name made one os.replace lose the file to the other.
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _tail(path: str, max_lines: int = 200) -> list:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            chunk = 8192
            data = b""
            lines: list = []
            while len(lines) <= max_lines and size > 0:
                step = min(chunk, size)
                size -= step
                f.seek(size)
                data = f.read(step) + data
                lines = data.splitlines()
            return [l.decode("utf-8", "replace") for l in lines[-max_lines:]]
    except Exception:
        return []


_URL_MARKER_RE = re.compile(rb"\[(\d{1,4})/(\d{1,4})\]\s+(https?://\S+)")


def _last_url_marker(path: str):
    """Newest gallery-dl ``[i/n] <url>`` marker in the log tail.

    gallery-dl prints one marker per input URL as it starts working on it; the
    per-file output has no percentages, so this is the only reliable per-URL
    progress signal. Read backwards in chunks and stop at the first hit, so a
    URL that downloaded thousands of files does not require a full log scan.
    """
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            data = b""
            budget = 8 * 1024 * 1024  # never read more than 8 MB from the end
            while size > 0 and budget > 0:
                step = min(64 * 1024, size)
                size -= step
                budget -= step
                fh.seek(size)
                data = fh.read(step) + data
                matches = list(_URL_MARKER_RE.finditer(data))
                if matches:
                    index, total, url = matches[-1].groups()
                    return int(index), int(total), url.decode("utf-8", "replace")
    except (OSError, ValueError):
        pass
    return None


def _tool_version(cmd: str) -> str:
    # Spawning `gallery-dl --version` / `yt-dlp --version` imports the whole
    # tool, so cache the answer instead of running it on every page poll.
    cached = _TOOL_CACHE.get(cmd)
    if cached:
        return cached
    try:
        r = subprocess.run([cmd, "--version"], capture_output=True, text=True, timeout=15)
        out = (r.stdout or r.stderr or "").strip().splitlines()
        value = out[0].strip() if out else "missing"
    except Exception:
        value = "missing"
    _TOOL_CACHE[cmd] = value
    return value


def _is_running(pid) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def _pid_belongs_to(pid, base: str) -> bool:
    """True when ``pid`` is a live process whose cwd is this task folder.

    PIDs are recycled by the kernel; a stale ``pid`` file must never make the
    stop endpoint signal an unrelated process.
    """
    try:
        value = int(pid)
    except (TypeError, ValueError):
        return False
    if value <= 0 or not _is_running(value):
        return False
    try:
        return os.path.realpath(f"/proc/{value}/cwd") == os.path.realpath(base)
    except OSError:
        # /proc is unavailable (non-Linux): fall back to the liveness check.
        return True


def _read_file_strip(path: str) -> str:
    """Read a plain-text file and return its stripped content, or ''."""
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def _task_info(slug: str) -> dict:
    base = os.path.join(tasks_dir(), slug)
    urls_path = os.path.join(base, "urls.txt")
    try:
        with open(urls_path, encoding="utf-8") as f:
            urls = [l.strip() for l in f if l.strip()]
    except Exception:
        urls = []
    st = _read_json(os.path.join(base, "status.json"), {})
    with _lock:
        proc = _procs.get(slug)
        alive = proc is not None and proc.poll() is None
    command = _read_file_strip(os.path.join(base, "command.txt")) or st.get("command") or "gallery-dl --input-file urls.txt"
    name = st.get("name") or _read_file_strip(os.path.join(base, "name.txt")) or slug
    state = st.get("state")
    if not state:
        state = "running" if os.path.exists(os.path.join(base, "lock")) else "paused" if os.path.exists(os.path.join(base, "paused")) else "error" if os.path.exists(os.path.join(base, "error")) else "idle"
    with _lock:
        if slug in _paused_tasks and state == "running":
            state = "paused"
    if not alive:
        pid = _read_file_strip(os.path.join(base, "pid"))
        alive = bool(pid and _pid_belongs_to(pid, base))
    if alive:
        state = "running"
    elif state == "running":
        state = "stopped"
    # Progress: gallery-dl prints "[i/n] <url>" when it starts each input URL.
    # That is the only per-URL signal in the log, so the percentage is i/n for
    # multi-URL tasks. yt-dlp-style "NN%" lines act as sub-progress.
    log_path = os.path.join(base, "stdout.log")
    if not os.path.exists(log_path):
        log_path = os.path.join(base, "logs.txt")
    url_index = url_total = 0
    current_url = ""
    within_pct = None
    if state == "running":
        marker = _last_url_marker(log_path)
        if marker:
            url_index, url_total, current_url = marker
        elif urls:
            # Older gallery-dl builds do not print the [i/n] marker; fall back
            # to the first URL so the counter still reflects the input list.
            url_index, url_total = 1, len(urls)
        for line in reversed(_tail(log_path, 80)):
            m = re.search(r"(\d{1,3}(?:\.\d+)?)%", line)
            if m:
                try:
                    within_pct = max(0.0, min(100.0, float(m.group(1))))
                    break
                except ValueError:
                    pass
    pct = None
    if url_total and url_index:
        if within_pct is not None:
            pct = round(((url_index - 1) + within_pct / 100) * 100 / url_total)
        else:
            pct = round(url_index * 100 / url_total)
        pct = max(0, min(100, pct))
    elif within_pct is not None:
        pct = round(within_pct)
    schedule = _read_file_strip(os.path.join(base, "cron.txt"))
    return {
        "id": slug,
        "slug": slug,
        "name": name,
        "command": command,
        "urls": len(urls),
        "url_index": url_index,
        "url_total": url_total,
        "current_url": current_url,
        "state": state,
        "schedule": schedule,
        "progress": pct,
        "started_at": st.get("started_at") or _read_file_strip(os.path.join(base, "last_run.txt")),
        "finished_at": st.get("finished_at"),
        "last_error": st.get("last_error"),
        "platform": _guess_platform(urls[0] if urls else ""),
        "last_run": _read_file_strip(os.path.join(base, "last_run.txt")) or st.get("started_at"),
        "next_run": _next_run(schedule),
        "has_archive": os.path.isfile(os.path.join(base, "archive.sqlite")),
        "has_cookies": os.path.isfile(os.path.join(base, "cookies.txt")),
        "cookie_path": "cookies.txt" if os.path.isfile(os.path.join(base, "cookies.txt")) else "",
        "oauth_site": _read_file_strip(os.path.join(base, "oauth_site.txt")),
        "oauth_authenticated": _cache_has_token(os.path.join(base, "gallery-dl-cache.sqlite3")),
        "timeout": _read_file_strip(os.path.join(base, "timeout.txt")),
        "last_error_detail": _read_file_strip(os.path.join(base, "last_error.txt")),
    }


def _next_run(expr: str):
    if not expr:
        return None
    try:
        from apscheduler.triggers.cron import CronTrigger
        nxt = CronTrigger.from_crontab(expr, timezone="UTC").get_next_fire_time(None, datetime.now(timezone.utc))
        return nxt.isoformat() if nxt else None
    except Exception:
        return None


def _cache_has_token(path: str) -> bool:
    if not os.path.isfile(path):
        return False
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)
        try:
            return con.execute("SELECT 1 FROM data LIMIT 1").fetchone() is not None
        finally:
            con.close()
    except sqlite3.Error:
        return False


def _guess_platform(url: str) -> str:
    u = (url or "").lower()
    for key in ("civitai", "artstation", "reddit", "pixiv", "twitter", "x.com",
                "instagram", "deviantart", "danbooru", "youtube", "youtu.be"):
        if key in u:
            return "Twitter/X" if key == "x.com" else key.capitalize()
    return "Web"


def _append_log(path: str, line: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def _pump_process_output(stream, *logs) -> None:
    pending = []
    while True:
        char = stream.read(1)
        if not char:
            break
        pending.append(char)
        if char in ("\r", "\n") or len(pending) >= 512:
            text = "".join(pending)
            for log in logs:
                log.write(text)
                log.flush()
            pending.clear()
    if pending:
        text = "".join(pending)
        for log in logs:
            log.write(text)
            log.flush()


def _rotate_task_logs(base: str) -> None:
    legacy = os.path.join(base, "logs.txt")
    try: has_log = os.path.isfile(legacy) and os.path.getsize(legacy) > 0
    except OSError: has_log = False
    if has_log:
        stamp = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        archived = os.path.join(base, f"logs-{stamp}.txt")
        try: os.replace(legacy, archived)
        except OSError: pass
        archives = sorted(fn for fn in os.listdir(base) if re.fullmatch(r"logs-\d{4}-\d{2}-\d{2}T\d{6}\.txt", fn))
        for old in archives[:-10]:
            try: os.remove(os.path.join(base, old))
            except OSError: pass
    for filename in ("stdout.log", "logs.txt"):
        try: Path(os.path.join(base, filename)).write_text("", encoding="utf-8")
        except OSError: pass


def _run_task(slug: str) -> None:
    """Run one persisted task using the legacy task-folder contract."""
    base = os.path.join(tasks_dir(), slug)
    log_path = os.path.join(base, "stdout.log")
    legacy_log = os.path.join(base, "logs.txt")
    st_path = os.path.join(base, "status.json")
    lock_path = os.path.join(base, "lock")
    pid_path = os.path.join(base, "pid")
    if not os.path.isdir(base) or os.path.exists(os.path.join(base, "paused")):
        return
    with _lock:
        if slug in _procs and _procs[slug].poll() is None:
            return
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
    except FileExistsError:
        pid_text = _read_file_strip(pid_path)
        stale_marker = not pid_text and time.time() - os.path.getmtime(lock_path) > 60
        if (pid_text and not _pid_belongs_to(pid_text, base)) or stale_marker:
            for marker in (lock_path, pid_path):
                try: os.remove(marker)
                except OSError: pass
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
            except OSError:
                return
        else:
            return
    global _queued_runs
    with _lock: _queued_runs += 1
    _slots.acquire()
    with _lock: _queued_runs = max(0, _queued_runs - 1)
    started = time.time()
    _rotate_task_logs(base)
    for filename in ("error", "last_error.txt", "stopped"):
        try: os.remove(os.path.join(base, filename))
        except FileNotFoundError: pass
    st = _read_json(st_path, {})
    st.update({"state": "running", "started_at": _now(), "finished_at": None, "last_error": None})
    _write_json(st_path, st)
    os.makedirs(base, exist_ok=True)
    try:
        command = _read_file_strip(os.path.join(base, "command.txt")) or st.get("command") or "gallery-dl --input-file urls.txt"
        cmd = shlex.split(command)
    except ValueError as e:
        st.update({"state": "error", "finished_at": _now(), "last_error": str(e)[:300]})
        _write_json(st_path, st)
        _slots.release()
        try: os.remove(lock_path)
        except OSError: pass
        return
    if not cmd:
        st.update({"state": "error", "finished_at": _now(), "last_error": "Empty command"})
        _write_json(st_path, st)
        _slots.release()
        try: os.remove(lock_path)
        except OSError: pass
        return
    if not os.path.isfile(os.path.join(base, "urls.txt")):
        st.update({"state": "error", "finished_at": _now(), "last_error": "urls.txt not found"})
        _write_json(st_path, st)
        _slots.release()
        try: os.remove(lock_path)
        except OSError: pass
        return
    log_line = f"\n\n==== Run at {_now()} ====\n$ {' '.join(shlex.quote(a) for a in cmd)}\n"
    _append_log(log_path, log_line)
    _append_log(legacy_log, log_line)
    try:
        env = os.environ.copy()
        env["GALLERY_DL_CONFIG"] = os.path.join(config_dir(), "gallery-dl.conf")
        env["PATH"] = env.get("PATH", "") + os.pathsep + "/usr/local/bin"
        proc = subprocess.Popen(
            cmd, cwd=base, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env,
        )
    except Exception as e:
        st.update({"state": "error", "finished_at": _now(), "last_error": str(e)[:300]})
        _write_json(st_path, st)
        _append_log(log_path, f"[launcher] spawn failed: {e}")
        _append_log(legacy_log, f"[launcher] spawn failed: {e}")
        _slots.release()
        try: os.remove(lock_path)
        except OSError: pass
        return
    with _lock:
        _procs[slug] = proc
    try:
        assert proc.stdout is not None
        def pump():
            with open(log_path, "a", encoding="utf-8") as lf, open(legacy_log, "a", encoding="utf-8") as oldlf:
                _pump_process_output(proc.stdout, lf, oldlf)
        reader = threading.Thread(target=pump, daemon=True)
        reader.start()
        timeout = 0
        try: timeout = int(_read_file_strip(os.path.join(base, "timeout.txt")) or os.environ.get("TASK_TIMEOUT_SECONDS", "0"))
        except (TypeError, ValueError): timeout = 0
        Path(pid_path).write_text(str(proc.pid), encoding="utf-8")
        timed_out = False
        try:
            rc = proc.wait(timeout=timeout if timeout > 0 else None)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = proc.wait()
            timed_out = True
        reader.join(timeout=5)
        st = _read_json(st_path, {})
        if rc == 0:
            st.update({"state": "done", "finished_at": _now()})
        elif st.get("state") == "running":
            st.update({"state": "error", "finished_at": _now(),
                       "last_error": f"Timed out after {timeout}s" if timed_out else f"exit code {rc}"})
        _write_json(st_path, st)
        Path(os.path.join(base, "last_run.txt")).write_text(_now(), encoding="utf-8")
        with open(os.path.join(base, "run_history.jsonl"), "a", encoding="utf-8") as hf:
            hf.write(json.dumps({"ts": _now(), "success": rc == 0, "duration": round(time.time() - started, 1), "exit": rc}) + "\n")
        if rc != 0:
            Path(os.path.join(base, "error")).touch()
            Path(os.path.join(base, "last_error.txt")).write_text(st.get("last_error") or f"exit code {rc}", encoding="utf-8")
    finally:
        with _lock:
            _procs.pop(slug, None)
        for marker in (lock_path, pid_path):
            try: os.remove(marker)
            except OSError: pass
        _slots.release()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def _epoch(value) -> int:
    """Coerce a status timestamp into Unix seconds for the task float."""
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        pass
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


# ------------------------------------------------------------- routes ---

@gdl_api.get("/engines")
def engines():
    if not _authed():
        return _deny()
    return jsonify({
        "gallery_dl": _tool_version("gallery-dl"),
        "yt_dlp": _tool_version("yt-dlp"),
    })


@gdl_api.get("/queue")
def queue_status():
    if not _authed(): return _deny()
    running = sum(1 for p in _procs.values() if p.poll() is None)
    return jsonify({"running": running, "queued": _queued_runs, "max_concurrent": _max_concurrent})


@gdl_api.get("/settings")
def settings_get():
    if not _authed(): return _deny()
    saved = _read_file_strip(os.path.join(config_dir(), "task_concurrent_max.txt"))
    try: value = max(1, min(64, int(saved))) if saved else _max_concurrent
    except ValueError: value = _max_concurrent
    return jsonify({"max_concurrent": value})


@gdl_api.put("/settings")
def settings_put():
    global _slots, _max_concurrent
    if not _authed(): return _deny()
    data = request.get_json(silent=True) or {}
    try: value = max(1, min(64, int(data.get("max_concurrent", _max_concurrent))))
    except (TypeError, ValueError): return jsonify({"error": "invalid concurrency"}), 400
    with _lock:
        running = sum(1 for p in _procs.values() if p.poll() is None)
        if running or _queued_runs: return jsonify({"error": "Wait for running and queued tasks before changing concurrency"}), 409
        _max_concurrent = value
        _slots = threading.BoundedSemaphore(value)
    Path(os.path.join(config_dir(), "task_concurrent_max.txt")).write_text(str(value), encoding="utf-8")
    return jsonify({"ok": True, "max_concurrent": value})


@gdl_api.get("/tasks")
def tasks_list():
    if not _authed():
        return _deny()
    out = []
    for slug in sorted(os.listdir(tasks_dir())):
        if _valid_slug(slug) and os.path.isdir(os.path.join(tasks_dir(), slug)):
            try:
                out.append(_task_info(slug))
            except Exception:
                # One unreadable task folder must not 500 the whole list.
                continue
    return jsonify({"tasks": out})


@gdl_api.post("/tasks")
def tasks_create():
    if not _authed():
        return _deny()
    data = request.get_json(silent=True) or request.form.to_dict()
    if request.files.get("urls_file") and request.files["urls_file"].filename:
        raw_urls = request.files["urls_file"].read(10 * 1024 * 1024 + 1)
        if len(raw_urls) > 10 * 1024 * 1024:
            return jsonify({"error": "URL file exceeds 10 MB"}), 413
        data["urls"] = raw_urls.decode("utf-8", "replace").splitlines()
    if request.files.get("cookies_file") and request.files["cookies_file"].filename:
        cookie_raw = request.files["cookies_file"].read(1024 * 1024 + 1)
        if len(cookie_raw) > 1024 * 1024:
            return jsonify({"error": "Cookie file exceeds 1 MB"}), 413
        data["_cookies_raw"] = cookie_raw
    if data.get("schedule_mode") == "manual":
        data["schedule"] = ""
    slug = str(data.get("slug") or data.get("original_slug") or "").strip() or None
    try:
        slug = _save_task(data, slug)
    except (ValueError, FileNotFoundError, FileExistsError, RuntimeError) as e:
        return jsonify({"error": str(e)[:300]}), 400
    if data.get("_cookies_raw") is not None:
        Path(os.path.join(tasks_dir(), slug, "cookies.txt")).write_bytes(data["_cookies_raw"])
    # FormData always sends `start` (possibly the string "false"), so a plain
    # truthiness check used to launch every save/edit. Accept real booleans
    # and the usual form-encoded truthy words only.
    start_flag = data.get("start") is True or str(data.get("start") or "").strip().lower() in ("1", "true", "on", "yes")
    if start_flag:
        threading.Thread(target=_run_task, args=(slug,), daemon=True).start()
    return jsonify({"ok": True, "task": _task_info(slug)})


@gdl_api.post("/tasks/<slug>/start")
def task_start(slug: str):
    if not _authed():
        return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base):
        return jsonify({"error": "unknown task"}), 404
    with _lock:
        proc = _procs.get(slug)
        if proc is not None and proc.poll() is None:
            return jsonify({"ok": True, "state": "running"})
    # refresh command from command.txt (editable on disk)
    try:
        with open(os.path.join(base, "command.txt"), encoding="utf-8") as f:
            cmd = f.read().strip()
        st = _read_json(os.path.join(base, "status.json"), {})
        st["command"] = cmd or st.get("command")
        _write_json(os.path.join(base, "status.json"), st)
    except Exception:
        pass
    threading.Thread(target=_run_task, args=(slug,), daemon=True).start()
    return jsonify({"ok": True, "state": "running"})


@gdl_api.put("/tasks/<slug>")
def task_update(slug: str):
    if not _authed():
        return _deny()
    if not _task_base(slug) or not os.path.isdir(_task_base(slug)):
        return jsonify({"error": "unknown task"}), 404
    data = request.get_json(silent=True) or {}
    data["original_slug"] = slug
    try:
        new_slug = _save_task(data, slug)
        return jsonify({"ok": True, "task": _task_info(new_slug)})
    except Exception as e:
        return jsonify({"error": str(e)[:300]}), 400


@gdl_api.post("/tasks/<slug>/action")
def task_action(slug: str):
    if not _authed():
        return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base):
        return jsonify({"error": "unknown task"}), 404
    action = (request.get_json(silent=True) or {}).get("action", "")
    if action == "run":
        return task_start(slug)
    if action == "stop":
        return task_stop(slug)
    if action == "pause":
        marker = os.path.join(base, "paused")
        if os.path.exists(marker):
            os.remove(marker)
            with _lock: _paused_tasks.discard(slug)
            state = "unpaused"
        else:
            Path(marker).touch()
            with _lock: _paused_tasks.add(slug)
            state = "paused"
        return jsonify({"ok": True, "state": state})
    if action == "duplicate":
        old_name = _read_file_strip(os.path.join(base, "name.txt")) or _read_json(os.path.join(base, "status.json"), {}).get("name") or slug
        name = f"{old_name} copy"
        new_slug = _slug(name)
        n = 2
        while os.path.exists(os.path.join(tasks_dir(), new_slug)):
            name = f"{old_name} copy {n}"; new_slug = _slug(name); n += 1
        new_base = os.path.join(tasks_dir(), new_slug); os.makedirs(new_base)
        for fn in ("urls.txt", "command.txt", "cron.txt", "cookies.txt", "oauth_site.txt", "timeout.txt"):
            src = os.path.join(base, fn)
            if os.path.isfile(src): shutil.copy2(src, os.path.join(new_base, fn))
        Path(os.path.join(new_base, "name.txt")).write_text(name, encoding="utf-8")
        _write_json(os.path.join(new_base, "status.json"), {"name": name, "state": "idle", "command": _read_file_strip(os.path.join(new_base, "command.txt"))})
        Path(os.path.join(new_base, "stdout.log")).touch()
        if os.path.isfile(os.path.join(new_base, "cron.txt")):
            try: _set_schedule(new_slug, _read_file_strip(os.path.join(new_base, "cron.txt")))
            except Exception as e: return jsonify({"error": str(e)}), 400
        return jsonify({"ok": True, "task": _task_info(new_slug)})
    if action in ("clear_logs", "clear-log"):
        for fn in ("stdout.log", "logs.txt"):
            Path(os.path.join(base, fn)).write_text("", encoding="utf-8")
        return jsonify({"ok": True})
    if action == "delete_archive":
        try: os.remove(os.path.join(base, "archive.sqlite"))
        except FileNotFoundError: pass
        return jsonify({"ok": True})
    if action == "delete_cookies":
        try: os.remove(os.path.join(base, "cookies.txt"))
        except FileNotFoundError: pass
        return jsonify({"ok": True})
    return jsonify({"error": "unknown action"}), 400


@gdl_api.post("/tasks/actions")
def tasks_bulk_action():
    if not _authed(): return _deny()
    action = (request.get_json(silent=True) or {}).get("action")
    if action == "pause_all":
        for slug in os.listdir(tasks_dir()):
            base = _task_base(slug)
            if base and os.path.isdir(base): Path(os.path.join(base, "paused")).touch()
        return jsonify({"ok": True})
    if action == "clear_completed":
        removed = 0
        for slug in os.listdir(tasks_dir()):
            base = _task_base(slug)
            if base and os.path.isdir(base) and _task_info(slug)["state"] in ("done", "stopped", "error"):
                shutil.rmtree(base, ignore_errors=True); _set_schedule(slug, ""); removed += 1
        return jsonify({"ok": True, "removed": removed})
    return jsonify({"error": "unknown action"}), 400


@gdl_api.post("/tasks/<slug>/stop")
def task_stop(slug: str):
    if not _authed():
        return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "unknown task"}), 404
    with _lock:
        proc = _procs.get(slug)
    if proc is None or proc.poll() is not None:
        pid_text = _read_file_strip(os.path.join(base, "pid"))
        if pid_text and _pid_belongs_to(pid_text, base):
            try: os.kill(int(pid_text), signal.SIGTERM)
            except OSError: pass
        else:
            return jsonify({"ok": True, "state": "stopped"})
        st_path = os.path.join(base, "status.json")
        Path(os.path.join(base, "stopped")).touch()
        st = _read_json(st_path, {})
        st.update({"state": "stopped", "finished_at": _now()})
        _write_json(st_path, st)
        return jsonify({"ok": True, "state": "stopped"})
    try:
        Path(os.path.join(base, "stopped")).touch()
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        st_path = os.path.join(base, "status.json")
        st = _read_json(st_path, {})
        st.update({"state": "stopped", "finished_at": _now()})
        _write_json(st_path, st)
        _append_log(os.path.join(base, "stdout.log"), "[launcher] stopped by user")
        _append_log(os.path.join(base, "logs.txt"), "[launcher] stopped by user")
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 500
    return jsonify({"ok": True, "state": "stopped"})


@gdl_api.delete("/tasks/<slug>")
def task_delete(slug: str):
    if not _authed():
        return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base):
        return jsonify({"error": "unknown task"}), 404
    _set_schedule(slug, "")
    with _lock:
        proc = _procs.get(slug)
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
    shutil.rmtree(base, ignore_errors=True)
    return jsonify({"ok": True})


@gdl_api.get("/tasks/<slug>/log")
@gdl_api.get("/tasks/<slug>/logs")
def task_log(slug: str):
    if not _authed():
        return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base):
        return jsonify({"error": "unknown task"}), 404
    lines = int(request.args.get("lines", 200))
    lines = max(1, min(lines, 2000))
    path = os.path.join(base, "stdout.log")
    if not os.path.isfile(path): path = os.path.join(base, "logs.txt")
    return jsonify({
        "slug": slug,
        "lines": _tail(path, lines),
    })


@gdl_api.get("/tasks/<slug>/urls")
def task_urls(slug: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "unknown task"}), 404
    return jsonify({"slug": slug, "content": _read_file_strip(os.path.join(base, "urls.txt"))})


@gdl_api.put("/tasks/<slug>/urls")
def task_urls_update(slug: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "unknown task"}), 404
    data = request.get_json(silent=True) or {}
    content = data.get("content", "")
    if not isinstance(content, str) or len(content) > 10 * 1024 * 1024:
        return jsonify({"error": "invalid URL content"}), 400
    Path(os.path.join(base, "urls.txt")).write_text(content.strip() + ("\n" if content.strip() else ""), encoding="utf-8")
    return jsonify({"ok": True})


@gdl_api.get("/task")
def plugin_task_status():
    """Aggregated background-task status for PluginTaskFloat.

    Never call ``_task_info`` while holding ``_lock``: it takes the same lock
    itself, and a plain Lock deadlocked the status endpoint (and every other
    task route) as soon as a task started — which in turn filled the waitress
    queue and stopped unrelated pages from loading.
    """
    with _lock:
        running_slugs = [slug for slug, proc in _procs.items() if proc.poll() is None]
    if not running_slugs:
        return jsonify({
            "active": False,
            "state": "idle",
            "current": 0,
            "total": 0,
            "detail": "",
            "message": "",
            "started_at": 0,
        })
    slug = running_slugs[0]
    info = _task_info(slug) if _task_base(slug) else {}
    name = info.get("name") or slug
    url_index = int(info.get("url_index") or 0)
    url_total = int(info.get("url_total") or info.get("urls") or 0)
    progress = info.get("progress")
    if url_total:
        current = max(0, min(url_index, url_total))
        total = url_total
        message = f"{name} · URL {current}/{total}" if current else f"{name} · starting"
    elif isinstance(progress, int) and 0 <= progress <= 100:
        current, total = progress, 100
        message = f"{name}: {progress}%"
    else:
        current, total = 0, max(1, len(running_slugs))
        message = f"{name} running"
    detail = slug
    current_url = info.get("current_url") or ""
    if current_url:
        host = re.sub(r"^[a-z]+://([^/]+).*$", r"\1", current_url)
        if host and host != current_url:
            detail = host
    return jsonify({
        "active": True,
        "state": "running",
        "current": current,
        "total": total,
        "detail": detail,
        "message": message,
        "started_at": _epoch(info.get("started_at")),
    })


@gdl_api.get("/tasks/<slug>/history")
def task_history(slug: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "unknown task"}), 404
    runs = []
    try:
        with open(os.path.join(base, "run_history.jsonl"), encoding="utf-8") as f:
            for line in f:
                try: runs.append(json.loads(line))
                except ValueError: continue
    except OSError: pass
    return jsonify({"slug": slug, "runs": list(reversed(runs[-100:]))})


@gdl_api.get("/tasks/<slug>/recent")
def task_recent(slug: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "unknown task"}), 404
    log_path = os.path.join(base, "logs.txt") if os.path.isfile(os.path.join(base, "logs.txt")) else os.path.join(base, "stdout.log")
    items = _recent_from_log(log_path, 20)
    return jsonify({"slug": slug, "items": items})


def _recent_from_log(path: str, limit: int):
    root = os.path.realpath(downloads_dir())
    found = []
    seen = set()
    for line in reversed(_tail(path, 2000)):
        clean = line.replace("\\", "/")
        i = clean.find(root.replace("\\", "/"))
        if i < 0: continue
        tail = clean[i + len(root):].strip(" \t\"'()[],:;")
        match = re.search(r"([^\s\"']+\.(?:jpg|jpeg|png|gif|webp|mp4|webm|mkv))", tail, re.I)
        if not match: continue
        rel = match.group(1).lstrip("/")
        candidate = os.path.realpath(os.path.join(root, rel))
        if os.path.commonpath((root, candidate)) != root or not os.path.isfile(candidate) or rel in seen: continue
        seen.add(rel)
        ext = os.path.splitext(candidate)[1].lower()
        found.append({"rel": rel, "name": os.path.basename(candidate), "ext": ext,
                      "url": "/gdl/api/media/file?path=" + __import__("urllib.parse", fromlist=["quote"]).quote(rel),
                      "is_image": ext in _IMAGE_EXTS, "is_video": ext in _MEDIA_EXTS - _IMAGE_EXTS,
                      "size": os.path.getsize(candidate), "mtime": os.path.getmtime(candidate)})
        if len(found) >= limit: break
    return found


@gdl_api.get("/tasks/<slug>/logs/archived")
def task_archived_logs(slug: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "unknown task"}), 404
    files = []
    for fn in sorted(os.listdir(base), reverse=True):
        if re.fullmatch(r"logs-\d{4}-\d{2}-\d{2}T\d{6}\.txt", fn):
            files.append({"name": fn, "size": os.path.getsize(os.path.join(base, fn))})
    return jsonify({"files": files})


@gdl_api.get("/tasks/<slug>/logs/download")
def task_log_download(slug: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "unknown task"}), 404
    path = os.path.join(base, "stdout.log")
    if not os.path.isfile(path): path = os.path.join(base, "logs.txt")
    if not os.path.isfile(path): return jsonify({"error": "log not found"}), 404
    return send_file(path, as_attachment=True, download_name=f"{slug}-logs.txt")


@gdl_api.get("/tasks/<slug>/logs/archive/<filename>")
def task_archived_log_download(slug: str, filename: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base) or not re.fullmatch(r"logs-\d{4}-\d{2}-\d{2}T\d{6}\.txt", filename):
        return jsonify({"error": "archive not found"}), 404
    path = os.path.join(base, filename)
    if not os.path.isfile(path): return jsonify({"error": "archive not found"}), 404
    return send_file(path, as_attachment=True, download_name=f"{slug}-{filename}")


@gdl_api.get("/tasks/<slug>/logs/stream")
def task_log_stream(slug: str):
    if not _authed(): return _deny()
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return Response("", status=404)
    path = os.path.join(base, "stdout.log")
    if not os.path.isfile(path): path = os.path.join(base, "logs.txt")
    def generate():
        pos = os.path.getsize(path) if os.path.isfile(path) else 0
        initial = "\n".join(_tail(path, 50))
        yield "data: " + json.dumps({"content": initial, "reset": True}) + "\n\n"
        while True:
            time.sleep(1)
            try:
                size = os.path.getsize(path)
                if size < pos:
                    initial = "\n".join(_tail(path, 50))
                    yield "data: " + json.dumps({"content": initial, "reset": True}) + "\n\n"; pos = size
                elif size > pos:
                    with open(path, encoding="utf-8", errors="replace") as f: f.seek(pos); text = f.read()
                    pos = size
                    yield f"data: {json.dumps({'content': text, 'reset': False})}\n\n"
            except GeneratorExit: return
            except OSError: continue
    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@gdl_api.post("/quick")
def quick_start():
    """One-shot download in the background. Returns a job id to poll."""
    if not _authed():
        return _deny()
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "url required"}), 400
    with _one_time_lock:
        if _one_time_proc is not None and _one_time_proc.poll() is None:
            return jsonify({"error": "a quick download is already running"}), 409
    dest = (data.get("dest") or "").strip() or downloads_dir()
    os.makedirs(dest, exist_ok=True)
    job_id = uuid.uuid4().hex[:12]
    log_path = os.path.join(tasks_dir(), f".quick-{job_id}.log")
    cmd = ["gallery-dl", "-d", dest]
    if data.get("save_metadata", True):
        cmd.append("--write-metadata")
    conf = (data.get("config") or "").strip()
    if conf:
        cmd += ["--config", conf]
    archive = (data.get("archive") or "").strip()
    if archive:
        cmd += ["--download-archive", archive]
    if data.get("images_only", True) or data.get("include_video", False):
        extensions = ["jpg", "jpeg", "png", "webp", "gif"]
        if data.get("include_video", False):
            extensions += ["mp4", "webm", "mkv", "mov"]
        quoted = ", ".join(repr(ext) for ext in extensions)
        cmd += ["--filter", f"extension in ({quoted})"]
    if data.get("min_width"):
        try:
            cmd += ["--filter", f"image_width >= {int(data['min_width'])}"]
        except (TypeError, ValueError):
            pass
    # cookie_file: path to an existing cookies-*.txt in config dir
    cookie_file = (data.get("cookie_file") or "").strip()
    if cookie_file:
        safe = os.path.abspath(os.path.join(config_dir(), os.path.basename(cookie_file)))
        if os.path.isfile(safe):
            cmd += ["--cookies", safe]
    elif data.get("cookies"):
        cp = os.path.join(config_dir(), f"cookies-quick-{job_id}.txt")
        with open(cp, "w", encoding="utf-8") as f:
            f.write(data["cookies"])
        cmd += ["--cookies", cp]
    cmd.append(url)

    meta_path = os.path.join(tasks_dir(), f".quick-{job_id}.json")
    shared_log = os.path.join(config_dir(), "one_time_download.log")
    also_add_task = bool(data.get("also_add_task"))
    task_slug = None
    if also_add_task:
        task_name = str(data.get("task_name") or "").strip()
        if not task_name: return jsonify({"error": "task name required when adding this URL as a task"}), 400
        try:
            task_slug = _save_task({"name": task_name, "urls": [url], "command": f"gallery-dl --config {conf or os.path.join(config_dir(), 'gallery-dl.conf')} --destination {dest} --input-file urls.txt", "start": False})
        except Exception as e:
            return jsonify({"error": f"could not create task: {e}"}), 400
    def _save_job():
        job = _quick.get(job_id, {})
        _write_json(meta_path, {k: v for k, v in job.items() if k != "proc"})
    def _run():
        global _one_time_proc
        _quick[job_id] = {"state": "running", "cmd": cmd, "started_at": _now(), "url": url, "dest": dest}
        if task_slug: _quick[job_id]["task_slug"] = task_slug
        _save_job()
        try:
            with open(log_path, "w", encoding="utf-8") as lf, open(shared_log, "a", encoding="utf-8") as shared:
                lf.write(f"$ {' '.join(cmd)}\n")
                shared.write(f"\n\n==== Quick download started at {_now()} ====\nURL: {url}\n$ {' '.join(shlex.quote(x) for x in cmd)}\n")
                lf.flush()
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        text=True, bufsize=1, cwd=dest)
                with _one_time_lock: _one_time_proc = proc
                _quick[job_id]["proc"] = proc
                assert proc.stdout is not None
                _pump_process_output(proc.stdout, lf, shared)
                rc = proc.wait()
                _quick[job_id].update({"state": "done" if rc == 0 else "error",
                                       "finished_at": _now(), "exit": rc})
                shared.write(f"\nQuick download exited with code {rc}.\n")
                _save_job()
        except Exception as e:
            _quick[job_id].update({"state": "error", "last_error": str(e)[:300]})
            _quick[job_id]["finished_at"] = _now()
            _save_job()
        finally:
            with _one_time_lock: _one_time_proc = None

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "job": job_id, "cmd": cmd})


@gdl_api.get("/quick/<job_id>")
def quick_status(job_id: str):
    if not _authed():
        return _deny()
    if not re.fullmatch(r"[a-f0-9]{12}", job_id): return jsonify({"error": "unknown job"}), 404
    job = _quick.get(job_id) or _read_json(os.path.join(tasks_dir(), f".quick-{job_id}.json"), None)
    if not job:
        return jsonify({"error": "unknown job"}), 404
    if job.get("state") == "running" and job_id not in _quick:
        job["state"] = "stopped"; job["finished_at"] = _now()
        _write_json(os.path.join(tasks_dir(), f".quick-{job_id}.json"), job)
    out = {k: v for k, v in job.items() if k != "proc"}
    out["log"] = _tail(os.path.join(tasks_dir(), f".quick-{job_id}.log"), 120)
    return jsonify(out)


@gdl_api.get("/quick")
def quick_list():
    if not _authed(): return _deny()
    jobs = []
    for fn in os.listdir(tasks_dir()):
        if fn.startswith(".quick-") and fn.endswith(".json"):
            job = _read_json(os.path.join(tasks_dir(), fn), {})
            job["job"] = fn[7:-5]
            if job.get("state") == "running" and job["job"] not in _quick:
                job["state"] = "stopped"; job["finished_at"] = _now()
                _write_json(os.path.join(tasks_dir(), fn), {k: v for k, v in job.items() if k != "job"})
            jobs.append(job)
    jobs.sort(key=lambda j: j.get("started_at", ""), reverse=True)
    return jsonify({"jobs": jobs[:50]})


@gdl_api.post("/quick/<job_id>/stop")
def quick_stop(job_id: str):
    if not _authed():
        return _deny()
    if not re.fullmatch(r"[a-f0-9]{12}", job_id): return jsonify({"error": "unknown job"}), 404
    job = _quick.get(job_id)
    if not job:
        return jsonify({"error": "unknown job"}), 404
    proc = job.get("proc")
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
        job["state"] = "stopped"
        job["finished_at"] = _now()
        _write_json(os.path.join(tasks_dir(), f".quick-{job_id}.json"), {k: v for k, v in job.items() if k != "proc"})
    return jsonify({"ok": True, "state": job.get("state")})


@gdl_api.get("/one-time/status")
def one_time_status():
    if not _authed(): return _deny()
    with _one_time_lock: running = bool(_one_time_proc is not None and _one_time_proc.poll() is None)
    return jsonify({"running": running})


@gdl_api.get("/one-time/logs")
def one_time_logs():
    if not _authed(): return _deny()
    path = os.path.join(config_dir(), "one_time_download.log")
    tail = max(1, min(2000, request.args.get("tail", type=int) or 200))
    return jsonify({"running": one_time_status_running(), "content": "\n".join(_tail(path, tail)) if os.path.isfile(path) else ""})


def one_time_status_running():
    with _one_time_lock: return bool(_one_time_proc is not None and _one_time_proc.poll() is None)


@gdl_api.get("/one-time/logs/download")
def one_time_log_download():
    if not _authed(): return _deny()
    path = os.path.join(config_dir(), "one_time_download.log")
    if not os.path.isfile(path): return jsonify({"error": "no log exists"}), 404
    return send_file(path, as_attachment=True, download_name="one_time_download.log")


@gdl_api.post("/one-time/clear-logs")
def one_time_log_clear():
    if not _authed(): return _deny()
    Path(os.path.join(config_dir(), "one_time_download.log")).write_text("", encoding="utf-8")
    return jsonify({"ok": True})


@gdl_api.post("/one-time/stop")
def one_time_stop():
    if not _authed(): return _deny()
    with _one_time_lock: proc = _one_time_proc
    if proc is None or proc.poll() is not None: return jsonify({"ok": True, "state": "stopped"})
    proc.terminate()
    return jsonify({"ok": True, "state": "stopping"})


@gdl_api.get("/one-time/recent")
def one_time_recent():
    if not _authed(): return _deny()
    return jsonify({"items": _scan_recent_media(downloads_dir(), 16)})


def _scan_recent_media(root: str, limit: int):
    found = []
    root = os.path.realpath(root)
    try:
        for dp, dirs, fns in os.walk(root):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fn in fns:
                if os.path.splitext(fn)[1].lower() not in _MEDIA_EXTS: continue
                path = os.path.join(dp, fn)
                try:
                    st = os.stat(path); rel = os.path.relpath(path, root)
                    ext = os.path.splitext(fn)[1].lower()
                    found.append({"name": fn, "filename": fn, "rel": rel, "size": st.st_size,
                                  "mtime": st.st_mtime, "ext": ext,
                                  "url": "/gdl/api/media/file?path=" + __import__("urllib.parse", fromlist=["quote"]).quote(rel),
                                  "is_image": ext in _IMAGE_EXTS, "is_video": ext in _MEDIA_EXTS - _IMAGE_EXTS})
                except OSError: continue
    except OSError: pass
    found.sort(key=lambda x: x["mtime"], reverse=True)
    return found[:limit]


def _telemetry_aux() -> dict:
    """Cached task-folder size + log counters.

    Walking the task folders and tailing every log took seconds and ran on
    every telemetry poll, which made the panel look frozen. These numbers do
    not need 10-second resolution, so cache them separately.
    """
    now = time.time()
    if _aux_cache["ts"] and now - _aux_cache["ts"] < _AUX_TTL:
        return _aux_cache
    tasks_bytes = 0
    filtered = errors = 0
    root = tasks_dir()
    try:
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in filenames:
                try:
                    tasks_bytes += os.path.getsize(os.path.join(dirpath, filename))
                except OSError:
                    pass
        for slug in os.listdir(root):
            base = _task_base(slug)
            if not base or not os.path.isdir(base):
                continue
            lp = os.path.join(base, "logs.txt") if os.path.isfile(os.path.join(base, "logs.txt")) else os.path.join(base, "stdout.log")
            for line in _tail(lp, 3000):
                low = line.lower()
                filtered += int(any(k in low for k in ("skip", "filtered", "already downloaded")))
                errors += int(any(k in low for k in ("error", "failed", "exception")))
    except OSError:
        pass
    _aux_cache.update({"ts": now, "tasks_bytes": tasks_bytes, "filtered": filtered, "errors": errors})
    return _aux_cache


@gdl_api.get("/telemetry")
def telemetry():
    if not _authed():
        return _deny()
    scan = _scan_downloads()
    aux = _telemetry_aux()
    images = scan["bytes"]
    meta = aux["tasks_bytes"]
    items_today = scan["today_items"]
    bytes_today = scan["today_bytes"]
    filtered = aux["filtered"]
    errors = aux["errors"]
    try: disk = shutil.disk_usage(downloads_dir())
    except OSError: disk = None
    return jsonify({
        "images_bytes": images,
        "tasks_bytes": meta,
        "images_gb": round(images / 1e9, 1),
        "tasks_gb": round(meta / 1e9, 1),
        "images_dir": downloads_dir(),
        "tasks_dir": tasks_dir(),
        "items_today": items_today,
        "bytes_today": bytes_today,
        "filtered": filtered,
        "errors": errors,
        "disk_total": disk.total if disk else 0,
        "disk_used": disk.used if disk else 0,
        "disk_free": disk.free if disk else 0,
        "disk_pct": round(disk.used / disk.total * 100, 1) if disk and disk.total else 0,
    })


@gdl_api.get("/kiosks")
def kiosks_list():
    if not _authed():
        return _deny()
    kiosks = _read_json(os.path.join(config_dir(), "kiosks.json"), [])
    if not isinstance(kiosks, list): kiosks = []
    known = {k.get("id") for k in kiosks}
    root = os.path.join(config_dir(), "kiosks")
    if os.path.isdir(root):
        for kid in sorted(os.listdir(root)):
            if not _valid_slug(kid) or kid in known: continue
            settings = _kiosk_settings(kid)
            kiosks.append({"id": kid, **settings, "live": False})
    for k in kiosks:
        k["image_count"] = len(_kiosk_images(k.get("id", "")))
        k["view_url"] = f"/gdl/api/kiosk/{k.get('id','')}/view"
    return jsonify({"kiosks": kiosks})


@gdl_api.post("/kiosks")
def kiosks_create():
    if not _authed():
        return _deny()
    data = request.get_json(force=True, silent=True) or {}
    path = os.path.join(config_dir(), "kiosks.json")
    kiosks = _read_json(path, [])
    if not isinstance(kiosks, list):
        kiosks = []
    name = (data.get("name") or "Untitled kiosk").strip()
    kid = _slug(name)
    if any(x.get("id") == kid for x in kiosks): return jsonify({"error": "kiosk already exists"}), 409
    dflt = _read_json(os.path.join(config_dir(), "kiosk_defaults.json"), {})
    k = {"id": kid, "name": name, "folder": data.get("folder") or downloads_dir(),
         "interval": int(data.get("interval") or dflt.get("interval", 30)),
         "shuffle": bool(data.get("shuffle", dflt.get("shuffle", True))),
         "blur_nsfw": bool(data.get("blur_nsfw", dflt.get("blur_nsfw", True))),
         "order": data.get("order") or ("random" if data.get("shuffle", dflt.get("shuffle", True)) else "sequential"),
         "transition": data.get("transition") or dflt.get("transition", "fade"),
         "trans_speed": float(data.get("trans_speed") or dflt.get("trans_speed", 0.9)),
         "fit": data.get("fit") or dflt.get("fit", "contain"),
         "background": data.get("background") or dflt.get("background", "black"),
         "ken_burns": bool(data.get("ken_burns", dflt.get("ken_burns", False))),
         "show_clock": bool(data.get("show_clock", dflt.get("show_clock", False))), "live": False}
    kiosks.append(k)
    _write_json(path, kiosks)
    os.makedirs(os.path.join(config_dir(), "kiosks", kid, "images"), exist_ok=True)
    _save_kiosk_settings(kid, k)
    k["image_count"] = 0; k["view_url"] = f"/gdl/api/kiosk/{kid}/view"
    return jsonify({"ok": True, "kiosk": k})


@gdl_api.post("/kiosks/<kid>/toggle")
def kiosk_toggle(kid: str):
    if not _authed():
        return _deny()
    path = os.path.join(config_dir(), "kiosks.json")
    kiosks = _read_json(path, [])
    for k in kiosks:
        if k.get("id") == kid:
            k["live"] = not k.get("live")
            _write_json(path, kiosks)
            return jsonify({"ok": True, "kiosk": k})
    if not _valid_slug(kid) or not os.path.isdir(_kiosk_dir(kid)):
        return jsonify({"error": "unknown kiosk"}), 404
    k = {"id": kid, **_kiosk_settings(kid), "live": True}
    kiosks.append(k)
    _write_json(path, kiosks)
    return jsonify({"ok": True, "kiosk": k})


@gdl_api.put("/kiosks/<kid>")
def kiosk_update(kid: str):
    if not _authed(): return _deny()
    if not _valid_slug(kid): return jsonify({"error": "invalid kiosk"}), 400
    data = request.get_json(silent=True) or {}
    settings = _kiosk_settings(kid)
    settings.update({k: data[k] for k in ("name", "interval", "shuffle", "blur_nsfw", "order", "transition", "trans_speed", "fit", "background", "ken_burns", "show_clock", "folder") if k in data})
    try: settings["interval"] = max(1, min(3600, int(settings.get("interval", 30))))
    except (TypeError, ValueError): return jsonify({"error": "invalid interval"}), 400
    _save_kiosk_settings(kid, settings)
    path = os.path.join(config_dir(), "kiosks.json")
    kiosks = _read_json(path, [])
    for k in kiosks:
        if k.get("id") == kid: k.update(settings)
    _write_json(path, kiosks)
    return jsonify({"ok": True, "kiosk": {"id": kid, **settings}})


@gdl_api.post("/kiosks/<kid>/images")
def kiosk_upload_images(kid: str):
    if not _authed(): return _deny()
    if not _valid_slug(kid): return jsonify({"error": "invalid kiosk"}), 400
    root = os.path.join(config_dir(), "kiosks", kid, "images"); os.makedirs(root, exist_ok=True)
    added, skipped = [], []
    for upload in request.files.getlist("images"):
        original = os.path.basename(upload.filename or "")
        filename = re.sub(r"[^A-Za-z0-9._ -]", "_", original).strip(" .")
        if not filename or os.path.splitext(filename)[1].lower() not in _IMAGE_EXTS:
            skipped.append(original); continue
        raw = upload.read(20 * 1024 * 1024 + 1)
        if len(raw) > 20 * 1024 * 1024: skipped.append(original); continue
        Path(os.path.join(root, filename)).write_bytes(raw); added.append(filename)
    return jsonify({"ok": True, "added": added, "skipped": skipped})


@gdl_api.delete("/kiosks/<kid>/images/<filename>")
def kiosk_remove_image(kid: str, filename: str):
    if not _authed(): return _deny()
    if not _valid_slug(kid) or os.path.basename(filename) != filename: return jsonify({"error": "invalid path"}), 400
    path = os.path.join(config_dir(), "kiosks", kid, "images", filename)
    try: os.remove(path)
    except FileNotFoundError: return jsonify({"error": "image not found"}), 404
    return jsonify({"ok": True})


@gdl_api.get("/kiosks/<kid>/images")
def kiosk_images_admin(kid: str):
    if not _authed(): return _deny()
    if not _valid_slug(kid): return jsonify({"error": "invalid kiosk"}), 400
    return jsonify({"images": _kiosk_images(kid)})


@gdl_api.get("/kiosk/<kid>/images")
def kiosk_public_images(kid: str):
    if not _valid_slug(kid): return jsonify({"error": "invalid kiosk"}), 400
    settings = _kiosk_settings(kid)
    images = [{"name": fn, "url": f"/gdl/api/kiosk/{kid}/media/{__import__('urllib.parse', fromlist=['quote']).quote(fn)}"} for fn in _kiosk_images(kid)]
    return jsonify({"slug": kid, "name": settings.get("name", kid), "interval": settings.get("interval", 30),
                    "order": settings.get("order", "random"), "settings": settings, "images": images})


@gdl_api.get("/kiosk/<kid>/media/<path:filename>")
def kiosk_public_media(kid: str, filename: str):
    if not _valid_slug(kid): return "Not found", 404
    root = os.path.realpath(os.path.join(config_dir(), "kiosks", kid, "images"))
    target = os.path.realpath(os.path.join(root, filename))
    if os.path.commonpath((root, target)) != root or not os.path.isfile(target): return "Not found", 404
    return send_file(target)


@gdl_api.get("/kiosk/<kid>/view")
@gdl_api.get("/kiosk/<kid>")
def kiosk_public_view(kid: str):
    if not _valid_slug(kid): return "Not found", 404
    settings = _kiosk_settings(kid)
    name = str(settings.get("name", kid)).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    background = str(settings.get("background", "black"))
    if background not in ("black", "white", "blur") and not re.fullmatch(r"#[0-9a-fA-F]{3,8}", background): background = "black"
    if background == "blur": background = "#20202a"
    fit = settings.get("fit", "contain") if settings.get("fit") in ("contain", "cover", "center") else "contain"
    transition = settings.get("transition", "fade") if settings.get("transition") in ("fade", "cut", "slide") else "fade"
    try: speed = max(0.1, min(3.0, float(settings.get("trans_speed", 0.9))))
    except (TypeError, ValueError): speed = 0.9
    show_clock = str(settings.get("show_clock", "")).lower() in ("1", "true", "yes", "on")
    ken_burns = str(settings.get("ken_burns", "")).lower() in ("1", "true", "yes", "on")
    try: interval = max(1, min(3600, int(settings.get("interval", 30))))
    except (TypeError, ValueError): interval = 30
    ken_burns_css = (
        "@keyframes kb{from{transform:scale(1)}to{transform:scale(1.12)}}"
        ".ken-burns{animation:kb " + str(interval) + "s ease-in-out infinite alternate}"
        if ken_burns else ""
    )
    clock_script = (
        'setInterval(()=>document.getElementById("clock").textContent=new Date().toLocaleTimeString([], {hour:"2-digit",minute:"2-digit"}),1000);'
        if show_clock else ""
    )
    html_doc = f'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{name}</title>
<style>*{{box-sizing:border-box}}body{{margin:0;background:{background};color:white;font:14px Inter,system-ui;overflow:hidden}}#stage{{height:100vh;display:grid;place-items:center;overflow:hidden}}#slide{{max-width:100vw;max-height:100vh;object-fit:{fit};transition:opacity {speed}s ease,transform {speed}s ease}}#bar{{position:fixed;left:0;right:0;bottom:0;padding:20px;background:linear-gradient(transparent,#000b);display:flex;align-items:center;justify-content:center;gap:12px;opacity:0;transition:opacity .2s}}body:hover #bar{{opacity:1}}button{{border:1px solid #ffffff55;background:#18202dcc;color:#fff;border-radius:5px;padding:8px 14px;cursor:pointer}}#clock{{position:fixed;right:2vw;top:2vh;font-size:2rem;text-shadow:0 1px 8px #000}}{ken_burns_css}</style></head>
<body><div id="stage"><img id="slide" alt=""></div>{'<div id="clock"></div>' if show_clock else ''}<div id="bar"><button id="prev">Previous</button><button id="pause">Pause</button><button id="next">Next</button><button id="full">Fullscreen</button><span id="counter"></span><a href="/api/plugins/gallery-dl/page/kiosks" style="color:#fff">Manage</a></div>
<script>const kid={json.dumps(kid)},effect={json.dumps(transition)},period={interval*1000};let imgs=[],idx=-1,paused=false,timer;const img=document.getElementById('slide');function show(n){{if(!imgs.length)return;idx=(n+imgs.length)%imgs.length;const set=()=>{{img.src=imgs[idx].url;img.className={json.dumps('ken-burns' if ken_burns else '')};document.getElementById('counter').textContent=(idx+1)+' / '+imgs.length}};if(effect==='cut'){{img.style.transition='none';set();return}}img.style.opacity=0;img.style.transform=effect==='slide'?'translateX(30px)':'none';setTimeout(()=>{{set();img.onload=()=>{{img.style.opacity=1;img.style.transform='none'}}}},250)}}function next(){{show(idx+1)}}function start(){{clearInterval(timer);timer=setInterval(()=>{{if(!paused)next()}},period)}}async function load(){{const r=await fetch('/gdl/api/kiosk/'+kid+'/images'),d=await r.json();imgs=d.images||[];if(d.order==='random')imgs.sort(()=>Math.random()-.5);if(imgs.length)show(0);else document.getElementById('counter').textContent='No images uploaded';start()}}document.getElementById('prev').onclick=()=>show(idx-1);document.getElementById('next').onclick=next;document.getElementById('pause').onclick=e=>{{paused=!paused;e.target.textContent=paused?'Resume':'Pause'}};document.getElementById('full').onclick=()=>document.fullscreenElement?document.exitFullscreen():document.documentElement.requestFullscreen?.();document.addEventListener('keydown',e=>{{if(e.key==='ArrowRight')next();if(e.key==='ArrowLeft')show(idx-1);if(e.key===' '){{e.preventDefault();paused=!paused;document.getElementById('pause').textContent=paused?'Resume':'Pause'}}if(e.key.toLowerCase()==='f')document.documentElement.requestFullscreen?.()}});load();{clock_script}</script></body></html>'''
    return Response(html_doc, mimetype="text/html")


def _kiosk_dir(kid: str):
    return os.path.join(config_dir(), "kiosks", kid)


def _kiosk_settings(kid: str):
    return _read_json(os.path.join(_kiosk_dir(kid), "settings.json"), {"name": kid, "interval": 30, "order": "random", "shuffle": True, "blur_nsfw": True})


def _save_kiosk_settings(kid: str, settings):
    os.makedirs(_kiosk_dir(kid), exist_ok=True)
    _write_json(os.path.join(_kiosk_dir(kid), "settings.json"), settings)


def _kiosk_images(kid: str):
    if not _valid_slug(kid): return []
    root = os.path.join(_kiosk_dir(kid), "images")
    if not os.path.isdir(root): return []
    return sorted(fn for fn in os.listdir(root) if os.path.isfile(os.path.join(root, fn)) and os.path.splitext(fn)[1].lower() in _IMAGE_EXTS)


@gdl_api.delete("/kiosks/<kid>")
def kiosk_delete(kid: str):
    if not _authed():
        return _deny()
    path = os.path.join(config_dir(), "kiosks.json")
    kiosks = [k for k in _read_json(path, []) if k.get("id") != kid]
    _write_json(path, kiosks)
    if _valid_slug(kid): shutil.rmtree(_kiosk_dir(kid), ignore_errors=True)
    return jsonify({"ok": True})


@gdl_api.get("/kiosk/<kid>/manifest.json")
def kiosk_manifest(kid: str):
    if not _valid_slug(kid): return jsonify({"error": "invalid kiosk"}), 400
    settings = _kiosk_settings(kid); name = settings.get("name", kid)
    return jsonify({"name": name, "short_name": name, "display": "fullscreen", "orientation": "landscape",
                    "start_url": f"/gdl/api/kiosk/{kid}/view", "background_color": settings.get("background", "#000000"), "theme_color": "#000000", "icons": []})


@gdl_api.get("/kiosks/defaults")
def kiosk_defaults_get():
    if not _authed(): return _deny()
    return jsonify({"defaults": _read_json(os.path.join(config_dir(), "kiosk_defaults.json"), {"interval": 30, "transition": "fade", "shuffle": True, "blur_nsfw": True})})


@gdl_api.put("/kiosks/defaults")
def kiosk_defaults_put():
    if not _authed(): return _deny()
    data = request.get_json(silent=True) or {}
    try: data["interval"] = max(1, min(3600, int(data.get("interval", 30))))
    except (TypeError, ValueError): return jsonify({"error": "invalid interval"}), 400
    _write_json(os.path.join(config_dir(), "kiosk_defaults.json"), data)
    return jsonify({"ok": True, "defaults": data})


@gdl_api.get("/config")
def config_read():
    """gallery-dl.conf content (single settings page)."""
    if not _authed():
        return _deny()
    path = os.path.join(config_dir(), "gallery-dl.conf")
    try:
        with open(path, encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        content = _read_file_strip(_DEFAULT_CONFIG_FILE)
    return jsonify({"path": path, "content": content})


@gdl_api.post("/config")
def config_write():
    if not _authed():
        return _deny()
    data = request.get_json(force=True, silent=True) or {}
    content = data.get("content", "")
    if not isinstance(content, str) or len(content) > 200_000:
        return jsonify({"error": "bad content"}), 400
    if content.strip():
        try:
            json.loads(content)
        except json.JSONDecodeError as e:
            return jsonify({"error": f"invalid JSON: {e.msg} (line {e.lineno}, column {e.colno})"}), 400
    path = os.path.join(config_dir(), "gallery-dl.conf")
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(content))
    return jsonify({"ok": True, "path": path})


@gdl_api.post("/config/reset")
def config_reset():
    if not _authed(): return _deny()
    try:
        with urlrequest.urlopen(_DEFAULT_CONFIG_URL, timeout=12) as resp:
            content = resp.read(2_000_000).decode("utf-8")
        json.loads(content)
    except Exception:
        content = _read_file_strip(_DEFAULT_CONFIG_FILE)
    if not content:
        return jsonify({"error": "Default config could not be loaded"}), 502
    path = os.path.join(config_dir(), "gallery-dl.conf")
    Path(path).write_text(content, encoding="utf-8")
    return jsonify({"ok": True, "path": path, "content": content})


@gdl_api.get("/config/check-update")
def config_check_update():
    if not _authed(): return _deny()
    try:
        with urlrequest.urlopen(_DEFAULT_CONFIG_URL, timeout=12) as resp:
            remote = json.loads(resp.read(2_000_000).decode("utf-8"))
        local = json.loads(_read_file_strip(os.path.join(config_dir(), "gallery-dl.conf")) or _read_file_strip(_DEFAULT_CONFIG_FILE) or "{}")
    except Exception as e:
        return jsonify({"error": str(e)[:300]}), 502
    missing = []
    def walk(a, b, prefix=""):
        if not isinstance(b, dict): return
        for key, val in b.items():
            if key not in a:
                missing.append((prefix + key, val))
            elif isinstance(a.get(key), dict) and isinstance(val, dict):
                walk(a[key], val, prefix + key + ".")
    walk(local, remote)
    return jsonify({"up_to_date": not missing, "total": len(missing),
                    "options": [k for k, _ in missing[:100]], "config_url": _DEFAULT_CONFIG_URL})


@gdl_api.post("/config/apply-update")
def config_apply_update():
    if not _authed(): return _deny()
    path = os.path.join(config_dir(), "gallery-dl.conf")
    try:
        with urlrequest.urlopen(_DEFAULT_CONFIG_URL, timeout=12) as resp:
            remote = json.loads(resp.read(2_000_000).decode("utf-8"))
        local = json.loads(_read_file_strip(path) or _read_file_strip(_DEFAULT_CONFIG_FILE) or "{}")
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if os.path.isfile(path): shutil.copy2(path, path + f".bak.{stamp}")
        def merge(a, b):
            out = dict(a)
            for key, val in b.items():
                if key in out and isinstance(out[key], dict) and isinstance(val, dict): out[key] = merge(out[key], val)
                elif key not in out: out[key] = json.loads(json.dumps(val))
            return out
        content = json.dumps(merge(local, remote), indent=2, ensure_ascii=False)
        Path(path).write_text(content, encoding="utf-8")
        return jsonify({"ok": True, "content": content})
    except Exception as e:
        return jsonify({"error": str(e)[:300]}), 502


@gdl_api.get("/tools/check-update")
def tools_check_update():
    if not _authed(): return _deny()
    result = {}
    for command, package in (("gallery-dl", "gallery-dl"), ("yt-dlp", "yt-dlp")):
        current = _tool_version(command)
        entry = {"current": current}
        try:
            with urlrequest.urlopen(f"https://pypi.org/pypi/{package}/json", timeout=12) as resp:
                latest = json.loads(resp.read(1_000_000).decode("utf-8"))["info"]["version"]
            entry["latest"] = latest
            entry["update_available"] = current not in ("missing", "not found") and current != latest
        except Exception as e:
            entry["error"] = str(e)[:200]
        result[command.replace("-", "_")] = entry
    return jsonify(result)


@gdl_api.post("/tools/update")
def tools_update():
    if not _authed(): return _deny()
    data = request.get_json(silent=True) or request.form.to_dict()
    package = data.get("tool")
    if package not in ("gallery-dl", "yt-dlp"):
        return jsonify({"error": "unknown tool"}), 400
    running = [slug for slug in os.listdir(tasks_dir()) if _valid_slug(slug) and os.path.isdir(os.path.join(tasks_dir(), slug)) and _task_info(slug)["state"] == "running"]
    if _quick_running(): running.append("quick download")
    if running and data.get("force") not in (True, "1", "true"):
        return jsonify({"warning": True, "running": running}), 409
    try:
        proc = subprocess.run([sys.executable, "-m", "pip", "install", "--no-cache-dir", "--upgrade", package], capture_output=True, text=True, timeout=240)
        output = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
        if proc.returncode:
            return jsonify({"error": "Package update failed", "output": output[-4000:]}), 500
        _TOOL_CACHE.pop(package, None)
        return jsonify({"ok": True, "new_version": _tool_version(package), "output": output[-4000:]})
    except Exception as e:
        return jsonify({"error": str(e)[:300]}), 500


def _quick_running():
    return any(job.get("state") == "running" for job in _quick.values())


@gdl_api.get("/backup")
def backup_download():
    """Task definitions as a tar.gz (urls, commands, statuses — no media)."""
    if not _authed():
        return _deny()
    import io
    import tarfile

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        base = tasks_dir()
        for slug in sorted(os.listdir(base)):
            bp = os.path.join(base, slug)
            if not os.path.isdir(bp):
                continue
            for fn in ("urls.txt", "command.txt", "status.json", "cron.txt"):
                fp = os.path.join(bp, fn)
                if os.path.isfile(fp):
                    tar.add(fp, arcname=os.path.join(slug, fn))
    buf.seek(0)
    from flask import send_file

    return send_file(buf, mimetype="application/gzip",
                     as_attachment=True, download_name="gallerydl-tasks-backup.tar.gz")


@gdl_api.post("/backup/create")
def backup_create():
    if not _authed(): return _deny()
    data = request.get_json(silent=True) or {}
    slugs = data.get("slugs") if "slugs" in data else [s for s in os.listdir(tasks_dir()) if _valid_slug(s) and os.path.isdir(_task_base(s))]
    if not isinstance(slugs, list): return jsonify({"error": "slugs must be a list"}), 400
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
        for slug in slugs:
            base = _task_base(str(slug))
            if not base or not os.path.isdir(base): continue
            for root, dirs, files in os.walk(base):
                dirs[:] = [d for d in dirs if d not in ("__pycache__",)]
                for filename in files:
                    if filename in ("lock", "pid", "stopped"): continue
                    path = os.path.join(root, filename)
                    archive.write(path, "tasks/" + os.path.relpath(path, tasks_dir()).replace(os.sep, "/"))
        if data.get("include_config"):
            for filename in ("gallery-dl.conf", "task_concurrent_max.txt", "kiosk_defaults.json", "kiosks.json"):
                path = os.path.join(config_dir(), filename)
                if os.path.isfile(path): archive.write(path, "config/" + filename)
        if data.get("include_kiosks", True):
            root = os.path.join(config_dir(), "kiosks")
            for folder, _, files in os.walk(root) if os.path.isdir(root) else []:
                for filename in files:
                    path = os.path.join(folder, filename)
                    archive.write(path, "config/kiosks/" + os.path.relpath(path, root).replace(os.sep, "/"))
    buf.seek(0)
    return send_file(buf, mimetype="application/zip", as_attachment=True, download_name="gallerydl-backup.zip")


@gdl_api.post("/restore")
def backup_restore():
    """Restore a backup tar.gz into the tasks dir (skips existing slugs)."""
    if not _authed():
        return _deny()
    import io
    import tarfile

    if "file" not in request.files:
        return jsonify({"error": "file field required"}), 400
    try:
        data = request.files["file"].read(200 * 1024 * 1024 + 1)
        if len(data) > 200 * 1024 * 1024: return jsonify({"error": "backup exceeds 200 MB"}), 413
        restored, skipped = [], []
        try:
            archive = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile:
            archive = None
        if archive is not None:
            restored_kiosks = set(); restored_config = False
            for info in archive.infolist():
                name = info.filename.replace("\\", "/")
                parts = name.split("/")
                if name.startswith("/") or ".." in parts or info.is_dir(): continue
                dest = None
                if len(parts) >= 3 and parts[0] == "tasks" and _valid_slug(parts[1]):
                    dest = os.path.join(tasks_dir(), parts[1], *parts[2:]); restored.append(parts[1])
                elif len(parts) >= 4 and parts[:2] == ["config", "kiosks"] and _valid_slug(parts[2]):
                    dest = os.path.join(config_dir(), *parts[1:]); restored_kiosks.add(parts[2])
                elif len(parts) == 2 and parts[0] == "config" and _valid_slug(_slug(parts[1])):
                    dest = os.path.join(config_dir(), parts[1]); restored_config = True
                if not dest: continue
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with archive.open(info) as src, open(dest, "wb") as out: shutil.copyfileobj(src, out)
            for slug in set(restored):
                cron = _read_file_strip(os.path.join(tasks_dir(), slug, "cron.txt"))
                if cron:
                    try: _set_schedule(slug, cron)
                    except Exception: pass
            return jsonify({"ok": True, "restored": len(set(restored)), "kiosks": len(restored_kiosks), "config": restored_config, "skipped": 0})
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                parts = member.name.replace("\\", "/").split("/")
                if len(parts) != 2 or parts[0] in ("", ".", "..") or ".." in parts:
                    continue
                dest = os.path.join(tasks_dir(), parts[0], parts[1])
                if os.path.exists(dest):
                    skipped.append(member.name)
                    continue
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "wb") as f:
                    f.write(tar.extractfile(member).read())
                restored.append(member.name)
        return jsonify({"ok": True, "restored": len(restored), "skipped": len(skipped)})
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 400


@gdl_api.get("/media/recent")
def media_recent():
    """Newest image/video files under the downloads dir (filmstrip).

    The disk walk is cached for a few seconds; files written by a task that is
    still running are merged straight from its log so the filmstrip grows while
    the scrape is in progress instead of waiting for the next walk.
    """
    if not _authed():
        return _deny()
    scan = _scan_downloads(12)
    media = list(scan["media"])
    seen = {item.get("rel") for item in media}
    with _lock:
        running = [slug for slug, proc in _procs.items() if proc.poll() is None]
    for slug in running:
        base = _task_base(slug)
        if not base or not os.path.isdir(base):
            continue
        log_path = os.path.join(base, "stdout.log")
        if not os.path.isfile(log_path):
            log_path = os.path.join(base, "logs.txt")
        for item in _recent_from_log(log_path, 12):
            if item["rel"] in seen:
                continue
            seen.add(item["rel"])
            media.append({key: item[key] for key in ("name", "rel", "size", "mtime")})
    media.sort(key=lambda item: item.get("mtime") or 0, reverse=True)
    return jsonify({"dir": downloads_dir(), "media": media[:12]})


@gdl_api.get("/media/file")
def media_file():
    """Serve a downloads-dir file (thumbnails). Path-jail enforced."""
    if not _authed():
        return _deny()
    from flask import send_file

    rel = request.args.get("path", "")
    root = os.path.realpath(downloads_dir())
    cand = os.path.realpath(os.path.join(root, rel))
    if os.path.commonpath((root, cand)) != root or not os.path.isfile(cand):
        return jsonify({"error": "not found"}), 404
    return send_file(cand)


@gdl_api.get("/oauth")
def oauth_status():
    """List credential metadata and tasks with configured OAuth sites."""
    if not _authed():
        return _deny()
    profiles = []
    # cookie jars in the config dir
    try:
        for fn in sorted(os.listdir(config_dir())):
            if fn.startswith("cookies") and fn.endswith(".txt"):
                st = os.stat(os.path.join(config_dir(), fn))
                profiles.append({
                    "platform": fn.replace("cookies", "").replace(".txt", "").strip("-_ ") or "cookies",
                    "account": fn, "method": "Cookie jar",
                    "status": "valid" if st.st_mtime > time.time() - 20 * 86400 else "expiring",
                })
    except OSError:
        pass
    tasks = []
    for slug in sorted(os.listdir(tasks_dir())):
        base = _task_base(slug)
        if not base or not os.path.isdir(base): continue
        site = _read_file_strip(os.path.join(base, "oauth_site.txt"))
        if site in _OAUTH_SITES:
            tasks.append({"slug": slug, "name": _read_file_strip(os.path.join(base, "name.txt")) or _read_json(os.path.join(base, "status.json"), {}).get("name") or slug,
                          "site": site, "site_label": _OAUTH_SITES[site],
                          "authenticated": _cache_has_token(os.path.join(base, "gallery-dl-cache.sqlite3")),
                          "has_client_id": _oauth_client_id_configured(site)})
    with _oauth_lock:
        active = _oauth_task if _oauth_proc is not None and _oauth_proc.poll() is None else ""
    return jsonify({"profiles": profiles, "tasks": tasks, "active_task": active})


def _oauth_client_id_configured(site: str) -> bool:
    try:
        config = json.loads(_read_file_strip(os.path.join(config_dir(), "gallery-dl.conf")) or "{}")
        return bool(config.get("extractor", {}).get(site, {}).get("client-id"))
    except Exception:
        return False


def _oauth_data():
    return request.get_json(silent=True) or request.form.to_dict()


@gdl_api.post("/oauth/cookies")
def oauth_save_cookies():
    if not _authed(): return _deny()
    data = _oauth_data()
    platform = re.sub(r"[^a-z0-9_-]", "", str(data.get("platform") or "").lower())
    content = data.get("cookies") or ""
    if platform not in _OAUTH_SITES or not isinstance(content, str) or not content.strip():
        return jsonify({"error": "Select a supported platform and provide a cookies.txt export"}), 400
    first = content.lstrip().splitlines()[0]
    if not first.startswith("#") and "\t" not in first:
        return jsonify({"error": "This does not look like a Netscape-format cookie file"}), 400
    path = os.path.join(config_dir(), f"cookies-{platform}.txt")
    Path(path).write_text(content, encoding="utf-8")
    return jsonify({"ok": True, "file": os.path.basename(path)})


@gdl_api.delete("/oauth/cookies/<filename>")
def oauth_delete_cookies(filename: str):
    if not _authed(): return _deny()
    if not re.fullmatch(r"cookies-[a-z0-9_-]+\.txt", filename): return jsonify({"error": "invalid profile"}), 400
    try: os.remove(os.path.join(config_dir(), filename))
    except FileNotFoundError: return jsonify({"error": "profile not found"}), 404
    return jsonify({"ok": True})


def _oauth_log_path():
    return os.path.join(config_dir(), ".oauth_run.log")


def _redact_oauth_log(content: str) -> str:
    content = re.sub(r"(?i)(access[_ -]?token|refresh[_ -]?token|client[_ -]?secret|password|PHPSESSID|code)=([^\s&]+)", r"\1=[redacted]", content)
    return re.sub(r"(?i)(authorization:\s*bearer\s+)[^\s]+", r"\1[redacted]", content)


@gdl_api.post("/oauth/start")
def oauth_start():
    global _oauth_proc, _oauth_task, _oauth_site
    if not _authed(): return _deny()
    data = _oauth_data()
    slug = str(data.get("task_slug") or "")
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "task not found"}), 404
    site = _read_file_strip(os.path.join(base, "oauth_site.txt"))
    if site not in _OAUTH_SITES: return jsonify({"error": "task has no valid OAuth site"}), 400
    with _oauth_lock:
        if _oauth_proc is not None and _oauth_proc.poll() is None:
            return jsonify({"error": f"OAuth for task '{_oauth_task}' is already running"}), 409
        cache_path = os.path.join(base, "gallery-dl-cache.sqlite3")
        cmd = ["gallery-dl", "--config", os.path.join(config_dir(), "gallery-dl.conf"), f"oauth:{site}", "--cache-file", cache_path]
        if site in _OAUTH_STDIN_SITES: cmd += ["-o", "input=true"]
        env = os.environ.copy(); env["PATH"] = env.get("PATH", "") + os.pathsep + "/usr/local/bin"
        try:
            log = open(_oauth_log_path(), "w", encoding="utf-8")
            log.write(f"OAuth: {_OAUTH_SITES[site]} · {slug}\n$ {' '.join(shlex.quote(x) for x in cmd)}\n\n"); log.flush()
            _oauth_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT, text=True, env=env, cwd=base)
            _oauth_task, _oauth_site = slug, site
        except Exception as e:
            return jsonify({"error": str(e)[:300]}), 500
    return jsonify({"ok": True, "task_slug": slug, "site": site})


@gdl_api.get("/oauth/log")
def oauth_log():
    if not _authed(): return _deny()
    with _oauth_lock: running = bool(_oauth_proc is not None and _oauth_proc.poll() is None)
    content = _redact_oauth_log(_read_file_strip(_oauth_log_path()))
    return jsonify({"running": running, "task_slug": _oauth_task, "content": content})


@gdl_api.post("/oauth/paste")
def oauth_paste():
    if not _authed(): return _deny()
    data = _oauth_data(); raw = str(data.get("value") or "").strip()
    with _oauth_lock:
        proc, site = _oauth_proc, _oauth_site
        running = bool(proc is not None and proc.poll() is None)
    if not running: return jsonify({"error": "No OAuth handshake is running"}), 400
    if site in _OAUTH_STDIN_SITES:
        if not raw: return jsonify({"error": "Paste the callback code first"}), 400
        try: proc.stdin.write(raw + "\n"); proc.stdin.flush()
        except Exception as e: return jsonify({"error": str(e)[:250]}), 500
        return jsonify({"ok": True})
    if "://" in raw: raw = raw.split("?", 1)[1] if "?" in raw else ""
    elif raw.startswith("?"): raw = raw[1:]
    if "code=" not in raw: return jsonify({"error": "Paste the failed redirect URL or its query string containing code="}), 400
    try:
        with urlrequest.urlopen("http://127.0.0.1:6414/?" + raw, timeout=15) as resp: resp.read()
        return jsonify({"ok": True})
    except http.client.RemoteDisconnected:
        return jsonify({"ok": True, "note": "Callback delivered; check the log"})
    except urlerror.URLError as e:
        return jsonify({"error": f"Could not deliver callback: {getattr(e, 'reason', e)}"}), 502


@gdl_api.post("/oauth/stop")
def oauth_stop():
    global _oauth_task, _oauth_site
    if not _authed(): return _deny()
    with _oauth_lock:
        proc = _oauth_proc
        if proc is not None and proc.poll() is None:
            try: proc.terminate(); proc.wait(timeout=5)
            except subprocess.TimeoutExpired: proc.kill()
            except Exception: pass
        _oauth_task, _oauth_site = "", ""
    _append_log(_oauth_log_path(), "[Stopped by user]")
    return jsonify({"ok": True})


@gdl_api.post("/oauth/reset")
def oauth_reset():
    if not _authed(): return _deny()
    slug = str(_oauth_data().get("task_slug") or "")
    base = _task_base(slug)
    if not base or not os.path.isdir(base): return jsonify({"error": "task not found"}), 404
    with _oauth_lock:
        if _oauth_task == slug and _oauth_proc is not None and _oauth_proc.poll() is None:
            return jsonify({"error": "stop the active OAuth handshake first"}), 409
    try: os.remove(os.path.join(base, "gallery-dl-cache.sqlite3"))
    except FileNotFoundError: pass
    return jsonify({"ok": True})
