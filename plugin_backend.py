"""SDCodex-GalleryDL slim JSON API (Stitch frontend, no OldCode templates).

Task storage mirrors OldCode artillery layout so existing task folders keep
working: <TASKS>/<slug>/{urls.txt, command.txt, status.json, stdout.log}.
"""

import json
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
import shlex

from flask import Blueprint, jsonify, request

gdl_api = Blueprint("gdl_api", __name__, url_prefix="/gdl/api")

_procs: dict = {}          # slug -> Popen (task runs)
_quick: dict = {}          # job_id -> {state, log file, proc, ...}
_lock = threading.Lock()
_scheduler = None


def _init_scheduler():
    """Start cron jobs declared by task folders, preserving the task layout."""
    global _scheduler
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

        u = core_auth.current_user()
        return u is not None
    except Exception:
        return False


def _deny():
    return jsonify({"error": "unauthorized"}), 401


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def tasks_dir() -> str:
    d = _env("TASKS", "/data/tasks")
    os.makedirs(d, exist_ok=True)
    return d


def config_dir() -> str:
    d = _env("CONFIG", "/data/config")
    os.makedirs(d, exist_ok=True)
    return d


def downloads_dir() -> str:
    return _env("IMAGES", "/data/downloads")


def _slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (name or "task").lower()).strip("-")
    return s or f"task-{int(time.time())}"


def _read_json(path: str, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, data) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


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


def _tool_version(cmd: str) -> str:
    try:
        r = subprocess.run([cmd, "--version"], capture_output=True, text=True, timeout=15)
        out = (r.stdout or r.stderr or "").strip().splitlines()
        return out[0].strip() if out else "missing"
    except Exception:
        return "missing"


def _is_running(pid) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


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
    state = st.get("state", "idle")
    if alive:
        state = "running"
    elif state == "running":
        state = "stopped"
    # progress: last NN% seen in the log tail.
    pct = None
    for line in reversed(_tail(os.path.join(base, "stdout.log"), 60)):
        m = re.search(r"(\d{1,3})%", line)
        if m:
            try:
                pct = max(0, min(100, int(m.group(1))))
                break
            except ValueError:
                pass
    schedule = _read_file_strip(os.path.join(base, "cron.txt"))
    return {
        "slug": slug,
        "name": st.get("name") or _read_file_strip(os.path.join(base, "name.txt")) or slug,
        "urls": len(urls),
        "state": state,
        "schedule": schedule,
        "progress": pct,
        "started_at": st.get("started_at"),
        "finished_at": st.get("finished_at"),
        "last_error": st.get("last_error"),
        "platform": _guess_platform(urls[0] if urls else ""),
    }


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


def _run_task(slug: str) -> None:
    """Worker: gallery-dl --input-file urls.txt, streaming into stdout.log."""
    base = os.path.join(tasks_dir(), slug)
    log_path = os.path.join(base, "stdout.log")
    st_path = os.path.join(base, "status.json")
    st = _read_json(st_path, {})
    st.update({"state": "running", "started_at": _now(), "finished_at": None, "last_error": None})
    _write_json(st_path, st)
    try:
        cmd = shlex.split(st.get("command") or "gallery-dl --input-file urls.txt")
    except ValueError as e:
        st.update({"state": "error", "finished_at": _now(), "last_error": str(e)[:300]})
        _write_json(st_path, st)
        return
    _append_log(log_path, f"[launcher] $ {' '.join(cmd)}")
    try:
        proc = subprocess.Popen(
            cmd, cwd=base, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
    except Exception as e:
        st.update({"state": "error", "finished_at": _now(), "last_error": str(e)[:300]})
        _write_json(st_path, st)
        _append_log(log_path, f"[launcher] spawn failed: {e}")
        return
    with _lock:
        _procs[slug] = proc
    try:
        assert proc.stdout is not None
        with open(log_path, "a", encoding="utf-8") as lf:
            for line in proc.stdout:
                lf.write(line)
        rc = proc.wait()
        st = _read_json(st_path, {})
        if rc == 0:
            st.update({"state": "done", "finished_at": _now()})
        elif st.get("state") == "running":
            st.update({"state": "error", "finished_at": _now(),
                       "last_error": f"exit code {rc}"})
        _write_json(st_path, st)
    finally:
        with _lock:
            _procs.pop(slug, None)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


# ------------------------------------------------------------- routes ---

@gdl_api.get("/engines")
def engines():
    if not _authed():
        return _deny()
    return jsonify({
        "gallery_dl": _tool_version("gallery-dl"),
        "yt_dlp": _tool_version("yt-dlp"),
    })


@gdl_api.get("/tasks")
def tasks_list():
    if not _authed():
        return _deny()
    out = []
    for slug in sorted(os.listdir(tasks_dir())):
        if os.path.isdir(os.path.join(tasks_dir(), slug)):
            out.append(_task_info(slug))
    return jsonify({"tasks": out})


@gdl_api.post("/tasks")
def tasks_create():
    if not _authed():
        return _deny()
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip() or "untitled"
    urls = data.get("urls") or []
    if isinstance(urls, str):
        urls = [urls]
    urls = [u.strip() for u in urls if u and u.strip()]
    if not urls:
        return jsonify({"error": "at least one URL required"}), 400
    slug = _slug(data.get("slug") or name)
    base = os.path.join(tasks_dir(), slug)
    if os.path.exists(base):
        slug = f"{slug}-{int(time.time())}"
        base = os.path.join(tasks_dir(), slug)
    os.makedirs(base)
    with open(os.path.join(base, "urls.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(urls) + "\n")
    with open(os.path.join(base, "command.txt"), "w", encoding="utf-8") as f:
        f.write((data.get("command") or "gallery-dl --input-file urls.txt") + "\n")
    _write_json(os.path.join(base, "status.json"), {
        "name": name, "state": "idle", "command": data.get("command") or "gallery-dl --input-file urls.txt",
    })
    open(os.path.join(base, "stdout.log"), "a").close()
    schedule = (data.get("schedule") or "").strip()
    if schedule:
        try:
            _set_schedule(slug, schedule)
        except Exception as e:
            shutil.rmtree(base, ignore_errors=True)
            return jsonify({"error": f"invalid or unavailable schedule: {str(e)[:200]}"}), 400
    if data.get("start"):
        threading.Thread(target=_run_task, args=(slug,), daemon=True).start()
    return jsonify({"ok": True, "task": _task_info(slug)})


@gdl_api.post("/tasks/<slug>/start")
def task_start(slug: str):
    if not _authed():
        return _deny()
    base = os.path.join(tasks_dir(), slug)
    if not os.path.isdir(base):
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


@gdl_api.post("/tasks/<slug>/stop")
def task_stop(slug: str):
    if not _authed():
        return _deny()
    with _lock:
        proc = _procs.get(slug)
    if proc is None or proc.poll() is not None:
        return jsonify({"ok": True, "state": "stopped"})
    try:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        st_path = os.path.join(tasks_dir(), slug, "status.json")
        st = _read_json(st_path, {})
        st.update({"state": "stopped", "finished_at": _now()})
        _write_json(st_path, st)
        _append_log(os.path.join(tasks_dir(), slug, "stdout.log"), "[launcher] stopped by user")
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 500
    return jsonify({"ok": True, "state": "stopped"})


@gdl_api.delete("/tasks/<slug>")
def task_delete(slug: str):
    if not _authed():
        return _deny()
    base = os.path.join(tasks_dir(), slug)
    if not os.path.isdir(base):
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
def task_log(slug: str):
    if not _authed():
        return _deny()
    lines = int(request.args.get("lines", 200))
    lines = max(1, min(lines, 2000))
    return jsonify({
        "slug": slug,
        "lines": _tail(os.path.join(tasks_dir(), slug, "stdout.log"), lines),
    })


@gdl_api.post("/quick")
def quick_start():
    """One-shot download in the background. Returns a job id to poll."""
    if not _authed():
        return _deny()
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "url required"}), 400
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

    def _run():
        _quick[job_id] = {"state": "running", "cmd": cmd, "started_at": _now()}
        try:
            with open(log_path, "w", encoding="utf-8") as lf:
                lf.write(f"$ {' '.join(cmd)}\n")
                lf.flush()
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        text=True, bufsize=1)
                _quick[job_id]["proc"] = proc
                assert proc.stdout is not None
                for line in proc.stdout:
                    lf.write(line)
                rc = proc.wait()
                _quick[job_id].update({"state": "done" if rc == 0 else "error",
                                       "finished_at": _now(), "exit": rc})
        except Exception as e:
            _quick[job_id].update({"state": "error", "last_error": str(e)[:300]})

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "job": job_id, "cmd": cmd})


@gdl_api.get("/quick/<job_id>")
def quick_status(job_id: str):
    if not _authed():
        return _deny()
    job = _quick.get(job_id)
    if not job:
        return jsonify({"error": "unknown job"}), 404
    out = {k: v for k, v in job.items() if k != "proc"}
    out["log"] = _tail(os.path.join(tasks_dir(), f".quick-{job_id}.log"), 120)
    return jsonify(out)


@gdl_api.post("/quick/<job_id>/stop")
def quick_stop(job_id: str):
    if not _authed():
        return _deny()
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
    return jsonify({"ok": True, "state": job.get("state")})


@gdl_api.get("/telemetry")
def telemetry():
    if not _authed():
        return _deny()
    def du(path: str) -> int:
        total = 0
        try:
            for dp, _, fns in os.walk(path):
                for fn in fns:
                    try:
                        total += os.path.getsize(os.path.join(dp, fn))
                    except OSError:
                        pass
        except OSError:
            pass
        return total

    images = du(downloads_dir())
    meta = du(tasks_dir())
    return jsonify({
        "images_bytes": images,
        "tasks_bytes": meta,
        "images_gb": round(images / 1e9, 1),
        "tasks_gb": round(meta / 1e9, 1),
        "images_dir": downloads_dir(),
        "tasks_dir": tasks_dir(),
    })


@gdl_api.get("/kiosks")
def kiosks_list():
    if not _authed():
        return _deny()
    kiosks = _read_json(os.path.join(config_dir(), "kiosks.json"), [])
    return jsonify({"kiosks": kiosks if isinstance(kiosks, list) else []})


@gdl_api.post("/kiosks")
def kiosks_create():
    if not _authed():
        return _deny()
    data = request.get_json(force=True, silent=True) or {}
    path = os.path.join(config_dir(), "kiosks.json")
    kiosks = _read_json(path, [])
    if not isinstance(kiosks, list):
        kiosks = []
    k = {
        "id": _slug(data.get("name") or "kiosk"),
        "name": data.get("name") or "Untitled kiosk",
        "folder": data.get("folder") or downloads_dir(),
        "interval": int(data.get("interval") or 10),
        "shuffle": bool(data.get("shuffle", True)),
        "blur_nsfw": bool(data.get("blur_nsfw", True)),
        "live": False,
    }
    kiosks.append(k)
    _write_json(path, kiosks)
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
    return jsonify({"error": "unknown kiosk"}), 404


@gdl_api.delete("/kiosks/<kid>")
def kiosk_delete(kid: str):
    if not _authed():
        return _deny()
    path = os.path.join(config_dir(), "kiosks.json")
    kiosks = [k for k in _read_json(path, []) if k.get("id") != kid]
    _write_json(path, kiosks)
    return jsonify({"ok": True})


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
        content = '{\n  "extractor": {\n    "filename": "{category}/{artist}_{id}_{title[:30]}",\n    "keywords": "images"\n  }\n}\n'
    return jsonify({"path": path, "content": content})


@gdl_api.post("/config")
def config_write():
    if not _authed():
        return _deny()
    data = request.get_json(force=True, silent=True) or {}
    content = data.get("content", "")
    if not isinstance(content, str) or len(content) > 200_000:
        return jsonify({"error": "bad content"}), 400
    # gallery-dl.conf is a plain-text INI-style config (not JSON). Accept both:
    # if the client sends JSON-structured content, keep it as-is; otherwise
    # treat the raw text directly so existing .conf files work.
    path = os.path.join(config_dir(), "gallery-dl.conf")
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(content))
    return jsonify({"ok": True, "path": path})


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
        data = request.files["file"].read()
        restored, skipped = [], []
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
    """Newest image/video files under the downloads dir (filmstrip)."""
    if not _authed():
        return _deny()
    exts = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".webm")
    found = []
    root = downloads_dir()
    try:
        for dp, _, fns in os.walk(root):
            for fn in fns:
                if fn.lower().endswith(exts):
                    fp = os.path.join(dp, fn)
                    try:
                        st = os.stat(fp)
                        found.append({
                            "name": fn,
                            "rel": os.path.relpath(fp, root),
                            "size": st.st_size,
                            "mtime": st.st_mtime,
                        })
                    except OSError:
                        pass
    except OSError:
        pass
    found.sort(key=lambda m: m["mtime"], reverse=True)
    return jsonify({"dir": root, "media": found[:12]})


@gdl_api.get("/media/file")
def media_file():
    """Serve a downloads-dir file (thumbnails). Path-jail enforced."""
    if not _authed():
        return _deny()
    from flask import send_file

    rel = request.args.get("path", "")
    root = os.path.abspath(downloads_dir())
    cand = os.path.abspath(os.path.join(root, rel))
    if not cand.startswith(root + os.sep) or not os.path.isfile(cand):
        return jsonify({"error": "not found"}), 404
    return send_file(cand)


@gdl_api.get("/oauth")
def oauth_status():
    """Profile presence only — secrets never leave the server."""
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
    # civitai key presence (core setting, value never exposed)
    try:
        from sdcodex import auth as core_auth  # noqa

        profiles.append({"platform": "Civitai", "account": "workstation key",
                         "method": "API key", "status": "unknown"})
    except Exception:
        pass
    return jsonify({"profiles": profiles})
