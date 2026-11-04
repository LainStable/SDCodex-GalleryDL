# GalleryDL Plugin

## Overview

The **GalleryDL** plugin drives the external `gallery-dl` (and `yt-dlp`) tools
from SDCodex. It manages download tasks with schedules, cookies and OAuth
handshakes, streams their logs, exposes a telemetry/filmstrip view of the
downloads directory, and publishes a public "kiosk" slideshow for a second
screen.

The plugin is split across two Python modules:

- `plugin.py` — the plugin entry point. Loads the backend under a
  plugin-private module name and registers the blueprint.
- `plugin_backend.py` — the whole backend: task CRUD, the scheduler, the
  process runner, the log tailer, the OAuth flow, the config editor, the
  tool updater, the backup / restore, the telemetry and the kiosks.

## Interface

### `plugin.py`

- `init_plugin(app, db, plugin_info=None)` — loads the backend, registers the
  `gdl_api` blueprint once and calls `backend._init_scheduler()`; returns `True`.
- `_load_backend` — loads `plugin_backend.py` under the plugin-private module
  name `_BACKEND_NAME` (`_sdcodex_gallerydl_backend`) via
  `importlib.util.spec_from_file_location` and caches it in `sys.modules`.

### `plugin_backend.py` — blueprint `gdl_api` (url_prefix `/gdl/api`)

Tasks and scheduling:

- `engines` — `GET /engines`.
- `queue_status` — `GET /queue`.
- `settings_get` — `GET /settings`.
- `settings_put` — `PUT /settings`.
- `tasks_list` — `GET /tasks`.
- `tasks_create` — `POST /tasks`.
- `task_start` — `POST /tasks/<slug>/start`.
- `task_update` — `PUT /tasks/<slug>`.
- `task_action` — `POST /tasks/<slug>/action` (actions `run`, `stop`, `pause`,
  `duplicate`, `clear_logs` / `clear-log`, `delete_archive`, `delete_cookies`).
- `tasks_bulk_action` — `POST /tasks/actions` (actions `pause_all`, `clear_completed`).
- `task_stop` — `POST /tasks/<slug>/stop`.
- `task_delete` — `DELETE /tasks/<slug>`.
- `task_log` — `GET /tasks/<slug>/log` and `GET /tasks/<slug>/logs`.
- `task_urls` — `GET /tasks/<slug>/urls`.
- `task_urls_update` — `PUT /tasks/<slug>/urls`.
- `stop_running_task` — `POST /task/stop`.
- `plugin_task_status` — `GET /task`.

Logs and history:

- `task_history` — `GET /tasks/<slug>/history`.
- `task_recent` — `GET /tasks/<slug>/recent`.
- `task_archived_logs` — `GET /tasks/<slug>/logs/archived`.
- `task_log_download` — `GET /tasks/<slug>/logs/download`.
- `task_archived_log_download` — `GET /tasks/<slug>/logs/archive/<filename>`.
- `task_log_stream` — `GET /tasks/<slug>/logs/stream` (SSE).

Quick and one-time downloads:

- `quick_start` — `POST /quick`.
- `quick_status` — `GET /quick/<job_id>`.
- `quick_list` — `GET /quick`.
- `quick_stop` — `POST /quick/<job_id>/stop`.
- `one_time_status` — `GET /one-time/status`.
- `one_time_logs` — `GET /one-time/logs`.
- `one_time_status_running` — a plain helper returning the running flag; it is
  not a route. It is called from `one_time_logs`, and the running flag is also
  exposed as the `running` field of `GET /one-time/status`.
- `one_time_log_download` — `GET /one-time/logs/download`.
- `one_time_log_clear` — `POST /one-time/clear-logs`.
- `one_time_stop` — `POST /one-time/stop`.
- `one_time_recent` — `GET /one-time/recent`.

Telemetry, kiosks, config, tools, backup, media and OAuth:

- `telemetry` — `GET /telemetry`.
- `kiosks_list` — `GET /kiosks`.
- `kiosks_create` — `POST /kiosks`.
- `kiosk_toggle` — `POST /kiosks/<kid>/toggle`.
- `kiosk_update` — `PUT /kiosks/<kid>`.
- `kiosk_upload_images` — `POST /kiosks/<kid>/images`.
- `kiosk_remove_image` — `DELETE /kiosks/<kid>/images/<filename>`.
- `kiosk_images_admin` — `GET /kiosks/<kid>/images`.
- `kiosk_public_images` — `GET /kiosk/<kid>/images`.
- `kiosk_public_media` — `GET /kiosk/<kid>/media/<path:filename>`.
- `kiosk_public_view` — `GET /kiosk/<kid>/view` and `GET /kiosk/<kid>`.
- `kiosk_manifest` — `GET /kiosk/<kid>/manifest.json`.
- `kiosk_delete` — `DELETE /kiosks/<kid>`.
- `kiosk_defaults_get` — `GET /kiosks/defaults`.
- `kiosk_defaults_put` — `PUT /kiosks/defaults`.
- `config_read` — `GET /config`.
- `config_write` — `POST /config`.
- `config_reset` — `POST /config/reset`.
- `config_check_update` — `GET /config/check-update`.
- `config_apply_update` — `POST /config/apply-update`.
- `tools_check_update` — `GET /tools/check-update`.
- `tools_update` — `POST /tools/update`.
- `backup_download` — `GET /backup`.
- `backup_create` — `POST /backup/create`.
- `backup_restore` — `POST /restore`.
- `media_recent` — `GET /media/recent`.
- `media_file` — `GET /media/file?path=`.
- `oauth_status` — `GET /oauth`.
- `oauth_save_cookies` — `POST /oauth/cookies`.
- `oauth_delete_cookies` — `DELETE /oauth/cookies/<filename>`.
- `oauth_start` — `POST /oauth/start`.
- `oauth_log` — `GET /oauth/log`.
- `oauth_paste` — `POST /oauth/paste`.
- `oauth_stop` — `POST /oauth/stop`.
- `oauth_reset` — `POST /oauth/reset`.

Public helpers and constants:

- `tasks_dir`, `config_dir`, `downloads_dir` — the resolved storage roots.
- `_task_info` — the canonical task record serialized by the list / create /
  update routes.
- `_init_scheduler`, `_set_schedule` — the APScheduler integration.
- `_save_task`, `_run_task`, `_slug`, `_valid_slug` — the task-store internals.
- `_OAUTH_SITES` — the supported platforms (deviantart, reddit, tumblr, flickr,
  mastodon, pixiv).
- `_OAUTH_STDIN_SITES` — the set of sites whose code is fed over stdin (pixiv).
- `_PUBLIC_GDL_PREFIXES` — `("/gdl/api/kiosk/",)`, the only unauthenticated route prefix.
- `_MEDIA_EXTS` / `_IMAGE_EXTS` — the media and image extension sets used for
  the downloads filmstrip.
- `_DEFAULT_CONFIG_URL` — the env-overridable `GALLERYDL_DEFAULT_CONFIG_URL`;
  `_DEFAULT_CONFIG_FILE` — the bundled `defaults/gallery-dl.conf`.
- `_TELEMETRY_TTL` (10.0 s) — the downloads-scan cache TTL; `_AUX_TTL` (60.0 s) —
  the task-aux cache TTL.

### Subdirectories

- `defaults/` — the bundled `gallery-dl.conf` shipped with the plugin.
- `pages/` — the static plugin pages (`tasks.html`, `logs.html`, `oauth.html`,
  `one-time.html`, `kiosks.html`, `settings.html`).

## Invariants

- `plugin_backend.py:_gdl_auth_guard` — the `before_request` hook on `gdl_api`
  rejects every `/gdl/api` request with 401 unless `_authed()`, except for the
  paths starting with an entry of `_PUBLIC_GDL_PREFIXES`.
- `plugin_backend.py:_PUBLIC_GDL_PREFIXES` — only `/gdl/api/kiosk/` is public
  (the kiosk display runs on an unauthenticated TV); the task logs and settings
  reads are not public.
- `plugin_backend.py:_authed` — returns `True` only when the core auth is
  unavailable (a standalone deployment); otherwise it requires
  `core_auth.current_user() is not None`, and any exception counts as unauthenticated.
- `plugin_backend.py:engines` — every route additionally re-checks `_authed()`
  and returns `_deny()` (401); the guard is duplicated inside each handler, not
  only in `before_request`.
- `plugin_backend.py:_valid_slug` — the slug must fully match
  `[a-z0-9][a-z0-9-]{0,119}`.
- `plugin_backend.py:_task_base` — returns `None` unless the slug is valid and
  `realpath(tasks_dir()/slug)` is contained in `realpath(tasks_dir())` (an
  `os.path.commonpath` check), which blocks path traversal.
- `plugin_backend.py:task_update` / `task_start` / `task_stop` / `task_delete` /
  `task_log` / `task_urls` / `task_urls_update` — each returns 404 when
  `_task_base(slug)` is `None` or the directory is missing.
- `plugin_backend.py:_pid_belongs_to` — a pid is only signalled when it is a
  positive int, alive (`os.kill(pid, 0)`) and its `/proc/<pid>/cwd` realpath
  equals the task-folder realpath (a PID-recycling guard); on non-Linux it falls
  back to the liveness check.
- `plugin_backend.py:_run_task` — single instance per task: an
  `O_CREAT|O_EXCL` `lock` file, plus an early return if `_procs[slug]` is still
  alive, plus a skip when a `paused` marker exists.
- `plugin_backend.py:_run_task` — a stale lock is reclaimed only when its `pid`
  file is empty and the lock is older than 60 s, or when the recorded pid no
  longer belongs to the task folder; otherwise the run is skipped.
- `plugin_backend.py:_run_task` — the concurrency semaphore slot (`_slots`) is
  released on every failure path (empty command, bad `shlex` split, missing
  `urls.txt`, spawn failure) as well as in the `finally`.
- `plugin_backend.py:task_start` — returns `{ok: true, state: running}` instead
  of launching a second process when `_procs[slug]` is still alive.
- `plugin_backend.py:settings_put` — the concurrency is clamped to 1..64 and is
  rejected with 409 while any task is running or queued; invalid values return 400.
- `plugin_backend.py:_init_scheduler` — the persisted concurrency is clamped
  with `max(1, min(64, saved))`; the jobs use `max_instances=1`, `coalesce=True`
  and `CronTrigger.from_crontab(expr, timezone="UTC")`; an invalid crontab is
  skipped rather than raising.
- `plugin_backend.py:_set_schedule` — an empty expression removes the job
  (ignoring errors) and deletes `cron.txt`; a non-empty one raises
  `RuntimeError("Scheduled tasks require APScheduler…")` when APScheduler is missing.
- `plugin_backend.py:_save_task` — it requires a non-empty name; a rename fails
  with a `FileExistsError` if the target slug exists; creating over an existing
  folder without `original_slug` also raises; it requires at least one URL or an
  existing `urls.txt` unless `keep_existing_urls`; the command must pass
  `shlex.split`; `timeout` must be all digits.
- `plugin_backend.py:_save_task` — a `cookie_file` / `cookie_path` is only
  copied when `os.path.realpath(config_dir()/basename(cookie))` exists, so the
  cookie paths are constrained to the plugin config directory.
- `plugin_backend.py:tasks_create` — the uploaded URL file is capped at 10 MB (413)
  and the cookie file at 1 MB (413).
- `plugin_backend.py:tasks_create` — a task is only auto-started when `start` is
  the boolean `True` or one of `1/true/on/yes` (the string `"false"` from a
  `FormData` body must not launch).
- `plugin_backend.py:tasks_create` — `schedule_mode == "manual"` forces an empty schedule.
- `plugin_backend.py:task_log` — `lines` is clamped to 1..2000.
- `plugin_backend.py:task_urls_update` — `content` must be a `str` of at most
  10 MB, else 400.
- `plugin_backend.py:task_action` — `pause` toggles both the `paused` marker file
  and the membership in `_paused_tasks`; unknown actions return 400.
- `plugin_backend.py:tasks_bulk_action` — `clear_completed` deletes only the
  tasks whose `_task_info` state is `done` / `stopped` / `error`, and removes
  their schedule first.
- `plugin_backend.py:task_delete` — it removes the cron schedule before killing
  the process and `shutil.rmtree`-ing the folder.
- `plugin_backend.py:plugin_task_status` — it must not call `_task_info` while
  holding `_lock` (a documented deadlock: `_task_info` re-acquires it).
- `plugin_backend.py:_write_json` — it writes to a per-pid / per-thread temp
  file and then `os.replace`s it, so concurrent status writers cannot lose the
  file; the temp file is always removed.
- `plugin_backend.py:_task_info` — the CWD / PID-derived `state` is overridden
  to `running` when the process is alive and downgraded to `stopped` when it
  claims to be running but is dead.
- `plugin_backend.py:_scan_downloads` — only one walk runs at a time
  (`_scan_lock.acquire(blocking=False)`); concurrent callers get the previous
  snapshot (or a zeroed placeholder), and the results are cached for `_TELEMETRY_TTL`.
- `plugin_backend.py:_tool_version` — the version probes are cached in
  `_TOOL_CACHE` (never re-spawned per page poll); failures resolve to `"missing"`.
- `plugin_backend.py:_last_url_marker` — it reads at most 8 MB backwards from the log end.
- `plugin.py:init_plugin` — the blueprint is registered only when
  `"gdl_api" not in app.blueprints` (an idempotent init).
- `plugin_backend.py:kiosk_public_media` — the kiosk media serving rejects the
  paths whose realpath escapes the kiosk images root or that are not a file.
- `plugin_backend.py:media_file` — `/media/file` enforces a realpath jail in
  `downloads_dir` plus a file-existence check.
- `plugin_backend.py:kiosk_remove_image` — the kiosk image deletion rejects the
  filenames that are not a bare basename.
- `plugin_backend.py:kiosk_upload_images` — the kiosk uploads sanitize the
  filenames, accept only `_IMAGE_EXTS` and skip the files over 20 MB.
- `plugin_backend.py:kiosk_update` / `kiosk_defaults_put` / `kiosk_public_view` —
  the kiosk interval is clamped to 1..3600; the background / fit / transition
  values are whitelisted; the kiosk name is HTML-escaped.
- `plugin_backend.py:kiosks_create` — a duplicate slug is rejected with 409;
  `kiosk_toggle` only re-registers an existing valid kiosk folder.
- `plugin_backend.py:kiosk_delete` — always rewrites `kiosks.json` and only
  `rmtree`s the folder when the slug is valid.
- `plugin_backend.py:quick_status` / `quick_stop` — the quick job ids must
  match `[a-f0-9]{12}`.
- `plugin_backend.py:quick_start` — only one one-time / quick download may run;
  it returns 409 while `_one_time_proc` is alive under `_one_time_lock`.
- `plugin_backend.py:quick_status` / `quick_list` — a persisted quick job still
  marked running but absent from the in-memory `_quick` is rewritten to the state `stopped`.
- `plugin_backend.py:quick_start` — it requires a `url`, and requires a
  `task_name` when `also_add_task` is set (400 otherwise).
- `plugin_backend.py:tools_update` — it only accepts `gallery-dl` / `yt-dlp` and
  refuses while tasks or quick jobs run unless `force` is `True` (409 with the
  running list).
- `plugin_backend.py:config_write` — it rejects a non-string or >200 000-character
  content and non-empty content that is not valid JSON.
- `plugin_backend.py:config_reset` — it fetches the remote default, validates it
  as JSON, else falls back to `_DEFAULT_CONFIG_FILE`, and returns 502 if nothing
  is available.
- `plugin_backend.py:config_check_update` / `config_apply_update` — the remote is
  authoritative for new keys only; `apply_update` backs up the existing config
  before the merge.
- `plugin_backend.py:backup_restore` — it caps the uploads at 200 MB, rejects
  the absolute or `..` archive paths, requires a valid slug segment and skips
  the existing tar members.
- `plugin_backend.py:task_archived_log_download` / `task_archived_logs` — the
  archived log filenames must match `logs-YYYY-MM-DDTHHMMSS.txt`.
- `plugin_backend.py:task_history` — it returns at most the last 100 runs in reverse order.
- `plugin_backend.py:_recent_from_log` — it only reports the files whose
  realpath stays under `downloads_dir`.
- `plugin_backend.py:oauth_start` — exactly one OAuth handshake may run at a
  time; it returns 409 while `_oauth_proc` is alive under `_oauth_lock`.
- `plugin_backend.py:oauth_start` — it requires the task to exist and its
  `oauth_site.txt` to be in `_OAUTH_SITES`.
- `plugin_backend.py:oauth_paste` — it requires a live handshake; the stdin-site
  sites require a callback code and the redirect sites require a query containing `code=`.
- `plugin_backend.py:oauth_reset` — it refuses while that task's handshake is
  still running (409).
- `plugin_backend.py:oauth_log` / `_redact_oauth_log` — the OAuth log output is
  redacted (tokens, secrets, passwords, session ids, bearer headers) before returning.
- `plugin_backend.py:oauth_save_cookies` / `oauth_delete_cookies` — the cookie
  save requires a platform in `_OAUTH_SITES` and non-empty content that looks
  like a Netscape cookie file; the delete filename must match
  `cookies-[a-z0-9_-]+\.txt`.
- `plugin_backend.py:_telemetry_aux` / `_scan_downloads` — the results are
  cached behind TTLs and never block concurrent callers on a fresh walk.
- `plugin_backend.py:media_recent` — it merges only the log-derived media that
  is not already present, then caps at the 12 newest by mtime.

## Side Effects

- `plugin_backend.py:tasks_dir` — creates the tasks root with
  `os.makedirs(..., exist_ok=True)`; it may fall back to a legacy `TASKS_DIR` /
  `/tasks` that already holds saved tasks.
- `plugin_backend.py:config_dir` — creates the config root with
  `os.makedirs(..., exist_ok=True)`.
- `plugin_backend.py:_save_task` — writes `urls.txt`, `name.txt`, `command.txt`,
  `oauth_site.txt`, `status.json`, `timeout.txt`, `cookies.txt`, touches
  `stdout.log` / `logs.txt` and `os.rename`s the task folder on a rename.
- `plugin_backend.py:_set_schedule` — writes / deletes `<tasks>/<slug>/cron.txt`
  and mutates the APScheduler job store.
- `plugin_backend.py:_init_scheduler` — starts a
  `BackgroundScheduler(daemon=True, timezone="UTC")` and adds the cron jobs; it
  reads `task_concurrent_max.txt`.
- `plugin_backend.py:settings_put` — writes `task_concurrent_max.txt` into
  `config_dir()` and replaces the in-memory `_slots` semaphore.
- `plugin_backend.py:_run_task` — spawns the task command via
  `subprocess.Popen(cmd, cwd=<task folder>, stdout=PIPE, stderr=STDOUT)` with
  `GALLERY_DL_CONFIG` set and `/usr/local/bin` appended to `PATH`; it writes
  `pid`, `status.json`, `last_run.txt`, `run_history.jsonl`, `stopped` / `error` /
  `last_error.txt`, appends to `stdout.log` and the legacy `logs.txt`, and removes
  the `lock` / `pid` files on exit.
- `plugin_backend.py:_run_task` — kills the process on the `timeout.txt` /
  `TASK_TIMEOUT_SECONDS` expiry, and sends `proc.terminate()` /
  `os.kill(pid, SIGTERM)` from `task_stop` and `proc.kill()` from `task_delete`.
- `plugin_backend.py:_pump_process_output` — streams the child stdout into both
  log files from a daemon thread (batched per line / 512 chars, flushed).
- `plugin_backend.py:_rotate_task_logs` — renames the legacy `logs.txt` to
  `logs-<stamp>.txt`, keeps the newest 10 archives and truncates `stdout.log` / `logs.txt`.
- `plugin_backend.py:_tool_version` — runs
  `subprocess.run([cmd, "--version"], timeout=15)` for `gallery-dl` and `yt-dlp`.
- `plugin_backend.py:task_action` — `clear_logs` truncates `stdout.log` /
  `logs.txt`; `delete_archive` / `delete_cookies` `os.remove`
  `archive.sqlite` / `cookies.txt`; `duplicate` copies `urls.txt`, `command.txt`,
  `cron.txt`, `cookies.txt`, `oauth_site.txt` and `timeout.txt` into a new task
  folder and writes its `name.txt` / `status.json` / `stdout.log`.
- `plugin_backend.py:tasks_bulk_action` — `pause_all` touches a `paused` marker
  in every task folder; `clear_completed` `shutil.rmtree`s the completed task folders.
- `plugin_backend.py:task_delete` — `shutil.rmtree`s the task folder.
- `plugin_backend.py:task_stop` — touches `stopped`, rewrites `status.json` and
  appends `[launcher] stopped by user` to both logs.
- `plugin_backend.py:_scan_downloads` — walks the whole `downloads_dir()` tree
  with `os.walk` / `os.stat` to compute the total bytes, today's counts and the
  newest media (telemetry / filmstrip) — a heavy mounted-library read.
- `plugin_backend.py:_cache_has_token` — opens `<task>/gallery-dl-cache.sqlite3`
  read-only (`file:…?mode=ro`) and runs `SELECT 1 FROM data LIMIT 1`.
- `plugin_backend.py:task_urls_update` — writes `<task>/urls.txt`.
- `plugin_backend.py:tasks_create` — writes the uploaded cookies to
  `<task>/cookies.txt` and starts a `_run_task` daemon thread when `start` is truthy.
- `plugin_backend.py:plugin_task_status` — it mutates no disk state, but it is
  polled continuously; it reads `_procs` and calls `_task_info` (which reads the
  task files and the log tail).
- `plugin_backend.py:_last_url_marker` / `_tail` — read the task log files
  backwards in chunks.
- `plugin.py:_load_backend` — imports / executes `plugin_backend.py` and installs
  the module in `sys.modules` under `_BACKEND_NAME` (creating the module-level
  blueprints, locks and caches at import time).
- `plugin_backend.py:quick_start` — spawns a gallery-dl subprocess for the quick
  downloads with `cwd=dest`; it tracks it in `_quick` / `_one_time_proc`.
- `plugin_backend.py:quick_start` — writes `.quick-<id>.log` and `.quick-<id>.json`
  under `tasks_dir` and appends to `config_dir/one_time_download.log`.
- `plugin_backend.py:quick_start` — creates the downloads destination directory
  and the cookie file `cookies-quick-<id>.txt` in the config directory.
- `plugin_backend.py:quick_stop` / `quick_status` — rewrites `.quick-<id>.json`
  on the stop / status reconciliation.
- `plugin_backend.py:one_time_stop` — terminates the one-time process.
- `plugin_backend.py:task_history` / `task_recent` — read `run_history.jsonl` and
  the per-task logs.
- `plugin_backend.py:task_log_stream` — tails and polls the log files every
  second for the SSE stream.
- `plugin_backend.py:telemetry` — walks `tasks_dir` and `downloads_dir` for the
  telemetry; it reads `shutil.disk_usage` of the downloads.
- `plugin_backend.py:_scan_recent_media` — walks `downloads_dir` recursively for the recent media.
- `plugin_backend.py:kiosks_create` / `kiosk_toggle` / `kiosk_update` /
  `kiosk_delete` / `kiosk_defaults_put` — writes `config_dir/kiosks.json`.
- `plugin_backend.py:kiosks_create` / `_save_kiosk_settings` — writes
  `settings.json` and creates the kiosk images directory.
- `plugin_backend.py:kiosk_upload_images` / `kiosk_remove_image` /
  `kiosk_delete` — writes / removes the kiosk image files and `rmtree`s the kiosk folders.
- `plugin_backend.py:config_write` / `config_reset` / `config_apply_update` —
  writes `config_dir/gallery-dl.conf` and creates the `.bak.<timestamp>` copies.
- `plugin_backend.py:config_reset` / `config_check_update` /
  `config_apply_update` — an HTTP GET of `_DEFAULT_CONFIG_URL`.
- `plugin_backend.py:tools_check_update` — an HTTP GET of
  `pypi.org/pypi/<package>/json` for the version checks.
- `plugin_backend.py:tools_update` — runs a
  `sys.executable -m pip install --upgrade <package>` subprocess and mutates `_TOOL_CACHE`.
- `plugin_backend.py:backup_download` / `backup_create` — builds the tar.gz /
  zip archives in memory and streams them via `send_file`.
- `plugin_backend.py:backup_restore` — extracts the uploaded zip / tar.gz members
  onto the tasks / config / kiosk directories and re-applies the saved cron
  schedules via `_set_schedule`.
- `plugin_backend.py:media_file` / `task_log_download` /
  `task_archived_log_download` — serves the downloads-dir files and the log files
  via `send_file`.
- `plugin_backend.py:oauth_save_cookies` / `oauth_delete_cookies` — writes
  `cookies-<platform>.txt` and removes the cookie jars.
- `plugin_backend.py:oauth_start` — writes `.oauth_run.log` in the config
  directory and spawns the gallery-dl oauth subprocess with the log file as stdout.
- `plugin_backend.py:oauth_paste` — writes the pasted callback value to the
  handshake stdin; it POSTs the callback query to `http://127.0.0.1:6414`.
- `plugin_backend.py:oauth_stop` — terminates the OAuth subprocess, clears
  `_oauth_task` / `_oauth_site` and appends `[Stopped by user]` to the OAuth log.
- `plugin_backend.py:oauth_reset` — deletes the task's `gallery-dl-cache.sqlite3`.
- `plugin_backend.py:_oauth_client_id_configured` / `oauth_status` — reads the
  config `gallery-dl.conf` for the OAuth client-id detection and the cookie mtime scan.
- `plugin_backend.py:quick_start` / `oauth_start` — maintains the in-memory
  `_quick` jobs, `_one_time_proc`, `_oauth_proc` / `_oauth_task` / `_oauth_site` globals.

## Verification

- Run: `<test-command>`
