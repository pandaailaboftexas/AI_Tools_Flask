"""
tools/olevod_downloader/routes.py

Endpoints:
  GET  /olevod/                     - UI
  POST /olevod/start/<job_id>       - One-click: sniff m3u8 then download (SSE)
  POST /olevod/stop/<job_id>        - Kill a specific job
  GET  /olevod/serve/<token>        - Serve completed file for browser download
  POST /olevod/delete/<token>       - Delete a completed file
  GET  /olevod/check_deps           - Check ffmpeg / playwright availability

Design notes:
  - Single "Download" button triggers /start which sniffs the stream URL via
    Playwright then immediately pipes it to ffmpeg — no second click needed.
  - Each request gets its own job_id so concurrent users never collide.
  - On ffmpeg failure the pipeline retries once after 3 seconds.
  - Files saved directly to ~/Downloads (no hidden staging subfolder).
  - A token map lets /serve/<token> send the file to the browser by name.
"""
import subprocess, json, shutil, os, re, random, uuid, threading, secrets, time, logging
from flask import (Blueprint, render_template, request, Response,
                   stream_with_context, send_file, abort)

olevod_bp = Blueprint('olevod', __name__, template_folder='templates')

# ── Download destination ───────────────────────────────────────────────────
DOWNLOADS_DIR = os.path.expanduser('~/Downloads')
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Shared error log
ERROR_LOG = os.path.join(DOWNLOADS_DIR, 'yt_errors.log')
_log_handler = logging.FileHandler(ERROR_LOG)
_log_handler.setFormatter(logging.Formatter('%(asctime)s  %(message)s', '%Y-%m-%d %H:%M:%S'))
_logger = logging.getLogger('olevod_downloader')
_logger.setLevel(logging.ERROR)
_logger.addHandler(_log_handler)

# ── Per-job registry (multi-user safe) ────────────────────────────────────
_jobs: dict = {}
_jobs_lock  = threading.Lock()

# token → absolute file path
_tokens: dict = {}

UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _ffmpeg():
    return shutil.which('ffmpeg') or 'ffmpeg'

def _ffmpeg_available():
    return shutil.which('ffmpeg') is not None

def _playwright_available():
    try:
        import importlib; importlib.import_module('playwright')
        return True
    except ImportError:
        return False

def _sse(event, data):
    return f'event: {event}\ndata: {json.dumps({"text": data})}\n\n'

def _stream(gen):
    return Response(stream_with_context(gen), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

def _fmt_time(seconds: float) -> str:
    s = int(seconds)
    h, m, s = s // 3600, (s % 3600) // 60, s % 60
    return f'{h}:{m:02d}:{s:02d}' if h else f'{m}:{s:02d}'

def _safe_name(name: str) -> str:
    name = re.sub(r'\.[^.]+$', '', name).strip()
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    return name or f'video_{random.randint(1000,9999)}'

def _register_job(job_id, proc):
    with _jobs_lock:
        _jobs[job_id] = {'proc': proc, 'stopped': False}

def _is_stopped(job_id):
    with _jobs_lock:
        return _jobs.get(job_id, {}).get('stopped', False)

def _remove_job(job_id):
    with _jobs_lock:
        _jobs.pop(job_id, None)


# ── Static routes ──────────────────────────────────────────────────────────

@olevod_bp.route('/')
def index():
    return render_template('olevod_downloader/index.html')

@olevod_bp.route('/check_deps')
def check_deps():
    return json.dumps({'ffmpeg': _ffmpeg_available(), 'playwright': _playwright_available()})


# ── File serving ───────────────────────────────────────────────────────────

@olevod_bp.route('/serve/<token>')
def serve_file(token):
    fpath = _tokens.get(token)
    if not fpath or not os.path.exists(fpath):
        abort(404)
    return send_file(fpath, mimetype='video/mp4', as_attachment=True,
                     download_name=os.path.basename(fpath))

@olevod_bp.route('/delete/<token>', methods=['POST'])
def delete_file(token):
    fpath = _tokens.pop(token, None)
    if not fpath:
        return json.dumps({'ok': False, 'msg': 'Invalid token'})
    try:
        if os.path.exists(fpath): os.remove(fpath)
        return json.dumps({'ok': True})
    except Exception as e:
        return json.dumps({'ok': False, 'msg': str(e)})


# ── Stop ───────────────────────────────────────────────────────────────────

@olevod_bp.route('/stop/<job_id>', methods=['POST'])
def stop(job_id):
    with _jobs_lock:
        entry = _jobs.get(job_id)
    if entry and entry['proc'] and entry['proc'].poll() is None:
        entry['stopped'] = True
        entry['proc'].terminate()
        try:   entry['proc'].wait(timeout=5)
        except subprocess.TimeoutExpired: entry['proc'].kill()
        _remove_job(job_id)
        return json.dumps({'ok': True, 'msg': 'Process terminated.'})
    return json.dumps({'ok': False, 'msg': 'No active process.'})


# ── Internal: sniff m3u8 from page ────────────────────────────────────────

def _sniff_stream(page_url: str, timeout: int, job_id: str):
    """
    Generator: sniffs m3u8 URL from page_url using Playwright.
    Yields SSE log events; final yield is either
      ('__stream__', url) on success or ('__stream__', None) on failure.
    """
    if not _playwright_available():
        yield _sse('error', 'playwright not installed.\nRun: pip install playwright && playwright install chromium')
        yield ('__stream__', None)
        return

    yield _sse('log', '🔍  Launching headless browser…')

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        yield _sse('error', 'Could not import playwright.')
        yield ('__stream__', None)
        return

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx     = browser.new_context(user_agent=UA, viewport={'width': 1280, 'height': 720})
            page    = ctx.new_page()
            captured = []

            page.on('request', lambda req: captured.append(req.url)
                    if '.m3u8' in req.url and req.url not in captured else None)

            yield _sse('log', f'🔍  Opening: {page_url}')
            page.goto(page_url, wait_until='domcontentloaded', timeout=timeout * 1000)
            yield _sse('log', '🔍  Waiting for video player…')

            for sel in ['button.play', '.play-btn', '.vjs-big-play-button',
                        '[class*="play"]', 'button[aria-label*="play" i]', 'video']:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=2000):
                        el.click(timeout=2000)
                        yield _sse('log', f'   Clicked: {sel}')
                        break
                except Exception:
                    pass

            deadline = time.time() + timeout
            while time.time() < deadline and not captured:
                page.wait_for_timeout(500)
                if _is_stopped(job_id):
                    browser.close()
                    yield ('__stream__', None)
                    return

            browser.close()

            if captured:
                def score(u):
                    if 'master' in u: return 3
                    if 'index'  in u: return 2
                    return 1
                best = sorted(captured, key=score, reverse=True)[0]
                yield _sse('log', f'   Found {len(captured)} stream URL(s)')
                yield _sse('log', f'   → {best[:100]}')
                yield ('__stream__', best)
            else:
                yield _sse('error', 'No m3u8 stream found. Try a specific episode/movie page.')
                yield ('__stream__', None)

    except Exception as exc:
        yield _sse('error', f'Browser error: {exc}')
        yield ('__stream__', None)


# ── Internal: ffmpeg download with retry ──────────────────────────────────

def _ffmpeg_download(stream_url: str, referer: str, out_path: str, job_id: str, retry_delay=3):
    """
    Generator: downloads stream_url with ffmpeg, retries once on failure.
    Yields SSE strings. On success yields ('__done__', True), on failure ('__done__', False).
    """
    for attempt in range(2):
        if _is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.')
            yield ('__done__', False)
            return

        if attempt == 1:
            yield _sse('log', f'⟳  Retrying download in {retry_delay}s…')
            time.sleep(retry_delay)
            # remove partial file before retry
            if os.path.exists(out_path):
                try: os.remove(out_path)
                except: pass

        cmd = [
            _ffmpeg(),
            '-user_agent', UA,
            '-headers', f'Referer: {referer}\r\nOrigin: https://www.olevod.com\r\n',
            '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
            '-reconnect', '1', '-reconnect_streamed', '1',
            '-reconnect_delay_max', '5', '-reconnect_at_eof', '1',
            '-timeout', '30000000',
            '-i', stream_url,
            '-c', 'copy', '-bsf:a', 'aac_adtstoasc', '-movflags', '+faststart',
            out_path, '-y', '-progress', 'pipe:1', '-loglevel', 'warning',
        ]

        yield _sse('cmd', ' '.join(cmd))
        yield _sse('log', f'💾  Saving to ~/Downloads/{os.path.basename(out_path)}')
        yield _sse('log', '')

        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            _register_job(job_id, proc)
            duration_s, current_s = None, 0.0

            for raw in proc.stdout:
                if _is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue

                if '=' in line and not line.startswith('['):
                    key, _, val = line.partition('=')
                    key, val = key.strip(), val.strip()
                    if key == 'out_time_us':
                        try: current_s = int(val) / 1_000_000
                        except ValueError: pass
                        if duration_s and duration_s > 0:
                            pct = min(current_s / duration_s * 100, 99.9)
                            yield _sse('progress', f'{pct:.1f}%  {_fmt_time(current_s)} / {_fmt_time(duration_s)}')
                        else:
                            yield _sse('progress', f'Downloaded: {_fmt_time(current_s)}')
                    elif key == 'progress' and val == 'end':
                        yield _sse('progress', '100%')
                    continue

                m = re.search(r'Duration:\s*(\d+):(\d+):(\d+\.?\d*)', line)
                if m:
                    h, mi, s = m.groups()
                    duration_s = int(h)*3600 + int(mi)*60 + float(s)

                if any(w in line.lower() for w in ['error', 'invalid', 'fail', 'warning']):
                    yield _sse('log', line)
                elif line.startswith('['):
                    yield _sse('log', line)

            proc.wait()
            rc = proc.returncode
            _remove_job(job_id)

        except Exception as exc:
            _remove_job(job_id)
            yield _sse('error', str(exc))
            yield ('__done__', False)
            return

        if _is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.')
            if os.path.exists(out_path):
                try: os.remove(out_path)
                except: pass
            yield ('__done__', False)
            return

        if rc == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            yield ('__done__', True)
            return

        # failure
        err = f'ffmpeg exit {rc} for stream: {stream_url}'
        if attempt == 0:
            yield _sse('log', f'⚠  Download failed (attempt 1/2) — retrying in {retry_delay}s')
            _logger.error(f'[attempt 1/2] {err}')
        else:
            _logger.error(f'[attempt 2/2 FINAL] {err}')
            yield _sse('error', f'Download failed after 2 attempts. Check ~/Downloads/yt_errors.log')
            yield ('__done__', False)


# ── One-click: sniff → download ────────────────────────────────────────────

@olevod_bp.route('/start/<job_id>', methods=['POST'])
def start(job_id):
    data     = request.get_json(force=True) or {}
    page_url = (data.get('page_url') or '').strip()
    filename = _safe_name(data.get('filename') or '')
    timeout  = int(data.get('timeout', 30))

    if not page_url:
        return _stream(iter([_sse('error', 'No page URL provided.')]))

    out_path = os.path.join(DOWNLOADS_DIR, f'{filename}.mp4')

    def generate():
        # ── Phase 1: sniff ────────────────────────────────────────────────
        stream_url = None
        for chunk in _sniff_stream(page_url, timeout, job_id):
            if isinstance(chunk, tuple) and chunk[0] == '__stream__':
                stream_url = chunk[1]
            else:
                yield chunk

        if not stream_url or _is_stopped(job_id):
            if not _is_stopped(job_id):
                yield _sse('error', 'Could not detect stream URL. Aborting.')
            return

        yield _sse('log', '')
        yield _sse('log', '▶  Stream found — starting download…')
        yield _sse('log', '')

        # ── Phase 2: download (with retry) ────────────────────────────────
        referer = page_url or 'https://www.olevod.com/'
        success = False

        for chunk in _ffmpeg_download(stream_url, referer, out_path, job_id):
            if isinstance(chunk, tuple) and chunk[0] == '__done__':
                success = chunk[1]
            else:
                yield chunk

        if success:
            size_mb = round(os.path.getsize(out_path) / 1_048_576, 1)
            token   = secrets.token_urlsafe(32)
            _tokens[token] = out_path
            yield _sse('ready', json.dumps({
                'token':    token,
                'filename': os.path.basename(out_path),
                'size_mb':  size_mb,
                'job_id':   job_id,
            }))
            yield _sse('done', f'✓  {os.path.basename(out_path)}  ({size_mb} MB)  →  ~/Downloads')

    return _stream(generate())
