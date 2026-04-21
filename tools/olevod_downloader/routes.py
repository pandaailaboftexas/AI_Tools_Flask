"""
tools/olevod_downloader/routes.py
One-click: sniff m3u8 via Playwright then ffmpeg download.
Uses tools.shared. 7-attempt ffmpeg retry with random 2-7 s delay.
"""
import subprocess, json, shutil, os, re, random, uuid, threading, secrets, time
from flask import (Blueprint, render_template, request, Response,
                   stream_with_context, send_file, abort)
from tools.shared import (
    DOWNLOADS_DIR, get_logger,
    acquire_slot, release_slot,
    increment_active, decrement_active,
    register_job, is_stopped, remove_job, stop_job,
    register_token, get_token_path, pop_token,
    retry_delays,
)

olevod_bp = Blueprint('olevod', __name__, template_folder='templates')
_logger   = get_logger('olevod_downloader')

UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

# ── Helpers ────────────────────────────────────────────────────────────────

def _ffmpeg():  return shutil.which('ffmpeg') or 'ffmpeg'
def _ff_ok():   return shutil.which('ffmpeg') is not None
def _sse(e, d): return f'event: {e}\ndata: {json.dumps({"text": d})}\n\n'

def _stream(gen):
    return Response(stream_with_context(gen), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

def _fmt_time(seconds):
    s = int(seconds); h, m, s = s//3600, (s%3600)//60, s%60
    return f'{h}:{m:02d}:{s:02d}' if h else f'{m}:{s:02d}'

def _safe_name(name):
    name = re.sub(r'\.[^.]+$', '', name).strip()
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    return name or f'video_{random.randint(1000,9999)}'

def _playwright_available():
    try:
        import importlib; importlib.import_module('playwright')
        return True
    except ImportError:
        return False

# ── Static routes ──────────────────────────────────────────────────────────

@olevod_bp.route('/')
def index(): return render_template('olevod_downloader/index.html')

@olevod_bp.route('/check_deps')
def check_deps():
    return json.dumps({'ffmpeg': _ff_ok(), 'playwright': _playwright_available()})

# ── File serving ───────────────────────────────────────────────────────────

@olevod_bp.route('/serve/<token>')
def serve_file(token):
    fpath = get_token_path(token)
    if not fpath or not os.path.exists(fpath): abort(404)
    return send_file(fpath, mimetype='video/mp4', as_attachment=True,
                     download_name=os.path.basename(fpath))

@olevod_bp.route('/delete/<token>', methods=['POST'])
def delete_file(token):
    fpath = pop_token(token)
    if not fpath: return json.dumps({'ok': False, 'msg': 'Invalid token'})
    try:
        if os.path.exists(fpath): os.remove(fpath)
        return json.dumps({'ok': True})
    except Exception as e:
        return json.dumps({'ok': False, 'msg': str(e)})

@olevod_bp.route('/stop/<job_id>', methods=['POST'])
def stop(job_id): return json.dumps(stop_job(job_id))

# ── Playwright sniff ───────────────────────────────────────────────────────

def _sniff_stream(page_url, timeout, job_id):
    if not _playwright_available():
        yield _sse('error', 'playwright not installed.\nRun: pip install playwright && playwright install chromium')
        yield ('__stream__', None); return

    yield _sse('log', '🔍  Launching headless browser…')
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        yield _sse('error', 'Could not import playwright.')
        yield ('__stream__', None); return

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
                        yield _sse('log', f'   Clicked: {sel}'); break
                except Exception: pass

            deadline = time.time() + timeout
            while time.time() < deadline and not captured:
                page.wait_for_timeout(500)
                if is_stopped(job_id):
                    browser.close(); yield ('__stream__', None); return

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

# ── ffmpeg download with 7-attempt retry ───────────────────────────────────

def _ffmpeg_download(stream_url, referer, out_path, job_id):
    for attempt, sleep_s in retry_delays():
        if is_stopped(job_id): yield _sse('stopped', 'Stopped by user.'); yield ('__done__', False); return
        if attempt > 0:
            yield _sse('log', f'⟳  Retry {attempt}/6 in {sleep_s}s…')
            time.sleep(sleep_s)
            if os.path.exists(out_path):
                try: os.remove(out_path)
                except: pass
        if is_stopped(job_id): yield _sse('stopped', 'Stopped by user.'); yield ('__done__', False); return

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
            register_job(job_id, proc)
            duration_s, current_s = None, 0.0

            for raw in proc.stdout:
                if is_stopped(job_id): break
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

            proc.wait(); rc = proc.returncode; remove_job(job_id)

        except Exception as exc:
            remove_job(job_id); yield _sse('error', str(exc)); yield ('__done__', False); return

        if is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.')
            if os.path.exists(out_path):
                try: os.remove(out_path)
                except: pass
            yield ('__done__', False); return

        if rc == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            yield ('__done__', True); return

        _logger.error(f'[olevod ffmpeg attempt {attempt+1}] exit {rc} for {stream_url}')
        if attempt == 0:
            yield _sse('log', '⚠  Download failed (attempt 1) — will retry with random delay')

    yield _sse('error', 'Download failed after 7 attempts. See ~/Downloads/yt_errors.log')
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

    if not acquire_slot():
        return _stream(iter([_sse('error', 'Server busy — too many downloads. Try again shortly.')]))

    out_path = os.path.join(DOWNLOADS_DIR, f'{filename}.mp4')

    def generate():
        increment_active()
        try:
            # Phase 1: sniff
            stream_url = None
            for chunk in _sniff_stream(page_url, timeout, job_id):
                if isinstance(chunk, tuple) and chunk[0] == '__stream__':
                    stream_url = chunk[1]
                else:
                    yield chunk

            if not stream_url or is_stopped(job_id):
                if not is_stopped(job_id):
                    yield _sse('error', 'Could not detect stream URL. Aborting.')
                return

            yield _sse('log', '')
            yield _sse('log', '▶  Stream found — starting download…')
            yield _sse('log', '')

            # Phase 2: download with retry
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
                register_token(token, out_path)
                yield _sse('ready', json.dumps({'token': token,
                    'filename': os.path.basename(out_path), 'size_mb': size_mb, 'job_id': job_id}))
                yield _sse('done', f'✓  {os.path.basename(out_path)}  ({size_mb} MB)  →  ~/Downloads')
        finally:
            decrement_active(); release_slot()

    return _stream(generate())
