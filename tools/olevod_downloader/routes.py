"""
tools/olevod_downloader/routes.py

Endpoints:
  GET  /olevod/              - UI
  POST /olevod/sniff         - Launch browser, intercept m3u8 URL (SSE)
  POST /olevod/download      - Download via ffmpeg from a pasted m3u8/mp4 URL (SSE)
  POST /olevod/stop          - Kill the running process
"""
import subprocess, json, shutil, os, re, random
from flask import Blueprint, render_template, request, Response, stream_with_context

olevod_bp = Blueprint('olevod', __name__, template_folder='templates')

_current_proc = None
_user_stopped = False

UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ffmpeg():
    return shutil.which('ffmpeg') or 'ffmpeg'

def _ffmpeg_available():
    return shutil.which('ffmpeg') is not None

def _playwright_available():
    try:
        import importlib
        importlib.import_module('playwright')
        return True
    except ImportError:
        return False

def _sse(event, data):
    return f'event: {event}\ndata: {json.dumps({"text": data})}\n\n'

def _expand(path):
    return os.path.expanduser((path or '~/Downloads').strip())

def _stream(gen):
    return Response(
        stream_with_context(gen),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )

def _random_name():
    return str(random.randint(100_000_000, 999_999_999))

def _fmt_time(seconds: float) -> str:
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f'{h}:{m:02d}:{s:02d}'
    return f'{m}:{s:02d}'


# ── Routes ─────────────────────────────────────────────────────────────────────

@olevod_bp.route('/')
def index():
    return render_template('olevod_downloader/index.html')


@olevod_bp.route('/stop', methods=['POST'])
def stop():
    global _current_proc, _user_stopped
    if _current_proc and _current_proc.poll() is None:
        _user_stopped = True
        _current_proc.terminate()
        try:
            _current_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _current_proc.kill()
        _current_proc = None
        return json.dumps({'ok': True, 'msg': 'Process terminated.'})
    return json.dumps({'ok': False, 'msg': 'No active process.'})


@olevod_bp.route('/check_deps')
def check_deps():
    return json.dumps({
        'ffmpeg':     _ffmpeg_available(),
        'playwright': _playwright_available(),
    })


# ── 1. Auto-sniff m3u8 via Playwright ─────────────────────────────────────────

@olevod_bp.route('/sniff', methods=['POST'])
def sniff():
    data      = request.get_json(force=True) or {}
    page_url  = (data.get('page_url') or '').strip()
    timeout   = int(data.get('timeout', 30))

    if not page_url:
        return _stream(iter([_sse('error', 'No page URL provided.')]))

    def generate():
        if not _playwright_available():
            yield _sse('error',
                       'playwright not installed.\n'
                       'Run:  pip install playwright && playwright install chromium')
            return

        yield _sse('log', '⟳  Launching browser …')

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            yield _sse('error', 'Could not import playwright.')
            return

        found_url = None

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=UA,
                    viewport={'width': 1280, 'height': 720},
                )
                page = context.new_page()

                captured = []

                def handle_request(req):
                    url = req.url
                    if '.m3u8' in url and url not in captured:
                        captured.append(url)

                page.on('request', handle_request)

                yield _sse('log', f'⟳  Opening: {page_url}')
                page.goto(page_url, wait_until='domcontentloaded', timeout=timeout * 1000)

                yield _sse('log', '⟳  Waiting for video player to load …')

                # Try clicking a play button if one exists
                for selector in [
                    'button.play', '.play-btn', '.vjs-big-play-button',
                    '[class*="play"]', 'button[aria-label*="play" i]',
                    'video', '.player',
                ]:
                    try:
                        el = page.locator(selector).first
                        if el.is_visible(timeout=2000):
                            el.click(timeout=2000)
                            yield _sse('log', f'   Clicked player element: {selector}')
                            break
                    except Exception:
                        pass

                # Wait up to `timeout` seconds for an m3u8 to appear
                import time
                deadline = time.time() + timeout
                while time.time() < deadline and not captured:
                    page.wait_for_timeout(500)

                browser.close()

                if captured:
                    # Prefer a master/index playlist over segment playlists
                    def score(u):
                        if 'master' in u:   return 3
                        if 'index' in u:    return 2
                        if '.m3u8' in u:    return 1
                        return 0
                    best = sorted(captured, key=score, reverse=True)[0]
                    found_url = best
                    yield _sse('log', f'   Found {len(captured)} stream URL(s)')
                    for u in captured[:5]:
                        yield _sse('log', f'   · {u[:90]}')
                else:
                    yield _sse('error',
                               'No m3u8 stream found on that page.\n'
                               'Make sure the URL is a specific movie/episode page and the video is embeddable.')
                    return

        except Exception as exc:
            yield _sse('error', f'Browser error: {exc}')
            return

        # Emit the found URL as a special event so the frontend can fill the input
        yield _sse('sniffed', found_url)
        yield _sse('done', 'Stream URL captured ✓')

    return _stream(generate())


# ── 2. Download via ffmpeg ─────────────────────────────────────────────────────

@olevod_bp.route('/download', methods=['POST'])
def download():
    data        = request.get_json(force=True) or {}
    stream_url  = (data.get('stream_url') or '').strip()
    referer     = (data.get('referer') or 'https://www.olevod.com/').strip()
    filename    = (data.get('filename') or '').strip()
    save_path   = _expand(data.get('save_path', '~/Downloads'))

    if not stream_url:
        return _stream(iter([_sse('error', 'No stream URL provided.')]))

    if not filename:
        filename = _random_name()
    filename = re.sub(r'\.[^.]+$', '', filename).strip() or _random_name()
    filename = re.sub(r'[\\/:*?"<>|]', '_', filename)
    out_path = os.path.join(save_path, filename + '.mp4')

    def generate():
        global _current_proc, _user_stopped
        _user_stopped = False

        if not _ffmpeg_available():
            yield _sse('error',
                       'ffmpeg not found.\n'
                       '  Mac:   brew install ffmpeg\n'
                       '  Linux: sudo apt install ffmpeg')
            return

        os.makedirs(save_path, exist_ok=True)

        cmd = [
            _ffmpeg(),
            '-user_agent', UA,
            '-headers', f'Referer: {referer}\r\nOrigin: https://www.olevod.com\r\n',
            '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
            '-i', stream_url,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-movflags', '+faststart',
            out_path,
            '-y',
            '-progress', 'pipe:1',
            '-loglevel', 'warning',
        ]

        yield _sse('cmd', ' '.join(cmd))
        yield _sse('log', f'Saving to: {out_path}')
        yield _sse('log', '')

        try:
            _current_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            duration_s = None
            current_s  = 0.0

            for raw in _current_proc.stdout:
                if _user_stopped:
                    break
                line = raw.rstrip('\n')
                if not line:
                    continue

                if '=' in line and not line.startswith('['):
                    key, _, val = line.partition('=')
                    key = key.strip(); val = val.strip()
                    if key == 'out_time_us':
                        try:
                            current_s = int(val) / 1_000_000
                        except ValueError:
                            pass
                        if duration_s and duration_s > 0:
                            pct = min(current_s / duration_s * 100, 99.9)
                            yield _sse('progress',
                                       f'{pct:.1f}%  {_fmt_time(current_s)} / {_fmt_time(duration_s)}')
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

            _current_proc.wait()
            rc = _current_proc.returncode
            _current_proc = None

        except Exception as exc:
            _current_proc = None
            yield _sse('error', str(exc))
            return

        if _user_stopped:
            yield _sse('stopped', 'Stopped by user.')
            if os.path.exists(out_path):
                try: os.remove(out_path)
                except: pass
            return

        if rc == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            size_mb = os.path.getsize(out_path) / 1_048_576
            yield _sse('done', f'Saved ({size_mb:.1f} MB)  →  {out_path}')
        else:
            yield _sse('error',
                       f'ffmpeg exited with code {rc}. '
                       'Check that the stream URL is valid and not expired.')

    return _stream(generate())
