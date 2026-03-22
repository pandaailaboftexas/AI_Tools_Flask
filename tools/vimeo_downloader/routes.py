"""
tools/vimeo_downloader/routes.py

Endpoints:
  GET  /vimeo/              - UI
  POST /vimeo/download      - Video download (SSE)
  POST /vimeo/mp3           - Audio-only MP3 (SSE)
  POST /vimeo/transcript    - Subtitles/transcript (SSE)
  POST /vimeo/stop/<job_id> - Kill a specific running job
  GET  /vimeo/check_ffmpeg  - Check if ffmpeg is available
  GET  /vimeo/serve/<token> - Serve a completed file for browser download
  POST /vimeo/delete/<token>- Delete a served file

Design notes:
  [1] Powered by yt-dlp which supports Vimeo natively.
  [2] Files saved directly to ~/Downloads — no staging subfolder.
      A token map lets the browser trigger a named download via /serve/<token>.
  [3] Multi-URL batch: comma/newline-separated URLs; each retries once after 5 s
      on failure, errors logged to ~/Downloads/yt_errors.log.
  [4] Multi-user safe: per-job process registry keyed by uuid job_id with a
      threading.Lock — no shared global process variables.
  [5] Vimeo-specific: password support, preferred quality fallback chain,
      no --no-playlist flag (Vimeo showcases are valid playlists).
"""
import subprocess, json, shutil, os, re, uuid, secrets, time, threading, logging
from flask import (Blueprint, render_template, request, Response,
                   stream_with_context, send_file, abort)

vimeo_bp = Blueprint('vimeo', __name__, template_folder='templates')

# ── Download destination ───────────────────────────────────────────────────
DOWNLOADS_DIR = os.path.expanduser('~/Downloads')
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Shared error log (same file used by yt_downloader)
ERROR_LOG = os.path.join(DOWNLOADS_DIR, 'yt_errors.log')
_log_handler = logging.FileHandler(ERROR_LOG)
_log_handler.setFormatter(logging.Formatter('%(asctime)s  %(message)s', '%Y-%m-%d %H:%M:%S'))
_logger = logging.getLogger('vimeo_downloader')
_logger.setLevel(logging.ERROR)
_logger.addHandler(_log_handler)

# ── Per-job process registry (multi-user safe) ─────────────────────────────
_jobs: dict = {}
_jobs_lock  = threading.Lock()

# token → absolute file path
_tokens: dict = {}


# ── Helpers ────────────────────────────────────────────────────────────────

def _ytdlp():
    return shutil.which('yt-dlp') or 'yt-dlp'

def _ffmpeg_available():
    return shutil.which('ffmpeg') is not None

def _sse(event, data):
    return f'event: {event}\ndata: {json.dumps({"text": data})}\n\n'

def _expand(path):
    return os.path.expanduser((path or '~/Downloads').strip())

def _stream(gen):
    return Response(stream_with_context(gen), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

def _split_urls(raw: str) -> list:
    return [p.strip() for p in re.split(r'[\n,]+', raw) if p.strip()]

def _register_job(job_id: str, proc):
    with _jobs_lock:
        _jobs[job_id] = {'proc': proc, 'stopped': False}

def _is_stopped(job_id: str) -> bool:
    with _jobs_lock:
        return _jobs.get(job_id, {}).get('stopped', False)

def _remove_job(job_id: str):
    with _jobs_lock:
        _jobs.pop(job_id, None)

def _extract_event(sse_str: str) -> str:
    for line in sse_str.split('\n'):
        if line.startswith('event: '):
            return line[7:].strip()
    return 'log'

def _extract_data(sse_str: str) -> str:
    for line in sse_str.split('\n'):
        if line.startswith('data: '):
            try:
                return json.loads(line[6:])['text']
            except Exception:
                return line[6:]
    return ''


# ── Stop / Check ───────────────────────────────────────────────────────────

@vimeo_bp.route('/stop/<job_id>', methods=['POST'])
def stop(job_id):
    with _jobs_lock:
        entry = _jobs.get(job_id)
    if entry and entry['proc'] and entry['proc'].poll() is None:
        entry['stopped'] = True
        entry['proc'].terminate()
        try:
            entry['proc'].wait(timeout=5)
        except subprocess.TimeoutExpired:
            entry['proc'].kill()
        _remove_job(job_id)
        return json.dumps({'ok': True, 'msg': 'Process terminated.'})
    return json.dumps({'ok': False, 'msg': 'No active process.'})


@vimeo_bp.route('/check_ffmpeg')
def check_ffmpeg():
    return json.dumps({'available': _ffmpeg_available()})


# ── File serving (token-protected) ────────────────────────────────────────

@vimeo_bp.route('/serve/<token>')
def serve_file(token):
    fpath = _tokens.get(token)
    if not fpath or not os.path.exists(fpath):
        abort(404)
    ext  = fpath.rsplit('.', 1)[-1] if '.' in fpath else 'mp4'
    mime = 'audio/mpeg' if ext == 'mp3' else 'video/mp4'
    return send_file(fpath, mimetype=mime, as_attachment=True,
                     download_name=os.path.basename(fpath))

@vimeo_bp.route('/delete/<token>', methods=['POST'])
def delete_file(token):
    fpath = _tokens.pop(token, None)
    if not fpath:
        return json.dumps({'ok': False, 'msg': 'Invalid token'})
    try:
        if os.path.exists(fpath):
            os.remove(fpath)
        return json.dumps({'ok': True})
    except Exception as e:
        return json.dumps({'ok': False, 'msg': str(e)})


# ── Page ───────────────────────────────────────────────────────────────────

@vimeo_bp.route('/')
def index():
    return render_template('vimeo_downloader/index.html')


# ── Core download helper (with retry) ─────────────────────────────────────

def _download_one_video(url, save_path, quality, fmt, overwrite,
                        password, subs, job_id, retry_delay=5):
    """
    Generator: runs yt-dlp for a single Vimeo URL, retries once on failure.
    Captures final file path via --print after_move:filepath.
    Yields SSE strings; on success yields _sse('ready', ...).
    """
    for attempt in range(2):
        if _is_stopped(job_id):
            return

        if attempt == 1:
            yield _sse('log', f'⟳  Retrying in {retry_delay}s: {url}')
            time.sleep(retry_delay)

        is_audio = quality.startswith('bestaudio')
        cmd = [_ytdlp()]
        if is_audio:
            cmd += ['-f', quality, '-x', '--audio-format',
                    fmt if fmt in ('mp3', 'm4a', 'opus', 'flac') else 'mp3']
        else:
            cmd += ['-f', quality, '--merge-output-format', fmt]

        cmd += ['-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                '--progress', '--no-mtime', '--newline',
                '--print', 'after_move:filepath']

        if overwrite: cmd += ['--no-continue', '--force-overwrites']
        if subs:      cmd += ['--write-auto-sub', '--embed-subs']
        if password:  cmd += ['--video-password', password]

        cmd.append(url)

        yield _sse('cmd', ' '.join(cmd))
        rc, lines, final_path = -1, [], None

        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            _register_job(job_id, proc)
            for raw in proc.stdout:
                if _is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue
                lines.append(line)
                # --print after_move:filepath emits the absolute path on its own line
                if os.sep in line and os.path.exists(line.strip()):
                    final_path = line.strip()
                elif re.search(r'\d+\.\d+%', line):
                    yield _sse('progress', line)
                else:
                    yield _sse('log', line)
            proc.wait()
            rc = proc.returncode
            _remove_job(job_id)
        except Exception as exc:
            _remove_job(job_id)
            yield _sse('error', str(exc))
            yield _sse('batch_error', url)
            return

        if _is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.')
            return

        if rc == 0 and final_path and os.path.exists(final_path):
            size_mb = round(os.path.getsize(final_path) / 1_048_576, 1)
            token   = secrets.token_urlsafe(32)
            _tokens[token] = final_path
            yield _sse('ready', json.dumps({
                'token': token, 'name': os.path.basename(final_path),
                'size_mb': size_mb, 'url': url,
            }))
            return  # success — no retry needed

        err_msg = f'yt-dlp exit {rc} for: {url}'
        if attempt == 0:
            yield _sse('log', f'⚠  Failed (attempt 1/2) — retrying in {retry_delay}s')
            _logger.error(f'[vimeo attempt 1/2] {err_msg}\n' + '\n'.join(lines))
        else:
            _logger.error(f'[vimeo attempt 2/2 FINAL] {err_msg}\n' + '\n'.join(lines))
            yield _sse('batch_error', url)


def _download_one_mp3(url, save_path, audio_quality, overwrite,
                      thumbnail, metadata, password, job_id, retry_delay=5):
    """Same pattern as video but extracts audio to mp3."""
    has_ffmpeg = _ffmpeg_available()

    for attempt in range(2):
        if _is_stopped(job_id): return

        if attempt == 1:
            yield _sse('log', f'⟳  Retrying in {retry_delay}s: {url}')
            time.sleep(retry_delay)

        if has_ffmpeg:
            cmd = [_ytdlp(), '-f', 'bestaudio/best',
                   '-x', '--audio-format', 'mp3',
                   '--audio-quality', audio_quality,
                   '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                   '--progress', '--no-mtime', '--newline',
                   '--print', 'after_move:filepath']
            if thumbnail: cmd += ['--embed-thumbnail', '--convert-thumbnails', 'jpg']
            if metadata:  cmd += ['--embed-metadata']
        else:
            cmd = [_ytdlp(), '-f', 'bestaudio[ext=m4a]/bestaudio/best',
                   '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                   '--progress', '--no-mtime', '--newline',
                   '--print', 'after_move:filepath']

        if overwrite: cmd += ['--no-continue', '--force-overwrites']
        if password:  cmd += ['--video-password', password]
        cmd.append(url)

        yield _sse('cmd', ' '.join(cmd))
        rc, lines, final_path = -1, [], None

        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            _register_job(job_id, proc)
            for raw in proc.stdout:
                if _is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue
                lines.append(line)
                if os.sep in line and os.path.exists(line.strip()):
                    final_path = line.strip()
                elif re.search(r'\d+\.\d+%', line):
                    yield _sse('progress', line)
                else:
                    yield _sse('log', line)
            proc.wait()
            rc = proc.returncode
            _remove_job(job_id)
        except Exception as exc:
            _remove_job(job_id)
            yield _sse('error', str(exc))
            yield _sse('batch_error', url)
            return

        if _is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.')
            return

        if rc == 0 and final_path and os.path.exists(final_path):
            size_mb = round(os.path.getsize(final_path) / 1_048_576, 1)
            token   = secrets.token_urlsafe(32)
            _tokens[token] = final_path
            yield _sse('ready', json.dumps({
                'token': token, 'name': os.path.basename(final_path),
                'size_mb': size_mb, 'url': url,
            }))
            return

        err_msg = f'yt-dlp mp3 exit {rc} for: {url}'
        if attempt == 0:
            yield _sse('log', f'⚠  Failed (attempt 1/2) — retrying in {retry_delay}s')
            _logger.error(f'[vimeo mp3 attempt 1/2] {err_msg}\n' + '\n'.join(lines))
        else:
            _logger.error(f'[vimeo mp3 attempt 2/2 FINAL] {err_msg}\n' + '\n'.join(lines))
            yield _sse('batch_error', url)


# ── Batch runner helper ────────────────────────────────────────────────────

def _run_batch(urls, per_url_gen, job_id, label):
    total, failed = len(urls), []
    if total > 1:
        yield _sse('log', f'📋  Batch: {total} URL(s) — saving to ~/Downloads')

    for i, url in enumerate(urls, 1):
        if _is_stopped(job_id): break
        if total > 1:
            yield _sse('log', f'\n── [{i}/{total}] {url}')
        for chunk in per_url_gen(url):
            if isinstance(chunk, str) and _extract_event(chunk) == 'batch_error':
                failed.append(_extract_data(chunk))
            yield chunk

    if failed:
        msg = ('Failed URLs (logged to ~/Downloads/yt_errors.log):\n'
               + '\n'.join(f'  • {u}' for u in failed))
        yield _sse('batch_failed', msg)

    if not _is_stopped(job_id):
        ok = total - len(failed)
        yield _sse('done', f'{label} ✓  ({ok}/{total} succeeded)')


# ── 1. Video ───────────────────────────────────────────────────────────────

@vimeo_bp.route('/download', methods=['POST'])
def download():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls:
        return _stream(iter([_sse('error', 'No URL provided.')]))

    urls      = _split_urls(raw_urls)
    quality   = data.get('quality', 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best')
    fmt       = data.get('format', 'mp4')
    save_path = _expand(data.get('save_path', '~/Downloads'))
    overwrite = data.get('overwrite') == 'true'
    subs      = data.get('subs') == 'true'
    password  = data.get('password', '').strip()
    job_id    = uuid.uuid4().hex

    os.makedirs(save_path, exist_ok=True)

    def per_url(url):
        yield from _download_one_video(
            url, save_path, quality, fmt, overwrite, password, subs, job_id)

    return _stream(_run_batch(urls, per_url, job_id, 'Finished'))


# ── 2. MP3 ─────────────────────────────────────────────────────────────────

@vimeo_bp.route('/mp3', methods=['POST'])
def mp3():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls:
        return _stream(iter([_sse('error', 'No URL provided.')]))

    urls          = _split_urls(raw_urls)
    audio_quality = data.get('audio_quality', '0')
    overwrite     = data.get('overwrite') == 'true'
    thumbnail     = data.get('thumbnail', 'true') == 'true'
    metadata      = data.get('metadata', 'true') == 'true'
    password      = data.get('password', '').strip()
    save_path     = _expand(data.get('save_path', '~/Downloads'))
    job_id        = uuid.uuid4().hex

    os.makedirs(save_path, exist_ok=True)

    def per_url(url):
        if not _ffmpeg_available():
            yield _sse('log', '⚠  ffmpeg not found — audio saved as .m4a (install ffmpeg for .mp3)')
        yield from _download_one_mp3(
            url, save_path, audio_quality, overwrite, thumbnail, metadata, password, job_id)

    return _stream(_run_batch(urls, per_url, job_id, 'Finished'))


# ── 3. Transcript ──────────────────────────────────────────────────────────

def _strip_timestamps_from_srt(text: str) -> str:
    lines, out, i = text.splitlines(), [], 0
    while i < len(lines):
        line = lines[i].strip()
        if re.match(r'^\d+$', line):                                   i += 1; continue
        if re.match(r'^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->', line):      i += 1; continue
        if line in ('WEBVTT', '') and i < 3:                           i += 1; continue
        if line: out.append(line)
        i += 1
    deduped = []
    for line in out:
        if not deduped or line != deduped[-1]:
            deduped.append(line)
    return '\n'.join(deduped)


@vimeo_bp.route('/transcript', methods=['POST'])
def transcript():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls:
        return _stream(iter([_sse('error', 'No URL provided.')]))

    urls             = _split_urls(raw_urls)
    save_path        = _expand(data.get('save_path', '~/Downloads'))
    lang             = data.get('lang', 'en')
    sub_format       = data.get('sub_format', 'srt')
    overwrite        = data.get('overwrite') == 'true'
    strip_timestamps = data.get('strip_timestamps', 'false') == 'true'
    password         = data.get('password', '').strip()
    job_id           = uuid.uuid4().hex

    os.makedirs(save_path, exist_ok=True)

    def run_transcript(url):
        cmd = [_ytdlp(), '--skip-download',
               '-o', f'{save_path}/%(title)s.%(ext)s', '--newline',
               '--write-sub', '--write-auto-sub',
               '--sub-langs', lang,
               '--sub-format', f'{sub_format}/best']
        if overwrite: cmd += ['--force-overwrites']
        if password:  cmd += ['--video-password', password]
        cmd.append(url)

        output_lines = []
        yield _sse('cmd', ' '.join(cmd))
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            _register_job(job_id, proc)
            for raw in proc.stdout:
                if _is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue
                output_lines.append(line)
                yield _sse('log', line)
            proc.wait()
            rc = proc.returncode
            _remove_job(job_id)
        except Exception as exc:
            _remove_job(job_id)
            yield _sse('error', str(exc))
            return

        if _is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.'); return

        if rc != 0:
            _logger.error(f'vimeo transcript exit {rc} for {url}\n' + '\n'.join(output_lines))
            yield _sse('batch_error', url); return

        saved_files = []
        for line in output_lines:
            m = re.search(r'(?:Writing.*?to|Destination):\s*(.+\.(?:srt|vtt|ass|json3|srv\d))',
                          line, re.I)
            if m: saved_files.append(m.group(1).strip())

        combined    = '\n'.join(output_lines).lower()
        has_written = any(('writing' in l.lower() or 'destination' in l.lower())
                          for l in output_lines)

        if not has_written:
            if any(p in combined for p in ['no subtitles', 'has no subtitles']) or not output_lines:
                yield _sse('no_transcript', f'No transcript available: {url}')
                return
            yield _sse('done', 'Transcript downloaded ✓')
            return

        if strip_timestamps and saved_files:
            yield _sse('log', '── Removing timestamps…')
            for fpath in saved_files:
                try:
                    clean    = _strip_timestamps_from_srt(open(fpath, encoding='utf-8').read())
                    txt_path = re.sub(r'\.[^.]+$', '.txt', fpath)
                    open(txt_path, 'w', encoding='utf-8').write(clean)
                    yield _sse('log', f'   Saved → {txt_path}')
                except Exception as e:
                    yield _sse('log', f'   Strip error: {e}')

        yield _sse('done', f'Transcript saved ✓  →  {saved_files[0]}'
                   if saved_files else 'Transcript downloaded ✓')

    def generate():
        total, failed = len(urls), []
        if total > 1:
            yield _sse('log', f'📋  Batch: {total} URL(s)')
        for i, url in enumerate(urls, 1):
            if _is_stopped(job_id): break
            if total > 1: yield _sse('log', f'\n── [{i}/{total}] {url}')
            for chunk in run_transcript(url):
                if isinstance(chunk, str) and _extract_event(chunk) == 'batch_error':
                    failed.append(_extract_data(chunk))
                yield chunk
        if failed:
            yield _sse('batch_failed', 'Failed:\n' + '\n'.join(f'  • {u}' for u in failed))
        if not _is_stopped(job_id):
            yield _sse('done', f'Done ✓  ({len(urls)-len(failed)}/{len(urls)} succeeded)')

    return _stream(generate())
