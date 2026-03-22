"""
tools/yt_downloader/routes.py

Endpoints:
  GET  /yt/              - UI
  POST /yt/download      - Video download (SSE)
  POST /yt/mp3           - High-quality MP3 (SSE)
  POST /yt/transcript    - Subtitles/transcript (SSE)
  POST /yt/comments      - Comments as clean .txt (SSE)
  POST /yt/stop/<job_id> - Kill a specific running job
  GET  /yt/check_ffmpeg  - Check if ffmpeg is available
  GET  /yt/serve/<token> - Serve a completed file for browser download
  POST /yt/delete/<token>- Delete a staged file

Design notes:
  [1] Files saved directly to ~/Downloads (no hidden staging subfolder).
      A token map lets the browser trigger a named download via /serve/<token>.
  [2] Multi-URL batch: comma/newline-separated URLs; failed ones retry after 5 s
      and are logged to ~/Downloads/yt_errors.log.
  [3] Multi-user safe: per-job process registry keyed by uuid job_id.
"""
import subprocess, json, shutil, os, re, uuid, secrets, time, threading, logging
from flask import (Blueprint, render_template, request, Response,
                   stream_with_context, send_file, abort)

yt_bp = Blueprint('yt', __name__, template_folder='templates')

# ── Download destination ───────────────────────────────────────────────────
DOWNLOADS_DIR = os.path.expanduser('~/Downloads')
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Error log
ERROR_LOG = os.path.join(DOWNLOADS_DIR, 'yt_errors.log')
_log_handler = logging.FileHandler(ERROR_LOG)
_log_handler.setFormatter(logging.Formatter('%(asctime)s  %(message)s', '%Y-%m-%d %H:%M:%S'))
_logger = logging.getLogger('yt_downloader')
_logger.setLevel(logging.ERROR)
_logger.addHandler(_log_handler)

# ── Per-job process registry (multi-user safe) ─────────────────────────────
_jobs: dict = {}
_jobs_lock  = threading.Lock()

# token → absolute file path  (for /serve and /delete)
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

def _no_playlist(url: str) -> str:
    import urllib.parse as up
    try:
        p  = up.urlparse(url)
        qs = up.parse_qs(p.query, keep_blank_values=True)
        clean = {k: v for k, v in qs.items() if k in ('v', 't')}
        return up.urlunparse(p._replace(query=up.urlencode(clean, doseq=True)))
    except Exception:
        return url

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


def _run_cmd(cmd: list, job_id: str):
    """
    Generator: runs cmd, yields SSE strings.
    On exit yields sentinel tuple ('__rc__', returncode, output_lines).
    """
    lines = []
    yield _sse('cmd', ' '.join(cmd))
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        _register_job(job_id, proc)
        for raw in proc.stdout:
            if _is_stopped(job_id):
                break
            line = raw.rstrip('\n')
            if not line:
                continue
            lines.append(line)
            if re.search(r'\d+\.\d+%', line):
                yield _sse('progress', line)
            else:
                yield _sse('log', line)
        proc.wait()
        rc = proc.returncode
        _remove_job(job_id)
        yield ('__rc__', rc, lines)
    except Exception as exc:
        _remove_job(job_id)
        yield _sse('error', str(exc))
        yield ('__rc__', -1, lines)


# ── Stop / Check ───────────────────────────────────────────────────────────

@yt_bp.route('/stop/<job_id>', methods=['POST'])
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


@yt_bp.route('/check_ffmpeg')
def check_ffmpeg():
    return json.dumps({'available': _ffmpeg_available()})


# ── File serving (token-protected) ────────────────────────────────────────

@yt_bp.route('/serve/<token>')
def serve_file(token):
    fpath = _tokens.get(token)
    if not fpath or not os.path.exists(fpath):
        abort(404)
    ext  = fpath.rsplit('.', 1)[-1] if '.' in fpath else 'mp4'
    mime = 'audio/mpeg' if ext == 'mp3' else 'video/mp4'
    return send_file(fpath, mimetype=mime, as_attachment=True,
                     download_name=os.path.basename(fpath))

@yt_bp.route('/delete/<token>', methods=['POST'])
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

@yt_bp.route('/')
def index():
    return render_template('yt_downloader/index.html')


# ── Internal: one URL with retry ───────────────────────────────────────────

def _download_one(url, cmd_builder, job_id, retry_delay=5):
    """
    Tries cmd_builder(url) once; on failure waits retry_delay s and tries once more.
    Yields SSE strings. Success → yields _sse('ready', ...). Failure → _sse('batch_error', url).
    """
    for attempt in range(2):
        if _is_stopped(job_id):
            return

        if attempt == 1:
            yield _sse('log', f'⟳  Retrying in {retry_delay}s: {url}')
            time.sleep(retry_delay)

        cmd, out_path = cmd_builder(url)
        rc, lines = -1, []

        for chunk in _run_cmd(cmd, job_id):
            if isinstance(chunk, tuple) and chunk[0] == '__rc__':
                rc, lines = chunk[1], chunk[2]
            else:
                yield chunk

        if _is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.')
            return

        if rc == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            size_mb = round(os.path.getsize(out_path) / 1_048_576, 1)
            token   = secrets.token_urlsafe(32)
            _tokens[token] = out_path
            yield _sse('ready', json.dumps({
                'token':   token,
                'name':    os.path.basename(out_path),
                'size_mb': size_mb,
                'url':     url,
            }))
            return  # success

        # failure
        err_msg = f'yt-dlp exit {rc} for: {url}'
        if attempt == 0:
            yield _sse('log', f'⚠  Failed (attempt 1/2) — retrying in {retry_delay}s')
            _logger.error(f'[attempt 1/2] {err_msg}\n' + '\n'.join(lines))
        else:
            _logger.error(f'[attempt 2/2 FINAL] {err_msg}\n' + '\n'.join(lines))
            yield _sse('batch_error', url)


def _extract_event(sse_str: str):
    """Parse event name from a raw SSE string."""
    for line in sse_str.split('\n'):
        if line.startswith('event: '):
            return line[7:].strip()
    return 'log'

def _extract_data(sse_str: str):
    """Parse data text from a raw SSE string."""
    for line in sse_str.split('\n'):
        if line.startswith('data: '):
            try:
                return json.loads(line[6:])['text']
            except Exception:
                return line[6:]
    return ''


def _batch_runner(urls, cmd_builder, job_id, label):
    """Shared generator for video/mp3 batch runs."""
    total  = len(urls)
    failed = []

    if total > 1:
        yield _sse('log', f'📋  Batch: {total} URL(s) queued — saving to ~/Downloads')

    for i, url in enumerate(urls, 1):
        if _is_stopped(job_id):
            break
        if total > 1:
            yield _sse('log', f'\n── [{i}/{total}] {url}')

        for chunk in _download_one(url, cmd_builder, job_id):
            if isinstance(chunk, str) and _extract_event(chunk) == 'batch_error':
                failed.append(_extract_data(chunk))
            yield chunk

    if failed:
        msg = 'Failed URLs (logged to ~/Downloads/yt_errors.log):\n' + \
              '\n'.join(f'  • {u}' for u in failed)
        yield _sse('batch_failed', msg)

    if not _is_stopped(job_id):
        ok = total - len(failed)
        yield _sse('done', f'{label} ✓  ({ok}/{total} succeeded)')


# ── 1. Video ───────────────────────────────────────────────────────────────

@yt_bp.route('/download', methods=['POST'])
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
    android   = data.get('android', 'true') == 'true'
    is_audio  = quality.startswith('bestaudio')
    job_id    = uuid.uuid4().hex

    os.makedirs(save_path, exist_ok=True)

    def cmd_builder(url):
        # Output template: ~/Downloads/Title.ext  (no uuid prefix needed — token handles identity)
        out_tmpl = os.path.join(save_path, '%(title)s.%(ext)s')
        cmd = [_ytdlp(), '--no-playlist']
        if is_audio:
            cmd += ['-f', quality, '-x', '--audio-format',
                    fmt if fmt in ('mp3', 'm4a', 'opus', 'flac') else 'mp3']
        else:
            cmd += ['-f', quality, '--merge-output-format', fmt]
        cmd += ['-o', out_tmpl, '--progress', '--no-mtime', '--newline',
                '--print', 'after_move:filepath']   # prints final path to stdout
        if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
        if overwrite: cmd += ['--no-continue', '--force-overwrites']
        if subs:      cmd += ['--write-auto-sub', '--embed-subs']
        cmd.append(_no_playlist(url))
        # out_path is resolved from yt-dlp stdout via --print after_move:filepath
        return cmd, save_path   # we'll scan stdout for the real path below

    # Override _download_one for video because we capture the output path from stdout
    def download_one_video(url, job_id, retry_delay=5):
        for attempt in range(2):
            if _is_stopped(job_id):
                return

            if attempt == 1:
                yield _sse('log', f'⟳  Retrying in {retry_delay}s: {url}')
                time.sleep(retry_delay)

            cmd = [_ytdlp(), '--no-playlist']
            if is_audio:
                cmd += ['-f', quality, '-x', '--audio-format',
                        fmt if fmt in ('mp3', 'm4a', 'opus', 'flac') else 'mp3']
            else:
                cmd += ['-f', quality, '--merge-output-format', fmt]
            cmd += ['-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                    '--progress', '--no-mtime', '--newline',
                    '--print', 'after_move:filepath']
            if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
            if overwrite: cmd += ['--no-continue', '--force-overwrites']
            if subs:      cmd += ['--write-auto-sub', '--embed-subs']
            cmd.append(_no_playlist(url))

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
                    # --print after_move:filepath outputs the final absolute path
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

            err_msg = f'yt-dlp exit {rc} for: {url}'
            if attempt == 0:
                yield _sse('log', f'⚠  Failed (attempt 1/2) — retrying in {retry_delay}s')
                _logger.error(f'[attempt 1/2] {err_msg}\n' + '\n'.join(lines))
            else:
                _logger.error(f'[attempt 2/2 FINAL] {err_msg}\n' + '\n'.join(lines))
                yield _sse('batch_error', url)

    def generate():
        total  = len(urls)
        failed = []

        if total > 1:
            yield _sse('log', f'📋  Batch: {total} URL(s) — saving to {save_path}')

        for i, url in enumerate(urls, 1):
            if _is_stopped(job_id): break
            if total > 1:
                yield _sse('log', f'\n── [{i}/{total}] {url}')

            for chunk in download_one_video(url, job_id):
                if isinstance(chunk, str) and _extract_event(chunk) == 'batch_error':
                    failed.append(_extract_data(chunk))
                yield chunk

        if failed:
            msg = 'Failed URLs (logged to ~/Downloads/yt_errors.log):\n' + \
                  '\n'.join(f'  • {u}' for u in failed)
            yield _sse('batch_failed', msg)

        if not _is_stopped(job_id):
            ok = total - len(failed)
            yield _sse('done', f'Finished ✓  ({ok}/{total} succeeded)')

    return _stream(generate())


# ── 2. MP3 ─────────────────────────────────────────────────────────────────

@yt_bp.route('/mp3', methods=['POST'])
def mp3():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls:
        return _stream(iter([_sse('error', 'No URL provided.')]))

    urls       = _split_urls(raw_urls)
    quality    = data.get('audio_quality', '0')
    overwrite  = data.get('overwrite') == 'true'
    android    = data.get('android', 'true') == 'true'
    thumbnail  = data.get('thumbnail', 'true') == 'true'
    metadata   = data.get('metadata', 'true') == 'true'
    has_ffmpeg = _ffmpeg_available()
    job_id     = uuid.uuid4().hex

    def download_one_mp3(url, job_id, retry_delay=5):
        for attempt in range(2):
            if _is_stopped(job_id): return
            if attempt == 1:
                yield _sse('log', f'⟳  Retrying in {retry_delay}s: {url}')
                time.sleep(retry_delay)

            save_path = DOWNLOADS_DIR
            if has_ffmpeg:
                cmd = [_ytdlp(), '--no-playlist', '-f', 'bestaudio/best',
                       '-x', '--audio-format', 'mp3', '--audio-quality', quality,
                       '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                       '--progress', '--no-mtime', '--newline',
                       '--print', 'after_move:filepath']
                if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
                if thumbnail: cmd += ['--embed-thumbnail', '--convert-thumbnails', 'jpg']
                if metadata:  cmd += ['--embed-metadata']
                if overwrite: cmd += ['--no-continue', '--force-overwrites']
            else:
                cmd = [_ytdlp(), '--no-playlist', '-f', 'bestaudio[ext=m4a]/bestaudio/best',
                       '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                       '--progress', '--no-mtime', '--newline',
                       '--print', 'after_move:filepath']
                if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
                if overwrite: cmd += ['--no-continue', '--force-overwrites']
            cmd.append(_no_playlist(url))

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

            err_msg = f'yt-dlp exit {rc} for: {url}'
            if attempt == 0:
                yield _sse('log', f'⚠  Failed (attempt 1/2) — retrying in {retry_delay}s')
                _logger.error(f'[attempt 1/2] {err_msg}\n' + '\n'.join(lines))
            else:
                _logger.error(f'[attempt 2/2 FINAL] {err_msg}\n' + '\n'.join(lines))
                yield _sse('batch_error', url)

    def generate():
        total  = len(urls)
        failed = []

        if not has_ffmpeg:
            yield _sse('log', '⚠  ffmpeg not found — audio saved as .m4a (install ffmpeg for .mp3)')

        if total > 1:
            yield _sse('log', f'📋  Batch: {total} URL(s) — saving to ~/Downloads')

        for i, url in enumerate(urls, 1):
            if _is_stopped(job_id): break
            if total > 1:
                yield _sse('log', f'\n── [{i}/{total}] {url}')

            for chunk in download_one_mp3(url, job_id):
                if isinstance(chunk, str) and _extract_event(chunk) == 'batch_error':
                    failed.append(_extract_data(chunk))
                yield chunk

        if failed:
            msg = 'Failed URLs (logged to ~/Downloads/yt_errors.log):\n' + \
                  '\n'.join(f'  • {u}' for u in failed)
            yield _sse('batch_failed', msg)

        if not _is_stopped(job_id):
            ok = total - len(failed)
            yield _sse('done', f'Finished ✓  ({ok}/{total} succeeded)')

    return _stream(generate())


# ── 3. Transcript ──────────────────────────────────────────────────────────

def _strip_timestamps_from_srt(text: str) -> str:
    lines = text.splitlines()
    out, i = [], 0
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


@yt_bp.route('/transcript', methods=['POST'])
def transcript():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls:
        return _stream(iter([_sse('error', 'No URL provided.')]))

    urls             = _split_urls(raw_urls)
    save_path        = _expand(data.get('save_path', '~/Downloads'))
    lang             = data.get('lang', 'en')
    sub_format       = data.get('sub_format', 'srt')
    auto_subs        = data.get('auto_subs', 'true') == 'true'
    manual           = data.get('manual_subs', 'true') == 'true'
    overwrite        = data.get('overwrite') == 'true'
    strip_timestamps = data.get('strip_timestamps', 'false') == 'true'
    job_id           = uuid.uuid4().hex

    os.makedirs(save_path, exist_ok=True)

    def run_transcript(url):
        cmd = [_ytdlp(), '--no-playlist', '--skip-download',
               '-o', f'{save_path}/%(title)s.%(ext)s', '--newline']
        if manual:    cmd += ['--write-sub']
        if auto_subs: cmd += ['--write-auto-sub']
        if not manual and not auto_subs:
            cmd += ['--write-sub', '--write-auto-sub']
        cmd += ['--sub-langs', lang, '--sub-format', f'{sub_format}/best']
        if overwrite: cmd += ['--force-overwrites']
        cmd.append(_no_playlist(url))

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
            _logger.error(f'transcript exit {rc} for {url}\n' + '\n'.join(output_lines))
            yield _sse('batch_error', url); return

        saved_files = []
        for line in output_lines:
            m = re.search(r'(?:Writing.*?to|Destination):\s*(.+\.(?:srt|vtt|ass|json3|srv\d))', line, re.I)
            if m: saved_files.append(m.group(1).strip())

        combined    = '\n'.join(output_lines).lower()
        has_written = any(('writing' in l.lower() or 'destination' in l.lower()) for l in output_lines)

        if not has_written:
            if any(p in combined for p in ['no subtitles', 'has no subtitles']) or not output_lines:
                yield _sse('no_transcript', f'No transcript available: {url}'); return
            yield _sse('done', f'Transcript downloaded ✓'); return

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

        yield _sse('done', f'Transcript saved ✓  →  {saved_files[0]}' if saved_files else 'Transcript downloaded ✓')

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


# ── 4. Comments ────────────────────────────────────────────────────────────

@yt_bp.route('/comments', methods=['POST'])
def comments():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls:
        return _stream(iter([_sse('error', 'No URL provided.')]))

    urls         = _split_urls(raw_urls)
    save_path    = _expand(data.get('save_path', '~/Downloads'))
    max_comments = data.get('max_comments', '200')
    sort_by      = data.get('sort_by', 'top')
    overwrite    = data.get('overwrite') == 'true'
    job_id       = uuid.uuid4().hex

    os.makedirs(save_path, exist_ok=True)

    def run_comments(url):
        cmd = [_ytdlp(), '--no-playlist', '--skip-download',
               '--write-info-json', '--write-comments',
               '-o', f'{save_path}/%(title)s.%(ext)s', '--newline']
        if max_comments and str(max_comments).lower() != 'all':
            try:
                cmd += ['--extractor-args',
                        f'youtube:max_comments={int(max_comments)},comment_sort={sort_by}']
            except ValueError:
                pass
        if overwrite: cmd += ['--force-overwrites']
        cmd.append(_no_playlist(url))

        output_lines, json_file = [], None
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
                m = re.search(r'\[info\] Writing video metadata as JSON to:\s*(.+\.info\.json)', line)
                if m: json_file = m.group(1).strip()
            proc.wait()
            rc = proc.returncode
            _remove_job(job_id)
        except Exception as exc:
            _remove_job(job_id)
            yield _sse('error', str(exc)); return

        if _is_stopped(job_id):
            yield _sse('stopped', 'Stopped by user.'); return
        if rc != 0:
            _logger.error(f'comments exit {rc} for {url}\n' + '\n'.join(output_lines))
            yield _sse('batch_error', url); return

        if not json_file:
            import glob as _glob
            candidates = _glob.glob(os.path.join(save_path, '*.info.json'))
            if candidates: json_file = max(candidates, key=os.path.getmtime)

        if not json_file or not os.path.exists(json_file):
            yield _sse('error', f'Could not find info JSON for {url}'); return

        yield _sse('log', '── Converting comments to text…')
        try:
            info = json.load(open(json_file, encoding='utf-8'))
            raw_comments = info.get('comments', [])
            if not raw_comments:
                yield _sse('no_transcript', f'No comments: {url}'); return

            lines_out = [
                f'Comments for: {info.get("title","?")}',
                f'Channel: {info.get("uploader","?")}',
                f'URL: {info.get("webpage_url", url)}',
                f'Total: {len(raw_comments)}', '='*60, '',
            ]
            for i, c in enumerate(raw_comments, 1):
                is_reply = c.get('parent') not in (None, 'root')
                prefix   = '  ↳ ' if is_reply else ''
                lines_out.append(f'{prefix}[{i}] {c.get("author","?")}  ·  👍 {c.get("like_count",0)}')
                for tl in c.get('text','').strip().splitlines():
                    lines_out.append(f'{prefix}    {tl}')
                lines_out.append('')

            txt_path = re.sub(r'\.info\.json$', '.comments.txt', json_file)
            open(txt_path, 'w', encoding='utf-8').write('\n'.join(lines_out))
            os.remove(json_file)
            yield _sse('done', f'{len(raw_comments)} comments saved ✓  →  {txt_path}')
        except Exception as exc:
            yield _sse('error', f'Convert error: {exc}')

    def generate():
        total, failed = len(urls), []
        if total > 1: yield _sse('log', f'📋  Batch: {total} URL(s)')

        for i, url in enumerate(urls, 1):
            if _is_stopped(job_id): break
            if total > 1: yield _sse('log', f'\n── [{i}/{total}] {url}')
            for chunk in run_comments(url):
                if isinstance(chunk, str) and _extract_event(chunk) == 'batch_error':
                    failed.append(_extract_data(chunk))
                yield chunk

        if failed:
            yield _sse('batch_failed', 'Failed:\n' + '\n'.join(f'  • {u}' for u in failed))
        if not _is_stopped(job_id):
            yield _sse('done', f'Done ✓  ({len(urls)-len(failed)}/{len(urls)} succeeded)')

    return _stream(generate())
