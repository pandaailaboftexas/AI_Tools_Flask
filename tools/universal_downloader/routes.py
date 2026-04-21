"""
tools/universal_downloader/routes.py
Works with 1000+ sites yt-dlp supports: Twitter/X, Instagram, TikTok,
SoundCloud, Twitch, Reddit, Facebook, Dailymotion, and many more.
Same architecture as yt_downloader but without YouTube-specific flags.
"""
import subprocess, json, shutil, os, re, uuid, secrets, time
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

uni_bp  = Blueprint('uni', __name__, template_folder='templates')
_logger = get_logger('universal_downloader')

# ── Helpers ────────────────────────────────────────────────────────────────

def _ytdlp():  return shutil.which('yt-dlp') or 'yt-dlp'
def _ffmpeg():  return shutil.which('ffmpeg') is not None
def _sse(e, d): return f'event: {e}\ndata: {json.dumps({"text": d})}\n\n'
def _expand(p): return os.path.expanduser((p or '~/Downloads').strip())

def _stream(gen):
    return Response(stream_with_context(gen), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

def _split_urls(raw):
    return [p.strip() for p in re.split(r'[\n,]+', raw) if p.strip()]

def _ev(s):
    for line in s.split('\n'):
        if line.startswith('event: '): return line[7:].strip()
    return 'log'

def _dv(s):
    for line in s.split('\n'):
        if line.startswith('data: '):
            try: return json.loads(line[6:])['text']
            except Exception: return line[6:]
    return ''

# ── Stop / Check ───────────────────────────────────────────────────────────

@uni_bp.route('/stop/<job_id>', methods=['POST'])
def stop(job_id): return json.dumps(stop_job(job_id))

@uni_bp.route('/check_ffmpeg')
def check_ffmpeg(): return json.dumps({'available': _ffmpeg()})

# ── File serving ───────────────────────────────────────────────────────────

@uni_bp.route('/serve/<token>')
def serve_file(token):
    fpath = get_token_path(token)
    if not fpath or not os.path.exists(fpath): abort(404)
    ext  = fpath.rsplit('.', 1)[-1] if '.' in fpath else 'mp4'
    mime = 'audio/mpeg' if ext == 'mp3' else 'video/mp4'
    return send_file(fpath, mimetype=mime, as_attachment=True,
                     download_name=os.path.basename(fpath))

@uni_bp.route('/delete/<token>', methods=['POST'])
def delete_file(token):
    fpath = pop_token(token)
    if not fpath: return json.dumps({'ok': False, 'msg': 'Invalid token'})
    try:
        if os.path.exists(fpath): os.remove(fpath)
        return json.dumps({'ok': True})
    except Exception as e:
        return json.dumps({'ok': False, 'msg': str(e)})

@uni_bp.route('/')
def index(): return render_template('universal_downloader/index.html')

# ── Core: download video ───────────────────────────────────────────────────

def _dl_video(url, save_path, quality, fmt, overwrite, subs, job_id):
    is_audio = quality.startswith('bestaudio')
    for attempt, sleep_s in retry_delays():
        if is_stopped(job_id): return
        if attempt > 0:
            yield _sse('log', f'⟳  Retry {attempt}/6 in {sleep_s}s — {url}')
            time.sleep(sleep_s)
        if is_stopped(job_id): return

        cmd = [_ytdlp()]
        if is_audio:
            cmd += ['-f', quality, '-x', '--audio-format',
                    fmt if fmt in ('mp3','m4a','opus','flac') else 'mp3']
        else:
            cmd += ['-f', quality, '--merge-output-format', fmt]
        cmd += ['-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                '--progress', '--no-mtime', '--newline', '--print', 'after_move:filepath']
        if overwrite: cmd += ['--no-continue', '--force-overwrites']
        if subs:      cmd += ['--write-auto-sub', '--embed-subs']
        cmd.append(url)

        yield _sse('cmd', ' '.join(cmd))
        rc, lines, final_path = -1, [], None
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            register_job(job_id, proc)
            for raw in proc.stdout:
                if is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue
                lines.append(line)
                if os.sep in line and os.path.exists(line.strip()): final_path = line.strip()
                elif re.search(r'\d+\.\d+%', line): yield _sse('progress', line)
                else: yield _sse('log', line)
            proc.wait(); rc = proc.returncode; remove_job(job_id)
        except Exception as exc:
            remove_job(job_id); yield _sse('error', str(exc)); return

        if is_stopped(job_id): yield _sse('stopped', 'Stopped by user.'); return

        if rc == 0 and final_path and os.path.exists(final_path):
            size_mb = round(os.path.getsize(final_path) / 1_048_576, 1)
            token   = secrets.token_urlsafe(32)
            register_token(token, final_path)
            yield _sse('ready', json.dumps({'token': token,
                'name': os.path.basename(final_path), 'size_mb': size_mb, 'url': url}))
            return

        _logger.error(f'[uni video attempt {attempt+1}] exit {rc} for {url}\n' + '\n'.join(lines))
        if attempt == 0:
            yield _sse('log', '⚠  Failed (attempt 1) — will retry with random delay')

    yield _sse('batch_error', url)

# ── Core: download MP3 ─────────────────────────────────────────────────────

def _dl_mp3(url, save_path, quality, overwrite, thumbnail, metadata, job_id):
    has_ff = _ffmpeg()
    for attempt, sleep_s in retry_delays():
        if is_stopped(job_id): return
        if attempt > 0:
            yield _sse('log', f'⟳  Retry {attempt}/6 in {sleep_s}s — {url}')
            time.sleep(sleep_s)
        if is_stopped(job_id): return

        if has_ff:
            cmd = [_ytdlp(), '-f', 'bestaudio/best', '-x', '--audio-format', 'mp3',
                   '--audio-quality', quality,
                   '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                   '--progress', '--no-mtime', '--newline', '--print', 'after_move:filepath']
            if thumbnail: cmd += ['--embed-thumbnail', '--convert-thumbnails', 'jpg']
            if metadata:  cmd += ['--embed-metadata']
        else:
            cmd = [_ytdlp(), '-f', 'bestaudio[ext=m4a]/bestaudio/best',
                   '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                   '--progress', '--no-mtime', '--newline', '--print', 'after_move:filepath']
        if overwrite: cmd += ['--no-continue', '--force-overwrites']
        cmd.append(url)

        yield _sse('cmd', ' '.join(cmd))
        rc, lines, final_path = -1, [], None
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            register_job(job_id, proc)
            for raw in proc.stdout:
                if is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue
                lines.append(line)
                if os.sep in line and os.path.exists(line.strip()): final_path = line.strip()
                elif re.search(r'\d+\.\d+%', line): yield _sse('progress', line)
                else: yield _sse('log', line)
            proc.wait(); rc = proc.returncode; remove_job(job_id)
        except Exception as exc:
            remove_job(job_id); yield _sse('error', str(exc)); return

        if is_stopped(job_id): yield _sse('stopped', 'Stopped by user.'); return

        if rc == 0 and final_path and os.path.exists(final_path):
            size_mb = round(os.path.getsize(final_path) / 1_048_576, 1)
            token   = secrets.token_urlsafe(32)
            register_token(token, final_path)
            yield _sse('ready', json.dumps({'token': token,
                'name': os.path.basename(final_path), 'size_mb': size_mb, 'url': url}))
            return

        _logger.error(f'[uni mp3 attempt {attempt+1}] exit {rc} for {url}\n' + '\n'.join(lines))
        if attempt == 0:
            yield _sse('log', '⚠  Failed (attempt 1) — will retry with random delay')

    yield _sse('batch_error', url)

# ── Batch runner ───────────────────────────────────────────────────────────

def _batch(urls, per_url_gen, job_id, label):
    total, failed = len(urls), []
    if total > 1: yield _sse('log', f'📋  Batch: {total} URL(s) — saving to ~/Downloads')
    for i, url in enumerate(urls, 1):
        if is_stopped(job_id): break
        if total > 1: yield _sse('log', f'\n── [{i}/{total}] {url}')
        for chunk in per_url_gen(url):
            if isinstance(chunk, str) and _ev(chunk) == 'batch_error':
                failed.append(_dv(chunk))
            yield chunk
    if failed:
        yield _sse('batch_failed',
            'Failed URLs (logged to ~/Downloads/yt_errors.log):\n'
            + '\n'.join(f'  • {u}' for u in failed))
    if not is_stopped(job_id):
        yield _sse('done', f'{label} ✓  ({len(urls)-len(failed)}/{len(urls)} succeeded)')

# ── 1. Video ───────────────────────────────────────────────────────────────

@uni_bp.route('/download', methods=['POST'])
def download():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls: return _stream(iter([_sse('error', 'No URL provided.')]))
    if not acquire_slot():
        return _stream(iter([_sse('error', 'Server busy — too many downloads. Try again shortly.')]))

    urls      = _split_urls(raw_urls)
    quality   = data.get('quality', 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best')
    fmt       = data.get('format', 'mp4')
    save_path = _expand(data.get('save_path', '~/Downloads'))
    overwrite = data.get('overwrite') == 'true'
    subs      = data.get('subs') == 'true'
    job_id    = uuid.uuid4().hex
    os.makedirs(save_path, exist_ok=True)

    def generate():
        increment_active()
        try:
            yield from _batch(urls,
                lambda url: _dl_video(url, save_path, quality, fmt, overwrite, subs, job_id),
                job_id, 'Finished')
        finally:
            decrement_active(); release_slot()

    return _stream(generate())

# ── 2. MP3 ─────────────────────────────────────────────────────────────────

@uni_bp.route('/mp3', methods=['POST'])
def mp3():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls: return _stream(iter([_sse('error', 'No URL provided.')]))
    if not acquire_slot():
        return _stream(iter([_sse('error', 'Server busy — too many downloads. Try again shortly.')]))

    urls       = _split_urls(raw_urls)
    quality    = data.get('audio_quality', '0')
    overwrite  = data.get('overwrite') == 'true'
    thumbnail  = data.get('thumbnail', 'true') == 'true'
    metadata   = data.get('metadata', 'true') == 'true'
    save_path  = _expand(data.get('save_path', '~/Downloads'))
    job_id     = uuid.uuid4().hex
    os.makedirs(save_path, exist_ok=True)

    def generate():
        increment_active()
        try:
            if not _ffmpeg():
                yield _sse('log', '⚠  ffmpeg not found — audio saved as .m4a')
            yield from _batch(urls,
                lambda url: _dl_mp3(url, save_path, quality, overwrite,
                                    thumbnail, metadata, job_id),
                job_id, 'Finished')
        finally:
            decrement_active(); release_slot()

    return _stream(generate())
