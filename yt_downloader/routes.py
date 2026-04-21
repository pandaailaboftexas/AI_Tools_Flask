"""
tools/yt_downloader/routes.py
Uses tools.shared for job registry, tokens, semaphore, cleanup, counter.
7-attempt retry with random 2-7 s delay between attempts (anti-bot).
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

yt_bp   = Blueprint('yt', __name__, template_folder='templates')
_logger = get_logger('yt_downloader')

# ── Helpers ────────────────────────────────────────────────────────────────

def _ytdlp():  return shutil.which('yt-dlp') or 'yt-dlp'
def _ffmpeg():  return shutil.which('ffmpeg') is not None
def _sse(e, d): return f'event: {e}\ndata: {json.dumps({"text": d})}\n\n'
def _expand(p): return os.path.expanduser((p or '~/Downloads').strip())

def _stream(gen):
    return Response(stream_with_context(gen), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

def _no_playlist(url):
    import urllib.parse as up
    try:
        p  = up.urlparse(url)
        qs = up.parse_qs(p.query, keep_blank_values=True)
        clean = {k: v for k, v in qs.items() if k in ('v', 't')}
        return up.urlunparse(p._replace(query=up.urlencode(clean, doseq=True)))
    except Exception:
        return url

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

@yt_bp.route('/stop/<job_id>', methods=['POST'])
def stop(job_id): return json.dumps(stop_job(job_id))

@yt_bp.route('/check_ffmpeg')
def check_ffmpeg(): return json.dumps({'available': _ffmpeg()})

# ── File serving ───────────────────────────────────────────────────────────

@yt_bp.route('/serve/<token>')
def serve_file(token):
    fpath = get_token_path(token)
    if not fpath or not os.path.exists(fpath): abort(404)
    ext  = fpath.rsplit('.', 1)[-1] if '.' in fpath else 'mp4'
    mime = 'audio/mpeg' if ext == 'mp3' else 'video/mp4'
    return send_file(fpath, mimetype=mime, as_attachment=True,
                     download_name=os.path.basename(fpath))

@yt_bp.route('/delete/<token>', methods=['POST'])
def delete_file(token):
    fpath = pop_token(token)
    if not fpath: return json.dumps({'ok': False, 'msg': 'Invalid token'})
    try:
        if os.path.exists(fpath): os.remove(fpath)
        return json.dumps({'ok': True})
    except Exception as e:
        return json.dumps({'ok': False, 'msg': str(e)})

@yt_bp.route('/')
def index(): return render_template('yt_downloader/index.html')

# ── Core: download video (captures final path from --print after_move) ─────

def _dl_video(url, save_path, quality, fmt, overwrite, android, subs, job_id):
    is_audio = quality.startswith('bestaudio')
    for attempt, sleep_s in retry_delays():
        if is_stopped(job_id): return
        if attempt > 0:
            yield _sse('log', f'⟳  Retry {attempt}/6 in {sleep_s}s — {url}')
            time.sleep(sleep_s)
        if is_stopped(job_id): return

        cmd = [_ytdlp(), '--no-playlist']
        if is_audio:
            cmd += ['-f', quality, '-x', '--audio-format',
                    fmt if fmt in ('mp3','m4a','opus','flac') else 'mp3']
        else:
            cmd += ['-f', quality, '--merge-output-format', fmt]
        cmd += ['-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                '--progress', '--no-mtime', '--newline', '--print', 'after_move:filepath']
        if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
        if overwrite: cmd += ['--no-continue', '--force-overwrites']
        if subs:      cmd += ['--write-auto-sub', '--embed-subs']
        cmd.append(_no_playlist(url))

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
                if os.sep in line and os.path.exists(line.strip()):
                    final_path = line.strip()
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

        _logger.error(f'[yt video attempt {attempt+1}] exit {rc} for {url}\n' + '\n'.join(lines))
        if attempt == 0:
            yield _sse('log', '⚠  Failed (attempt 1) — will retry with random delay')

    yield _sse('batch_error', url)

# ── Core: download MP3 ─────────────────────────────────────────────────────

def _dl_mp3(url, save_path, quality, overwrite, android, thumbnail, metadata, job_id):
    has_ff = _ffmpeg()
    for attempt, sleep_s in retry_delays():
        if is_stopped(job_id): return
        if attempt > 0:
            yield _sse('log', f'⟳  Retry {attempt}/6 in {sleep_s}s — {url}')
            time.sleep(sleep_s)
        if is_stopped(job_id): return

        if has_ff:
            cmd = [_ytdlp(), '--no-playlist', '-f', 'bestaudio/best',
                   '-x', '--audio-format', 'mp3', '--audio-quality', quality,
                   '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                   '--progress', '--no-mtime', '--newline', '--print', 'after_move:filepath']
            if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
            if thumbnail: cmd += ['--embed-thumbnail', '--convert-thumbnails', 'jpg']
            if metadata:  cmd += ['--embed-metadata']
            if overwrite: cmd += ['--no-continue', '--force-overwrites']
        else:
            cmd = [_ytdlp(), '--no-playlist', '-f', 'bestaudio[ext=m4a]/bestaudio/best',
                   '-o', os.path.join(save_path, '%(title)s.%(ext)s'),
                   '--progress', '--no-mtime', '--newline', '--print', 'after_move:filepath']
            if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
            if overwrite: cmd += ['--no-continue', '--force-overwrites']
        cmd.append(_no_playlist(url))

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

        _logger.error(f'[yt mp3 attempt {attempt+1}] exit {rc} for {url}\n' + '\n'.join(lines))
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

@yt_bp.route('/download', methods=['POST'])
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
    android   = data.get('android', 'true') == 'true'
    job_id    = uuid.uuid4().hex
    os.makedirs(save_path, exist_ok=True)

    def generate():
        increment_active()
        try:
            yield from _batch(urls,
                lambda url: _dl_video(url, save_path, quality, fmt, overwrite, android, subs, job_id),
                job_id, 'Finished')
        finally:
            decrement_active(); release_slot()

    return _stream(generate())

# ── 2. MP3 ─────────────────────────────────────────────────────────────────

@yt_bp.route('/mp3', methods=['POST'])
def mp3():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls: return _stream(iter([_sse('error', 'No URL provided.')]))
    if not acquire_slot():
        return _stream(iter([_sse('error', 'Server busy — too many downloads. Try again shortly.')]))

    urls       = _split_urls(raw_urls)
    quality    = data.get('audio_quality', '0')
    overwrite  = data.get('overwrite') == 'true'
    android    = data.get('android', 'true') == 'true'
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
                                    android, thumbnail, metadata, job_id),
                job_id, 'Finished')
        finally:
            decrement_active(); release_slot()

    return _stream(generate())

# ── 3. Transcript ──────────────────────────────────────────────────────────

def _strip_srt(text):
    lines, out, i = text.splitlines(), [], 0
    while i < len(lines):
        line = lines[i].strip()
        if re.match(r'^\d+$', line): i += 1; continue
        if re.match(r'^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->', line): i += 1; continue
        if line in ('WEBVTT', '') and i < 3: i += 1; continue
        if line: out.append(line)
        i += 1
    deduped = []
    for line in out:
        if not deduped or line != deduped[-1]: deduped.append(line)
    return '\n'.join(deduped)

@yt_bp.route('/transcript', methods=['POST'])
def transcript():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls: return _stream(iter([_sse('error', 'No URL provided.')]))

    urls             = _split_urls(raw_urls)
    save_path        = _expand(data.get('save_path', '~/Downloads'))
    lang             = data.get('lang', 'en')
    sub_format       = data.get('sub_format', 'srt')
    auto_subs        = data.get('auto_subs', 'true') == 'true'
    manual           = data.get('manual_subs', 'true') == 'true'
    overwrite        = data.get('overwrite') == 'true'
    strip_ts         = data.get('strip_timestamps', 'false') == 'true'
    job_id           = uuid.uuid4().hex
    os.makedirs(save_path, exist_ok=True)

    def run_one(url):
        cmd = [_ytdlp(), '--no-playlist', '--skip-download',
               '-o', f'{save_path}/%(title)s.%(ext)s', '--newline']
        if manual:    cmd += ['--write-sub']
        if auto_subs: cmd += ['--write-auto-sub']
        if not manual and not auto_subs: cmd += ['--write-sub', '--write-auto-sub']
        cmd += ['--sub-langs', lang, '--sub-format', f'{sub_format}/best']
        if overwrite: cmd += ['--force-overwrites']
        cmd.append(_no_playlist(url))

        out_lines = []
        yield _sse('cmd', ' '.join(cmd))
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            register_job(job_id, proc)
            for raw in proc.stdout:
                if is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue
                out_lines.append(line); yield _sse('log', line)
            proc.wait(); rc = proc.returncode; remove_job(job_id)
        except Exception as exc:
            remove_job(job_id); yield _sse('error', str(exc)); return

        if is_stopped(job_id): yield _sse('stopped', 'Stopped by user.'); return
        if rc != 0:
            _logger.error(f'transcript exit {rc} for {url}\n' + '\n'.join(out_lines))
            yield _sse('batch_error', url); return

        saved = []
        for line in out_lines:
            m = re.search(r'(?:Writing.*?to|Destination):\s*(.+\.(?:srt|vtt|ass|json3|srv\d))', line, re.I)
            if m: saved.append(m.group(1).strip())

        has_written = any(('writing' in l.lower() or 'destination' in l.lower()) for l in out_lines)
        combined = '\n'.join(out_lines).lower()

        if not has_written:
            if any(p in combined for p in ['no subtitles', 'has no subtitles']) or not out_lines:
                yield _sse('no_transcript', f'No transcript available: {url}'); return
            yield _sse('done', 'Transcript downloaded ✓'); return

        if strip_ts and saved:
            yield _sse('log', '── Removing timestamps…')
            for fpath in saved:
                try:
                    clean = _strip_srt(open(fpath, encoding='utf-8').read())
                    txt = re.sub(r'\.[^.]+$', '.txt', fpath)
                    open(txt, 'w', encoding='utf-8').write(clean)
                    yield _sse('log', f'   Saved → {txt}')
                except Exception as e: yield _sse('log', f'   Strip error: {e}')

        yield _sse('done', f'Transcript saved ✓  →  {saved[0]}' if saved else 'Transcript downloaded ✓')

    def generate():
        total, failed = len(urls), []
        if total > 1: yield _sse('log', f'📋  Batch: {total} URL(s)')
        for i, url in enumerate(urls, 1):
            if is_stopped(job_id): break
            if total > 1: yield _sse('log', f'\n── [{i}/{total}] {url}')
            for chunk in run_one(url):
                if isinstance(chunk, str) and _ev(chunk) == 'batch_error':
                    failed.append(_dv(chunk))
                yield chunk
        if failed:
            yield _sse('batch_failed', 'Failed:\n' + '\n'.join(f'  • {u}' for u in failed))
        if not is_stopped(job_id):
            yield _sse('done', f'Done ✓  ({len(urls)-len(failed)}/{len(urls)} succeeded)')

    return _stream(generate())

# ── 4. Comments ────────────────────────────────────────────────────────────

@yt_bp.route('/comments', methods=['POST'])
def comments():
    data = request.get_json(force=True) or {}
    raw_urls = data.get('url', '').strip()
    if not raw_urls: return _stream(iter([_sse('error', 'No URL provided.')]))

    urls         = _split_urls(raw_urls)
    save_path    = _expand(data.get('save_path', '~/Downloads'))
    max_comments = data.get('max_comments', '200')
    sort_by      = data.get('sort_by', 'top')
    overwrite    = data.get('overwrite') == 'true'
    job_id       = uuid.uuid4().hex
    os.makedirs(save_path, exist_ok=True)

    def run_one(url):
        cmd = [_ytdlp(), '--no-playlist', '--skip-download',
               '--write-info-json', '--write-comments',
               '-o', f'{save_path}/%(title)s.%(ext)s', '--newline']
        if max_comments and str(max_comments).lower() != 'all':
            try: cmd += ['--extractor-args',
                         f'youtube:max_comments={int(max_comments)},comment_sort={sort_by}']
            except ValueError: pass
        if overwrite: cmd += ['--force-overwrites']
        cmd.append(_no_playlist(url))

        out_lines, json_file = [], None
        yield _sse('cmd', ' '.join(cmd))
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            register_job(job_id, proc)
            for raw in proc.stdout:
                if is_stopped(job_id): break
                line = raw.rstrip('\n')
                if not line: continue
                out_lines.append(line); yield _sse('log', line)
                m = re.search(r'\[info\] Writing video metadata as JSON to:\s*(.+\.info\.json)', line)
                if m: json_file = m.group(1).strip()
            proc.wait(); rc = proc.returncode; remove_job(job_id)
        except Exception as exc:
            remove_job(job_id); yield _sse('error', str(exc)); return

        if is_stopped(job_id): yield _sse('stopped', 'Stopped by user.'); return
        if rc != 0:
            _logger.error(f'comments exit {rc} for {url}\n' + '\n'.join(out_lines))
            yield _sse('batch_error', url); return

        if not json_file:
            import glob as _g
            cands = _g.glob(os.path.join(save_path, '*.info.json'))
            if cands: json_file = max(cands, key=os.path.getmtime)

        if not json_file or not os.path.exists(json_file):
            yield _sse('error', f'Could not find info JSON for {url}'); return

        yield _sse('log', '── Converting comments to text…')
        try:
            info = json.load(open(json_file, encoding='utf-8'))
            coms = info.get('comments', [])
            if not coms: yield _sse('no_transcript', f'No comments: {url}'); return
            lines_out = [f'Comments for: {info.get("title","?")}',
                         f'Channel: {info.get("uploader","?")}',
                         f'URL: {info.get("webpage_url", url)}',
                         f'Total: {len(coms)}', '='*60, '']
            for i, c in enumerate(coms, 1):
                is_reply = c.get('parent') not in (None, 'root')
                px = '  ↳ ' if is_reply else ''
                lines_out.append(f'{px}[{i}] {c.get("author","?")}  ·  👍 {c.get("like_count",0)}')
                for tl in c.get('text','').strip().splitlines():
                    lines_out.append(f'{px}    {tl}')
                lines_out.append('')
            txt_path = re.sub(r'\.info\.json$', '.comments.txt', json_file)
            open(txt_path, 'w', encoding='utf-8').write('\n'.join(lines_out))
            os.remove(json_file)
            yield _sse('done', f'{len(coms)} comments saved ✓  →  {txt_path}')
        except Exception as exc:
            yield _sse('error', f'Convert error: {exc}')

    def generate():
        total, failed = len(urls), []
        if total > 1: yield _sse('log', f'📋  Batch: {total} URL(s)')
        for i, url in enumerate(urls, 1):
            if is_stopped(job_id): break
            if total > 1: yield _sse('log', f'\n── [{i}/{total}] {url}')
            for chunk in run_one(url):
                if isinstance(chunk, str) and _ev(chunk) == 'batch_error':
                    failed.append(_dv(chunk))
                yield chunk
        if failed:
            yield _sse('batch_failed', 'Failed:\n' + '\n'.join(f'  • {u}' for u in failed))
        if not is_stopped(job_id):
            yield _sse('done', f'Done ✓  ({len(urls)-len(failed)}/{len(urls)} succeeded)')

    return _stream(generate())
