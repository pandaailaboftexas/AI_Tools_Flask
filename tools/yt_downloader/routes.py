"""
tools/yt_downloader/routes.py

Endpoints:
  GET  /yt/              - UI
  POST /yt/download      - Video download (SSE)
  POST /yt/mp3           - High-quality MP3 (SSE)
  POST /yt/transcript    - Subtitles/transcript (SSE)
  POST /yt/comments      - Comments as clean .txt (SSE)
  POST /yt/stop          - Kill the running process
  GET  /yt/check_ffmpeg  - Check if ffmpeg is available
"""
import subprocess, json, shutil, os, re, glob, uuid, secrets
from flask import Blueprint, render_template, request, Response, stream_with_context, send_file, abort

yt_bp = Blueprint('yt', __name__, template_folder='templates')

_current_proc = None
_user_stopped = False

_HERE     = os.path.dirname(os.path.abspath(__file__))
SERVE_DIR = os.path.join(_HERE, 'downloads')
os.makedirs(SERVE_DIR, exist_ok=True)

# token → file_id  (only the holder of the token can download/delete their file)
_tokens: dict = {}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ytdlp():
    return shutil.which('yt-dlp') or 'yt-dlp'

def _ffmpeg_available():
    return shutil.which('ffmpeg') is not None

def _sse(event, data):
    return f'event: {event}\ndata: {json.dumps({"text": data})}\n\n'

def _expand(path):
    return os.path.expanduser((path or '~/Downloads').strip())

def _safe_name(name: str) -> str:
    name = re.sub(r'\.[^.]+$', '', name).strip()
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    return name or 'video'

def _list_files(ext=None):
    files = []
    for fname in sorted(os.listdir(SERVE_DIR)):
        fpath = os.path.join(SERVE_DIR, fname)
        if not os.path.isfile(fpath): continue
        if ext and not fname.endswith(ext): continue
        parts = fname.rsplit('.', 1)
        name_part = parts[0]
        file_ext  = parts[1] if len(parts) > 1 else ''
        id_name   = name_part.split('__', 1)
        file_id      = id_name[0]
        display_name = id_name[1] if len(id_name) > 1 else name_part
        files.append({
            'id':      file_id,
            'name':    display_name.replace('_', ' '),
            'ext':     file_ext,
            'size_mb': round(os.path.getsize(fpath) / 1_048_576, 1),
            'filename': fname,
        })
    return files

def _no_playlist(url: str) -> str:
    import urllib.parse as up
    try:
        p  = up.urlparse(url)
        qs = up.parse_qs(p.query, keep_blank_values=True)
        clean = {k: v for k, v in qs.items() if k in ('v', 't')}
        return up.urlunparse(p._replace(query=up.urlencode(clean, doseq=True)))
    except Exception:
        return url

def _stream(gen):
    return Response(
        stream_with_context(gen),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )

def _check(data):
    if not data or not data.get('url'):
        return data, _stream(iter([_sse('error', 'No URL provided.')]))
    return data, None

def _run_proc(cmd: list):
    """Run a command, stream SSE lines, return (returncode, all_output_lines)."""
    global _current_proc, _user_stopped
    _user_stopped = False
    lines = []
    yield _sse('cmd', ' '.join(cmd))
    try:
        _current_proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        for raw in _current_proc.stdout:
            line = raw.rstrip('\n')
            if not line:
                continue
            lines.append(line)
            if re.search(r'\d+\.\d+%', line):
                yield _sse('progress', line)
            else:
                yield _sse('log', line)
        _current_proc.wait()
        rc = _current_proc.returncode
        _current_proc = None
    except Exception as exc:
        _current_proc = None
        yield _sse('error', str(exc))
        return
    return rc, lines   # NOTE: generator returns value via StopIteration.value


def _run_cmd(cmd: list):
    """Simple wrapper: run cmd and emit done/stopped/error."""
    global _user_stopped
    _user_stopped = False
    yield _sse('cmd', ' '.join(cmd))
    try:
        _current_proc_ref = [None]
        global _current_proc
        _current_proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        for raw in _current_proc.stdout:
            line = raw.rstrip('\n')
            if not line: continue
            if re.search(r'\d+\.\d+%', line):
                yield _sse('progress', line)
            else:
                yield _sse('log', line)
        _current_proc.wait()
        rc = _current_proc.returncode
        _current_proc = None
        if _user_stopped:
            yield _sse('stopped', 'Stopped by user.')
        elif rc == 0:
            yield _sse('done', 'Finished ✓')
        else:
            yield _sse('error', f'yt-dlp exited with code {rc}')
    except Exception as exc:
        _current_proc = None
        yield _sse('error', str(exc))


# ── Stop ───────────────────────────────────────────────────────────────────────

@yt_bp.route('/stop', methods=['POST'])
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
        return json.dumps({'ok': True,  'msg': 'Process terminated.'})
    return json.dumps({'ok': False, 'msg': 'No active process.'})


@yt_bp.route('/check_ffmpeg')
def check_ffmpeg():
    return json.dumps({'available': _ffmpeg_available()})


# ── File serving (token-protected) ────────────────────────────────────────────

@yt_bp.route('/serve/<token>')
def serve_file(token):
    file_id = _tokens.get(token)
    if not file_id:
        abort(403)
    for fname in os.listdir(SERVE_DIR):
        if fname.startswith(file_id + '__'):
            fpath = os.path.join(SERVE_DIR, fname)
            parts = fname.rsplit('.', 1)
            name_part = parts[0].split('__', 1)
            display = (name_part[1] if len(name_part) > 1 else name_part[0])
            ext = parts[1] if len(parts) > 1 else 'mp4'
            mime = 'audio/mpeg' if ext == 'mp3' else 'video/mp4'
            return send_file(fpath, mimetype=mime, as_attachment=True,
                             download_name=f'{display}.{ext}')
    abort(404)

@yt_bp.route('/delete/<token>', methods=['POST'])
def delete_file(token):
    file_id = _tokens.get(token)
    if not file_id:
        return json.dumps({'ok': False, 'msg': 'Invalid token'})
    for fname in os.listdir(SERVE_DIR):
        if fname.startswith(file_id + '__'):
            try:
                os.remove(os.path.join(SERVE_DIR, fname))
                _tokens.pop(token, None)
                return json.dumps({'ok': True})
            except Exception as e:
                return json.dumps({'ok': False, 'msg': str(e)})
    return json.dumps({'ok': False, 'msg': 'File not found'})


# ── Page ───────────────────────────────────────────────────────────────────────

@yt_bp.route('/')
def index():
    return render_template('yt_downloader/index.html')


# ── 1. Video ───────────────────────────────────────────────────────────────────

@yt_bp.route('/download', methods=['POST'])
def download():
    data, err = _check(request.get_json(force=True))
    if err: return err

    url       = _no_playlist(data['url'].strip())
    quality   = data.get('quality', 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best')
    fmt       = data.get('format', 'mp4')
    save_path = _expand(data.get('save_path', '~/Downloads'))
    overwrite = data.get('overwrite') == 'true'
    subs      = data.get('subs') == 'true'
    android   = data.get('android', 'true') == 'true'
    is_audio  = quality.startswith('bestaudio')

    cmd = [_ytdlp(), '--no-playlist']
    if is_audio:
        cmd += ['-f', quality, '-x', '--audio-format',
                fmt if fmt in ('mp3', 'm4a', 'opus', 'flac') else 'mp3']
    else:
        cmd += ['-f', quality, '--merge-output-format', fmt]

    file_id  = uuid.uuid4().hex[:12]
    out_tmpl = os.path.join(SERVE_DIR, f'{file_id}__%(title)s.%(ext)s')
    cmd += ['-o', out_tmpl, '--progress', '--no-mtime', '--newline']
    if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
    if overwrite: cmd += ['--no-continue', '--force-overwrites']
    if subs:      cmd += ['--write-auto-sub', '--embed-subs']
    cmd.append(url)

    def generate():
        global _current_proc, _user_stopped
        _user_stopped = False
        yield _sse('cmd', ' '.join(cmd))
        try:
            _current_proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            for raw in _current_proc.stdout:
                line = raw.rstrip('\n')
                if not line: continue
                if re.search(r'\d+\.\d+%', line):
                    yield _sse('progress', line)
                else:
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
            return

        if rc != 0:
            yield _sse('error', f'yt-dlp exited with code {rc}')
            return

        # Find the saved file, generate a token, emit ready + done
        for fname in sorted(os.listdir(SERVE_DIR)):
            if fname.startswith(file_id + '__'):
                fpath   = os.path.join(SERVE_DIR, fname)
                size_mb = round(os.path.getsize(fpath) / 1_048_576, 1)
                parts   = fname.rsplit('.', 1)
                name    = parts[0].split('__', 1)[1] if '__' in parts[0] else parts[0]
                token   = secrets.token_urlsafe(32)
                _tokens[token] = file_id
                yield _sse('ready', json.dumps({'token': token, 'name': name, 'size_mb': size_mb}))
                break

        yield _sse('done', 'Finished ✓')

    return _stream(generate())


# ── 2. MP3 ─────────────────────────────────────────────────────────────────────

@yt_bp.route('/mp3', methods=['POST'])
def mp3():
    data, err = _check(request.get_json(force=True))
    if err: return err

    url        = _no_playlist(data['url'].strip())
    quality    = data.get('audio_quality', '0')
    overwrite  = data.get('overwrite') == 'true'
    android    = data.get('android', 'true') == 'true'
    thumbnail  = data.get('thumbnail', 'true') == 'true'
    metadata   = data.get('metadata', 'true') == 'true'
    has_ffmpeg = _ffmpeg_available()
    file_id    = uuid.uuid4().hex[:12]

    def generate():
        global _current_proc, _user_stopped
        _user_stopped = False

        if not has_ffmpeg:
            yield _sse('log', '⚠  ffmpeg not found — audio will be saved as .m4a (best available without conversion).')
            yield _sse('log', '   Install ffmpeg for true .mp3 output: https://ffmpeg.org/download.html')

        if has_ffmpeg:
            cmd = [
                _ytdlp(), '--no-playlist',
                '-f', 'bestaudio/best',
                '-x', '--audio-format', 'mp3',
                '--audio-quality', quality,
                '-o', os.path.join(SERVE_DIR, f'{file_id}__%(title)s.mp3'),
                '--progress', '--no-mtime', '--newline',
            ]
            if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
            if thumbnail: cmd += ['--embed-thumbnail', '--convert-thumbnails', 'jpg']
            if metadata:  cmd += ['--embed-metadata']
            if overwrite: cmd += ['--no-continue', '--force-overwrites']
        else:
            cmd = [
                _ytdlp(), '--no-playlist',
                '-f', 'bestaudio[ext=m4a]/bestaudio/best',
                '-o', os.path.join(SERVE_DIR, f'{file_id}__%(title)s.%(ext)s'),
                '--progress', '--no-mtime', '--newline',
            ]
            if android:   cmd += ['--extractor-args', 'youtube:player_client=android']
            if overwrite: cmd += ['--no-continue', '--force-overwrites']

        cmd.append(url)
        yield _sse('cmd', ' '.join(cmd))

        try:
            _current_proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            for raw in _current_proc.stdout:
                line = raw.rstrip('\n')
                if not line: continue
                if re.search(r'\d+\.\d+%', line):
                    yield _sse('progress', line)
                else:
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
            return
        if rc != 0:
            yield _sse('error', f'yt-dlp exited with code {rc}')
            return

        for fname in sorted(os.listdir(SERVE_DIR)):
            if fname.startswith(file_id + '__'):
                fpath   = os.path.join(SERVE_DIR, fname)
                size_mb = round(os.path.getsize(fpath) / 1_048_576, 1)
                parts   = fname.rsplit('.', 1)
                name    = parts[0].split('__', 1)[1] if '__' in parts[0] else parts[0]
                token   = secrets.token_urlsafe(32)
                _tokens[token] = file_id
                yield _sse('ready', json.dumps({'token': token, 'name': name, 'size_mb': size_mb}))
                break

        yield _sse('done', 'Finished ✓')

    return _stream(generate())


# ── 3. Transcript ──────────────────────────────────────────────────────────────

def _strip_timestamps_from_srt(text: str) -> str:
    """Remove SRT sequence numbers and timestamp lines, keep only dialogue."""
    lines = text.splitlines()
    out = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Skip sequence number lines (pure integers)
        if re.match(r'^\d+$', line):
            i += 1
            continue
        # Skip timestamp lines like 00:00:01,000 --> 00:00:04,000
        if re.match(r'^\d{2}:\d{2}:\d{2}[,\.]\d{3}\s*-->', line):
            i += 1
            continue
        # Skip VTT header
        if line in ('WEBVTT', '') and i < 3:
            i += 1
            continue
        if line:
            out.append(line)
        i += 1
    # Deduplicate consecutive identical lines (common in auto-subs)
    deduped = []
    for line in out:
        if not deduped or line != deduped[-1]:
            deduped.append(line)
    return '\n'.join(deduped)


@yt_bp.route('/transcript', methods=['POST'])
def transcript():
    data, err = _check(request.get_json(force=True))
    if err: return err

    url              = _no_playlist(data['url'].strip())
    save_path        = _expand(data.get('save_path', '~/Downloads'))
    lang             = data.get('lang', 'en')
    sub_format       = data.get('sub_format', 'srt')
    auto_subs        = data.get('auto_subs', 'true') == 'true'
    manual           = data.get('manual_subs', 'true') == 'true'
    overwrite        = data.get('overwrite') == 'true'
    strip_timestamps = data.get('strip_timestamps', 'false') == 'true'

    cmd = [
        _ytdlp(), '--no-playlist', '--skip-download',
        '-o', f'{save_path}/%(title)s.%(ext)s',
        '--newline',
    ]
    if manual:    cmd += ['--write-sub']
    if auto_subs: cmd += ['--write-auto-sub']
    if not manual and not auto_subs:
        cmd += ['--write-sub', '--write-auto-sub']

    cmd += ['--sub-langs', lang]
    cmd += ['--sub-format', f'{sub_format}/best']
    if overwrite: cmd += ['--force-overwrites']
    cmd.append(url)

    def generate():
        global _current_proc, _user_stopped
        _user_stopped = False
        yield _sse('cmd', ' '.join(cmd))

        output_lines = []
        try:
            _current_proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            for raw in _current_proc.stdout:
                line = raw.rstrip('\n')
                if not line: continue
                output_lines.append(line)
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
            return

        if rc != 0:
            yield _sse('error', f'yt-dlp exited with code {rc}')
            return

        # Detect if subtitles were actually written
        combined = '\n'.join(output_lines).lower()
        has_written = any(
            ('writing' in l.lower() or 'destination' in l.lower())
            for l in output_lines
        )
        no_sub_phrases = ['there are no subtitles', 'no subtitles', 'has no subtitles']

        if not has_written:
            if any(p in combined for p in no_sub_phrases) or not output_lines:
                yield _sse('no_transcript',
                           'No transcript available for this video in the requested language.')
                return
            else:
                # yt-dlp succeeded but we didn't detect the write line — still report done
                yield _sse('done', 'Transcript downloaded ✓')
                return

        # Find the subtitle file(s) that were written
        saved_files = []
        for line in output_lines:
            # Match lines like: [write] Writing video subtitles to: /path/file.srt
            m = re.search(r'(?:Writing.*?to|Destination):\s*(.+\.(?:srt|vtt|ass|json3|srv\d))', line, re.I)
            if m:
                saved_files.append(m.group(1).strip())

        if strip_timestamps and saved_files:
            yield _sse('log', '')
            yield _sse('log', '── Removing timestamps…')
            for fpath in saved_files:
                try:
                    raw_text = open(fpath, encoding='utf-8').read()
                    clean    = _strip_timestamps_from_srt(raw_text)
                    # Save as a plain .txt alongside the original
                    txt_path = re.sub(r'\.[^.]+$', '.txt', fpath)
                    open(txt_path, 'w', encoding='utf-8').write(clean)
                    yield _sse('log', f'   Saved clean text → {txt_path}')
                except Exception as e:
                    yield _sse('log', f'   Could not strip timestamps: {e}')

        if saved_files:
            yield _sse('done', f'Transcript saved ✓  →  {saved_files[0]}')
        else:
            yield _sse('done', 'Transcript downloaded ✓')

    return _stream(generate())


# ── 4. Comments → clean .txt ───────────────────────────────────────────────────

@yt_bp.route('/comments', methods=['POST'])
def comments():
    data, err = _check(request.get_json(force=True))
    if err: return err

    url          = _no_playlist(data['url'].strip())
    save_path    = _expand(data.get('save_path', '~/Downloads'))
    max_comments = data.get('max_comments', '200')
    sort_by      = data.get('sort_by', 'top')      # top | new
    overwrite    = data.get('overwrite') == 'true'

    cmd = [
        _ytdlp(), '--no-playlist', '--skip-download',
        '--write-info-json', '--write-comments',
        '-o', f'{save_path}/%(title)s.%(ext)s',
        '--newline',
    ]
    if max_comments and str(max_comments).lower() != 'all':
        try:
            n = int(max_comments)
            cmd += ['--extractor-args', f'youtube:max_comments={n},comment_sort={sort_by}']
        except ValueError:
            pass
    if overwrite: cmd += ['--force-overwrites']
    cmd.append(url)

    def generate():
        global _current_proc, _user_stopped
        _user_stopped = False
        yield _sse('cmd', ' '.join(cmd))

        output_lines = []
        json_file = None
        try:
            _current_proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            for raw in _current_proc.stdout:
                line = raw.rstrip('\n')
                if not line: continue
                output_lines.append(line)
                yield _sse('log', line)
                # Capture the .info.json path from output
                m = re.search(r'\[info\] Writing video metadata as JSON to:\s*(.+\.info\.json)', line)
                if m:
                    json_file = m.group(1).strip()
            _current_proc.wait()
            rc = _current_proc.returncode
            _current_proc = None
        except Exception as exc:
            _current_proc = None
            yield _sse('error', str(exc))
            return

        if _user_stopped:
            yield _sse('stopped', 'Stopped by user.')
            return

        if rc != 0:
            yield _sse('error', f'yt-dlp exited with code {rc}')
            return

        # Fall back to glob search if we didn't catch the path from output
        if not json_file:
            candidates = glob.glob(os.path.join(save_path, '*.info.json'))
            if candidates:
                json_file = max(candidates, key=os.path.getmtime)

        if not json_file or not os.path.exists(json_file):
            yield _sse('error', 'Could not find the downloaded info JSON file.')
            return

        # ── Convert JSON → clean readable .txt ────────────────────────────────
        yield _sse('log', '')
        yield _sse('log', '── Converting comments to readable text…')
        try:
            info = json.load(open(json_file, encoding='utf-8'))
            raw_comments = info.get('comments', [])

            if not raw_comments:
                yield _sse('no_transcript', 'No comments found in this video.')
                return

            title    = info.get('title', 'Unknown title')
            uploader = info.get('uploader', 'Unknown')
            vid_url  = info.get('webpage_url', url)

            lines_out = [
                f'Comments for: {title}',
                f'Channel: {uploader}',
                f'URL: {vid_url}',
                f'Total fetched: {len(raw_comments)}',
                '=' * 60,
                '',
            ]

            for i, c in enumerate(raw_comments, 1):
                author    = c.get('author', 'Unknown')
                text      = c.get('text', '').strip()
                likes     = c.get('like_count', 0)
                is_reply  = c.get('parent') not in (None, 'root')
                prefix    = '  ↳ ' if is_reply else ''
                lines_out.append(f'{prefix}[{i}] {author}  ·  👍 {likes}')
                # Indent reply text slightly
                for tline in text.splitlines():
                    lines_out.append(f'{prefix}    {tline}')
                lines_out.append('')

            txt_path = re.sub(r'\.info\.json$', '.comments.txt', json_file)
            open(txt_path, 'w', encoding='utf-8').write('\n'.join(lines_out))

            # Delete the raw JSON — user doesn't need it
            os.remove(json_file)
            yield _sse('log', f'   Deleted raw JSON: {os.path.basename(json_file)}')
            yield _sse('done',
                f'{len(raw_comments)} comments saved ✓  →  {txt_path}')

        except Exception as exc:
            yield _sse('error', f'Failed to convert comments: {exc}')

    return _stream(generate())
