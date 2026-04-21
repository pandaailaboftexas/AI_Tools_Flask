"""
app.py — Toolbox main application

Production startup (recommended):
    gunicorn "app:app" --workers 4 --threads 4 --worker-class gthread \
             --timeout 600 --keep-alive 5 --bind 0.0.0.0:5000

Development:
    python app.py
"""
import os, socket, json
from flask import Flask, render_template, jsonify
from tools.yt_downloader.routes      import yt_bp
from tools.olevod_downloader.routes  import olevod_bp
from tools.vimeo_downloader.routes   import vimeo_bp
from tools.universal_downloader.routes import uni_bp
from tools.shared import get_active_count, active_job_count

# ── App setup ──────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(32))

# ── Blueprints ─────────────────────────────────────────────────────────────
app.register_blueprint(yt_bp,     url_prefix='/yt')
app.register_blueprint(olevod_bp, url_prefix='/olevod')
app.register_blueprint(vimeo_bp,  url_prefix='/vimeo')
app.register_blueprint(uni_bp,    url_prefix='/uni')

# ── Tool registry (drives homepage cards) ─────────────────────────────────
TOOLS = [
    {
        'id':          'yt_downloader',
        'name':        'YouTube',
        'description': 'Download videos up to 1080p, MP3s, transcripts and comments.',
        'icon':        'YT',
        'url':         '/yt/',
        'color':       '#ff0000',
        'sites':       'youtube.com',
    },
    {
        'id':          'vimeo_downloader',
        'name':        'Vimeo',
        'description': 'Download Vimeo videos and audio. Supports password-protected videos.',
        'icon':        'VI',
        'url':         '/vimeo/',
        'color':       '#1ab7ea',
        'sites':       'vimeo.com',
    },
    {
        'id':          'olevod_downloader',
        'name':        'OleVOD',
        'description': 'One-click download from olevod.com with automatic stream detection.',
        'icon':        'OV',
        'url':         '/olevod/',
        'color':       '#f4845f',
        'sites':       'olevod.com',
    },
    {
        'id':          'universal_downloader',
        'name':        'Universal',
        'description': 'Download from 1000+ sites — Twitter/X, Instagram, TikTok, SoundCloud, Reddit and more.',
        'icon':        'UNI',
        'url':         '/uni/',
        'color':       '#a78bfa',
        'sites':       '1000+ sites',
    },
]

# ── Pages ──────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html', tools=TOOLS)

# ── Status API (used by nav badge + queue page) ────────────────────────────

@app.route('/api/status')
def status():
    return jsonify({
        'active_downloads': get_active_count(),
        'active_jobs':      active_job_count(),
    })

@app.route('/queue')
def queue():
    return render_template('queue.html', tools=TOOLS)

# ── Dev server ─────────────────────────────────────────────────────────────

def find_free_port(start=5000, end=5100):
    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('', port)); return port
            except OSError:
                continue
    raise RuntimeError(f'No free port found between {start} and {end}')

if __name__ == '__main__':
    port = find_free_port()
    print(f'\n  Toolbox running → http://localhost:{port}')
    print(f'  For production use:  gunicorn "app:app" --workers 4 --threads 4 --timeout 600\n')
    app.run(debug=False, host='0.0.0.0', port=port, use_reloader=False, threaded=True)
