import socket
from flask import Flask, render_template
from tools.yt_downloader.routes import yt_bp

app = Flask(__name__)
app.secret_key = 'change-me-in-production'

# ── Register tool blueprints ──────────────────────────────────────────────────
app.register_blueprint(yt_bp, url_prefix='/yt')

# ── Tool registry (drives the homepage cards) ────────────────────────────────
TOOLS = [
    {
        'id': 'yt_downloader',
        'name': 'YT Downloader',
        'description': 'Download YouTube videos up to 1080p as MP4, MKV, or audio-only.',
        'icon': 'yt',
        'url': '/yt/',
        'status': 'stable',
    },
    # Add more tools here — they'll appear as cards on the homepage automatically.
]

@app.route('/')
def index():
    return render_template('index.html', tools=TOOLS)


def find_free_port(start=5000, end=5100):
    """Return the first free TCP port in [start, end)."""
    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('', port))
                return port
            except OSError:
                continue
    raise RuntimeError(f'No free port found between {start} and {end}')


if __name__ == '__main__':
    port = find_free_port()
    print(f'\n  Toolbox running → http://localhost:{port}\n')
    app.run(debug=True, host='0.0.0.0', port=port, use_reloader=False)
