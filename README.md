# 🧰 Toolbox

A lightweight Flask app that hosts small utility tools. Currently includes:
- **YT Downloader** — download YouTube videos up to 1080p via `yt-dlp`

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run
python app.py
# → open http://localhost:5000
```

---

## Project Structure

```
toolbox/
├── app.py                   ← Flask app, tool registry, blueprint registration
├── requirements.txt
├── templates/
│   ├── base.html            ← Shared layout (nav, styles, footer)
│   └── index.html           ← Homepage with tool cards
└── tools/
    └── yt_downloader/
        ├── __init__.py
        ├── routes.py        ← Blueprint + SSE download endpoint
        └── templates/
            └── yt_downloader/
                └── index.html
```

---

## ➕ Adding a New Tool

Each tool is a Flask Blueprint. Here's the recipe:

### 1. Create the folder

```
tools/
└── my_tool/
    ├── __init__.py      ← empty
    ├── routes.py        ← Blueprint definition
    └── templates/
        └── my_tool/
            └── index.html
```

### 2. Write `routes.py`

```python
from flask import Blueprint, render_template

my_bp = Blueprint('my_tool', __name__, template_folder='templates')

@my_bp.route('/')
def index():
    return render_template('my_tool/index.html')
```

### 3. Register in `app.py`

```python
from tools.my_tool.routes import my_bp
app.register_blueprint(my_bp, url_prefix='/my-tool')
```

### 4. Add a card to the homepage

In `app.py`, append to the `TOOLS` list:

```python
{
    'id':          'my_tool',
    'name':        'My Tool',
    'description': 'What it does in one sentence.',
    'icon':        'default',
    'url':         '/my-tool/',
    'status':      'stable',   # or 'beta'
},
```

That's it — the card appears on the homepage automatically.

---

## Real-time Output (SSE pattern)

The YT Downloader streams `yt-dlp` stdout back to the browser using
**Server-Sent Events** over a standard POST response body.

```
POST /yt/download  (JSON body)
  → text/event-stream response

event types:
  cmd       – the command being run
  log       – regular output line
  progress  – line containing a % value
  done      – success
  error     – failure
```

Reuse this pattern in any tool that runs a long subprocess.

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `flask` | Web framework |
| `yt-dlp` | YouTube downloader backend |
