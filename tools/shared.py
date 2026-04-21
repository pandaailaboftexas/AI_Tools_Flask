"""
tools/shared.py

Single shared module imported by all downloader blueprints.
Provides:
  - Global concurrency semaphore (MAX_CONCURRENT_DOWNLOADS slots)
  - Thread-safe job registry (_jobs) — keyed by job_id
  - Thread-safe token store (_tokens) — token → {path, ts}
    with background cleanup thread that deletes files older than TOKEN_TTL seconds
  - Active download counter for the status API
  - _retry_with_backoff() — 7-attempt helper with random 2-7 s delay
"""
import os, time, threading, random, logging

# ── Configuration ──────────────────────────────────────────────────────────
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get('MAX_CONCURRENT_DOWNLOADS', '8'))
TOKEN_TTL                = int(os.environ.get('TOKEN_TTL_HOURS', '2')) * 3600  # seconds
CLEANUP_INTERVAL         = 1800   # check every 30 minutes
MAX_RETRIES              = 7
RETRY_MIN                = 2      # seconds
RETRY_MAX                = 7      # seconds

DOWNLOADS_DIR = os.path.expanduser('~/Downloads')
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

ERROR_LOG = os.path.join(DOWNLOADS_DIR, 'yt_errors.log')

# ── Logging ────────────────────────────────────────────────────────────────
# One shared FileHandler — all blueprints get a named child logger
_root_log_handler = logging.FileHandler(ERROR_LOG)
_root_log_handler.setFormatter(
    logging.Formatter('%(asctime)s [%(name)s] %(message)s', '%Y-%m-%d %H:%M:%S'))

def get_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    if not lg.handlers:
        lg.setLevel(logging.ERROR)
        lg.addHandler(_root_log_handler)
    return lg

# ── Concurrency semaphore ──────────────────────────────────────────────────
_semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

def acquire_slot(timeout: float = 30.0) -> bool:
    """
    Try to acquire a download slot.
    Returns True if acquired, False if server is too busy.
    """
    return _semaphore.acquire(timeout=timeout)

def release_slot():
    _semaphore.release()

# ── Active counter (for status page / nav badge) ───────────────────────────
_active_lock  = threading.Lock()
_active_count = 0

def increment_active():
    global _active_count
    with _active_lock:
        _active_count += 1

def decrement_active():
    global _active_count
    with _active_lock:
        _active_count = max(0, _active_count - 1)

def get_active_count() -> int:
    with _active_lock:
        return _active_count

# ── Job registry ───────────────────────────────────────────────────────────
_jobs: dict = {}          # job_id → {'proc': Popen, 'stopped': bool}
_jobs_lock   = threading.Lock()

def register_job(job_id: str, proc):
    with _jobs_lock:
        _jobs[job_id] = {'proc': proc, 'stopped': False}

def is_stopped(job_id: str) -> bool:
    with _jobs_lock:
        return _jobs.get(job_id, {}).get('stopped', False)

def remove_job(job_id: str):
    with _jobs_lock:
        _jobs.pop(job_id, None)

def stop_job(job_id: str) -> dict:
    """Stop a running job. Returns {'ok': bool, 'msg': str}."""
    import subprocess
    with _jobs_lock:
        entry = _jobs.get(job_id)
    if entry and entry['proc'] and entry['proc'].poll() is None:
        entry['stopped'] = True
        entry['proc'].terminate()
        try:
            entry['proc'].wait(timeout=5)
        except subprocess.TimeoutExpired:
            entry['proc'].kill()
        remove_job(job_id)
        return {'ok': True, 'msg': 'Process terminated.'}
    return {'ok': False, 'msg': 'No active process.'}

def active_job_count() -> int:
    with _jobs_lock:
        return len(_jobs)

# ── Token store with TTL ───────────────────────────────────────────────────
_tokens: dict  = {}       # token → {'path': str, 'ts': float}
_tokens_lock   = threading.Lock()

def register_token(token: str, fpath: str):
    with _tokens_lock:
        _tokens[token] = {'path': fpath, 'ts': time.time()}

def get_token_path(token: str):
    """Return file path for token, or None if expired/missing."""
    with _tokens_lock:
        entry = _tokens.get(token)
    if not entry:
        return None
    if time.time() - entry['ts'] > TOKEN_TTL:
        with _tokens_lock:
            _tokens.pop(token, None)
        return None
    return entry['path']

def pop_token(token: str):
    """Remove and return file path, or None."""
    with _tokens_lock:
        entry = _tokens.pop(token, None)
    return entry['path'] if entry else None

# ── Background cleanup thread ──────────────────────────────────────────────

def _cleanup_loop():
    """Daemon thread: evict expired tokens and delete their files."""
    logger = get_logger('shared.cleanup')
    while True:
        time.sleep(CLEANUP_INTERVAL)
        now = time.time()
        expired = []
        with _tokens_lock:
            for tok, entry in list(_tokens.items()):
                if now - entry['ts'] > TOKEN_TTL:
                    expired.append((tok, entry['path']))
            for tok, _ in expired:
                _tokens.pop(tok, None)

        for tok, fpath in expired:
            try:
                if fpath and os.path.exists(fpath):
                    os.remove(fpath)
                    logger.error(f'[cleanup] deleted stale file: {fpath}')
            except Exception as e:
                logger.error(f'[cleanup] could not delete {fpath}: {e}')

_cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True, name='token-cleanup')
_cleanup_thread.start()

# ── Retry helper ───────────────────────────────────────────────────────────

def retry_delays(max_retries: int = MAX_RETRIES,
                 min_s: float = RETRY_MIN,
                 max_s: float = RETRY_MAX):
    """
    Generator of sleep durations for retry loop.
    Yields (attempt_number_0_indexed, sleep_seconds).
    First yield is (0, 0) — no sleep before first attempt.
    Subsequent yields are (n, random float in [min_s, max_s]).
    """
    yield 0, 0.0
    for i in range(1, max_retries):
        yield i, round(random.uniform(min_s, max_s), 1)
