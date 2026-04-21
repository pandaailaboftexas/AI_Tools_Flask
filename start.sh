#!/usr/bin/env bash
# Run from inside the project folder:  ./start.sh
cd "$(dirname "$0")"
 
PORT=${PORT:-8000}
 
# Print access URLs
LOCAL_IP=$(ipconfig getifaddr en1 2>/dev/null || ipconfig getifaddr en0 2>/dev/null || echo "unknown")
 
echo ""
echo "  Starting Toolbox..."
echo "  Local:   http://localhost:$PORT"
echo "  Network: http://$LOCAL_IP:$PORT"
echo ""
 
gunicorn app:app \
  --workers 4 \
  --threads 4 \
  --worker-class gthread \
  --timeout 600 \
  --bind "0.0.0.0:$PORT"
 