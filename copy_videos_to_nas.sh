#!/usr/bin/env bash

set -u

SRC="/home/panda-ai-lab-of-texas/Downloads"
DST="/run/user/1000/gvfs/smb-share:server=wdmycloud.local,share=public/yt_test"
LOG="/home/panda-ai-lab-of-texas/Desktop/GitHub/AI_Tools_Flask/copy_videos_to_nas.log"

# 支持的视频扩展名
find "$SRC" -maxdepth 1 -type f \( \
    -iname "*.mp4" -o \
    -iname "*.mkv" -o \
    -iname "*.avi" -o \
    -iname "*.mov" -o \
    -iname "*.wmv" -o \
    -iname "*.flv" -o \
    -iname "*.webm" -o \
    -iname "*.m4v" -o \
    -iname "*.mpeg" -o \
    -iname "*.mpg" \
\) -print0 | while IFS= read -r -d '' file; do
    base="$(basename "$file")"

    if [ ! -d "$DST" ]; then
        echo "$(date '+%F %T') ERROR: destination not available: $DST" >> "$LOG"
        exit 1
    fi

    if [ -e "$DST/$base" ]; then
        echo "$(date '+%F %T') SKIP: already exists: $base" >> "$LOG"
        continue
    fi

    if cp -n "$file" "$DST/"; then
        echo "$(date '+%F %T') COPIED: $base" >> "$LOG"
    else
        echo "$(date '+%F %T') ERROR: failed to copy: $base" >> "$LOG"
    fi
done
