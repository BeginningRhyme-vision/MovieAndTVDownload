#!/bin/bash
# 启动下载任务，先把文件句柄软限制提到硬限制上限。
cd /mnt/MovieAndTVDownload/MovieDownloader || exit 1
ulimit -n 1048576
echo "nofile soft limit = $(ulimit -Sn)"
setsid nohup python3 -u download_movies.py > run.log 2>&1 < /dev/null &
echo "started pid=$!"
