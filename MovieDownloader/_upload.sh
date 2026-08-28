#!/bin/bash
# 把已下载完成的 mp4 一次性搬到 R2，上传成功后删除本地文件以释放磁盘。
SRC=/mnt/MovieAndTVDownload/downloads/movie_000001
DST=r2:tmp-transfer2/veeda-tmp/movie_000001

ulimit -n 1048576

rclone move "$SRC" "$DST" \
  --include "*.mp4" \
  --min-age 2m \
  --transfers 4 \
  --checkers 8 \
  --s3-chunk-size 128M \
  --s3-upload-concurrency 8 \
  --s3-no-check-bucket \
  --retries 5 \
  --low-level-retries 20 \
  --stats 30s \
  --stats-one-line \
  --log-level INFO \
  --log-file /mnt/MovieAndTVDownload/MovieDownloader/upload.log
