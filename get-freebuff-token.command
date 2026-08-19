#!/bin/sh
set -u
cd "$(dirname "$0")"
if ! command -v python3 >/dev/null 2>&1; then
  echo "未找到 Python 3，请先安装 Python 3。"
  read -r _
  exit 1
fi
python3 tools/freebuff/get_token.py
printf '\n按回车关闭窗口。'
read -r _
