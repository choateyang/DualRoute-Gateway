#!/usr/bin/env python3
"""Get a FreeBuff/CodeBuff authToken through the official CLI login flow."""

import json
import os
import time
import uuid
import webbrowser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = os.environ.get("CODEBUFF_API", "https://www.codebuff.com").rstrip("/")
POLL_SECONDS = int(os.environ.get("FREEBUFF_POLL_TIMEOUT", "300"))
OUTPUT_FILE = Path(__file__).resolve().parent / "freebuff_token.txt"


def request_json(method, path, payload=None, query=None):
    url = BASE_URL + path
    if query:
        url += "?" + urlencode(query)
    body = None
    headers = {"Accept": "application/json", "User-Agent": "freebuff-token-helper/1.0"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        with urlopen(Request(url, data=body, headers=headers, method=method), timeout=30) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else {}
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            data = {"message": raw[:300]}
        return exc.code, data
    except (URLError, TimeoutError) as exc:
        raise RuntimeError("无法连接 FreeBuff 官方接口: %s" % exc) from exc


def main():
    fingerprint = str(uuid.uuid4())
    print("正在申请 FreeBuff 授权链接...")
    status, data = request_json("POST", "/api/auth/cli/code", {"fingerprintId": fingerprint})
    if status != 200:
        raise RuntimeError("申请授权链接失败 (HTTP %s): %s" % (status, data))

    login_url = data.get("loginUrl")
    fingerprint_hash = data.get("fingerprintHash")
    expires_at = data.get("expiresAt")
    if not login_url or not fingerprint_hash or not expires_at:
        raise RuntimeError("官方接口返回的数据不完整，无法继续授权。")

    print("已打开浏览器，请登录并完成授权。")
    print("如果浏览器没有自动打开，请手动访问：")
    print(login_url)
    webbrowser.open(login_url)

    deadline = time.monotonic() + POLL_SECONDS
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        status, data = request_json(
            "GET",
            "/api/auth/cli/status",
            query={
                "fingerprintId": fingerprint,
                "fingerprintHash": fingerprint_hash,
                "expiresAt": expires_at,
            },
        )
        user = data.get("user") if isinstance(data, dict) else None
        token = user.get("authToken") if isinstance(user, dict) else None
        if status == 200 and token:
            OUTPUT_FILE.write_text(token.strip() + "\n", encoding="utf-8")
            print("授权成功。")
            print("Token 已保存到：%s" % OUTPUT_FILE)
            print("请将该 Token 添加到网关控制台的 FreeBuff 上游密钥。")
            print("Token（仅显示在本地窗口，请勿公开）：")
            print(token)
            return 0
        if status not in (200, 202, 204) and attempt == 1:
            message = data.get("message", data) if isinstance(data, dict) else data
            print("授权状态暂不可用 (HTTP %s): %s" % (status, message))
        time.sleep(2)

    raise RuntimeError("授权超时（%s 秒）。请重新运行脚本。" % POLL_SECONDS)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n已取消。")
        raise SystemExit(130)
    except Exception as exc:
        print("错误：%s" % exc)
        raise SystemExit(1)
