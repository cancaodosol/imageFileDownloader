#!/usr/bin/env python3
"""
target.csvを読み込み、2列目の画像URLをdownloads/に保存するスクリプト。
"""

from __future__ import annotations

import csv
import itertools
import mimetypes
import os
import re
import time
from pathlib import Path
from typing import Optional
from urllib.error import URLError, HTTPError, ContentTooShortError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


CSV_PATH = Path("target.csv")
DOWNLOAD_DIR = Path("downloads")
LOG_DIR = Path("logs")
SUCCESS_LOG = LOG_DIR / "success.log"
FAILED_LOG = LOG_DIR / "failed.log"
USER_AGENT = "image-downloader/1.0 (canac0.d0.s0lh1de.m24w@gmail.com)"
DELAY_SECONDS = 0.5  # サイト負荷軽減のための待機時間（秒）
BATCH_PAUSE_EVERY = 50  # この件数ごとに追加の待機を入れる
BATCH_PAUSE_SECONDS = 5  # 追加待機の長さ（秒）
INVALID_CHARS = re.compile(r'[\\\\/:*?"<>|]')


def sanitize_filename(name: str) -> str:
    """ファイル名として問題になる文字を置換する。"""
    cleaned = INVALID_CHARS.sub("_", name).strip()
    return cleaned or "unnamed"


def ensure_unique_path(base: Path, suffix: str) -> Path:
    """
    既存と衝突しないパスを返す（必要なら連番を付与）。
    例: base="file", suffix=".jpg" -> "file.jpg", "file_1.jpg", ...
    """
    candidate = base.with_suffix(suffix)
    for idx in itertools.count(1):
        if not candidate.exists():
            return candidate
        candidate = base.with_name(f"{base.name}_{idx}").with_suffix(suffix)


def guess_extension(url: str, content_type: Optional[str]) -> str:
    """URLかContent-Typeから拡張子を推定する。"""
    parsed = urlparse(url)
    url_ext = Path(parsed.path).suffix
    if url_ext:
        return url_ext
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if guessed:
            return guessed
    return ".bin"


def append_log(path: Path, message: str) -> None:
    """ログファイルに1行追記する。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(f"{message}\n")


def load_logged_urls(path: Path) -> set[str]:
    """既存のログからURLの集合を読み込む（ファイルが無ければ空集合）。"""
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as fp:
        return {line.strip() for line in fp if line.strip()}


def download_file(name: str, url: str) -> Optional[Path]:
    """1ファイルをダウンロードし、保存パスを返す（失敗時はNone）。"""
    parsed = urlparse(url)
    orig_filename = Path(parsed.path).name
    if orig_filename:
        safe_name = sanitize_filename(orig_filename)
        base = Path(safe_name).stem
    else:
        safe_name = sanitize_filename(name or "image")
        base = safe_name
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=30) as response:
            content_type = response.headers.get("Content-Type")
            ext = Path(safe_name).suffix or guess_extension(url, content_type)
            target_path = ensure_unique_path(DOWNLOAD_DIR / base, ext)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with target_path.open("wb") as fp:
                fp.write(response.read())
            append_log(SUCCESS_LOG, url)
            return target_path
    except (HTTPError, URLError, ContentTooShortError, TimeoutError) as exc:
        print(f"[NG] {safe_name} <- {url} ({exc})")
        append_log(FAILED_LOG, f"{url} ({exc})")
        return None


def iter_csv_rows(csv_path: Path):
    with csv_path.open("r", newline="", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        for row in reader:
            if not row or len(row) < 2:
                continue
            name, url = row[0].strip(), row[1].strip()
            if not url:
                continue
            yield name, url


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    already_success = load_logged_urls(SUCCESS_LOG)
    total = 0
    saved = 0

    for total, (name, url) in enumerate(iter_csv_rows(CSV_PATH), start=1):
        if url in already_success:
            print(f"[SKIP] {name} <- {url} (success.logに記録済み)")
            continue
        path = download_file(name, url)
        if path:
            saved += 1
            print(f"[OK] {name} -> {path}")
        # 50件ごとに少し長めの休憩を入れる
        if total % BATCH_PAUSE_EVERY == 0:
            time.sleep(BATCH_PAUSE_SECONDS)

    print(f"Done. {saved}/{total} files saved.")


if __name__ == "__main__":
    main()
