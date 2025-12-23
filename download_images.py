#!/usr/bin/env python3
"""
target.csvを読み込み、2列目の画像URLをdownloads/に保存するスクリプト。
成功した保存ファイル名をsuccess.logに記録し、同名ファイルはスキップする。
"""

from __future__ import annotations

import csv
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


def build_base_and_ext(name: str, url: str) -> tuple[str, str]:
    """保存ファイルのベース名と拡張子を決定する（ダウンロード前に判定）。"""
    parsed = urlparse(url)
    orig_filename = Path(parsed.path).name

    if orig_filename:
        safe_name = sanitize_filename(orig_filename)
        base = Path(safe_name).stem
        ext = Path(safe_name).suffix
    else:
        safe_name = sanitize_filename(name or "image")
        base = safe_name
        ext = ""

    if not ext:
        ext = guess_extension(url, None)
    if not ext.startswith("."):
        ext = f".{ext}"
    return base, ext


def append_log(path: Path, message: str) -> None:
    """ログファイルに1行追記する。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(f"{message}\n")


def load_logged_names(path: Path) -> set[str]:
    """
    既存のログから保存ファイル名の集合を読み込む（ファイルが無ければ空集合）。
    過去ログがURL形式の場合も、パス部分からファイル名を抽出して併せて登録する。
    """
    if not path.exists():
        return set()
    names: set[str] = set()
    with path.open("r", encoding="utf-8") as fp:
        for raw in fp:
            line = raw.strip()
            if not line:
                continue
            names.add(line)
            parsed = urlparse(line)
            url_name = Path(parsed.path).name
            if url_name:
                names.add(sanitize_filename(url_name))
    return names


def download_file(name: str, url: str, base: str, ext: str) -> Optional[Path]:
    """1ファイルをダウンロードし、保存パスを返す（失敗時はNone）。"""
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=30) as response:
            content_type = response.headers.get("Content-Type")
            resolved_ext = ext or guess_extension(url, content_type)
            if not resolved_ext.startswith("."):
                resolved_ext = f".{resolved_ext}"
            target_path = (DOWNLOAD_DIR / f"{base}{resolved_ext}")
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if target_path.exists():
                print(f"[SKIP] {name or base} <- {url} (同名ファイルが既に存在)")
                append_log(SUCCESS_LOG, target_path.name)
                return None
            with target_path.open("wb") as fp:
                fp.write(response.read())
            append_log(SUCCESS_LOG, target_path.name)
            return target_path
    except (HTTPError, URLError, ContentTooShortError, TimeoutError) as exc:
        print(f"[NG] {name or base} <- {url} ({exc})")
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

    already_success = load_logged_names(SUCCESS_LOG)
    total = 0
    saved = 0

    for total, (name, url) in enumerate(iter_csv_rows(CSV_PATH), start=1):
        base, ext = build_base_and_ext(name, url)
        candidate_name = f"{base}{ext}"
        candidate_path = DOWNLOAD_DIR / candidate_name
        if candidate_name in already_success:
            print(f"[SKIP] {name} <- {url} (success.logに記録済みファイル名)")
            continue
        if candidate_path.exists():
            print(f"[SKIP] {name} <- {url} (同名ファイルが既に存在)")
            append_log(SUCCESS_LOG, candidate_name)
            already_success.add(candidate_name)
            continue
        path = download_file(name, url, base, ext)
        if path:
            saved += 1
            already_success.add(path.name)
            print(f"[OK] {name} -> {path}")
        # 50件ごとに少し長めの休憩を入れる
        if total % BATCH_PAUSE_EVERY == 0:
            time.sleep(BATCH_PAUSE_SECONDS)

    print(f"Done. {saved}/{total} files saved.")


if __name__ == "__main__":
    main()
