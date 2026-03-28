#!/usr/bin/env python3
"""Small cached HTTP helpers for historical reference exporters."""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


class CachedHttpClient:
    def __init__(
        self,
        *,
        timeout: int = 45,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.0,
        cache_dir: str | None = None,
        user_agent: str = "uni-v4-hook-historical-replay/1.0",
    ) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.user_agent = user_agent
        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_json(self, url: str, params: list[tuple[str, str]] | None = None) -> Any:
        text = self.get_text(url, params)
        return json.loads(text)

    def get_text(self, url: str, params: list[tuple[str, str]] | None = None) -> str:
        data = self.get_bytes(url, params)
        return data.decode("utf-8")

    def get_bytes(self, url: str, params: list[tuple[str, str]] | None = None) -> bytes:
        full_url = self._build_url(url, params)
        cached = self._load_cached_bytes(full_url)
        if cached is not None:
            return cached

        attempt = 0
        while True:
            request = urllib.request.Request(
                full_url,
                headers={
                    "Accept": "*/*",
                    "User-Agent": self.user_agent,
                },
                method="GET",
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = response.read()
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode(errors="replace")
                if exc.code == 429 and attempt < self.max_retries:
                    time.sleep(self.retry_backoff_seconds * (2 ** attempt))
                    attempt += 1
                    continue
                raise RuntimeError(f"HTTP error {exc.code} for {full_url}: {detail}") from exc
            except urllib.error.URLError as exc:
                raise RuntimeError(f"HTTP transport error for {full_url}: {exc}") from exc

            self._store_cached_bytes(full_url, payload)
            return payload

    def _build_url(self, url: str, params: list[tuple[str, str]] | None) -> str:
        if not params:
            return url
        query = urllib.parse.urlencode(params, doseq=True)
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}{query}"

    def _cache_path(self, full_url: str) -> Path | None:
        if self.cache_dir is None:
            return None
        digest = hashlib.sha256(full_url.encode()).hexdigest()
        return self.cache_dir / f"{digest}.bin"

    def _load_cached_bytes(self, full_url: str) -> bytes | None:
        cache_path = self._cache_path(full_url)
        if cache_path is None or not cache_path.exists():
            return None
        return cache_path.read_bytes()

    def _store_cached_bytes(self, full_url: str, payload: bytes) -> None:
        cache_path = self._cache_path(full_url)
        if cache_path is None:
            return
        tmp_path = cache_path.with_name(f"{cache_path.name}.{os.getpid()}.tmp")
        tmp_path.write_bytes(payload)
        tmp_path.replace(cache_path)
