#!/usr/bin/env python3
"""Export normalized historical replay inputs from Chainlink and Uniswap V3 state/logs.

This sidecar fetches:
- `AnswerUpdated(int256,uint256,uint256)` and OCR `NewTransmission(...)` logs from two Chainlink feeds
- `Swap(address,address,int256,int256,uint160,uint128,int24)` logs from a Uniswap V3-style pool
- initialized ticks from the Uniswap V3 pool's `tickBitmap` and `ticks` storage at `from_block`

It writes four files under `--output-dir`:
- `oracle_updates.csv`
- `oracle_updates.json`
- `swap_samples.csv`
- `swap_samples.json`

`oracle_updates.*` rows are normalized into a combined reference price that mirrors the
`ChainlinkReferenceOracle` pattern used by the hook:

    reference_price = base_feed_price / quote_feed_price

Each oracle row contains both the raw feed answers and the derived reference price.

`swap_samples.*` rows are normalized into replay-friendly flow records with explicit direction
plus token-in/token-out amounts inferred from the pool's signed swap deltas.

The default `--blocks-per-request=10` is intentionally conservative for free-tier RPC plans that
limit `eth_getLogs` block ranges.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import http.client
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any


getcontext().prec = 80

WAD = 10**18
DECIMALS_SELECTOR = "0x313ce567"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
SLOT0_SELECTOR = "0x3850c7bd"
LIQUIDITY_SELECTOR = "0x1a686502"
FEE_SELECTOR = "0xddca3f43"
TICK_SPACING_SELECTOR = "0xd0c93a7c"
AGGREGATOR_SELECTOR = "0x245a7bfc"

ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
OCR1_NEW_TRANSMISSION_TOPIC = "0xf6a97944f31ea060dfde0566e4167c1a1082551e64b60ecb14d599a9d023d451"
OCR2_NEW_TRANSMISSION_TOPIC = "0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a"
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
MINT_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
BURN_TOPIC = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"

UNISWAP_V3_MIN_TICK = -887272
UNISWAP_V3_MAX_TICK = 887272
UNISWAP_V3_TICKS_MAPPING_SLOT = 5
UNISWAP_V3_TICK_BITMAP_MAPPING_SLOT = 6
ETH_GET_STORAGE_BATCH_SIZE = 100
ETH_GET_LOGS_BATCH_SIZE = 50


@dataclass(frozen=True)
class FeedUpdate:
    feed: str
    label: str
    block_number: int
    block_timestamp: int
    tx_hash: str
    log_index: int
    round_id: int
    answer: int
    updated_at: int


@dataclass(frozen=True)
class OracleUpdateRow:
    timestamp: int
    block_number: int
    block_timestamp: int
    tx_hash: str
    log_index: int
    source_feed: str
    source_label: str
    base_feed: str
    quote_feed: str
    base_answer: str
    quote_answer: str
    base_decimals: int
    quote_decimals: int
    reference_price_wad: str
    reference_price: str


@dataclass(frozen=True)
class SwapSampleRow:
    timestamp: int
    block_number: int
    tx_hash: str
    log_index: int
    pool: str
    token0: str
    token1: str
    token0_decimals: int
    token1_decimals: int
    sender: str
    recipient: str
    direction: str
    token0_in: str
    token1_in: str
    token0_out: str
    token1_out: str
    amount0: str
    amount1: str
    sqrt_price_x96: str
    sqrtPriceX96: str
    liquidity: str
    tick: int
    pre_swap_tick: int


@dataclass(frozen=True)
class OracleStaleWindowRow:
    feed: str
    start_timestamp: int
    end_timestamp: int
    start_block: int
    end_block: int


@dataclass(frozen=True)
class LiquidityEventRow:
    block_number: int
    timestamp: int
    tx_hash: str
    log_index: int
    event_type: str
    tick_lower: int
    tick_upper: int
    amount: str
    amount0: str
    amount1: str


@dataclass(frozen=True)
class InitializedTickRow:
    tick_index: int
    liquidity_net: str
    liquidity_gross: str


@dataclass(frozen=True)
class MarketReferenceUpdateRow:
    timestamp: int
    block_number: int
    tx_hash: str
    log_index: int
    price_wad: str
    price: str


class RpcClient:
    def __init__(
        self,
        rpc_url: str,
        timeout: int,
        max_retries: int = 10,
        retry_backoff_seconds: float = 1.0,
        max_retry_sleep_seconds: float = 30.0,
        cache_dir: str | None = None,
    ) -> None:
        self.rpc_url = rpc_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.max_retry_sleep_seconds = max_retry_sleep_seconds
        self._next_id = 1
        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, method: str, params: list[Any]) -> Path | None:
        if self.cache_dir is None:
            return None
        key = json.dumps(
            {
                "rpc_url": self.rpc_url,
                "method": method,
                "params": params,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        digest = hashlib.sha256(key).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def _load_cached_result(self, method: str, params: list[Any]) -> Any | None:
        cache_path = self._cache_path(method, params)
        if cache_path is None or not cache_path.exists():
            return None
        with cache_path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("method") != method:
            raise RuntimeError(f"RPC cache entry mismatch for {cache_path}")
        return payload["result"]

    def _store_cached_result(self, method: str, params: list[Any], result: Any) -> None:
        cache_path = self._cache_path(method, params)
        if cache_path is None:
            return
        payload = {
            "method": method,
            "params": params,
            "result": result,
        }
        tmp_path = cache_path.with_name(f"{cache_path.name}.{os.getpid()}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        tmp_path.replace(cache_path)

    def call(self, method: str, params: list[Any]) -> Any:
        cached = self._load_cached_result(method, params)
        if cached is not None:
            return cached

        attempt = 0
        while True:
            payload = {
                "jsonrpc": "2.0",
                "id": self._next_id,
                "method": method,
                "params": params,
            }
            self._next_id += 1
            body = json.dumps(payload).encode()
            request = urllib.request.Request(
                self.rpc_url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "uni-v4-hook-historical-replay/1.0",
                    "Accept": "application/json",
                },
                method="POST",
            )

            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    result = json.load(response)
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode(errors="replace")
                if _retryable_http_status(exc.code) and attempt < self.max_retries:
                    self._sleep_before_retry(attempt)
                    attempt += 1
                    continue
                raise RuntimeError(f"RPC HTTP error {exc.code}: {detail}") from exc
            except (urllib.error.URLError, http.client.IncompleteRead, TimeoutError, ConnectionError) as exc:
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt)
                    attempt += 1
                    continue
                raise RuntimeError(f"RPC transport error: {exc}") from exc

            error = result.get("error")
            if error is None:
                self._store_cached_result(method, params, result["result"])
                return result["result"]

            if _retryable_rpc_error(error) and attempt < self.max_retries:
                self._sleep_before_retry(attempt)
                attempt += 1
                continue
            raise RuntimeError(f"RPC error for {method}: {error}")

    def batch_call(self, requests: list[tuple[str, list[Any]]]) -> list[Any]:
        if not requests:
            return []

        cached_results: list[Any | None] = []
        pending_requests: list[tuple[str, list[Any]]] = []
        pending_positions: list[int] = []
        for index, (method, params) in enumerate(requests):
            cached = self._load_cached_result(method, params)
            cached_results.append(cached)
            if cached is None:
                pending_requests.append((method, params))
                pending_positions.append(index)

        if not pending_requests:
            return [item for item in cached_results]

        attempt = 0
        while True:
            payload = []
            response_ids: list[int] = []
            for method, params in pending_requests:
                payload.append(
                    {
                        "jsonrpc": "2.0",
                        "id": self._next_id,
                        "method": method,
                        "params": params,
                    }
                )
                response_ids.append(self._next_id)
                self._next_id += 1

            body = json.dumps(payload).encode()
            request = urllib.request.Request(
                self.rpc_url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "uni-v4-hook-historical-replay/1.0",
                    "Accept": "application/json",
                },
                method="POST",
            )

            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    result = json.load(response)
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode(errors="replace")
                if _retryable_http_status(exc.code) and attempt < self.max_retries:
                    self._sleep_before_retry(attempt)
                    attempt += 1
                    continue
                raise RuntimeError(f"RPC HTTP error {exc.code}: {detail}") from exc
            except (urllib.error.URLError, http.client.IncompleteRead, TimeoutError, ConnectionError) as exc:
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt)
                    attempt += 1
                    continue
                raise RuntimeError(f"RPC transport error: {exc}") from exc

            if not isinstance(result, list):
                raise RuntimeError(f"RPC batch returned unexpected payload: {result}")

            indexed_results = {item["id"]: item for item in result}
            ordered_results: list[Any] = []
            retry_needed = False
            for response_id, (method, params), position in zip(
                response_ids,
                pending_requests,
                pending_positions,
                strict=True,
            ):
                item = indexed_results.get(response_id)
                if item is None:
                    raise RuntimeError(f"RPC batch missing response for id={response_id} method={method}")
                error = item.get("error")
                if error is None:
                    ordered_results.append(item["result"])
                    cached_results[position] = item["result"]
                    self._store_cached_result(method, params, item["result"])
                    continue
                if _retryable_rpc_error(error) and attempt < self.max_retries:
                    retry_needed = True
                    break
                raise RuntimeError(f"RPC error for {method}: {error}")

            if retry_needed:
                self._sleep_before_retry(attempt)
                attempt += 1
                continue
            return [item for item in cached_results]

    def _sleep_before_retry(self, attempt: int) -> None:
        sleep_seconds = self.retry_backoff_seconds * (2 ** attempt)
        if self.max_retry_sleep_seconds >= 0:
            sleep_seconds = min(sleep_seconds, self.max_retry_sleep_seconds)
        time.sleep(sleep_seconds)


def _retryable_http_status(status_code: int) -> bool:
    return status_code in {408, 409, 425, 429, 500, 502, 503, 504}


def _retryable_rpc_error(error: Any) -> bool:
    code = error.get("code") if isinstance(error, dict) else None
    if code == 429:
        return True
    message = str(error.get("message", "") if isinstance(error, dict) else error).lower()
    retryable_fragments = (
        "rate limit",
        "too many",
        "timeout",
        "internal error",
        "temporarily",
        "try again",
        "overloaded",
        "capacity",
    )
    return any(fragment in message for fragment in retryable_fragments)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-url", required=True, help="RPC URL used for eth_call / eth_getLogs / eth_getBlockByNumber.")
    parser.add_argument("--from-block", required=True, type=int, help="Inclusive starting block.")
    parser.add_argument("--to-block", required=True, type=int, help="Inclusive ending block.")
    parser.add_argument("--base-feed", required=True, help="Base Chainlink feed address.")
    parser.add_argument("--quote-feed", required=True, help="Quote Chainlink feed address.")
    parser.add_argument("--pool", required=True, help="Uniswap V3-style pool address.")
    parser.add_argument("--output-dir", required=True, help="Directory to write normalized replay files into.")
    parser.add_argument(
        "--blocks-per-request",
        type=int,
        default=10,
        help="Max block span per eth_getLogs request. Default 10 is free-tier friendly.",
    )
    parser.add_argument("--base-label", default="base_feed", help="Optional label used in output rows.")
    parser.add_argument("--quote-label", default="quote_feed", help="Optional label used in output rows.")
    parser.add_argument(
        "--market-base-feed",
        default=None,
        help="Optional base feed for a faster ex-post market reference series.",
    )
    parser.add_argument(
        "--market-quote-feed",
        default=None,
        help="Optional quote feed for a faster ex-post market reference series.",
    )
    parser.add_argument(
        "--market-base-label",
        default="market_base_feed",
        help="Optional label used in market reference rows.",
    )
    parser.add_argument(
        "--market-quote-label",
        default="market_quote_feed",
        help="Optional label used in market reference rows.",
    )
    parser.add_argument(
        "--market-to-block",
        type=int,
        default=None,
        help="Optional inclusive end block for market_reference_updates.csv. Defaults to --to-block.",
    )
    parser.add_argument(
        "--oracle-lookback-blocks",
        type=int,
        default=0,
        help="Optional feed warmup span loaded before --from-block. Carries at most one pre-window combined row.",
    )
    parser.add_argument(
        "--max-oracle-age-seconds",
        type=int,
        default=3600,
        help="Threshold used to materialize oracle_stale_windows.csv.",
    )
    parser.add_argument("--rpc-timeout", type=int, default=45, help="RPC timeout in seconds.")
    parser.add_argument(
        "--rpc-cache-dir",
        default=None,
        help="Optional directory for persistent RPC response caching.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=10,
        help="Max retries for transient/rate-limited RPC requests. Default: 10.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=1.0,
        help="Initial exponential backoff in seconds for retried RPC requests. Default: 1.0.",
    )
    parser.add_argument(
        "--max-retry-sleep-seconds",
        type=float,
        default=30.0,
        help="Cap per-retry RPC sleep in seconds. Use a negative value for no cap. Default: 30.",
    )
    return parser.parse_args()


def _normalize_address(value: str) -> str:
    value = value.strip()
    if not value.startswith("0x") or len(value) != 42:
        raise ValueError(f"Invalid address: {value}")
    return value.lower()


def _topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def _hex_to_uint(value: str) -> int:
    return int(value, 16)


def _hex_to_int256(value: str) -> int:
    raw = int(value, 16)
    if raw >= 1 << 255:
        raw -= 1 << 256
    return raw


def _hex_to_int24(value: str) -> int:
    raw = int(value, 16) & ((1 << 24) - 1)
    if raw >= 1 << 23:
        raw -= 1 << 24
    return raw


def _hex_to_int128(value: str) -> int:
    raw = int(value, 16)
    if raw >= 1 << 127:
        raw -= 1 << 128
    return raw


def _split_words(data: str) -> list[str]:
    body = data[2:] if data.startswith("0x") else data
    if len(body) % 64 != 0:
        raise ValueError(f"Invalid ABI data payload length: {len(body)}")
    return ["0x" + body[index:index + 64] for index in range(0, len(body), 64)]


def _format_decimal_wad(value_wad: int) -> str:
    return format(Decimal(value_wad) / Decimal(WAD), "f")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w") as handle:
        json.dump(rows, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _eth_call_uint8(client: RpcClient, to: str, selector: str) -> int:
    result = client.call("eth_call", [{"to": to, "data": selector}, "latest"])
    return _hex_to_uint(result)


def _eth_call_address(client: RpcClient, to: str, selector: str) -> str:
    result = client.call("eth_call", [{"to": to, "data": selector}, "latest"])
    return "0x" + result[-40:].lower()


def _eth_call(client: RpcClient, to: str, selector: str, block_tag: str = "latest") -> str:
    return client.call("eth_call", [{"to": to, "data": selector}, block_tag])


def _eth_call_uint(client: RpcClient, to: str, selector: str, block_tag: str = "latest") -> int:
    return _hex_to_uint(_eth_call(client, to, selector, block_tag))


def _eth_get_storage_at(client: RpcClient, to: str, slot: int, block_tag: str = "latest") -> str:
    return client.call("eth_getStorageAt", [to, hex(slot), block_tag])


def _eth_get_storage_many(client: RpcClient, to: str, slots: list[int], block_tag: str = "latest") -> list[str]:
    if not slots:
        return []

    if hasattr(client, "batch_call"):
        results: list[str] = []
        chunk_size = ETH_GET_STORAGE_BATCH_SIZE
        for offset in range(0, len(slots), chunk_size):
            chunk = slots[offset:offset + chunk_size]
            requests = [("eth_getStorageAt", [to, hex(slot), block_tag]) for slot in chunk]
            results.extend(client.batch_call(requests))
        return results

    return [_eth_get_storage_at(client, to, slot, block_tag) for slot in slots]


def _keccak256(payload: bytes) -> bytes:
    try:
        from eth_utils import keccak

        return keccak(payload)
    except ImportError:
        try:
            from Crypto.Hash import keccak as crypto_keccak

            digest = crypto_keccak.new(digest_bits=256)
            digest.update(payload)
            return digest.digest()
        except ImportError:
            result = subprocess.run(
                ["cast", "keccak", "0x" + payload.hex()],
                check=True,
                capture_output=True,
                text=True,
            )
            digest_hex = result.stdout.strip()
            if not digest_hex.startswith("0x"):
                raise RuntimeError(f"cast keccak returned an unexpected payload: {digest_hex}")
            return bytes.fromhex(digest_hex.removeprefix("0x"))


def _encode_mapping_key(value: int) -> bytes:
    if value < 0:
        value = (1 << 256) + value
    return value.to_bytes(32, byteorder="big", signed=False)


def _mapping_storage_slot(mapping_slot: int, key: int) -> int:
    payload = _encode_mapping_key(key) + mapping_slot.to_bytes(32, byteorder="big", signed=False)
    return int.from_bytes(_keccak256(payload), byteorder="big", signed=False)


def _get_logs(
    client: RpcClient,
    address: str,
    topic0: str,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
) -> list[dict[str, Any]]:
    if blocks_per_request <= 0:
        raise ValueError("--blocks-per-request must be > 0")

    requests: list[tuple[str, list[Any]]] = []
    cursor = from_block
    while cursor <= to_block:
        window_end = min(cursor + blocks_per_request - 1, to_block)
        requests.append(
            (
                "eth_getLogs",
                [
                    {
                        "fromBlock": hex(cursor),
                        "toBlock": hex(window_end),
                        "address": address,
                        "topics": [topic0],
                    }
                ],
            )
        )
        cursor = window_end + 1

    logs: list[dict[str, Any]] = []
    if hasattr(client, "batch_call"):
        for offset in range(0, len(requests), ETH_GET_LOGS_BATCH_SIZE):
            for result in client.batch_call(requests[offset:offset + ETH_GET_LOGS_BATCH_SIZE]):
                logs.extend(result)
        return logs

    for method, params in requests:
        assert method == "eth_getLogs"
        logs.extend(client.call(method, params))
    return logs


def _get_block_timestamp(client: RpcClient, cache: dict[int, int], block_number: int) -> int:
    cached = cache.get(block_number)
    if cached is not None:
        return cached

    result = client.call("eth_getBlockByNumber", [hex(block_number), False])
    timestamp = _hex_to_uint(result["timestamp"])
    cache[block_number] = timestamp
    return timestamp


def _resolve_aggregator_address(client: RpcClient, feed: str) -> str | None:
    try:
        result = _eth_call(client, feed, AGGREGATOR_SELECTOR)
    except RuntimeError as exc:
        message = str(exc).lower()
        if "revert" in message or "execution reverted" in message:
            return None
        raise

    if result in {"0x", "0x0"}:
        return None

    aggregator = "0x" + result[-40:].lower()
    if int(aggregator, 16) == 0:
        return None
    return aggregator


def _load_answer_updated_logs(
    client: RpcClient,
    address: str,
    source_feed: str,
    label: str,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    block_timestamps: dict[int, int],
) -> list[FeedUpdate]:
    raw_logs = _get_logs(client, address, ANSWER_UPDATED_TOPIC, from_block, to_block, blocks_per_request)
    updates: list[FeedUpdate] = []

    for entry in raw_logs:
        topics = entry["topics"]
        if len(topics) < 3:
            continue

        words = _split_words(entry["data"])
        if len(words) != 1:
            continue

        block_number = _hex_to_uint(entry["blockNumber"])
        updates.append(
            FeedUpdate(
                feed=source_feed,
                label=label,
                block_number=block_number,
                block_timestamp=_get_block_timestamp(client, block_timestamps, block_number),
                tx_hash=entry["transactionHash"].lower(),
                log_index=_hex_to_uint(entry["logIndex"]),
                round_id=_hex_to_uint(topics[2]),
                answer=_hex_to_int256(topics[1]),
                updated_at=_hex_to_uint(words[0]),
            )
        )

    return updates


def _load_ocr_transmission_logs(
    client: RpcClient,
    address: str,
    source_feed: str,
    label: str,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    block_timestamps: dict[int, int],
) -> list[FeedUpdate]:
    raw_logs = _get_logs(client, address, OCR1_NEW_TRANSMISSION_TOPIC, from_block, to_block, blocks_per_request)
    raw_logs.extend(_get_logs(client, address, OCR2_NEW_TRANSMISSION_TOPIC, from_block, to_block, blocks_per_request))

    updates: list[FeedUpdate] = []
    for entry in raw_logs:
        topics = entry["topics"]
        if len(topics) < 2:
            continue

        words = _split_words(entry["data"])
        topic0 = topics[0].lower()
        if topic0 == OCR1_NEW_TRANSMISSION_TOPIC:
            if len(words) < 5:
                continue
        elif topic0 == OCR2_NEW_TRANSMISSION_TOPIC:
            if len(words) < 8:
                continue
        else:
            continue

        block_number = _hex_to_uint(entry["blockNumber"])
        block_timestamp = _get_block_timestamp(client, block_timestamps, block_number)
        updated_at = block_timestamp if topic0 == OCR1_NEW_TRANSMISSION_TOPIC else _hex_to_uint(words[2])
        updates.append(
            FeedUpdate(
                feed=source_feed,
                label=label,
                block_number=block_number,
                block_timestamp=block_timestamp,
                tx_hash=entry["transactionHash"].lower(),
                log_index=_hex_to_uint(entry["logIndex"]),
                round_id=_hex_to_uint(topics[1]),
                answer=_hex_to_int256(words[0]),
                updated_at=updated_at,
            )
        )

    return updates


def _load_feed_updates(
    client: RpcClient,
    feed: str,
    label: str,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    block_timestamps: dict[int, int],
) -> list[FeedUpdate]:
    updates = _load_answer_updated_logs(
        client,
        feed,
        feed,
        label,
        from_block,
        to_block,
        blocks_per_request,
        block_timestamps,
    )
    if updates:
        updates.sort(key=lambda item: (item.updated_at, item.block_number, item.log_index, item.feed))
        return updates

    log_source = _resolve_aggregator_address(client, feed) or feed
    if log_source != feed:
        updates = _load_answer_updated_logs(
            client,
            log_source,
            feed,
            label,
            from_block,
            to_block,
            blocks_per_request,
            block_timestamps,
        )
        if updates:
            updates.sort(key=lambda item: (item.updated_at, item.block_number, item.log_index, item.feed))
            return updates

    updates = _load_ocr_transmission_logs(
        client,
        log_source,
        feed,
        label,
        from_block,
        to_block,
        blocks_per_request,
        block_timestamps,
    )
    updates.sort(key=lambda item: (item.updated_at, item.block_number, item.log_index, item.feed))
    return updates


def _combine_reference_updates(
    base_updates: list[FeedUpdate],
    quote_updates: list[FeedUpdate],
    base_decimals: int,
    quote_decimals: int,
    base_feed: str,
    quote_feed: str,
) -> list[OracleUpdateRow]:
    latest_base: FeedUpdate | None = None
    latest_quote: FeedUpdate | None = None
    combined: list[OracleUpdateRow] = []

    merged = sorted(
        base_updates + quote_updates,
        key=lambda item: (item.updated_at, item.block_number, item.log_index, item.feed),
    )

    for update in merged:
        if update.feed == base_feed:
            latest_base = update
        elif update.feed == quote_feed:
            latest_quote = update
        else:
            continue

        if latest_base is None or latest_quote is None:
            continue
        if latest_base.answer <= 0 or latest_quote.answer <= 0:
            continue

        price_wad = (
            latest_base.answer
            * (10**quote_decimals)
            * WAD
            // (latest_quote.answer * (10**base_decimals))
        )

        combined.append(
            OracleUpdateRow(
                timestamp=update.updated_at,
                block_number=update.block_number,
                block_timestamp=update.block_timestamp,
                tx_hash=update.tx_hash,
                log_index=update.log_index,
                source_feed=update.feed,
                source_label=update.label,
                base_feed=base_feed,
                quote_feed=quote_feed,
                base_answer=str(latest_base.answer),
                quote_answer=str(latest_quote.answer),
                base_decimals=base_decimals,
                quote_decimals=quote_decimals,
                reference_price_wad=str(price_wad),
                reference_price=_format_decimal_wad(price_wad),
            )
        )

    return combined


def _trim_reference_rows(
    rows: list[OracleUpdateRow],
    from_block: int,
    to_block: int,
) -> list[OracleUpdateRow]:
    latest_before_window: OracleUpdateRow | None = None
    in_window_rows: list[OracleUpdateRow] = []

    for row in rows:
        if row.block_number < from_block:
            latest_before_window = row
            continue
        if row.block_number <= to_block:
            in_window_rows.append(row)

    if latest_before_window is None:
        return in_window_rows
    return [latest_before_window, *in_window_rows]


def _load_swap_samples(
    client: RpcClient,
    pool: str,
    token0: str,
    token1: str,
    token0_decimals: int,
    token1_decimals: int,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    block_timestamps: dict[int, int],
) -> list[SwapSampleRow]:
    raw_logs = _get_logs(client, pool, SWAP_TOPIC, from_block, to_block, blocks_per_request)
    swaps: list[SwapSampleRow] = []

    for entry in raw_logs:
        topics = entry["topics"]
        if len(topics) < 3:
            continue

        words = _split_words(entry["data"])
        if len(words) != 5:
            continue

        amount0 = _hex_to_int256(words[0])
        amount1 = _hex_to_int256(words[1])
        sqrt_price_x96 = _hex_to_uint(words[2])
        liquidity = _hex_to_uint(words[3])
        tick = _hex_to_int24(words[4])

        token0_in = max(amount0, 0)
        token1_in = max(amount1, 0)
        token0_out = max(-amount0, 0)
        token1_out = max(-amount1, 0)

        if token0_in > 0 and token1_out > 0:
            direction = "zero_for_one"
        elif token1_in > 0 and token0_out > 0:
            direction = "one_for_zero"
        else:
            direction = "unknown"

        block_number = _hex_to_uint(entry["blockNumber"])
        swaps.append(
            SwapSampleRow(
                timestamp=_get_block_timestamp(client, block_timestamps, block_number),
                block_number=block_number,
                tx_hash=entry["transactionHash"].lower(),
                log_index=_hex_to_uint(entry["logIndex"]),
                pool=pool,
                token0=token0,
                token1=token1,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                sender=_topic_address(topics[1]),
                recipient=_topic_address(topics[2]),
                direction=direction,
                token0_in=str(token0_in),
                token1_in=str(token1_in),
                token0_out=str(token0_out),
                token1_out=str(token1_out),
                amount0=str(amount0),
                amount1=str(amount1),
                sqrt_price_x96=str(sqrt_price_x96),
                sqrtPriceX96=str(sqrt_price_x96),
                liquidity=str(liquidity),
                tick=tick,
                pre_swap_tick=tick,
            )
        )

    swaps.sort(key=lambda item: (item.block_number, item.log_index))
    return swaps


def _load_liquidity_events(
    client: RpcClient,
    pool: str,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    block_timestamps: dict[int, int],
) -> list[LiquidityEventRow]:
    events: list[LiquidityEventRow] = []
    raw_logs = _get_logs(client, pool, MINT_TOPIC, from_block, to_block, blocks_per_request)
    raw_logs.extend(_get_logs(client, pool, BURN_TOPIC, from_block, to_block, blocks_per_request))

    for entry in raw_logs:
        topics = entry["topics"]
        if len(topics) != 4:
            continue

        words = _split_words(entry["data"])
        topic0 = topics[0].lower()
        block_number = _hex_to_uint(entry["blockNumber"])

        if topic0 == MINT_TOPIC:
            event_type = "mint"
            if len(words) != 4:
                continue
            # Uniswap V3 Mint includes a non-indexed sender before amount/amount0/amount1.
            amount = _hex_to_uint(words[1])
            amount0 = _hex_to_uint(words[2])
            amount1 = _hex_to_uint(words[3])
        elif topic0 == BURN_TOPIC:
            event_type = "burn"
            if len(words) != 3:
                continue
            amount = _hex_to_uint(words[0])
            amount0 = _hex_to_uint(words[1])
            amount1 = _hex_to_uint(words[2])
        else:
            continue

        events.append(
            LiquidityEventRow(
                block_number=block_number,
                timestamp=_get_block_timestamp(client, block_timestamps, block_number),
                tx_hash=entry["transactionHash"].lower(),
                log_index=_hex_to_uint(entry["logIndex"]),
                event_type=event_type,
                tick_lower=_hex_to_int24(topics[2]),
                tick_upper=_hex_to_int24(topics[3]),
                amount=str(amount),
                amount0=str(amount0),
                amount1=str(amount1),
            )
        )

    events.sort(key=lambda item: (item.block_number, item.log_index))
    return events


def _load_initialized_ticks_from_storage(
    client: RpcClient,
    pool: str,
    tick_spacing: int,
    from_block: int,
) -> list[InitializedTickRow]:
    if tick_spacing <= 0:
        raise ValueError("tick_spacing must be > 0.")

    block_tag = hex(from_block)
    min_compressed_tick = UNISWAP_V3_MIN_TICK // tick_spacing
    max_compressed_tick = UNISWAP_V3_MAX_TICK // tick_spacing
    start_word = min_compressed_tick >> 8
    end_word = max_compressed_tick >> 8

    word_positions = list(range(start_word, end_word + 1))
    bitmap_slots = [_mapping_storage_slot(UNISWAP_V3_TICK_BITMAP_MAPPING_SLOT, word_position) for word_position in word_positions]
    bitmap_words = _eth_get_storage_many(client, pool, bitmap_slots, block_tag)

    tick_indices: list[int] = []
    for word_position, bitmap_raw in zip(word_positions, bitmap_words, strict=True):
        bitmap = _hex_to_uint(bitmap_raw)
        if bitmap == 0:
            continue

        for bit_position in range(256):
            if ((bitmap >> bit_position) & 1) == 0:
                continue

            compressed_tick = (word_position << 8) + bit_position
            tick_index = compressed_tick * tick_spacing
            if tick_index < UNISWAP_V3_MIN_TICK or tick_index > UNISWAP_V3_MAX_TICK:
                continue
            tick_indices.append(tick_index)

    tick_slots = [_mapping_storage_slot(UNISWAP_V3_TICKS_MAPPING_SLOT, tick_index) for tick_index in tick_indices]
    tick_words = _eth_get_storage_many(client, pool, tick_slots, block_tag)

    rows: list[InitializedTickRow] = []
    for tick_index, tick_word_raw in zip(tick_indices, tick_words, strict=True):
        tick_word = _hex_to_uint(tick_word_raw)
        liquidity_gross = tick_word & ((1 << 128) - 1)
        if liquidity_gross == 0:
            continue

        liquidity_net = _hex_to_int128(hex(tick_word >> 128))
        rows.append(
            InitializedTickRow(
                tick_index=tick_index,
                liquidity_net=str(liquidity_net),
                liquidity_gross=str(liquidity_gross),
            )
        )

    return rows


def _first_stale_block(
    client: RpcClient,
    block_timestamps: dict[int, int],
    start_block: int,
    end_block: int,
    stale_after_timestamp: int,
) -> int | None:
    if start_block > end_block:
        return None
    if _get_block_timestamp(client, block_timestamps, end_block) <= stale_after_timestamp:
        return None

    left = start_block
    right = end_block
    while left < right:
        midpoint = (left + right) // 2
        midpoint_timestamp = _get_block_timestamp(client, block_timestamps, midpoint)
        if midpoint_timestamp > stale_after_timestamp:
            right = midpoint
        else:
            left = midpoint + 1
    return left


def _build_oracle_stale_windows(
    client: RpcClient,
    feed_updates: list[FeedUpdate],
    from_block: int,
    to_block: int,
    max_oracle_age_seconds: int,
    block_timestamps: dict[int, int],
) -> list[OracleStaleWindowRow]:
    if max_oracle_age_seconds <= 0:
        raise ValueError("--max-oracle-age-seconds must be > 0")

    windows: list[OracleStaleWindowRow] = []
    for index, update in enumerate(feed_updates):
        next_block = feed_updates[index + 1].block_number - 1 if index + 1 < len(feed_updates) else to_block
        candidate_start = max(from_block, update.block_number)
        start_block = _first_stale_block(
            client,
            block_timestamps,
            candidate_start,
            next_block,
            update.updated_at + max_oracle_age_seconds,
        )
        if start_block is None:
            continue
        windows.append(
            OracleStaleWindowRow(
                feed=update.feed,
                start_timestamp=_get_block_timestamp(client, block_timestamps, start_block),
                end_timestamp=_get_block_timestamp(client, block_timestamps, next_block),
                start_block=start_block,
                end_block=next_block,
            )
        )
    return windows


def _fetch_pool_snapshot(
    client: RpcClient,
    pool: str,
    token0: str,
    token1: str,
    token0_decimals: int,
    token1_decimals: int,
    from_block: int,
    to_block: int,
) -> dict[str, Any]:
    block_tag = hex(from_block)
    slot0_words = _split_words(_eth_call(client, pool, SLOT0_SELECTOR, block_tag))
    if len(slot0_words) < 2:
        raise ValueError("slot0() returned an unexpected payload.")

    return {
        "pool": pool,
        "token0": token0,
        "token1": token1,
        "token0_decimals": token0_decimals,
        "token1_decimals": token1_decimals,
        "sqrtPriceX96": _hex_to_uint(slot0_words[0]),
        "tick": _hex_to_int24(slot0_words[1]),
        "liquidity": _eth_call_uint(client, pool, LIQUIDITY_SELECTOR, block_tag),
        "fee": _eth_call_uint(client, pool, FEE_SELECTOR, block_tag),
        "tickSpacing": _hex_to_int24(_eth_call(client, pool, TICK_SPACING_SELECTOR, block_tag)),
        "from_block": from_block,
        "to_block": to_block,
    }


def _build_market_reference_updates(
    rows: list[OracleUpdateRow],
) -> list[MarketReferenceUpdateRow]:
    return [
        MarketReferenceUpdateRow(
            timestamp=row.timestamp,
            block_number=row.block_number,
            tx_hash=row.tx_hash,
            log_index=row.log_index,
            price_wad=row.reference_price_wad,
            price=row.reference_price,
        )
        for row in rows
    ]


def export_historical_replay_data(
    args: argparse.Namespace,
    client: RpcClient | None = None,
) -> dict[str, Any]:
    if args.from_block > args.to_block:
        raise ValueError("--from-block must be <= --to-block")
    if args.max_oracle_age_seconds <= 0:
        raise ValueError("--max-oracle-age-seconds must be > 0")
    oracle_lookback_blocks = int(getattr(args, "oracle_lookback_blocks", 0) or 0)
    if oracle_lookback_blocks < 0:
        raise ValueError("--oracle-lookback-blocks must be >= 0")
    market_to_block = getattr(args, "market_to_block", None)
    if market_to_block is None:
        market_to_block = args.to_block
    if market_to_block < args.to_block:
        raise ValueError("--market-to-block must be >= --to-block")
    feed_from_block = max(args.from_block - oracle_lookback_blocks, 0)

    rpc_client = client or RpcClient(
        args.rpc_url,
        timeout=args.rpc_timeout,
        max_retries=getattr(args, "max_retries", 10),
        retry_backoff_seconds=getattr(args, "retry_backoff_seconds", 1.0),
        max_retry_sleep_seconds=getattr(args, "max_retry_sleep_seconds", 30.0),
        cache_dir=getattr(args, "rpc_cache_dir", None),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_feed = _normalize_address(args.base_feed)
    quote_feed = _normalize_address(args.quote_feed)
    pool = _normalize_address(args.pool)

    market_base_feed = None
    market_quote_feed = None
    if args.market_base_feed or args.market_quote_feed:
        if not args.market_base_feed or not args.market_quote_feed:
            raise ValueError("Both --market-base-feed and --market-quote-feed are required together.")
        market_base_feed = _normalize_address(args.market_base_feed)
        market_quote_feed = _normalize_address(args.market_quote_feed)

    base_decimals = _eth_call_uint8(rpc_client, base_feed, DECIMALS_SELECTOR)
    quote_decimals = _eth_call_uint8(rpc_client, quote_feed, DECIMALS_SELECTOR)
    token0 = _eth_call_address(rpc_client, pool, TOKEN0_SELECTOR)
    token1 = _eth_call_address(rpc_client, pool, TOKEN1_SELECTOR)
    token0_decimals = _eth_call_uint8(rpc_client, token0, DECIMALS_SELECTOR)
    token1_decimals = _eth_call_uint8(rpc_client, token1, DECIMALS_SELECTOR)

    block_timestamps: dict[int, int] = {}

    base_updates = _load_feed_updates(
        rpc_client,
        base_feed,
        args.base_label,
        feed_from_block,
        args.to_block,
        args.blocks_per_request,
        block_timestamps,
    )
    quote_updates = _load_feed_updates(
        rpc_client,
        quote_feed,
        args.quote_label,
        feed_from_block,
        args.to_block,
        args.blocks_per_request,
        block_timestamps,
    )
    oracle_rows = _trim_reference_rows(
        _combine_reference_updates(
            base_updates,
            quote_updates,
            base_decimals,
            quote_decimals,
            base_feed,
            quote_feed,
        ),
        args.from_block,
        args.to_block,
    )

    swap_rows = _load_swap_samples(
        rpc_client,
        pool,
        token0,
        token1,
        token0_decimals,
        token1_decimals,
        args.from_block,
        args.to_block,
        args.blocks_per_request,
        block_timestamps,
    )

    stale_rows = _build_oracle_stale_windows(
        rpc_client,
        base_updates,
        args.from_block,
        args.to_block,
        args.max_oracle_age_seconds,
        block_timestamps,
    )
    stale_rows.extend(
        _build_oracle_stale_windows(
            rpc_client,
            quote_updates,
            args.from_block,
            args.to_block,
            args.max_oracle_age_seconds,
            block_timestamps,
        )
    )
    stale_rows.sort(key=lambda item: (item.feed, item.start_block, item.end_block))

    pool_snapshot = _fetch_pool_snapshot(
        rpc_client,
        pool,
        token0,
        token1,
        token0_decimals,
        token1_decimals,
        args.from_block,
        args.to_block,
    )
    liquidity_events = _load_liquidity_events(
        rpc_client,
        pool,
        args.from_block,
        args.to_block,
        args.blocks_per_request,
        block_timestamps,
    )
    initialized_ticks = _load_initialized_ticks_from_storage(
        rpc_client,
        pool,
        int(pool_snapshot["tickSpacing"]),
        args.from_block,
    )

    market_reference_rows: list[MarketReferenceUpdateRow] = []
    if market_base_feed is not None and market_quote_feed is not None:
        market_base_decimals = _eth_call_uint8(rpc_client, market_base_feed, DECIMALS_SELECTOR)
        market_quote_decimals = _eth_call_uint8(rpc_client, market_quote_feed, DECIMALS_SELECTOR)
        market_base_updates = _load_feed_updates(
            rpc_client,
            market_base_feed,
            args.market_base_label,
            feed_from_block,
            market_to_block,
            args.blocks_per_request,
            block_timestamps,
        )
        market_quote_updates = _load_feed_updates(
            rpc_client,
            market_quote_feed,
            args.market_quote_label,
            feed_from_block,
            market_to_block,
            args.blocks_per_request,
            block_timestamps,
        )
        market_reference_rows = _build_market_reference_updates(
            _trim_reference_rows(
                _combine_reference_updates(
                    market_base_updates,
                    market_quote_updates,
                    market_base_decimals,
                    market_quote_decimals,
                    market_base_feed,
                    market_quote_feed,
                ),
                args.from_block,
                market_to_block,
            )
        )

    oracle_dicts = [asdict(item) for item in oracle_rows]
    swap_dicts = [asdict(item) for item in swap_rows]
    stale_dicts = [asdict(item) for item in stale_rows]
    liquidity_event_dicts = [asdict(item) for item in liquidity_events]
    initialized_tick_dicts = [asdict(item) for item in initialized_ticks]
    market_reference_dicts = [asdict(item) for item in market_reference_rows]

    _write_csv(
        output_dir / "oracle_updates.csv",
        list(OracleUpdateRow.__dataclass_fields__.keys()),
        oracle_dicts,
    )
    _write_json(output_dir / "oracle_updates.json", oracle_dicts)
    _write_csv(
        output_dir / "swap_samples.csv",
        list(SwapSampleRow.__dataclass_fields__.keys()),
        swap_dicts,
    )
    _write_json(output_dir / "swap_samples.json", swap_dicts)
    _write_csv(
        output_dir / "oracle_stale_windows.csv",
        list(OracleStaleWindowRow.__dataclass_fields__.keys()),
        stale_dicts,
    )
    _write_json(output_dir / "pool_snapshot.json", pool_snapshot)
    _write_csv(
        output_dir / "initialized_ticks.csv",
        list(InitializedTickRow.__dataclass_fields__.keys()),
        initialized_tick_dicts,
    )
    _write_csv(
        output_dir / "liquidity_events.csv",
        list(LiquidityEventRow.__dataclass_fields__.keys()),
        liquidity_event_dicts,
    )
    _write_csv(
        output_dir / "market_reference_updates.csv",
        list(MarketReferenceUpdateRow.__dataclass_fields__.keys()),
        market_reference_dicts,
    )

    return {
        "base_feed": base_feed,
        "quote_feed": quote_feed,
        "pool": pool,
        "token0": token0,
        "token1": token1,
        "token0_decimals": token0_decimals,
        "token1_decimals": token1_decimals,
        "base_decimals": base_decimals,
        "quote_decimals": quote_decimals,
        "from_block": args.from_block,
        "to_block": args.to_block,
        "blocks_per_request": args.blocks_per_request,
        "max_oracle_age_seconds": args.max_oracle_age_seconds,
        "oracle_updates": len(oracle_rows),
        "swap_samples": len(swap_rows),
        "oracle_stale_windows": len(stale_rows),
        "liquidity_events": len(liquidity_events),
        "initialized_ticks": len(initialized_ticks),
        "market_reference_updates": len(market_reference_rows),
        "market_reference_to_block": market_to_block if market_reference_rows else None,
        "output_dir": str(output_dir),
    }


def main() -> None:
    args = parse_args()
    summary = export_historical_replay_data(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
