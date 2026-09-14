#!/usr/bin/env python3
"""Build primary-source tokenized-asset evidence from issuer reports and Ethereum."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
REGISTRY_PATH = ROOT / "data" / "registry.json"
ISSUER_PATH = ROOT / "data" / "issuer-observations.json"
DEFAULT_DATA_ROOT = ROOT / "data" / "tokenized-assets"
DEFAULT_API_DIR = ROOT / "api" / "v1" / "tokenized-assets"
DEFAULT_RPC_URL = "https://eth.drpc.org"
CHAIN_ID = 1
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x" + "0" * 64
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
DECIMALS_SELECTOR = "0x313ce567"


def canonical_json(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def topic_address(value: str) -> str:
    return "0x" + value[-40:].lower()


def normalize_log(raw: dict[str, Any], decimals: int = 6) -> dict[str, Any]:
    topics = raw.get("topics") or []
    if len(topics) != 3 or str(topics[0]).lower() != TRANSFER_TOPIC:
        raise ValueError("unexpected ERC-20 Transfer log schema")
    sender_topic = str(topics[1]).lower()
    recipient_topic = str(topics[2]).lower()
    if sender_topic == ZERO_TOPIC:
        event_type = "mint"
    elif recipient_topic == ZERO_TOPIC:
        event_type = "burn"
    else:
        event_type = "transfer"
    amount_raw = int(str(raw["data"]), 16)
    row: dict[str, Any] = {
        "event_type": event_type,
        "block_number": int(str(raw["blockNumber"]), 16),
        "block_hash": raw["blockHash"],
        "transaction_hash": raw["transactionHash"],
        "log_index": int(str(raw["logIndex"]), 16),
        "from": topic_address(sender_topic),
        "to": topic_address(recipient_topic),
        "amount_raw": amount_raw,
        "decimals": decimals,
        "amount": amount_raw / (10**decimals),
    }
    if decimals == 6:
        row["amount_usdc"] = row["amount"]
    return row


class EvidenceStore:
    def __init__(self, data_root: Path):
        self.data_root = data_root
        self.objects = data_root / "raw" / "objects"
        self.objects.mkdir(parents=True, exist_ok=True)
        self.entries: dict[str, dict[str, Any]] = {}

    def save(
        self,
        key: str,
        raw: bytes,
        source_url: str,
        content_type: str,
        request: object | None = None,
    ) -> dict[str, Any]:
        digest = sha256(raw)
        suffix = ".json" if "json" in content_type else ".bin"
        path = self.objects / f"{digest}{suffix}"
        if not path.exists():
            path.write_bytes(raw)
        entry: dict[str, Any] = {
            "source_url": source_url,
            "sha256": digest,
            "path": path.relative_to(self.data_root).as_posix(),
            "content_type": content_type,
            "size_bytes": len(raw),
        }
        if request is not None:
            entry["request"] = request
        self.entries[key] = entry
        return entry

    def write_manifest(self, retrieved_at: str, rpc_transport: str) -> dict[str, Any]:
        manifest = {
            "schema_version": 1,
            "retrieved_at": retrieved_at,
            "rpc_transport": rpc_transport,
            "evidence": dict(sorted(self.entries.items())),
        }
        raw = canonical_json(manifest)
        manifests = self.data_root / "raw" / "manifests"
        manifests.mkdir(parents=True, exist_ok=True)
        digest = sha256(raw)
        (manifests / f"{digest}.json").write_bytes(raw)
        (self.data_root / "raw" / "latest-manifest.json").write_bytes(raw)
        return manifest


class EthereumRPC:
    def __init__(self, url: str, store: EvidenceStore):
        self.url = url
        self.store = store
        self.counter = 0

    def _request(self, body: object, key: str) -> object:
        raw_request = json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(
            self.url,
            data=raw_request,
            headers={"Content-Type": "application/json", "User-Agent": "KAFKA2306/tokenized-assets"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read()
        self.store.save(key, raw, self.url, "application/json", request=body)
        return json.loads(raw)

    def call(self, method: str, params: list[Any], key: str | None = None) -> Any:
        self.counter += 1
        request_id = self.counter
        body = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        payload = self._request(body, key or f"rpc:{request_id}:{method}")
        if not isinstance(payload, dict):
            raise ValueError(f"unexpected JSON-RPC response for {method}")
        if payload.get("error"):
            raise RuntimeError(f"{method}: {payload['error']}")
        if "result" not in payload:
            raise ValueError(f"missing JSON-RPC result for {method}")
        return payload["result"]

    def batch_blocks(self, block_numbers: list[int], key: str) -> dict[int, dict[str, Any]]:
        result: dict[int, dict[str, Any]] = {}
        for offset in range(0, len(block_numbers), 2):
            chunk = block_numbers[offset : offset + 2]
            bodies = []
            id_to_block: dict[int, int] = {}
            for block_num in chunk:
                self.counter += 1
                request_id = self.counter
                id_to_block[request_id] = block_num
                bodies.append({"jsonrpc": "2.0", "id": request_id, "method": "eth_getBlockByNumber", "params": [hex(block_num), False]})
            payload = self._request(bodies, f"{key}:chunk:{offset // 2}")
            if not isinstance(payload, list):
                raise ValueError("Ethereum RPC does not support JSON-RPC batch responses")
            for row in payload:
                if row.get("error"):
                    raise RuntimeError(f"batch eth_getBlockByNumber: {row['error']}")
                block_num = id_to_block[int(row["id"])]
                block = row.get("result")
                if not block:
                    raise ValueError(f"missing block {block_num} in batch response")
                result[block_num] = block
        if set(result) != set(block_numbers):
            raise ValueError("incomplete block batch response")
        return result


def fetch_document(url: str, store: EvidenceStore, key: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "KAFKA2306/tokenized-assets"})
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read()
        content_type = response.headers.get_content_type()
    if len(raw) < 1000:
        raise ValueError(f"issuer evidence unexpectedly small: {url}")
    return store.save(key, raw, url, content_type)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_registry(registry: dict[str, Any]) -> None:
    assets = registry.get("assets") or []
    ids: set[str] = set()
    deployments: set[str] = set()
    for asset in assets:
        asset_id = str(asset.get("asset_id") or "")
        if not asset_id or asset_id in ids:
            raise ValueError(f"duplicate or missing asset_id: {asset_id!r}")
        ids.add(asset_id)
        for deployment in asset.get("token_deployments") or []:
            deployment_id = str(deployment.get("deployment_id") or "")
            if not deployment_id or deployment_id in deployments:
                raise ValueError(f"duplicate or missing deployment_id: {deployment_id!r}")
            deployments.add(deployment_id)


def validate_issuer_observations(issuer: dict[str, Any]) -> None:
    rows = issuer.get("observations") or []
    if len(rows) < 1:
        raise ValueError("issuer observations missing")
    for row in rows:
        if float(row["circulation_usdc"]) <= 0 or float(row["reserve_fair_value_usd"]) <= 0:
            raise ValueError("issuer observation must be positive")
        if not str(row["source_url"]).startswith("https://"):
            raise ValueError("issuer source must be HTTPS")


def block_number(block: dict[str, Any]) -> int:
    return int(str(block["number"]), 16)


def block_timestamp(block: dict[str, Any]) -> int:
    return int(str(block["timestamp"]), 16)


def find_block_at_or_before(rpc: EthereumRPC, target_time: int, low: int, high: int, key_prefix: str) -> dict[str, Any]:
    best: dict[str, Any] | None = None
    while low <= high:
        mid = (low + high) // 2
        block = rpc.call("eth_getBlockByNumber", [hex(mid), False], key=f"{key_prefix}:block:{mid}")
        timestamp = block_timestamp(block)
        if timestamp <= target_time:
            best = block
            low = mid + 1
        else:
            high = mid - 1
    if best is None:
        raise ValueError(f"no block found at or before timestamp {target_time}")
    return best


def eth_call_uint(rpc: EthereumRPC, address: str, data: str, block_tag: str, key: str) -> int:
    result = rpc.call("eth_call", [{"to": address, "data": data}, block_tag], key=key)
    if not isinstance(result, str) or not result.startswith("0x"):
        raise ValueError(f"invalid eth_call uint result for {address}")
    return int(result, 16)


def collect_weekly_usdc_supply(rpc: EthereumRPC, finalized: dict[str, Any], lookback_days: int) -> list[dict[str, Any]]:
    if lookback_days < 90:
        raise ValueError("chain lookback must be at least 90 days")
    final_num = block_number(finalized)
    final_time = block_timestamp(finalized)
    targets = list(range(lookback_days, -1, -7))
    if targets[-1] != 0:
        targets.append(0)
    records: list[dict[str, Any]] = []
    initial_low = max(0, final_num - int((lookback_days * 86400 / 12) * 1.15) - 10000)
    for days_ago in targets:
        target_time = final_time - days_ago * 86400
        if records:
            low = records[-1]["block_number"]
            high = min(final_num, low + 60000)
        else:
            low = initial_low
            high = final_num
        block = find_block_at_or_before(rpc, target_time, low, high, key_prefix=f"weekly:{days_ago}d")
        number = block_number(block)
        total_raw = eth_call_uint(rpc, USDC, TOTAL_SUPPLY_SELECTOR, hex(number), key=f"weekly:{days_ago}d:usdc-total-supply:{number}")
        records.append({"target_timestamp": datetime.fromtimestamp(target_time, UTC).isoformat(), "observed_at": datetime.fromtimestamp(block_timestamp(block), UTC).isoformat(), "block_number": number, "block_hash": block["hash"], "chain_id": CHAIN_ID, "contract_address": USDC, "total_supply_raw": total_raw, "decimals": 6, "total_supply": total_raw / 1_000_000})
    records = sorted(records, key=lambda row: row["block_number"])
    span = datetime.fromisoformat(records[-1]["observed_at"]) - datetime.fromisoformat(records[0]["observed_at"])
    if span.days < 90:
        raise ValueError("weekly USDC chain observations span less than 90 days")
    return records


def collect_deployment_snapshots(rpc: EthereumRPC, registry: dict[str, Any], finalized: dict[str, Any]) -> list[dict[str, Any]]:
    number = block_number(finalized)
    tag = hex(number)
    observed_at = datetime.fromtimestamp(block_timestamp(finalized), UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for asset in registry["assets"]:
        for deployment in asset.get("token_deployments") or []:
            deployment_id = str(deployment["deployment_id"])
            address = str(deployment["contract_address"])
            code = rpc.call("eth_getCode", [address, tag], key=f"deployment:{deployment_id}:code:{number}")
            if not isinstance(code, str) or code in {"0x", "0x0"}:
                raise ValueError(f"official contract has no code at finalized block: {deployment_id}")
            decimals = eth_call_uint(rpc, address, DECIMALS_SELECTOR, tag, key=f"deployment:{deployment_id}:decimals:{number}")
            if decimals < 0 or decimals > 36:
                raise ValueError(f"unreasonable ERC-20 decimals: {deployment_id}={decimals}")
            total_supply_raw = eth_call_uint(rpc, address, TOTAL_SUPPLY_SELECTOR, tag, key=f"deployment:{deployment_id}:total-supply:{number}")
            rows.append({"asset_id": asset["asset_id"], "asset_type": asset["asset_type"], "deployment_id": deployment_id, "chain": deployment["chain"], "chain_id": CHAIN_ID, "contract_address": address, "contract_source_url": deployment["contract_source_url"], "block_number": number, "block_hash": finalized["hash"], "observed_at": observed_at, "decimals": decimals, "total_supply_raw": total_supply_raw, "total_supply": total_supply_raw / (10**decimals), "legal_asset_name": asset["legal_asset"]["name"], "legal_issuer": asset["legal_asset"]["issuer"]})
    return rows


def collect_mint_burn_window(rpc: EthereumRPC, finalized: dict[str, Any], block_window: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    final_num = block_number(finalized)
    start_num = max(0, final_num - block_window + 1)
    mint_logs = rpc.call("eth_getLogs", [{"address": USDC, "fromBlock": hex(start_num), "toBlock": hex(final_num), "topics": [TRANSFER_TOPIC, ZERO_TOPIC]}], key=f"usdc-mint-logs:{start_num}:{final_num}")
    burn_logs = rpc.call("eth_getLogs", [{"address": USDC, "fromBlock": hex(start_num), "toBlock": hex(final_num), "topics": [TRANSFER_TOPIC, None, ZERO_TOPIC]}], key=f"usdc-burn-logs:{start_num}:{final_num}")
    events = [normalize_log(row) for row in mint_logs] + [normalize_log(row) for row in burn_logs]
    events = sorted(events, key=lambda row: (row["block_number"], row["log_index"], row["transaction_hash"]))
    unique_blocks = sorted({int(row["block_number"]) for row in events})
    blocks: dict[int, dict[str, Any]] = {}
    for offset in range(0, len(unique_blocks), 100):
        chunk = unique_blocks[offset : offset + 100]
        blocks.update(rpc.batch_blocks(chunk, key=f"mint-burn-blocks:{offset // 100}"))
    for event in events:
        block = blocks[event["block_number"]]
        if str(block["hash"]).lower() != str(event["block_hash"]).lower():
            raise ValueError("mint/burn log block hash changed during collection")
        event["block_timestamp"] = datetime.fromtimestamp(block_timestamp(block), UTC).isoformat()
        event["chain_id"] = CHAIN_ID
        event["contract_address"] = USDC
    mint_events = [row for row in events if row["event_type"] == "mint"]
    burn_events = [row for row in events if row["event_type"] == "burn"]
    summary = {"from_block": start_num, "to_block": final_num, "to_block_hash": finalized["hash"], "chain_id": CHAIN_ID, "contract_address": USDC, "mint_event_count": len(mint_events), "mint_amount_usdc": sum(float(row["amount_usdc"]) for row in mint_events), "burn_event_count": len(burn_events), "burn_amount_usdc": sum(float(row["amount_usdc"]) for row in burn_events)}
    summary["net_mint_minus_burn_usdc"] = summary["mint_amount_usdc"] - summary["burn_amount_usdc"]
    return events, summary


def enrich_issuer_sources(issuer: dict[str, Any], store: EvidenceStore) -> list[dict[str, Any]]:
    source_entries: dict[str, dict[str, Any]] = {}
    urls = sorted({str(row["source_url"]) for row in issuer["observations"]})
    for index, url in enumerate(urls):
        source_entries[url] = fetch_document(url, store, key=f"issuer-report:{index}")
    rows = []
    for source in issuer["observations"]:
        evidence = source_entries[str(source["source_url"])]
        rows.append({**source, "asset_id": "usdc", "issuer_scope": "all Circle-approved blockchains", "source_sha256": evidence["sha256"], "source_evidence": evidence["path"]})
    return rows


def filing_ledger(registry: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for asset in registry["assets"]:
        legal = asset["legal_asset"]
        if legal.get("filing_accession"):
            rows.append({"asset_id": asset["asset_id"], "legal_asset_name": legal["name"], "legal_issuer": legal["issuer"], "cik": legal.get("cik"), "filing_accession": legal["filing_accession"], "filing_date": legal["filing_date"], "source_url": legal["filing_source_url"]})
    return rows


def reconciliation_rows(issuer_rows: list[dict[str, Any]], chain_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for issuer in issuer_rows:
        target = datetime.fromisoformat(str(issuer["as_of"])).replace(tzinfo=UTC)
        nearest = min(chain_rows, key=lambda row: abs((datetime.fromisoformat(row["observed_at"]) - target).total_seconds()))
        chain_time = datetime.fromisoformat(nearest["observed_at"])
        delta_days = abs((chain_time - target).total_seconds()) / 86400
        if delta_days > 4:
            continue
        issuer_value = float(issuer["circulation_usdc"])
        ethereum_value = float(nearest["total_supply"])
        result.append({"issuer_as_of": issuer["as_of"], "issuer_all_chain_circulation_usdc": issuer_value, "ethereum_observed_at": nearest["observed_at"], "ethereum_block_number": nearest["block_number"], "ethereum_block_hash": nearest["block_hash"], "ethereum_native_total_supply_usdc": ethereum_value, "issuer_all_chain_minus_ethereum_native_usdc": issuer_value - ethereum_value, "observation_distance_days": delta_days, "comparison_scope": "not_like_for_like_all_chain_vs_ethereum_native", "correction_applied": False})
    return result


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_normalized(data_root: Path, retrieved_at: str, issuer_rows: list[dict[str, Any]], chain_rows: list[dict[str, Any]], deployments: list[dict[str, Any]], mint_burn_events: list[dict[str, Any]], mint_burn_summary: dict[str, Any]) -> dict[str, Any]:
    normalized = data_root / "normalized"
    normalized.mkdir(parents=True, exist_ok=True)
    payloads = {"issuer": {"schema_version": 1, "retrieved_at": retrieved_at, "records": issuer_rows}, "chain_weekly": {"schema_version": 1, "retrieved_at": retrieved_at, "records": chain_rows}, "deployments": {"schema_version": 1, "retrieved_at": retrieved_at, "records": deployments}, "mint_burn": {"schema_version": 2, "retrieved_at": retrieved_at, "window": mint_burn_summary, "events": mint_burn_events}}
    for name, payload in payloads.items():
        (normalized / f"{name}.json").write_bytes(canonical_json(payload))
    return payloads


def load_normalized(data_root: Path) -> dict[str, Any]:
    normalized = data_root / "normalized"
    return {name: load_json(normalized / f"{name}.json") for name in ("issuer", "chain_weekly", "deployments", "mint_burn")}


def verify_raw_manifest(data_root: Path) -> dict[str, Any]:
    manifest = load_json(data_root / "raw" / "latest-manifest.json")
    for key, entry in manifest["evidence"].items():
        path = data_root / str(entry["path"])
        if not path.exists():
            raise ValueError(f"missing raw evidence object: {key}")
        if sha256(path.read_bytes()) != entry["sha256"]:
            raise ValueError(f"raw evidence hash mismatch: {key}")
    return manifest


def _coverage_time(window: dict[str, Any], events: list[dict[str, Any]], key: str, fallback: str) -> datetime | None:
    value = window.get(key)
    if value:
        return datetime.fromisoformat(str(value))
    timestamps = [datetime.fromisoformat(str(row["block_timestamp"])) for row in events if row.get("block_timestamp")]
    if not timestamps:
        return None
    return min(timestamps) if fallback == "min" else max(timestamps)


def build_flow_daily(mint_burn: dict[str, Any]) -> dict[str, Any]:
    """Aggregate only UTC dates fully enclosed by continuous finalized block coverage."""
    events = [dict(row) for row in mint_burn.get("events") or []]
    window = dict(mint_burn.get("window") or {})
    start = _coverage_time(window, events, "coverage_start_timestamp", "min")
    end = _coverage_time(window, events, "coverage_end_timestamp", "max")
    rule = "Only UTC dates strictly between the continuous coverage start date and coverage end date are complete. Boundary dates are partial and excluded."
    if start is None or end is None or end <= start:
        return {"schema_version": 1, "status": "no_complete_period", "rule": rule, "coverage": {"from_block": window.get("from_block"), "to_block": window.get("to_block"), "coverage_start_timestamp": start.isoformat() if start else None, "coverage_end_timestamp": end.isoformat() if end else None}, "records": []}

    first_complete = start.date() + timedelta(days=1)
    last_complete = end.date() - timedelta(days=1)
    records: list[dict[str, Any]] = []
    if first_complete <= last_complete:
        by_date: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            timestamp = event.get("block_timestamp")
            if not timestamp:
                continue
            date = datetime.fromisoformat(str(timestamp)).date().isoformat()
            by_date.setdefault(date, []).append(event)
        date = first_complete
        while date <= last_complete:
            date_key = date.isoformat()
            day_events = sorted(by_date.get(date_key, []), key=lambda row: (int(row["block_number"]), int(row["log_index"]), str(row["transaction_hash"])))
            mints = [row for row in day_events if row["event_type"] == "mint"]
            burns = [row for row in day_events if row["event_type"] == "burn"]
            evidence = []
            for row in day_events:
                item = {"event_type": row["event_type"], "amount_usdc": row["amount_usdc"], "block_number": row["block_number"], "block_hash": row["block_hash"], "transaction_hash": row["transaction_hash"], "log_index": row["log_index"]}
                if row.get("source_evidence"):
                    item.update({"source_evidence": row["source_evidence"], "source_sha256": row.get("source_sha256"), "source_url": row.get("source_url")})
                evidence.append(item)
            mint_amount = sum(float(row["amount_usdc"]) for row in mints)
            burn_amount = sum(float(row["amount_usdc"]) for row in burns)
            records.append({"date": date_key, "chain_id": window.get("chain_id", CHAIN_ID), "contract_address": window.get("contract_address", USDC), "mint_event_count": len(mints), "burn_event_count": len(burns), "mint_amount_usdc": mint_amount, "burn_amount_usdc": burn_amount, "net_mint_minus_burn_usdc": mint_amount - burn_amount, "evidence_status": "content_addressed_event_refs" if day_events and all(row.get("source_sha256") for row in day_events) else ("complete_covered_no_events" if not day_events else "chain_event_refs_without_raw_locator"), "events": evidence})
            date += timedelta(days=1)
    return {"schema_version": 1, "status": "complete_periods_available" if records else "no_complete_period", "rule": rule, "coverage": {"from_block": window.get("from_block"), "to_block": window.get("to_block"), "coverage_start_timestamp": start.isoformat(), "coverage_end_timestamp": end.isoformat()}, "records": records}


def build_api(registry: dict[str, Any], normalized: dict[str, Any], manifest: dict[str, Any], api_dir: Path) -> dict[str, Any]:
    issuer_rows = normalized["issuer"]["records"]
    chain_rows = normalized["chain_weekly"]["records"]
    deployments = normalized["deployments"]["records"]
    mint_burn = normalized["mint_burn"]
    flow_daily = build_flow_daily(mint_burn)
    retrieved_at = normalized["issuer"]["retrieved_at"]
    issuer_dates = [datetime.fromisoformat(row["as_of"]).date() for row in issuer_rows]
    chain_times = [datetime.fromisoformat(row["observed_at"]) for row in chain_rows]
    nonstable = [asset for asset in registry["assets"] if asset["asset_type"] != "stablecoin"]
    filings = filing_ledger(registry)
    reconciliation = reconciliation_rows(issuer_rows, chain_rows)
    api_dir.mkdir(parents=True, exist_ok=True)
    outputs = {"registry.json": registry, "issuer.json": normalized["issuer"], "chain-weekly.json": normalized["chain_weekly"], "deployments.json": normalized["deployments"], "mint-burn.json": mint_burn, "flow-daily.json": flow_daily, "filings.json": {"schema_version": 1, "records": filings}, "reconciliation.json": {"schema_version": 1, "records": reconciliation}, "provenance.json": manifest}
    for filename, payload in outputs.items():
        (api_dir / filename).write_bytes(canonical_json(payload))
    write_csv(api_dir / "issuer.csv", issuer_rows, ["as_of", "asset_id", "circulation_usdc", "reserve_fair_value_usd", "report_published_at", "source_url", "source_sha256"])
    write_csv(api_dir / "chain-weekly.csv", chain_rows, ["observed_at", "block_number", "block_hash", "chain_id", "contract_address", "total_supply", "total_supply_raw", "decimals"])
    coverage = {"issuer_first_date": min(issuer_dates).isoformat(), "issuer_last_date": max(issuer_dates).isoformat(), "issuer_span_days": (max(issuer_dates) - min(issuer_dates)).days, "issuer_observation_count": len(issuer_rows), "chain_first_time": min(chain_times).isoformat(), "chain_last_time": max(chain_times).isoformat(), "chain_span_days": int((max(chain_times) - min(chain_times)).total_seconds() // 86400), "chain_observation_count": len(chain_rows), "asset_count": len(registry["assets"]), "non_stablecoin_asset_count": len(nonstable), "deployment_count": len(deployments), "filing_count": len(filings), "mint_event_count": mint_burn["window"]["mint_event_count"], "burn_event_count": mint_burn["window"]["burn_event_count"], "mint_burn_event_count": len(mint_burn["events"]), "mint_burn_from_block": mint_burn["window"].get("from_block"), "mint_burn_to_block": mint_burn["window"].get("to_block"), "complete_flow_day_count": len(flow_daily["records"]), "complete_flow_first_date": flow_daily["records"][0]["date"] if flow_daily["records"] else None, "complete_flow_last_date": flow_daily["records"][-1]["date"] if flow_daily["records"] else None, "raw_evidence_count": len(manifest["evidence"])}
    index = {"schema_version": 2, "dataset": "Tokenized assets primary evidence", "retrieved_at": retrieved_at, "coverage": coverage, "views": {"registry": "registry.json", "issuer": "issuer.json", "issuer_csv": "issuer.csv", "chain_weekly": "chain-weekly.json", "chain_weekly_csv": "chain-weekly.csv", "deployments": "deployments.json", "mint_burn": "mint-burn.json", "flow_daily": "flow-daily.json", "filings": "filings.json", "reconciliation": "reconciliation.json", "provenance": "provenance.json"}, "rules": ["issuer-reported USDC circulation and Ethereum contract totalSupply are separate observations", "reserve fair value and circulation are separate fields", "legal asset identity and token deployment identity are separate records", "multiple official contracts are not merged implicitly", "every on-chain observation is bound to chain_id, block_number and block_hash", "mint and burn are classified only from canonical ERC-20 Transfer zero-address topics", "ordinary peer-to-peer Transfer logs are not persisted because they do not change token supply", "daily mint/burn flow is published only for complete UTC dates strictly inside continuous finalized block coverage", "boundary partial UTC dates are excluded and unavailable coverage is never rendered as zero", "reconciliation keeps observed differences and never applies guessed corrections", "RPC provider is transport; Ethereum block hashes are the chain provenance authority"]}
    (api_dir / "index.json").write_bytes(canonical_json(index))
    return index


def collect(registry: dict[str, Any], issuer: dict[str, Any], data_root: Path, rpc_url: str, lookback_days: int, mint_burn_blocks: int) -> tuple[dict[str, Any], dict[str, Any]]:
    retrieved_at = datetime.now(UTC).isoformat()
    store = EvidenceStore(data_root)
    issuer_rows = enrich_issuer_sources(issuer, store)
    rpc = EthereumRPC(rpc_url, store)
    chain_id = int(rpc.call("eth_chainId", [], key="ethereum:chain-id"), 16)
    if chain_id != CHAIN_ID:
        raise ValueError(f"expected Ethereum mainnet chain_id=1, received {chain_id}")
    finalized = rpc.call("eth_getBlockByNumber", ["finalized", False], key="ethereum:finalized-block")
    if not finalized or not finalized.get("hash"):
        raise ValueError("Ethereum finalized block unavailable")
    chain_rows = collect_weekly_usdc_supply(rpc, finalized, lookback_days)
    deployments = collect_deployment_snapshots(rpc, registry, finalized)
    mint_burn_events, mint_burn_summary = collect_mint_burn_window(rpc, finalized, mint_burn_blocks)
    normalized = write_normalized(data_root, retrieved_at, issuer_rows, chain_rows, deployments, mint_burn_events, mint_burn_summary)
    manifest = store.write_manifest(retrieved_at, rpc_url)
    return normalized, manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--issuer", type=Path, default=ISSUER_PATH)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--api-dir", type=Path, default=DEFAULT_API_DIR)
    parser.add_argument("--rpc-url", default=os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL))
    parser.add_argument("--lookback-days", type=int, default=98)
    parser.add_argument("--mint-burn-blocks", type=int, default=1000)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    registry = load_json(args.registry)
    issuer = load_json(args.issuer)
    validate_registry(registry)
    validate_issuer_observations(issuer)
    if args.offline:
        normalized = load_normalized(args.data_root)
        manifest = verify_raw_manifest(args.data_root)
    else:
        normalized, manifest = collect(registry, issuer, args.data_root, args.rpc_url, args.lookback_days, args.mint_burn_blocks)
    index = build_api(registry, normalized, manifest, args.api_dir)
    print(json.dumps(index["coverage"], sort_keys=True))


if __name__ == "__main__":
    main()
