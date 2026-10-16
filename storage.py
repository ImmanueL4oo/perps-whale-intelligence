"""MongoDB persistence for whale events.

Design notes
------------
- The bot keeps working if Mongo is unreachable. `connect()` returns False
  in that case and every subsequent insert/query is a silent no-op so the
  rest of the system can carry on.
- Each event document uses a deterministic `_id` derived from
  (venue, tx_ref, trader, coin, kind). This makes inserts idempotent: a
  bot restart that re-receives a recent event silently dedupes via the
  unique index.
- Indexes are created on first connect; safe to re-run.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import DuplicateKeyError, PyMongoError
import asyncio
import json
import os
from typing import Dict

log = logging.getLogger("storage")


def doc_id(payload: dict) -> str:
    """Stable, human-readable _id used for upsert/dedupe."""
    return (
        f"{payload.get('venue', '?')}|{payload.get('tx_ref', '?')}|"
        f"{payload.get('trader', '?')}|{payload.get('coin', '?')}|"
        f"{payload.get('kind', '?')}"
    )


def build_query(filters: dict[str, Any]) -> dict[str, Any]:
    """Translate the JSON-ish filter parameters used by /api/history into Mongo query syntax."""
    q: dict[str, Any] = {}
    if v := filters.get("venue"):
        q["venue"] = v
    if v := filters.get("kind"):
        q["kind"] = v
    if v := filters.get("coin"):
        q["coin"] = v
    if v := filters.get("side"):
        q["side"] = v
    if v := filters.get("tier"):
        q["track_record.tier"] = v
    if (v := filters.get("trader")) is not None and v != "":
        q["trader"] = {"$regex": re.escape(v), "$options": "i"}
    if (v := filters.get("min_notional")) is not None and v > 0:
        q["notional_usd"] = {"$gte": float(v)}
    if (v := filters.get("min_score")) is not None and v > 0:
        q["track_record.score"] = {"$gte": float(v)}

    ts: dict[str, Any] = {}
    if (v := filters.get("before_ts")) is not None:
        ts["$lt"] = int(v)
    if (v := filters.get("after_ts")) is not None:
        ts["$gte"] = int(v)
    if ts:
        q["ts_ms"] = ts
    return q


class MongoStorage:
    def __init__(
        self,
        uri: str = "mongodb://localhost:27017",
        db_name: str = "perps_whales",
        collection_name: str = "events",
    ) -> None:
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self._client: Optional[AsyncIOMotorClient] = None
        self._coll: Optional[AsyncIOMotorCollection] = None
        self._available = False

    @property
    def available(self) -> bool:
        return self._available

    async def connect(self, *, ping_timeout_s: float = 3.0) -> bool:
        """Returns True on success, False on failure (logs a warning)."""
        try:
            self._client = AsyncIOMotorClient(
                self.uri,
                serverSelectionTimeoutMS=int(ping_timeout_s * 1000),
                connectTimeoutMS=int(ping_timeout_s * 1000),
            )
            await self._client.admin.command("ping")
            self._coll = self._client[self.db_name][self.collection_name]
            await self._ensure_indexes()
            self._available = True
            log.info("MongoDB connected (%s/%s)", self.db_name, self.collection_name)
            return True
        except Exception as exc:
            log.warning(
                "MongoDB unavailable at %s (%s) — running without persistence",
                self.uri, type(exc).__name__,
            )
            self._available = False
            if self._client is not None:
                self._client.close()
                self._client = None
            return False

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._available = False

    async def _ensure_indexes(self) -> None:
        assert self._coll is not None
        await self._coll.create_index([("ts_ms", -1)], name="ts_desc")
        await self._coll.create_index(
            [("trader", 1), ("venue", 1), ("ts_ms", -1)],
            name="wallet_history",
        )
        await self._coll.create_index(
            [("venue", 1), ("ts_ms", -1)],
            name="venue_ts",
        )
        await self._coll.create_index(
            [("kind", 1), ("ts_ms", -1)],
            name="kind_ts",
        )

    # ── Writes ────────────────────────────────────────────────────────────────
    async def insert(self, payload: dict) -> bool:
        """Idempotent insert. Returns True if a new document was written."""
        if not self._available or self._coll is None:
            return False
        doc = dict(payload)
        doc["_id"] = doc_id(payload)
        try:
            await self._coll.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False
        except PyMongoError as exc:
            log.warning("insert failed: %s", exc)
            return False

    # ── Reads ─────────────────────────────────────────────────────────────────
    async def recent(self, limit: int = 500) -> list[dict]:
        if not self._available or self._coll is None:
            return []
        cursor = self._coll.find({}).sort("ts_ms", -1).limit(limit)
        out: list[dict] = []
        async for doc in cursor:
            doc.pop("_id", None)
            out.append(doc)
        out.reverse()  # oldest first → matches bus._recent ordering (newest at end)
        return out

    async def history(self, *, limit: int = 200, **filters) -> list[dict]:
        """Paginated, filtered query backing /api/history."""
        if not self._available or self._coll is None:
            return []
        limit = max(1, min(1000, limit))
        q = build_query(filters)
        cursor = self._coll.find(q).sort("ts_ms", -1).limit(limit)
        out: list[dict] = []
        async for doc in cursor:
            doc.pop("_id", None)
            out.append(doc)
        return out

    async def wallet_history(
        self, venue: str, trader: str, limit: int = 500,
    ) -> list[dict]:
        if not self._available or self._coll is None:
            return []
        cursor = (
            self._coll.find({"venue": venue, "trader": trader})
            .sort("ts_ms", -1)
            .limit(max(1, min(2000, limit)))
        )
        out: list[dict] = []
        async for doc in cursor:
            doc.pop("_id", None)
            out.append(doc)
        return out

    async def total_count(self) -> int:
        if not self._available or self._coll is None:
            return 0
        try:
            return await self._coll.estimated_document_count()
        except Exception:
            return 0


class FileStorage:
    """Simple file-backed fallback storage used when MongoDB isn't available.

    This implements the same async methods used by the bot but persists
    documents to a JSON file so events survive restarts without requiring
    a MongoDB server.
    """

    def __init__(
        self,
        path: str = "./data/local_events.json",
        db_name: str = "perps_whales",
        collection_name: str = "events",
    ) -> None:
        self.path = path
        self.db_name = db_name
        self.collection_name = collection_name
        self._available = False
        self._lock = asyncio.Lock()
        self._docs: Dict[str, dict] = {}

    @property
    def available(self) -> bool:
        return self._available

    async def connect(self, *, ping_timeout_s: float = 3.0) -> bool:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                    # raw expected to be dict of id->doc
                    if isinstance(raw, dict):
                        self._docs = {k: v for k, v in raw.items()}
            else:
                self._docs = {}
            self._available = True
            log.info("FileStorage ready (%s)", self.path)
            return True
        except Exception as exc:
            log.warning("FileStorage unavailable (%s)", type(exc).__name__)
            self._available = False
            return False

    async def close(self) -> None:
        await self._flush()
        self._available = False

    async def _flush(self) -> None:
        async with self._lock:
            tmp = f"{self.path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._docs, f)
            os.replace(tmp, self.path)

    # Writes
    async def insert(self, payload: dict) -> bool:
        if not self._available:
            return False
        _id = doc_id(payload)
        async with self._lock:
            if _id in self._docs:
                return False
            doc = dict(payload)
            doc["_id"] = _id
            self._docs[_id] = doc
            await self._flush()
            return True

    # Reads
    async def recent(self, limit: int = 500) -> list[dict]:
        if not self._available:
            return []
        docs = list(self._docs.values())
        docs.sort(key=lambda d: d.get("ts_ms", 0), reverse=True)
        out = [dict(d) for d in docs[:limit]]
        for d in out:
            d.pop("_id", None)
        out.reverse()
        return out

    async def history(self, *, limit: int = 200, **filters) -> list[dict]:
        if not self._available:
            return []
        limit = max(1, min(1000, limit))
        q = build_query(filters)
        def matches(doc: dict) -> bool:
            for k, v in q.items():
                if k == "ts_ms":
                    ts = doc.get("ts_ms", 0)
                    for op, val in v.items():
                        if op == "$lt" and not (ts < val):
                            return False
                        if op == "$gte" and not (ts >= val):
                            return False
                else:
                    if isinstance(v, dict) and "$regex" in v:
                        import re as _re

                        if not _re.search(v["$regex"], str(doc.get(k, "")), flags=_re.I):
                            return False
                    elif isinstance(v, dict) and "$gte" in v:
                        if not (doc.get(k, 0) >= v["$gte"]):
                            return False
                    else:
                        if doc.get(k) != v:
                            return False
            return True

        docs = [d for d in self._docs.values() if matches(d)]
        docs.sort(key=lambda d: d.get("ts_ms", 0), reverse=True)
        out = [dict(d) for d in docs[:limit]]
        for d in out:
            d.pop("_id", None)
        return out

    async def wallet_history(self, venue: str, trader: str, limit: int = 500) -> list[dict]:
        if not self._available:
            return []
        docs = [d for d in self._docs.values() if d.get("venue") == venue and d.get("trader") == trader]
        docs.sort(key=lambda d: d.get("ts_ms", 0), reverse=True)
        out = [dict(d) for d in docs[:max(1, min(2000, limit))]]
        for d in out:
            d.pop("_id", None)
        return out

    async def total_count(self) -> int:
        if not self._available:
            return 0
        return len(self._docs)
