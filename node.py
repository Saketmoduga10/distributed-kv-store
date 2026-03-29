"""FastAPI node that exposes an in-memory key-value store over HTTP."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel

from cluster import ClusterConfig
from replicator import Replicator
from store import InMemoryKVStore


app = FastAPI(title="Distributed KV Store Node", version="0.1.0")
store = InMemoryKVStore[str, Any]()
replicator: Optional[Replicator] = None
node_id: Optional[str] = None


class PutValueRequest(BaseModel):
    value: Any


class KeyValueResponse(BaseModel):
    key: str
    value: Any


class KeysResponse(BaseModel):
    items: Dict[str, Any]


class DeleteResponse(BaseModel):
    key: str
    deleted: bool


class HealthResponse(BaseModel):
    status: str
    node_id: Optional[str] = None


def _load_cluster_config(path: str) -> ClusterConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Support both Pydantic v1 and v2.
    if hasattr(ClusterConfig, "model_validate_json"):
        return ClusterConfig.model_validate_json(raw)  # type: ignore[attr-defined]
    return ClusterConfig.parse_raw(raw)  # type: ignore[attr-defined]


@app.on_event("startup")
async def _startup() -> None:
    global replicator, node_id

    node_id = os.getenv("NODE_ID")
    cluster_path = os.getenv("CLUSTER_CONFIG_PATH")

    if not node_id:
        raise RuntimeError("NODE_ID environment variable must be set")
    if not cluster_path:
        raise RuntimeError("CLUSTER_CONFIG_PATH environment variable must be set")

    cluster = _load_cluster_config(cluster_path)
    replicator = Replicator(cluster=cluster, node_id=node_id)


@app.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
def health() -> HealthResponse:
    return HealthResponse(status="ok", node_id=node_id)


@app.put("/keys/{key}", response_model=KeyValueResponse, status_code=status.HTTP_200_OK)
async def put_key(
    key: str,
    body: PutValueRequest,
    replicate: bool = Query(True),
) -> KeyValueResponse:
    store.put(key, body.value)
    if replicate:
        if replicator is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Replicator not initialized",
            )
        await replicator.replicate_put(key, body.value)
    return KeyValueResponse(key=key, value=body.value)


@app.get("/keys/{key}", response_model=KeyValueResponse, status_code=status.HTTP_200_OK)
def get_key(key: str) -> KeyValueResponse:
    try:
        value = store.get(key)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Key not found")
    return KeyValueResponse(key=key, value=value)


@app.delete(
    "/keys/{key}",
    response_model=DeleteResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_key(key: str, replicate: bool = Query(True)) -> DeleteResponse:
    try:
        store.delete(key)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Key not found")
    if replicate:
        if replicator is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Replicator not initialized",
            )
        await replicator.replicate_delete(key)
    return DeleteResponse(key=key, deleted=True)


@app.get("/keys", response_model=KeysResponse, status_code=status.HTTP_200_OK)
def list_keys() -> KeysResponse:
    return KeysResponse(items=store.get_all())

