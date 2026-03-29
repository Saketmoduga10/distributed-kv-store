"""FastAPI node that exposes an in-memory key-value store over HTTP."""

from __future__ import annotations

from typing import Any, Dict

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from store import InMemoryKVStore


app = FastAPI(title="Distributed KV Store Node", version="0.1.0")
store = InMemoryKVStore[str, Any]()


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


@app.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.put("/keys/{key}", response_model=KeyValueResponse, status_code=status.HTTP_200_OK)
def put_key(key: str, body: PutValueRequest) -> KeyValueResponse:
    store.put(key, body.value)
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
def delete_key(key: str) -> DeleteResponse:
    try:
        store.delete(key)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Key not found")
    return DeleteResponse(key=key, deleted=True)


@app.get("/keys", response_model=KeysResponse, status_code=status.HTTP_200_OK)
def list_keys() -> KeysResponse:
    return KeysResponse(items=store.get_all())

