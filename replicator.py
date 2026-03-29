"""Replication helper for forwarding KV operations to peer nodes."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Iterable, Optional

import httpx

from cluster import ClusterConfig, NodeConfig

logger = logging.getLogger(__name__)


class Replicator:
    """Replicates key/value operations to all other nodes in a cluster."""

    def __init__(
        self,
        cluster: ClusterConfig,
        node_id: str,
        *,
        client: Optional[httpx.AsyncClient] = None,
        timeout_s: float = 2.0,
    ) -> None:
        self._cluster = cluster
        self._node_id = node_id
        self._timeout = httpx.Timeout(timeout_s)
        self._client = client

    def _peers(self) -> Iterable[NodeConfig]:
        return self._cluster.get_all_nodes_except(self._node_id)

    @staticmethod
    def _base_url(node: NodeConfig) -> str:
        return f"http://{node.host}:{node.port}"

    async def replicate_put(self, key: str, value: Any) -> None:
        """Replicate a PUT operation to all peer nodes concurrently."""

        async def _send(client: httpx.AsyncClient, node: NodeConfig) -> None:
            url = f"{self._base_url(node)}/keys/{key}"
            try:
                resp = await client.put(url, json={"value": value})
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001 - best-effort replication
                logger.warning("replicate_put failed for node=%s url=%s: %s", node.id, url, exc)

        await self._fanout(_send)

    async def replicate_delete(self, key: str) -> None:
        """Replicate a DELETE operation to all peer nodes concurrently."""

        async def _send(client: httpx.AsyncClient, node: NodeConfig) -> None:
            url = f"{self._base_url(node)}/keys/{key}"
            try:
                resp = await client.delete(url)
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001 - best-effort replication
                logger.warning(
                    "replicate_delete failed for node=%s url=%s: %s", node.id, url, exc
                )

        await self._fanout(_send)

    async def _fanout(self, fn) -> None:
        peers = list(self._peers())
        if not peers:
            return

        if self._client is not None:
            await asyncio.gather(*(fn(self._client, node) for node in peers), return_exceptions=True)
            return

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            await asyncio.gather(*(fn(client, node) for node in peers), return_exceptions=True)

