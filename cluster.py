"""Cluster configuration models for KV store nodes."""

from __future__ import annotations

from typing import List

from pydantic import BaseModel


class NodeConfig(BaseModel):
    """Configuration for a single KV store node."""

    id: str
    host: str
    port: int


class ClusterConfig(BaseModel):
    """Configuration for a cluster of KV store nodes."""

    nodes: List[NodeConfig]

    def get_all_nodes(self) -> List[NodeConfig]:
        """Return all nodes in the cluster."""

        return list(self.nodes)

    def get_node_by_id(self, node_id: str) -> NodeConfig:
        """Return the node with the given id.

        Raises:
            KeyError: If no node exists with the given id.
        """

        for node in self.nodes:
            if node.id == node_id:
                return node
        raise KeyError(node_id)

    def get_all_nodes_except(self, node_id: str) -> List[NodeConfig]:
        """Return all nodes except the one matching ``node_id``."""

        return [node for node in self.nodes if node.id != node_id]

