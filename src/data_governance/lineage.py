"""
Data Lineage Tracking System.

Provides comprehensive data lineage tracking including:
- Data flow tracking from source to destination
- Transformation tracking
- Impact analysis
- Dependency mapping
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


class LineageEventType(Enum):
    """Types of lineage events."""

    READ = "read"
    WRITE = "write"
    TRANSFORM = "transform"
    DELETE = "delete"
    COPY = "copy"


@dataclass
class LineageEvent:
    """Represents a data lineage event."""

    event_id: str
    timestamp: datetime
    event_type: LineageEventType
    source_assets: List[str]
    target_assets: List[str]
    transformation: Optional[str]
    job_id: Optional[str]
    user: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        data["event_type"] = self.event_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageEvent":
        """Create from dictionary."""
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["event_type"] = LineageEventType(data["event_type"])
        return cls(**data)


@dataclass
class DataNode:
    """Represents a node in the lineage graph."""

    asset_name: str
    asset_type: str  # table, file, view, etc.
    path: str
    first_seen: datetime
    last_accessed: datetime
    access_count: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["first_seen"] = self.first_seen.isoformat()
        data["last_accessed"] = self.last_accessed.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataNode":
        """Create from dictionary."""
        data["first_seen"] = datetime.fromisoformat(data["first_seen"])
        data["last_accessed"] = datetime.fromisoformat(data["last_accessed"])
        return cls(**data)


@dataclass
class DataEdge:
    """Represents an edge in the lineage graph."""

    source_asset: str
    target_asset: str
    transformation: Optional[str]
    first_seen: datetime
    last_seen: datetime
    event_count: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["first_seen"] = self.first_seen.isoformat()
        data["last_seen"] = self.last_seen.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataEdge":
        """Create from dictionary."""
        data["first_seen"] = datetime.fromisoformat(data["first_seen"])
        data["last_seen"] = datetime.fromisoformat(data["last_seen"])
        return cls(**data)


class DataLineageTracker:
    """
    Data Lineage Tracking System.

    Tracks data flow and transformations across the platform to provide:
    - Complete data lineage from source to consumption
    - Impact analysis for changes
    - Dependency mapping
    - Audit trails for compliance
    """

    def __init__(self, lineage_path: str = "data/lineage"):
        """
        Initialize lineage tracker.

        Args:
            lineage_path: Path to store lineage data
        """
        self.lineage_path = Path(lineage_path)
        self.lineage_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

        # Storage files
        self.events_file = self.lineage_path / "events.json"
        self.nodes_file = self.lineage_path / "nodes.json"
        self.edges_file = self.lineage_path / "edges.json"

        # Load existing data
        self.events = self._load_events()
        self.nodes = self._load_nodes()
        self.edges = self._load_edges()

    def _load_events(self) -> List[LineageEvent]:
        """Load existing events from storage."""
        if not self.events_file.exists():
            return []

        try:
            with open(self.events_file, "r") as f:
                data = json.load(f)
            return [LineageEvent.from_dict(event_data) for event_data in data]
        except Exception as e:
            self.logger.error(f"Error loading events: {e}")
            return []

    def _load_nodes(self) -> Dict[str, DataNode]:
        """Load existing nodes from storage."""
        if not self.nodes_file.exists():
            return {}

        try:
            with open(self.nodes_file, "r") as f:
                data = json.load(f)
            return {
                name: DataNode.from_dict(node_data) for name, node_data in data.items()
            }
        except Exception as e:
            self.logger.error(f"Error loading nodes: {e}")
            return {}

    def _load_edges(self) -> Dict[str, DataEdge]:
        """Load existing edges from storage."""
        if not self.edges_file.exists():
            return {}

        try:
            with open(self.edges_file, "r") as f:
                data = json.load(f)
            return {
                key: DataEdge.from_dict(edge_data) for key, edge_data in data.items()
            }
        except Exception as e:
            self.logger.error(f"Error loading edges: {e}")
            return {}

    def _save_events(self) -> None:
        """Save events to storage."""
        try:
            # Keep only recent events to prevent file from growing too large
            recent_events = self.events[-10000:]  # Keep last 10k events
            data = [event.to_dict() for event in recent_events]
            with open(self.events_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving events: {e}")

    def _save_nodes(self) -> None:
        """Save nodes to storage."""
        try:
            data = {name: node.to_dict() for name, node in self.nodes.items()}
            with open(self.nodes_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving nodes: {e}")

    def _save_edges(self) -> None:
        """Save edges to storage."""
        try:
            data = {key: edge.to_dict() for key, edge in self.edges.items()}
            with open(self.edges_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving edges: {e}")

    def track_event(
        self,
        event_type: LineageEventType,
        source_assets: List[str],
        target_assets: List[str],
        transformation: Optional[str] = None,
        job_id: Optional[str] = None,
        user: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Track a lineage event.

        Args:
            event_type: Type of lineage event
            source_assets: Source asset names
            target_assets: Target asset names
            transformation: Transformation description
            job_id: Associated job ID
            user: User performing the operation
            metadata: Additional metadata

        Returns:
            Event ID
        """
        import uuid

        metadata = metadata or {}
        now = datetime.now()
        event_id = str(uuid.uuid4())

        # Create event
        event = LineageEvent(
            event_id=event_id,
            timestamp=now,
            event_type=event_type,
            source_assets=source_assets,
            target_assets=target_assets,
            transformation=transformation,
            job_id=job_id,
            user=user,
            metadata=metadata,
        )

        # Store event
        self.events.append(event)

        # Update graph
        self._update_graph(event)

        # Save changes
        self._save_events()
        self._save_nodes()
        self._save_edges()

        self.logger.info(f"Tracked lineage event: {event_id} ({event_type.value})")
        return event_id

    def _update_graph(self, event: LineageEvent):
        """Update lineage graph with new event."""
        now = event.timestamp

        # Update nodes
        all_assets = event.source_assets + event.target_assets
        for asset in all_assets:
            if asset in self.nodes:
                node = self.nodes[asset]
                node.last_accessed = now
                node.access_count += 1
            else:
                # Create new node (asset type and path would be enriched from catalog)
                self.nodes[asset] = DataNode(
                    asset_name=asset,
                    asset_type="unknown",
                    path="",
                    first_seen=now,
                    last_accessed=now,
                    access_count=1,
                )

        # Update edges
        for source in event.source_assets:
            for target in event.target_assets:
                edge_key = f"{source}->{target}"

                if edge_key in self.edges:
                    edge = self.edges[edge_key]
                    edge.last_seen = now
                    edge.event_count += 1
                    if event.transformation:
                        edge.transformation = event.transformation
                else:
                    self.edges[edge_key] = DataEdge(
                        source_asset=source,
                        target_asset=target,
                        transformation=event.transformation,
                        first_seen=now,
                        last_seen=now,
                        event_count=1,
                    )

    def get_upstream_lineage(
        self, asset_name: str, max_depth: int = 10
    ) -> Dict[str, Any]:
        """
        Get upstream lineage for an asset.

        Args:
            asset_name: Target asset name
            max_depth: Maximum depth to traverse

        Returns:
            Upstream lineage tree
        """
        visited = set()
        lineage = self._build_upstream_tree(asset_name, max_depth, visited)

        return {
            "asset": asset_name,
            "upstream": lineage,
            "depth": max_depth,
            "total_assets": len(visited),
        }

    def _build_upstream_tree(
        self, asset_name: str, remaining_depth: int, visited: Set[str]
    ) -> List[Dict[str, Any]]:
        """Recursively build upstream lineage tree."""
        if remaining_depth <= 0 or asset_name in visited:
            return []

        visited.add(asset_name)
        upstream_assets = []

        # Find all edges pointing to this asset
        for _, edge in self.edges.items():
            if edge.target_asset == asset_name:
                source_asset = edge.source_asset

                upstream_assets.append(
                    {
                        "asset": source_asset,
                        "transformation": edge.transformation,
                        "first_seen": edge.first_seen.isoformat(),
                        "last_seen": edge.last_seen.isoformat(),
                        "event_count": edge.event_count,
                        "upstream": self._build_upstream_tree(
                            source_asset, remaining_depth - 1, visited
                        ),
                    }
                )

        return upstream_assets

    def get_downstream_lineage(
        self, asset_name: str, max_depth: int = 10
    ) -> Dict[str, Any]:
        """
        Get downstream lineage for an asset.

        Args:
            asset_name: Source asset name
            max_depth: Maximum depth to traverse

        Returns:
            Downstream lineage tree
        """
        visited = set()
        lineage = self._build_downstream_tree(asset_name, max_depth, visited)

        return {
            "asset": asset_name,
            "downstream": lineage,
            "depth": max_depth,
            "total_assets": len(visited),
        }

    def _build_downstream_tree(
        self, asset_name: str, remaining_depth: int, visited: Set[str]
    ) -> List[Dict[str, Any]]:
        """Recursively build downstream lineage tree."""
        if remaining_depth <= 0 or asset_name in visited:
            return []

        visited.add(asset_name)
        downstream_assets = []

        # Find all edges starting from this asset
        for _, edge in self.edges.items():
            if edge.source_asset == asset_name:
                target_asset = edge.target_asset

                downstream_assets.append(
                    {
                        "asset": target_asset,
                        "transformation": edge.transformation,
                        "first_seen": edge.first_seen.isoformat(),
                        "last_seen": edge.last_seen.isoformat(),
                        "event_count": edge.event_count,
                        "downstream": self._build_downstream_tree(
                            target_asset, remaining_depth - 1, visited
                        ),
                    }
                )

        return downstream_assets

    def get_impact_analysis(self, asset_name: str) -> Dict[str, Any]:
        """
        Perform impact analysis for an asset.

        Args:
            asset_name: Asset to analyze

        Returns:
            Impact analysis results
        """
        # Get all downstream assets
        downstream = self.get_downstream_lineage(asset_name, max_depth=50)

        # Collect all impacted assets
        impacted_assets = set()
        self._collect_impacted_assets(downstream["downstream"], impacted_assets)

        # Categorize by distance
        impact_by_distance = {}
        self._categorize_by_distance(downstream["downstream"], impact_by_distance, 1)

        return {
            "source_asset": asset_name,
            "total_impacted_assets": len(impacted_assets),
            "impacted_assets": list(impacted_assets),
            "impact_by_distance": impact_by_distance,
            "analysis_timestamp": datetime.now().isoformat(),
        }

    def _collect_impacted_assets(
        self, lineage_tree: List[Dict[str, Any]], impacted_assets: Set[str]
    ):
        """Recursively collect all impacted assets."""
        for item in lineage_tree:
            impacted_assets.add(item["asset"])
            self._collect_impacted_assets(item.get("downstream", []), impacted_assets)

    def _categorize_by_distance(
        self,
        lineage_tree: List[Dict[str, Any]],
        impact_by_distance: Dict[int, List[str]],
        distance: int,
    ):
        """Categorize impacted assets by distance."""
        if distance not in impact_by_distance:
            impact_by_distance[distance] = []

        for item in lineage_tree:
            impact_by_distance[distance].append(item["asset"])
            self._categorize_by_distance(
                item.get("downstream", []), impact_by_distance, distance + 1
            )

    def get_lineage_summary(self) -> Dict[str, Any]:
        """Get summary statistics for lineage data."""
        return {
            "total_events": len(self.events),
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "event_types": {
                event_type.value: sum(
                    1 for e in self.events if e.event_type == event_type
                )
                for event_type in LineageEventType
            },
            "most_active_assets": self._get_most_active_assets(),
            "recent_activity": self._get_recent_activity(),
        }

    def _get_most_active_assets(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most active assets by access count."""
        sorted_nodes = sorted(
            self.nodes.values(), key=lambda n: n.access_count, reverse=True
        )

        return [
            {
                "asset": node.asset_name,
                "access_count": node.access_count,
                "last_accessed": node.last_accessed.isoformat(),
            }
            for node in sorted_nodes[:limit]
        ]

    def _get_recent_activity(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent lineage activity."""
        cutoff = datetime.now() - timedelta(hours=hours)

        recent_events = [e for e in self.events if e.timestamp >= cutoff]

        return [
            {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "timestamp": event.timestamp.isoformat(),
                "source_assets": event.source_assets,
                "target_assets": event.target_assets,
                "user": event.user,
            }
            for event in recent_events[-50:]  # Last 50 events
        ]

    def export_lineage_graph(self, output_path: str, format: str = "json"):
        """
        Export lineage graph in various formats.

        Args:
            output_path: Output file path
            format: Export format (json, graphviz, cytoscape)
        """
        if format.lower() == "json":
            graph_data = {
                "nodes": {name: node.to_dict() for name, node in self.nodes.items()},
                "edges": {key: edge.to_dict() for key, edge in self.edges.items()},
                "metadata": {
                    "export_timestamp": datetime.now().isoformat(),
                    "total_nodes": len(self.nodes),
                    "total_edges": len(self.edges),
                },
            }

            with open(output_path, "w") as f:
                json.dump(graph_data, f, indent=2)

        elif format.lower() == "graphviz":
            self._export_graphviz(output_path)

        elif format.lower() == "cytoscape":
            self._export_cytoscape(output_path)

        else:
            raise ValueError(f"Unsupported format: {format}")

        self.logger.info(f"Exported lineage graph to {output_path} ({format})")

    def _export_graphviz(self, output_path: str):
        """Export as Graphviz DOT format."""
        with open(output_path, "w") as f:
            f.write("digraph lineage {\n")
            f.write("  rankdir=LR;\n")
            f.write("  node [shape=box];\n\n")

            # Write nodes
            for node_name in self.nodes:
                f.write(f'  "{node_name}";\n')

            f.write("\n")

            # Write edges
            for edge in self.edges.values():
                label = edge.transformation if edge.transformation else ""
                f.write(f'  "{edge.source_asset}" -> "{edge.target_asset}"')
                if label:
                    f.write(f' [label="{label}"]')
                f.write(";\n")

            f.write("}\n")

    def _export_cytoscape(self, output_path: str):
        """Export as Cytoscape JSON format."""
        elements = []

        # Add nodes
        for node_name, node in self.nodes.items():
            elements.append(
                {
                    "data": {
                        "id": node_name,
                        "label": node_name,
                        "access_count": node.access_count,
                        "asset_type": node.asset_type,
                    }
                }
            )

        # Add edges
        for edge_key, edge in self.edges.items():
            elements.append(
                {
                    "data": {
                        "id": edge_key,
                        "source": edge.source_asset,
                        "target": edge.target_asset,
                        "transformation": edge.transformation,
                        "event_count": edge.event_count,
                    }
                }
            )

        cytoscape_data = {
            "elements": elements,
            "style": [
                {
                    "selector": "node",
                    "style": {
                        "content": "data(label)",
                        "text-valign": "center",
                        "text-halign": "center",
                    },
                },
                {
                    "selector": "edge",
                    "style": {
                        "curve-style": "bezier",
                        "target-arrow-shape": "triangle",
                    },
                },
            ],
        }

        with open(output_path, "w") as f:
            json.dump(cytoscape_data, f, indent=2)
