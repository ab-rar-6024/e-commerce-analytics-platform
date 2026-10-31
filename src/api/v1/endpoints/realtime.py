"""
Real-time endpoints for API v1.

System health, stream metrics, business metrics and benchmarks, measured from the
database, this machine and the API's own request metrics (src/api/repositories/).
The WebSocket streams the same real-time metrics to connected clients.
"""

import asyncio
import json
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import get_database_session, get_session_local
from ...repositories import analytics as an
from ...repositories import realtime as repo

router = APIRouter()
READ = Depends(require_permission(Permission.READ_ANALYTICS))


class ConnectionManager:
    """Tracks open WebSocket connections."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()


@router.get("/system-health", summary="Get system health")
async def get_system_health(
    include_details: bool = Query(default=False, description="Include detailed component health"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.system_health(db, include_details)


@router.get("/stream-metrics", summary="Get stream / ingestion metrics")
async def get_stream_metrics(
    stream_type: str = Query(default="all", description="all, transactions, user_events"),
    time_window: str = Query(default="5m", description="Time window: 1m, 5m, 15m, 1h"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.stream_metrics(db, stream_type, time_window)


@router.get("/business-metrics", summary="Get business metrics")
async def get_business_metrics(
    metric_category: str = Query(default="all", description="all, sales, customer, inventory"),
    time_window: str = Query(default="1h", description="Time window: 15m, 1h, 4h, 24h"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.business_metrics(db, metric_category, time_window)


@router.get("/activity", summary="Get recent orders, hourly volume and regional split")
async def get_activity(
    hours: int = Query(default=24, ge=1, le=168, description="Hours of history"),
    limit: int = Query(default=10, ge=1, le=100, description="Recent transactions to return"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.activity(db, hours, limit)


@router.get("/performance-benchmarks", summary="Get performance benchmarks")
async def get_performance_benchmarks(
    benchmark_type: str = Query(default="all", description="Benchmark type"),
    time_range: str = Query(default="24h", description="Time range"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.performance_benchmarks(db, benchmark_type, time_range)


@router.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """Push real-time sales/traffic metrics every few seconds."""
    await manager.connect(websocket)
    try:
        while True:
            with get_session_local()() as db:
                payload = {"client_id": client_id, "metrics": an.realtime_metrics(db, "5m")}
            await websocket.send_text(json.dumps(payload, default=str))
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)
