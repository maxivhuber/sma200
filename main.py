from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

from market_manager import manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan by initializing and stopping all servers."""
    await manager.initialize_all_servers()
    try:
        yield
    finally:
        await manager.stop_all()


app = FastAPI(lifespan=lifespan)


@app.get("/history")
async def get_history(
    symbol: str = Query(..., description="e.g. ^GSPC"),
) -> list[dict[str, Any]]:
    """Return historical market data for the given symbol as a list of records."""
    try:
        server = await manager.get_server(symbol)
    except KeyError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol '{symbol}' not configured or loaded",
        ) from exc

    if server.data is None:
        raise HTTPException(status_code=503, detail="Historical data not ready")

    df = server.data.copy()
    df.index = df.index.strftime("%Y-%m-%d")  # Format index
    return df.to_dict(orient="records")


@app.websocket("/live")
async def intraday_data_ws(websocket: WebSocket, symbol: str = Query(...)) -> None:
    """Provide a live data websocket stream for the requested symbol."""
    pool_name = "live"

    try:
        server = await manager.get_server(symbol)
    except KeyError:
        await websocket.close(code=4004, reason=f"Symbol '{symbol}' not available")
        return

    await websocket.accept()
    server.register_websocket(pool_name, websocket)

    try:
        while True:
            await websocket.receive()  # Keep connection alive
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        server.unregister_websocket(pool_name, websocket)


@app.websocket("/analytics/{strat}/ws")
async def analytics_ws(
    websocket: WebSocket,
    strat: str,
    symbol: str = Query(..., description="e.g. ^GSPC"),
) -> None:
    """Provide analytics data for a given symbol and strategy via WebSocket."""
    pool_name = f"analytics-{strat}"

    try:
        server = await manager.get_server(symbol)
    except KeyError:
        await websocket.close(
            code=4004, reason=f"Symbol '{symbol}' not configured or loaded"
        )
        return

    if not server.analytics.exists(strat):
        await websocket.close(
            code=4004, reason=f"Strategy '{strat}' not configured or loaded"
        )
        return

    await websocket.accept()
    server.register_websocket(pool_name, websocket)

    try:
        while True:
            await websocket.receive()  # Keep connection alive
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        server.unregister_websocket(pool_name, websocket)
