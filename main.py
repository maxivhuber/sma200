from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from config import config
from market_manager import manager
from sma200.utils import format_analytics_payload, market_is_open

origins = ["https://invest.mhuber.dev"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan by initializing and stopping all servers."""
    await manager.initialize_all_servers()
    try:
        yield
    finally:
        await manager.stop_all()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/history")
async def get_history(
    symbol: str = Query(..., description="e.g. ^GSPC"),
) -> list[dict[str, Any]]:
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
    df.index = df.index.strftime("%Y-%m-%d")
    df = df.reset_index()
    df = df.rename(columns={"index": "Date"})

    return df.to_dict(orient="records")


@app.get("/symbols")
async def get_all_symbols() -> list[str]:
    servers = await manager.get_all_servers()
    if not servers:
        raise HTTPException(status_code=503, detail="No market servers available")

    name_map = config.get("symbols", {})
    symbols = [name_map.get(server.symbol, server.symbol) for server in servers]

    if not symbols:
        raise HTTPException(status_code=404, detail="No symbols found")

    return symbols


@app.get("/strategies")
async def get_all_strategies() -> list[str]:
    servers = await manager.get_all_servers()
    if not servers:
        raise HTTPException(status_code=503, detail="No market servers available")

    strategies = servers[0].analytics.get_all_strategies()
    if not strategies:
        raise HTTPException(status_code=404, detail="No strategies found")

    return strategies


@app.get("/analytics/{strat}")
async def analytics_rest(
    strat: str,
    symbol: str = Query(..., description="e.g. ^GSPC"),
) -> dict:
    try:
        server = await manager.get_server(symbol)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Symbol '{symbol}' not configured or loaded"
        )

    if not server.analytics.exists(strat):
        raise HTTPException(
            status_code=404, detail=f"Strategy '{strat}' not configured or loaded"
        )

    if server.data is None:
        raise HTTPException(status_code=503, detail="Server data not ready")

    df = server.data.copy()
    try:
        result, _ = server.analytics.execute(
            strat, df, server.symbol, streaming_update=False
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Error running strategy '{strat}': {exc}"
        )

    if market_is_open():
        ts = result.get("time_series", {})
        filtered_ts = {k: v[:-1] for k, v in ts.items() if isinstance(v, list)}
        result["time_series"] = filtered_ts

    return format_analytics_payload(server.symbol, strat, result)


@app.websocket("/ws/live")
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


@app.websocket("/ws/analytics/{strat}")
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
