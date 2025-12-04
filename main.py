from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

import pandas as pd
from fastapi import (Depends, FastAPI, HTTPException, Query, WebSocket,
                     WebSocketDisconnect, status)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import config, logger
from market_manager import manager
from sma200.utils import format_analytics_payload, market_is_open

origins = ["https://invest.mhuber.dev"]

class SymbolResponse(BaseModel):
    value: str
    label: str

class StrategyResponse(BaseModel):
    value: str
    label: str

@asynccontextmanager
async def lifespan(app: FastAPI):
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

async def get_market_server(
    symbol: Annotated[str, Query(description="e.g. ^GSPC")]
):
    try:
        server = await manager.get_server(symbol)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Symbol '{symbol}' not configured or loaded",
        ) from exc
    
    if server.data is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="Historical data not ready"
        )
    return server

@app.get("/history")
async def get_history(
    server=Depends(get_market_server)
) -> list[dict]:
    df = server.data.copy()
    df.index = df.index.strftime("%Y-%m-%d")
    df = df.reset_index().rename(columns={"index": "Date"})
    return df.to_dict(orient="records")

@app.get("/symbols", response_model=list[SymbolResponse])
async def get_all_symbols():
    servers = await manager.get_all_servers()
    if not servers:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="No market servers available"
        )
    name_map = config.get("symbols", {})
    return [
        SymbolResponse(
            value=server.symbol, 
            label=name_map.get(server.symbol, server.symbol)
        )
        for server in servers
    ]

@app.get("/strategies", response_model=list[StrategyResponse])
async def get_all_strategies():
    servers = await manager.get_all_servers()
    if not servers:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="No market servers available"
        )
    strategies = servers[0].analytics.get_all_strategies()
    if not strategies:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="No strategies found"
        )
    return [
        StrategyResponse(value=internal, label=human)
        for internal, human in strategies
    ]

@app.get("/analytics/{strat}")
async def analytics_rest(
    strat: str,
    server=Depends(get_market_server),
) -> dict:
    if not server.analytics.exists(strat):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Strategy '{strat}' not configured or loaded"
        )
        
    df = server.data.copy()
    
    try:
        result, _ = server.analytics.execute(
            strat, df, server.symbol, streaming_update=False
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error running strategy '{strat}': {exc}"
        )

    if market_is_open():
        ts = result.get("time_series", {})
        result["time_series"] = {
            k: v[:-1] for k, v in ts.items() if isinstance(v, list)
        }

    return format_analytics_payload(server.symbol, strat, result)

@app.websocket("/ws/live")
async def intraday_data_ws(
    websocket: WebSocket, 
    symbol: Annotated[str, Query(description="The symbol to stream, e.g. ^GSPC")]
) -> None:
    pool_name = "live"
    try:
        server = await manager.get_server(symbol)
    except KeyError:
        logger.warning(f"Symbol '{symbol}' not found. Closing connection.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=f"Symbol '{symbol}' not available")
        return

    await websocket.accept()
    server.register_websocket(pool_name, websocket)
    
    try:
        async for _ in websocket.iter_text():
            pass
    except Exception as exc:
        logger.error(f"Unexpected error in live stream: {exc}")
    finally:
        server.unregister_websocket(pool_name, websocket)

@app.websocket("/ws/analytics/{strat}")
async def analytics_ws(
    websocket: WebSocket,
    strat: str,
    symbol: Annotated[str, Query(description="e.g. ^GSPC")],
) -> None:
    pool_name = f"analytics-{strat}"
    try:
        server = await manager.get_server(symbol)
    except KeyError:
        logger.warning(f"Symbol '{symbol}' not found. Closing connection.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=f"Symbol '{symbol}' not configured")
        return

    if not server.analytics.exists(strat):
        logger.warning(f"Strategy '{strat}' not found. Closing connection.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=f"Strategy '{strat}' not configured")
        return

    await websocket.accept()
    server.register_websocket(pool_name, websocket)

    try:
        async for _ in websocket.iter_text():
            pass
    except Exception as exc:
        logger.error(f"Unexpected WebSocket error: {exc}")
    finally:
        server.unregister_websocket(pool_name, websocket)