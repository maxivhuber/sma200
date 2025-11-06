from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

from market_manager import manager


@asynccontextmanager
async def lifespan(app: FastAPI):
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
    try:
        server = await manager.get_server(symbol)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Symbol '{symbol}' not configured or loaded"
        )

    if server.data is None:
        raise HTTPException(status_code=503, detail="Historical data not ready")

    df = server.data
    result_df = df.copy()

    # Format the date index to string in the desired format
    result_df.index = result_df.index.strftime("%Y-%m-%d")

    # Convert to list of dicts
    return result_df.to_dict(orient="records")


@app.websocket("/live")
async def websocket_live(websocket: WebSocket, symbol: str = Query(...)) -> None:
    await websocket.accept()

    try:
        server = await manager.get_server(symbol)
    except KeyError:
        await websocket.close(code=4004, reason=f"Symbol '{symbol}' not available")
        return

    server._ws_subscribers.add(websocket)

    try:
        # Keep the connection alive; ignore client messages
        while True:
            # Use receive() instead of receive_text() to handle pings automatically
            await websocket.receive()
    except WebSocketDisconnect:
        pass
    except Exception:
        # Log here in production (e.g., logger.exception("WS error"))
        pass
    finally:
        server._ws_subscribers.discard(websocket)
