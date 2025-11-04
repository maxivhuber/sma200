from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from market_manager import manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: nothing to do (servers created on-demand)
    yield
    # Shutdown
    await manager.stop_all()


app = FastAPI(lifespan=lifespan)


@app.get("/history")
async def get_history(symbol: str = Query(..., description="e.g. ^GSPC")):
    server = await manager.get_server(symbol)
    if server.data is None:
        return JSONResponse(
            status_code=503, content={"error": "Historical data not ready"}
        )

    df = server.data.copy()
    # Ensure it's a DataFrame with 'Close' column
    if isinstance(df, pd.Series):
        df = df.to_frame(name="Close")

    df = df.reset_index()
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    return df.to_dict(orient="records")


@app.websocket("/live")
async def websocket_live(websocket: WebSocket, symbol: str = Query(...)):
    await websocket.accept()

    server = await manager.get_server(symbol)
    server._ws_subscribers.add(websocket)

    try:
        # Keep connection alive; ignore messages (or implement ping/pong)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        server._ws_subscribers.discard(websocket)
