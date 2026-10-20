"""main.py — Bazian Oil Web WMS — FastAPI backend with real-time WebSocket sync"""
from __future__ import annotations
import asyncio
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

import database
from auth import (
    verify_password, create_access_token, get_current_user, require_admin,
    hash_password, decode_token_plain,
)

database.init_db()

app = FastAPI(title="Bazian Oil WMS API", version="4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_FRONTEND = Path(__file__).resolve().parent.parent / "frontend"
if _FRONTEND.exists():
    app.mount("/static", StaticFiles(directory=str(_FRONTEND)), name="static")

_ASSETS = database.ASSETS_DIR
if _ASSETS.exists():
    app.mount("/assets", StaticFiles(directory=str(_ASSETS)), name="assets")


@app.get("/", include_in_schema=False)
def root():
    fp = _FRONTEND / "index.html"
    if fp.exists():
        return FileResponse(str(fp))
    return {"message": "Bazian Oil WMS API running — v4.0"}


# ── WebSocket real-time hub ────────────────────────────────────────────────────
class _Hub:
    def __init__(self):
        self._sockets: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        async with self._lock:
            self._sockets.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._sockets.discard(ws)

    async def broadcast(self, data: dict):
        async with self._lock:
            dead: set[WebSocket] = set()
            for ws in self._sockets:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)
            self._sockets -= dead


_hub = _Hub()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = ""):
    await websocket.accept()
    if not token:
        await websocket.close(code=4001)
        return
    try:
        decode_token_plain(token)
    except Exception:
        await websocket.close(code=4001)
        return
    await _hub.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await _hub.disconnect(websocket)


# ── Auth ──────────────────────────────────────────────────────────────────────
class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    user:         dict


@app.post("/auth/login", response_model=TokenResponse, tags=["auth"])
def login(form: OAuth2PasswordRequestForm = Depends()):
    user = database.get_user_by_username(form.username)
    if not user or not verify_password(form.password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    token = create_access_token({
        "sub":          str(user["id"]),
        "username":     user["username"],
        "role":         user["role"],
        "can_checkin":  bool(user["can_checkin"]),
        "can_checkout": bool(user["can_checkout"]),
    })
    return TokenResponse(access_token=token, user={
        "id":           user["id"],
        "username":     user["username"],
        "full_name":    user.get("full_name"),
        "role":         user["role"],
        "can_checkin":  bool(user["can_checkin"]),
        "can_checkout": bool(user["can_checkout"]),
    })


@app.get("/auth/me", tags=["auth"])
def me(current: dict = Depends(get_current_user)):
    return current


# ── Dashboard ─────────────────────────────────────────────────────────────────
@app.get("/dashboard/stats", tags=["dashboard"])
def dashboard_stats(_: dict = Depends(get_current_user)):
    return database.get_dashboard_stats()


# ── Items ─────────────────────────────────────────────────────────────────────
class ItemCreate(BaseModel):
    name:             str
    category:         str
    quantity:         float = 0
    unit:             str
    unit_size:        Optional[float] = None
    brand:            Optional[str]   = None
    location:         Optional[str]   = None
    min_stock:        float = 0
    notes:            Optional[str]   = None
    glass_volume:     Optional[float] = None
    glass_dimensions: Optional[str]   = None
    custom_spec:      Optional[str]   = None
    expiry_date:      Optional[str]   = None
    batch_ref:        Optional[str]   = None
    image_path:       Optional[str]   = None


class ItemUpdate(BaseModel):
    name:             str
    category:         str
    unit:             str
    unit_size:        Optional[float] = None
    brand:            Optional[str]   = None
    location:         Optional[str]   = None
    min_stock:        float = 0
    notes:            Optional[str]   = None
    glass_volume:     Optional[float] = None
    glass_dimensions: Optional[str]   = None
    custom_spec:      Optional[str]   = None
    expiry_date:      Optional[str]   = None
    image_path:       Optional[str]   = None


@app.get("/items", tags=["items"])
def list_items(category: Optional[str] = None, search: Optional[str] = None,
               _: dict = Depends(get_current_user)):
    return database.get_items(category=category, search=search)


@app.post("/items", tags=["items"], status_code=201)
async def create_item(body: ItemCreate, current: dict = Depends(get_current_user)):
    if not current.get("can_checkin") and current.get("role") != "admin":
        raise HTTPException(403, "No permission to register items")
    item_id, merged = database.add_item(body.model_dump(), performed_by=current["username"])
    item = database.get_item(item_id)
    await _hub.broadcast({
        "type": "item_registered",
        "item": item["name"] if item else str(item_id),
        "merged": merged,
        "user": current["username"],
        "message": f"{current['username']} {'restocked' if merged else 'registered'} \"{item['name'] if item else ''}\"",
    })
    return {"id": item_id, "merged": merged}


@app.get("/items/{item_id}", tags=["items"])
def get_item(item_id: int, _: dict = Depends(get_current_user)):
    item = database.get_item(item_id)
    if not item: raise HTTPException(404, "Item not found")
    return item


@app.put("/items/{item_id}", tags=["items"])
async def update_item(item_id: int, body: ItemUpdate, current: dict = Depends(get_current_user)):
    if not database.get_item(item_id): raise HTTPException(404, "Item not found")
    database.update_item(item_id, body.model_dump())
    item = database.get_item(item_id)
    await _hub.broadcast({
        "type": "item_updated",
        "item": item["name"] if item else str(item_id),
        "user": current["username"],
        "message": f"{current['username']} updated \"{item['name'] if item else ''}\"",
    })
    return {"ok": True}


@app.delete("/items/{item_id}", tags=["items"])
async def delete_item(item_id: int, current: dict = Depends(require_admin)):
    item = database.get_item(item_id)
    if not item: raise HTTPException(404, "Item not found")
    name = item["name"]
    database.delete_item(item_id)
    await _hub.broadcast({
        "type": "item_deleted",
        "item": name,
        "user": current["username"],
        "message": f"{current['username']} deleted \"{name}\"",
    })
    return {"ok": True}


@app.get("/items/{item_id}/batches", tags=["items"])
def item_batches(item_id: int, _: dict = Depends(get_current_user)):
    return database.get_item_batches(item_id)


# ── Batches ───────────────────────────────────────────────────────────────────
class BatchCreate(BaseModel):
    quantity:    float
    expiry_date: Optional[str] = None
    batch_ref:   Optional[str] = None
    notes:       Optional[str] = None


@app.post("/items/{item_id}/batches", tags=["batches"], status_code=201)
async def add_batch(item_id: int, body: BatchCreate, current: dict = Depends(get_current_user)):
    if not current.get("can_checkin") and current.get("role") != "admin":
        raise HTTPException(403, "No permission to restock items")
    try:
        database.add_batch(
            item_id, body.quantity,
            expiry_date=body.expiry_date, batch_ref=body.batch_ref or "",
            notes=body.notes or "", performed_by=current["username"],
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    item = database.get_item(item_id)
    await _hub.broadcast({
        "type": "restock",
        "item": item["name"] if item else str(item_id),
        "qty": body.quantity,
        "user": current["username"],
        "message": f"{current['username']} restocked {body.quantity:.4g} units of \"{item['name'] if item else ''}\"",
    })
    return {"ok": True}


@app.get("/batches/expired", tags=["batches"])
def expired_batches(_: dict = Depends(get_current_user)):
    return database.get_expired_batches()


@app.get("/batches/near-expiry", tags=["batches"])
def near_expiry_batches(days: int = 30, _: dict = Depends(get_current_user)):
    return database.get_near_expiry_batches(days)


# ── Checkout ──────────────────────────────────────────────────────────────────
class CheckoutRequest(BaseModel):
    quantity: float
    notes:    Optional[str] = None


@app.post("/items/{item_id}/checkout", tags=["checkout"])
async def checkout_item(item_id: int, body: CheckoutRequest, current: dict = Depends(get_current_user)):
    if not current.get("can_checkout") and current.get("role") != "admin":
        raise HTTPException(403, "No permission to check out items")
    try:
        result = database.checkout_item(
            item_id, body.quantity,
            notes=body.notes or "", performed_by=current["username"],
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    await _hub.broadcast({
        "type": "checkout",
        "item": result.get("item", ""),
        "qty": body.quantity,
        "user": current["username"],
        "message": f"{current['username']} withdrew {body.quantity:.4g} units of \"{result.get('item', '')}\"",
    })
    return result


# ── Activity Log ──────────────────────────────────────────────────────────────
@app.get("/activity", tags=["activity"])
def activity_log(
    performed_by:     Optional[str] = None,
    date_from:        Optional[str] = None,
    date_to:          Optional[str] = None,
    transaction_type: Optional[str] = None,
    search:           Optional[str] = None,
    limit:            int = 500,
    _:                dict = Depends(get_current_user),
):
    return database.get_activity_log(
        performed_by=performed_by, date_from=date_from, date_to=date_to,
        transaction_type=transaction_type, search=search, limit=limit,
    )


# ── Reports ───────────────────────────────────────────────────────────────────
@app.get("/reports", tags=["reports"])
def report(date_from: str, date_to: str, _: dict = Depends(get_current_user)):
    return database.get_report_data(date_from, date_to)


# ── Users ─────────────────────────────────────────────────────────────────────
class UserCreate(BaseModel):
    username:     str
    password:     str
    full_name:    Optional[str] = None
    role:         str = "user"
    can_checkin:  bool = True
    can_checkout: bool = True
    is_active:    bool = True


class UserUpdate(BaseModel):
    full_name:    Optional[str] = None
    role:         str = "user"
    can_checkin:  bool = True
    can_checkout: bool = True
    is_active:    bool = True
    password:     Optional[str] = None


@app.get("/users", tags=["users"])
def list_users(_: dict = Depends(require_admin)):
    return database.get_users()


@app.post("/users", tags=["users"], status_code=201)
def create_user(body: UserCreate, _: dict = Depends(require_admin)):
    try:
        uid = database.add_user(body.model_dump())
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"id": uid}


@app.put("/users/{user_id}", tags=["users"])
def update_user(user_id: int, body: UserUpdate, _: dict = Depends(require_admin)):
    database.update_user(user_id, body.model_dump())
    return {"ok": True}


@app.delete("/users/{user_id}", tags=["users"])
def delete_user(user_id: int, _: dict = Depends(require_admin)):
    database.delete_user(user_id)
    return {"ok": True}


# ── Image upload ──────────────────────────────────────────────────────────────
from fastapi import UploadFile, File

@app.post("/upload/image", tags=["upload"])
async def upload_image(file: UploadFile = File(...), _: dict = Depends(get_current_user)):
    import uuid, shutil
    ext   = Path(file.filename).suffix.lower() if file.filename else ".bin"
    fname = uuid.uuid4().hex + ext
    dest  = database.ASSETS_DIR / fname
    database.ASSETS_DIR.mkdir(exist_ok=True)
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"filename": fname, "url": f"/assets/{fname}"}


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8081))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=not os.getenv("DATABASE_URL"))
