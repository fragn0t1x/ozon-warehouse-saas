# backend/app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.api import warehouse_router, store_router, supply_router, settings_router
from app.api import auth_router, products_router, sync_router, matching_router
from app.api import calendar_router, shipments_router, dashboard_router, warehouse_product_router
from app.api import notifications_router, closed_months_router
from app.config import settings
from app.database import init_db, SessionLocal
from app.metrics import setup_metrics
from app.services.admin_bootstrap import ensure_admin_user
from app.services.admin_notifications import notify_backend_error
from app.services.bootstrap_sync import enqueue_startup_bootstrap_syncs
from app.services.export_status import ensure_export_root_dir
from loguru import logger


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.validate_runtime_settings()
    ensure_export_root_dir()
    if settings.AUTO_CREATE_SCHEMA:
        await init_db()
    await ensure_admin_user(SessionLocal)
    queued_bootstrap = await enqueue_startup_bootstrap_syncs(SessionLocal)
    logger.info("Queued startup bootstrap syncs: {}", queued_bootstrap)
    yield


app = FastAPI(
    title="Ozon Warehouse SaaS",
    version="2026.03",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.API_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
setup_metrics(app)

# Подключаем роутеры
app.include_router(auth_router.router)
app.include_router(warehouse_router.router)
app.include_router(store_router.router)
app.include_router(supply_router.router)
app.include_router(settings_router.router)
app.include_router(products_router.router)
app.include_router(sync_router.router)
app.include_router(matching_router.router)
app.include_router(calendar_router.router)
app.include_router(shipments_router.router)
app.include_router(dashboard_router.router)
app.include_router(warehouse_product_router.router)
app.include_router(notifications_router.router)
app.include_router(closed_months_router.router)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    await notify_backend_error(
        "fastapi",
        exc,
        details={
            "method": request.method,
            "path": request.url.path,
            "query": str(request.url.query or "-"),
        },
    )
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

@app.get("/")
async def root():
    return {"status": "ok", "message": "Ozon Warehouse SaaS API"}


@app.get("/healthz")
async def healthcheck():
    return {"status": "ok"}


@app.get("/readyz")
async def readiness():
    return {"status": "ready"}
