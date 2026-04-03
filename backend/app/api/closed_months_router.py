import os

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_current_user, get_db
from app.models.store import Store
from app.models.user import User
from app.schemas.closed_month_finance import (
    ClosedMonthFinanceDetailResponse,
    ClosedMonthFinanceResponse,
    ClosedMonthOfferFinanceResponse,
    ClosedMonthSyncResponse,
)
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.closed_month_history_service import ClosedMonthHistoryService
from app.services.export_status import (
    clear_export_status,
    get_export_status,
    has_export_lock,
    mark_export_queued,
)
from app.services.closed_months_recalc_queue import ClosedMonthsRecalcQueue
from app.services.sync_dispatcher import (
    celery_app,
    cancel_closed_month_history_sync,
    preempt_background_syncs_for_closed_months,
    schedule_closed_month_history_sync,
)
from app.services.sync_locks import has_any_sync_lock


router = APIRouter(prefix="/stores/{store_id}/closed-months", tags=["closed-months"])


async def _get_owned_store(db: AsyncSession, store_id: int, owner_user_id: int) -> Store:
    store = await db.get(Store, store_id)
    if not store or store.user_id != owner_user_id:
        raise HTTPException(status_code=404, detail="Store not found")
    return store


@router.get("", response_model=list[ClosedMonthFinanceResponse])
@router.get("/", response_model=list[ClosedMonthFinanceResponse], include_in_schema=False)
async def list_closed_months(
    store_id: int,
    limit: int = Query(24, ge=1, le=24),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    service = ClosedMonthHistoryService(db)
    return await service.list_months(
        store_id=store_id,
        owner_user_id=owner_id,
        limit=limit,
        include_non_ready=bool(current_user.is_admin),
    )


@router.get("/{month}", response_model=ClosedMonthFinanceDetailResponse)
async def get_closed_month(
    store_id: int,
    month: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    service = ClosedMonthHistoryService(db)
    month_row = await service.get_month(
        store_id=store_id,
        owner_user_id=owner_id,
        month=month,
        include_non_ready=bool(current_user.is_admin),
    )
    if month_row is None:
        raise HTTPException(status_code=404, detail="Closed month not found")
    offers = await service.list_month_offers(store_id=store_id, owner_user_id=owner_id, month=month)
    return ClosedMonthFinanceDetailResponse(
        month=ClosedMonthFinanceResponse.model_validate(month_row),
        offers=[ClosedMonthOfferFinanceResponse.model_validate(item) for item in offers],
    )


@router.get("/{month}/offers", response_model=list[ClosedMonthOfferFinanceResponse])
async def get_closed_month_offers(
    store_id: int,
    month: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    service = ClosedMonthHistoryService(db)
    offers = await service.list_month_offers(
        store_id=store_id,
        owner_user_id=owner_id,
        month=month,
        include_non_ready=bool(current_user.is_admin),
    )
    return [ClosedMonthOfferFinanceResponse.model_validate(item) for item in offers]


@router.post("/actions/sync", response_model=ClosedMonthSyncResponse, status_code=status.HTTP_202_ACCEPTED)
async def sync_closed_months(
    store_id: int,
    months_back: int = Query(3, ge=1, le=24),
    start_month: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    await preempt_background_syncs_for_closed_months(store_id)
    if await has_any_sync_lock(store_id, ("closed_months",)):
        raise HTTPException(
            status_code=409,
            detail="Выгрузка истории закрытых месяцев уже выполняется для этого магазина",
        )
    if start_month:
        try:
            requested_months = len(ClosedMonthHistoryService._closed_months_from_start(start_month))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if requested_months <= 0:
            raise HTTPException(status_code=400, detail="Выбранный месяц позже последнего закрытого месяца Ozon")
        result = await schedule_closed_month_history_sync(store_id, months_back=requested_months, start_month=start_month)
        latest_closed_month = ClosedMonthHistoryService._latest_closed_month()
        effective_start_month = ClosedMonthHistoryService._shift_month(
            latest_closed_month,
            -(requested_months - 1),
        ) if requested_months > 0 else start_month
        return ClosedMonthSyncResponse(
            status="queued",
            store_id=store_id,
            months_requested=requested_months,
            start_month=effective_start_month,
            end_month=latest_closed_month,
            task_queued=result.get("status") in {"queued", "deferred_after_full"},
        )

    effective_months_back = min(months_back, ClosedMonthHistoryService.MAX_HISTORY_MONTHS)
    result = await schedule_closed_month_history_sync(store_id, months_back=effective_months_back)
    return ClosedMonthSyncResponse(
        status="queued",
        store_id=store_id,
        months_requested=effective_months_back,
        start_month=None,
        end_month=None,
        task_queued=result.get("status") in {"queued", "deferred_after_full"},
    )


@router.post("/actions/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_closed_months_sync(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    try:
        return await cancel_closed_month_history_sync(store_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/actions/export")
async def start_closed_months_export(
    store_id: int,
    year: int = Query(..., ge=2000, le=2100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    service = ClosedMonthHistoryService(db)
    months = await service.list_months(
        store_id=store_id,
        owner_user_id=owner_id,
        limit=24,
        include_non_ready=bool(current_user.is_admin),
    )
    if not any(str(item.month).startswith(f"{year}-") for item in months):
        raise HTTPException(status_code=404, detail=f"За {year} год нет закрытых месяцев")
    if await ClosedMonthsRecalcQueue().has_pending(store_id):
        raise HTTPException(
            status_code=409,
            detail="После изменений себестоимости закрытые месяцы еще пересчитываются. Дождись завершения и сформируй Excel заново.",
        )

    current_status = get_export_status("closed_months", store_id, current_user.id)
    if has_export_lock("closed_months", store_id) or current_status.get("status") in {"queued", "running"}:
        return {
            **current_status,
            "status": current_status.get("status") if current_status.get("status") in {"queued", "running"} else "running",
            "phase": current_status.get("phase") or "queued",
            "phase_label": current_status.get("phase_label") or "Уже формируется",
            "progress_percent": current_status.get("progress_percent") or 5,
            "message": current_status.get("message") or f"Excel по закрытым месяцам за {year} год уже формируется",
            "duplicate_request": True,
        }

    task = celery_app.send_task(
        "worker.tasks.export_closed_months_excel_task",
        args=[store_id, current_user.id, year, await ClosedMonthsRecalcQueue().get_revision(store_id)],
    )
    return mark_export_queued(
        "closed_months",
        store_id,
        current_user.id,
        task_id=str(task.id),
        message=f"Excel по закрытым месяцам за {year} год поставлен в очередь",
        selection_label=f"{year} г.",
    )


@router.get("/actions/export/status")
async def get_closed_months_export_status(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    return get_export_status("closed_months", store_id, current_user.id)


@router.get("/actions/export/download")
async def download_closed_months_export(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)

    status_payload = get_export_status("closed_months", store_id, current_user.id)
    file_path = str(status_payload.get("file_path") or "")
    file_name = str(status_payload.get("file_name") or "closed_months.xlsx")
    if status_payload.get("status") != "success" or not file_path:
        raise HTTPException(status_code=404, detail="Готовый Excel пока не найден")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Файл выгрузки устарел или недоступен. Сформируй Excel заново.")
    return FileResponse(path=file_path, filename=file_name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@router.delete("/actions/export")
async def clear_closed_months_export(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    await _get_owned_store(db, store_id, owner_id)
    if has_export_lock("closed_months", store_id):
        raise HTTPException(status_code=409, detail="Нельзя очистить отчет, пока он формируется")
    return clear_export_status("closed_months", store_id, current_user.id)
