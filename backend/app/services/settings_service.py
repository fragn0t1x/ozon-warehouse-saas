from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status
from app.models.user_settings import UserSettings
from app.models.store import Store
from app.models.user import User
from app.services.notification_schedule import build_notification_schedule, local_now
from app.services.warehouse_selector import ensure_shared_warehouse, ensure_store_warehouse
from app.services.user_settings_helper import get_or_create_user_settings
from app.schemas.user_settings import UserSettingsCreate
from app.utils.redis_cache import get_redis
from app.services.cabinet_access import can_manage_business_settings, get_cabinet_owner_id
from loguru import logger

class SettingsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _get_user(self, user_id: int) -> User:
        user = await self.db.get(User, user_id)
        if not user:
            raise ValueError(f"User {user_id} not found")
        return user

    async def _get_or_create_settings(self, user_id: int) -> UserSettings:
        settings, changed = await get_or_create_user_settings(self.db, user_id)

        if changed:
            await self.db.commit()
            await self.db.refresh(settings)

        return settings

    async def _get_owner_and_settings(self, user_id: int) -> tuple[User, UserSettings, UserSettings]:
        user = await self._get_user(user_id)
        owner_id = get_cabinet_owner_id(user)
        owner = await self._get_user(owner_id)
        owner_settings = await self._get_or_create_settings(owner.id)
        current_settings = owner_settings if owner.id == user.id else await self._get_or_create_settings(user.id)

        if owner_settings.warehouse_mode == "shared" and not owner_settings.shared_warehouse_id:
            await ensure_shared_warehouse(self.db, owner.id, owner_settings)
            await self.db.commit()
            await self.db.refresh(owner_settings)

        return owner, owner_settings, current_settings

    def _build_response(
            self,
            *,
            current_user: User,
            owner_settings: UserSettings,
            current_settings: UserSettings,
    ) -> UserSettings:
        response = UserSettings(
            id=current_settings.id,
            user_id=current_user.id,
            warehouse_mode=owner_settings.warehouse_mode,
            packing_mode=owner_settings.packing_mode,
            shipments_start_date=owner_settings.shipments_start_date,
            shipments_accounting_enabled=owner_settings.shipments_accounting_enabled,
            shipments_accounting_enabled_at=owner_settings.shipments_accounting_enabled_at,
            sync_products_interval_minutes=owner_settings.sync_products_interval_minutes,
            sync_supplies_interval_minutes=owner_settings.sync_supplies_interval_minutes,
            sync_stocks_interval_minutes=owner_settings.sync_stocks_interval_minutes,
            sync_reports_interval_minutes=owner_settings.sync_reports_interval_minutes,
            sync_finance_interval_minutes=owner_settings.sync_finance_interval_minutes,
            telegram_chat_id=current_settings.telegram_chat_id,
            notification_timezone=current_settings.notification_timezone,
            today_supplies_time_local=current_settings.today_supplies_time_local,
            daily_report_time_local=current_settings.daily_report_time_local,
            notify_today_supplies=current_settings.notify_today_supplies,
            notify_losses=current_settings.notify_losses,
            notify_daily_report=current_settings.notify_daily_report,
            notify_rejection=current_settings.notify_rejection,
            notify_acceptance_status=current_settings.notify_acceptance_status,
            email_notifications_enabled=current_settings.email_notifications_enabled,
            email_today_supplies=current_settings.email_today_supplies,
            email_losses=current_settings.email_losses,
            email_daily_report=current_settings.email_daily_report,
            email_rejection=current_settings.email_rejection,
            email_acceptance_status=current_settings.email_acceptance_status,
            web_push_notifications_enabled=current_settings.web_push_notifications_enabled,
            discrepancy_mode=owner_settings.discrepancy_mode,
            shared_warehouse_id=owner_settings.shared_warehouse_id,
            is_first_login=owner_settings.is_first_login if can_manage_business_settings(current_user) and not current_user.is_admin else False,
            created_at=current_settings.created_at,
            updated_at=current_settings.updated_at,
        )
        setattr(response, "role", current_user.role or "owner")
        setattr(response, "cabinet_owner_id", current_user.cabinet_owner_id)
        setattr(response, "can_manage_business_settings", can_manage_business_settings(current_user))
        return response

    async def _reset_notification_delivery_state(
            self,
            *,
            user_id: int,
            previous_settings: UserSettings,
            current_settings: UserSettings,
    ) -> None:
        redis = await get_redis()
        if not redis:
            return

        previous_schedule = build_notification_schedule(previous_settings)
        current_schedule = build_notification_schedule(current_settings)

        changed_fields = {
            "notification_timezone",
            "today_supplies_time_local",
            "daily_report_time_local",
        }
        if not any(
            getattr(previous_settings, field, None) != getattr(current_settings, field, None)
            for field in changed_fields
        ):
            return

        affected_dates = {
            local_now(previous_schedule).date().isoformat(),
            local_now(current_schedule).date().isoformat(),
        }

        report_types: set[str] = set()
        if (
            previous_settings.notification_timezone != current_settings.notification_timezone
            or previous_settings.today_supplies_time_local != current_settings.today_supplies_time_local
        ):
            report_types.add("today_supplies")
        if (
            previous_settings.notification_timezone != current_settings.notification_timezone
            or previous_settings.daily_report_time_local != current_settings.daily_report_time_local
        ):
            report_types.add("daily_report")

        if not report_types:
            return

        keys_to_delete = []
        for report_type in report_types:
            for report_date in affected_dates:
                keys_to_delete.append(f"notifications:sent:{report_type}:{user_id}:{report_date}")
                keys_to_delete.append(f"notifications:processing:{report_type}:{user_id}:{report_date}")

        if keys_to_delete:
            await redis.delete(*keys_to_delete)
            logger.info(f"♻️ Reset notification delivery state for user {user_id}: {sorted(report_types)}")

    async def get_settings(self, user_id: int) -> UserSettings:
        current_user = await self._get_user(user_id)
        _owner, owner_settings, current_settings = await self._get_owner_and_settings(user_id)
        return self._build_response(
            current_user=current_user,
            owner_settings=owner_settings,
            current_settings=current_settings,
        )

    async def update_settings(
            self,
            user_id: int,
            settings_data: UserSettingsCreate
    ) -> UserSettings:
        current_user = await self._get_user(user_id)
        owner, owner_settings, current_settings = await self._get_owner_and_settings(user_id)
        previous_personal_snapshot = UserSettings(
            user_id=current_settings.user_id,
            notification_timezone=current_settings.notification_timezone,
            today_supplies_time_local=current_settings.today_supplies_time_local,
            daily_report_time_local=current_settings.daily_report_time_local,
        )

        payload = settings_data.model_dump(exclude_unset=True)
        business_fields = {
            "warehouse_mode",
            "packing_mode",
            "shipments_start_date",
            "shipments_accounting_enabled",
            "discrepancy_mode",
            "sync_products_interval_minutes",
            "sync_supplies_interval_minutes",
            "sync_stocks_interval_minutes",
            "sync_reports_interval_minutes",
            "sync_finance_interval_minutes",
        }
        personal_fields = {
            "telegram_chat_id",
            "notification_timezone",
            "today_supplies_time_local",
            "daily_report_time_local",
            "notify_today_supplies",
            "notify_losses",
            "notify_daily_report",
            "notify_rejection",
            "notify_acceptance_status",
            "email_notifications_enabled",
            "email_today_supplies",
            "email_losses",
            "email_daily_report",
            "email_rejection",
            "email_acceptance_status",
            "web_push_notifications_enabled",
        }

        requested_business_fields = business_fields.intersection(payload.keys())
        if requested_business_fields and not can_manage_business_settings(current_user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the owner can change shared cabinet settings",
            )

        previous_mode = owner_settings.warehouse_mode
        previous_shipments_accounting_enabled = owner_settings.shipments_accounting_enabled
        previous_shipments_start_date = owner_settings.shipments_start_date

        for key in requested_business_fields:
            setattr(owner_settings, key, payload[key])

        if owner_settings.shipments_accounting_enabled:
            if owner_settings.shipments_start_date is not None:
                owner_settings.shipments_accounting_enabled_at = None
            else:
                should_reset_accounting_anchor = (
                    (not previous_shipments_accounting_enabled)
                    or (
                        "shipments_start_date" in requested_business_fields
                        and previous_shipments_start_date is not None
                        and owner_settings.shipments_start_date is None
                    )
                    or owner_settings.shipments_accounting_enabled_at is None
                )
                if should_reset_accounting_anchor:
                    owner_settings.shipments_accounting_enabled_at = datetime.now(timezone.utc)
        else:
            owner_settings.shipments_accounting_enabled_at = None

        for key in personal_fields.intersection(payload.keys()):
            setattr(current_settings, key, payload[key])

        if owner_settings.warehouse_mode == "shared" and not owner_settings.shared_warehouse_id:
            await ensure_shared_warehouse(self.db, owner.id, owner_settings)

        if previous_mode != owner_settings.warehouse_mode and owner_settings.warehouse_mode == "per_store":
            stores = await self.db.execute(select(Store).where(Store.user_id == owner.id))
            for store in stores.scalars().all():
                await ensure_store_warehouse(self.db, owner.id, store.id)

        await self.db.commit()
        await self.db.refresh(owner_settings)
        if current_settings.id != owner_settings.id:
            await self.db.refresh(current_settings)
        await self._reset_notification_delivery_state(
            user_id=user_id,
            previous_settings=previous_personal_snapshot,
            current_settings=current_settings,
        )

        logger.info(f"✅ Settings updated for user {user_id}")
        return self._build_response(
            current_user=current_user,
            owner_settings=owner_settings,
            current_settings=current_settings,
        )

    async def complete_onboarding(self, user_id: int) -> UserSettings:
        current_user = await self._get_user(user_id)
        owner, owner_settings, current_settings = await self._get_owner_and_settings(user_id)
        if can_manage_business_settings(current_user) and owner_settings.is_first_login:
            owner_settings.is_first_login = False
            await self.db.commit()
            await self.db.refresh(owner_settings)
        return self._build_response(
            current_user=current_user,
            owner_settings=owner_settings,
            current_settings=current_settings,
        )

    async def first_login_setup(
            self,
            user_id: int,
            settings_data: UserSettingsCreate
    ) -> dict:
        settings = await self.update_settings(user_id, settings_data)
        settings = await self.complete_onboarding(user_id)

        return {
            "status": "success",
            "message": "Настройки сохранены",
            "settings": settings,
            "next_step": "add_store"
        }
