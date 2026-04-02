import pytest

from app.services import admin_notifications
from app.services.admin_notifications import (
    _format_supply_status,
    _should_notify_timeslot_change,
    deliver_pending_supply_notification_events,
)


def test_format_supply_status_shows_label_and_code():
    assert _format_supply_status("READY_TO_SUPPLY") == "Готова к отгрузке (READY_TO_SUPPLY)"
    assert _format_supply_status("UNKNOWN_STATUS") == "UNKNOWN_STATUS"


def test_timeslot_change_notification_allowed_only_before_acceptance_stage():
    assert _should_notify_timeslot_change("READY_TO_SUPPLY", "READY_TO_SUPPLY")
    assert not _should_notify_timeslot_change("READY_TO_SUPPLY", "ACCEPTED_AT_SUPPLY_WAREHOUSE")
    assert not _should_notify_timeslot_change("IN_TRANSIT", "IN_TRANSIT")


@pytest.mark.asyncio
async def test_sync_success_is_recorded_but_not_sent_to_telegram_by_default(monkeypatch):
    recorded = []
    sent = {"called": False}

    async def fake_record_admin_event(*args, **kwargs):
        recorded.append((args, kwargs))

    async def fake_send_admin_message(*args, **kwargs):
        sent["called"] = True
        return True

    monkeypatch.setattr(admin_notifications, "record_admin_event", fake_record_admin_event)
    monkeypatch.setattr(admin_notifications, "send_admin_message", fake_send_admin_message)

    result = await admin_notifications.notify_sync_success(
        sync_type="Фоновая синхронизация остатков",
        store_id=1,
        store_name="Тестовый магазин",
    )

    assert result is False
    assert recorded
    assert sent["called"] is False


@pytest.mark.asyncio
async def test_sync_skipped_is_recorded_but_not_sent_to_telegram_by_default(monkeypatch):
    recorded = []
    sent = {"called": False}

    async def fake_record_admin_event(*args, **kwargs):
        recorded.append((args, kwargs))

    async def fake_send_admin_message(*args, **kwargs):
        sent["called"] = True
        return True

    monkeypatch.setattr(admin_notifications, "record_admin_event", fake_record_admin_event)
    monkeypatch.setattr(admin_notifications, "send_admin_message", fake_send_admin_message)

    result = await admin_notifications.notify_sync_skipped(
        sync_type="Фоновая синхронизация остатков",
        store_id=2,
        store_name="Тестовый магазин",
        reason="already_running",
    )

    assert result is False
    assert recorded
    assert sent["called"] is False


@pytest.mark.asyncio
async def test_deliver_pending_supply_notification_events_marks_event_as_sent(monkeypatch):
    class _FakeEvent:
        def __init__(self):
            self.supply_id = 11
            self.event_type = "supply_status_changed"
            self.dedupe_key = "dedupe-1"
            self.order_number = "2000001"
            self.store_id = 2
            self.store_name = "Паша"
            self.user_email = "admin@example.com"
            self.status_before = "READY_TO_SUPPLY"
            self.status_after = "IN_TRANSIT"
            self.timeslot_from = None
            self.timeslot_to = None
            self.old_timeslot_from = None
            self.old_timeslot_to = None
            self.telegram_sent_at = None
            self.last_error = None
            self.attempts = 0
            self.created_at = None

    class _FakeResult:
        def __init__(self, events):
            self._events = events

        def scalars(self):
            return self

        def all(self):
            return self._events

    class _FakeDb:
        def __init__(self, events):
            self.events = events
            self.commits = 0

        async def execute(self, _stmt):
            return _FakeResult(self.events)

        async def commit(self):
            self.commits += 1

    event = _FakeEvent()
    db = _FakeDb([event])

    async def fake_send_admin_message(*args, **kwargs):
        return True

    monkeypatch.setattr(admin_notifications, "send_admin_message", fake_send_admin_message)

    sent_count, failed_count = await deliver_pending_supply_notification_events(db)

    assert sent_count == 1
    assert failed_count == 0
    assert event.telegram_sent_at is not None
    assert event.last_error is None
    assert event.attempts == 1
    assert db.commits == 1
