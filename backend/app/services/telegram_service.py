from collections import defaultdict
from datetime import datetime
from html import escape
import logging
from urllib.parse import quote
from typing import List, Dict, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

from app.config import settings

logger = logging.getLogger(__name__)


class TelegramService:
    TELEGRAM_LIMIT = 4096
    _cached_bot_username: str | None = None

    def __init__(self):
        self.token = settings.TELEGRAM_TOKEN.get_secret_value() if settings.TELEGRAM_TOKEN else None
        self.bot = Bot(token=self.token) if self.token else None
        self.default_chat_id = settings.TELEGRAM_CHAT_ID

    # ---------------------------------------------------------
    # helpers
    # ---------------------------------------------------------

    def _sanitize(self, text: str) -> str:
        """Удаляем неподдерживаемые HTML теги"""
        return text.replace("<br/>", "\n").replace("<br>", "\n")

    def _split_message(self, text: str) -> List[str]:
        """Безопасное разделение длинных сообщений"""
        if len(text) <= self.TELEGRAM_LIMIT:
            return [text]

        parts = []
        current = ""

        for line in text.split("\n"):
            if len(current) + len(line) + 1 < self.TELEGRAM_LIMIT:
                current += line + "\n"
            else:
                if current:
                    parts.append(current)
                current = line + "\n"

        if current:
            parts.append(current)

        return parts

    async def get_bot_username(self) -> Optional[str]:
        if not self.bot:
            return None

        if self.__class__._cached_bot_username:
            return self.__class__._cached_bot_username

        try:
            me = await self.bot.get_me()
        except Exception as e:
            logger.error(f"Failed to get Telegram bot username: {e}")
            return None

        self.__class__._cached_bot_username = me.username
        return me.username

    @staticmethod
    def build_connect_url(bot_username: Optional[str], token: str) -> Optional[str]:
        if not bot_username or not token:
            return None
        return f"https://t.me/{bot_username}?start={token}"

    @staticmethod
    def build_qr_code_url(connect_url: Optional[str]) -> Optional[str]:
        if not connect_url:
            return None
        return f"https://api.qrserver.com/v1/create-qr-code/?size=240x240&data={quote(connect_url, safe='')}"

    # ---------------------------------------------------------
    # send message
    # ---------------------------------------------------------

    async def send_message(self, chat_id: str, text: str, parse_mode="HTML") -> bool:
        if not self.bot:
            logger.warning("Telegram bot not configured")
            return False

        text = self._sanitize(text)

        try:
            parts = self._split_message(text)

            for part in parts:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=part,
                    parse_mode=parse_mode
                )

            return True

        except TelegramForbiddenError:
            logger.error(f"Bot blocked by user {chat_id}")
            return False

        except TelegramBadRequest as e:
            logger.error(f"Telegram error: {e}")
            return False

        except Exception as e:
            logger.error(f"Send message failed: {e}")
            return False

    # ---------------------------------------------------------
    # daily report
    # ---------------------------------------------------------

    def build_daily_report_text(
        self,
        stats: Dict,
        *,
        title: Optional[str] = None,
        store_stats: Optional[List[Dict]] = None,
    ) -> str:
        report_date_raw = str(stats.get("report_date") or "")
        report_date = report_date_raw
        if report_date_raw:
            try:
                report_date = datetime.fromisoformat(report_date_raw).strftime("%d.%m.%Y")
            except ValueError:
                report_date = report_date_raw
        else:
            report_date = datetime.now().strftime("%d.%m.%Y")

        def fmt_money(value: float | int | None) -> str:
            number = float(value or 0)
            return f"{number:,.2f}".replace(",", " ").replace(".00", "")

        def fmt_units(value: float | int | None) -> str:
            number = int(round(float(value or 0)))
            return f"{number:,}".replace(",", " ")

        def fmt_signed_units(value: float | int | None) -> str:
            number = int(round(float(value or 0)))
            if number > 0:
                return f"+{fmt_units(number)}"
            return fmt_units(number)

        def fmt_signed_money(value: float | int | None) -> str:
            number = float(value or 0)
            if number > 0:
                return f"+{fmt_money(number)}"
            if number < 0:
                return f"-{fmt_money(abs(number))}"
            return fmt_money(number)

        def pluralize(count: int, one: str, few: str, many: str) -> str:
            value = abs(int(count))
            if value % 10 == 1 and value % 100 != 11:
                return one
            if 2 <= value % 10 <= 4 and not 12 <= value % 100 <= 14:
                return few
            return many

        text = f"📊 <b>{title or f'Ежедневный отчет за {report_date}'}</b>\n\n"

        text += "<b>Общая сводка за вчера:</b>\n"
        text += f"• Заказано: {fmt_units(stats.get('ordered_units_yesterday'))} шт. на {fmt_money(stats.get('orders_amount_yesterday'))} ₽\n"
        returns_units_delta = int(stats.get("returns_units_yesterday") or 0) - int(stats.get("returns_units_prev_day") or 0)
        returns_amount_delta = float(stats.get("returns_amount_yesterday") or 0) - float(stats.get("returns_amount_prev_day") or 0)
        if stats.get("returns_units_available"):
            if stats.get("returns_units_delta_available"):
                text += (
                    f"• Возвращено: {fmt_units(stats.get('returns_units_yesterday'))} шт. "
                    f"({fmt_signed_units(returns_units_delta)} шт.) "
                    f"на {fmt_money(stats.get('returns_amount_yesterday'))} ₽ "
                    f"({fmt_signed_money(returns_amount_delta)} ₽)\n"
                )
            else:
                text += (
                    f"• Возвращено: {fmt_units(stats.get('returns_units_yesterday'))} шт. "
                    f"на {fmt_money(stats.get('returns_amount_yesterday'))} ₽\n"
                )
        else:
            text += (
                f"• Возвращено: сумма {fmt_money(stats.get('returns_amount_yesterday'))} ₽ "
                f"({fmt_signed_money(returns_amount_delta)} ₽)\n"
            )
        text += (
            f"• Магазинов с заказами: {stats.get('stores_with_orders_yesterday', 0)} "
            f"из {stats.get('stores_total', 0)}\n"
        )
        if stats.get("top_store_name"):
            text += (
                f"• Лидер дня: {stats.get('top_store_name')} — "
                f"{fmt_units(stats.get('top_store_units_yesterday'))} шт. "
                f"на {fmt_money(stats.get('top_store_revenue_yesterday'))} ₽\n"
            )

        text += "\n<b>Сейчас в работе:</b>\n"
        text += f"• Активные поставки: {stats.get('active_supplies', 0)}\n"
        text += f"• Поставки на сегодня: {stats.get('today_supplies', 0)}\n"
        text += f"• Доступно на складе: {fmt_units(stats.get('total_available'))} шт.\n"
        text += f"• Зарезервировано: {fmt_units(stats.get('total_reserved'))} шт.\n"

        gainers = stats.get("top_gainers") or []
        losers = stats.get("top_losers") or []

        if gainers:
            text += "\n<b>Выросли в заказах по всем магазинам:</b>\n"
            for item in gainers[:5]:
                text += (
                    f"• <code>{item.get('offer_id') or '-'}</code> — "
                    f"{fmt_units(item.get('units_yesterday'))} шт. на {fmt_money(item.get('revenue_yesterday'))} ₽ "
                    f"(+{item.get('delta_units', 0)})\n"
                )

        if losers:
            text += "\n<b>Просели в заказах по всем магазинам:</b>\n"
            for item in losers[:5]:
                text += (
                    f"• <code>{item.get('offer_id') or '-'}</code> — "
                    f"{fmt_units(item.get('units_yesterday'))} шт. на {fmt_money(item.get('revenue_yesterday'))} ₽ "
                    f"({item.get('delta_units', 0)})\n"
                )

        if store_stats:
            text += "\n<b>По каждому магазину:</b>\n"
            sorted_store_stats = sorted(
                store_stats,
                key=lambda store: (
                    int(store.get("ordered_units_yesterday") or 0),
                    float(store.get("ordered_revenue_yesterday") or 0),
                ),
                reverse=True,
            )
            for store in sorted_store_stats:
                delta_units = int(store.get("delta_units_yesterday") or 0)
                revenue_delta = float(store.get("ordered_revenue_yesterday") or 0) - float(store.get("ordered_revenue_prev_day") or 0)
                text += f"\n🏪 <b>{store.get('store_name', 'Без названия')}</b>\n"
                text += (
                    f"• Заказано: {fmt_units(store.get('ordered_units_yesterday'))} шт. "
                    f"({fmt_signed_units(delta_units)} шт.) "
                    f"на {fmt_money(store.get('ordered_revenue_yesterday'))} ₽ "
                    f"({fmt_signed_money(revenue_delta)} ₽)\n"
                )
                store_returns_amount_delta = float(store.get("returns_amount_yesterday") or 0) - float(store.get("returns_amount_prev_day") or 0)
                if store.get("returns_units_available"):
                    if store.get("returns_units_delta_available"):
                        store_returns_units_delta = int(store.get("returns_units_yesterday") or 0) - int(store.get("returns_units_prev_day") or 0)
                        text += (
                            f"• Возвращено: {fmt_units(store.get('returns_units_yesterday'))} шт. "
                            f"({fmt_signed_units(store_returns_units_delta)} шт.) "
                            f"на {fmt_money(store.get('returns_amount_yesterday'))} ₽ "
                            f"({fmt_signed_money(store_returns_amount_delta)} ₽)\n"
                        )
                    else:
                        text += (
                            f"• Возвращено: {fmt_units(store.get('returns_units_yesterday'))} шт. "
                            f"на {fmt_money(store.get('returns_amount_yesterday'))} ₽\n"
                        )
                elif float(store.get("returns_amount_yesterday") or 0) or float(store.get("returns_amount_prev_day") or 0):
                    text += (
                        f"• Возвращено: сумма {fmt_money(store.get('returns_amount_yesterday'))} ₽ "
                        f"({fmt_signed_money(store_returns_amount_delta)} ₽)\n"
                    )
                text += f"• Активных поставок: {store.get('active_supplies', 0)}\n"
                text += f"• Поставок на сегодня: {store.get('today_supplies', 0)}\n"

                store_gainers = store.get("top_gainers") or []
                if store_gainers:
                    text += "• Выросли:\n"
                    for item in store_gainers[:3]:
                        text += (
                            f"  - <code>{item.get('offer_id') or '-'}</code> — "
                            f"{fmt_units(item.get('units_yesterday'))} шт. "
                            f"на {fmt_money(item.get('revenue_yesterday'))} ₽ "
                            f"(+{item.get('delta_units', 0)})\n"
                        )

                store_losers = store.get("top_losers") or []
                if store_losers:
                    text += "• Просели:\n"
                    for item in store_losers[:3]:
                        text += (
                            f"  - <code>{item.get('offer_id') or '-'}</code> — "
                            f"{fmt_units(item.get('units_yesterday'))} шт. "
                            f"на {fmt_money(item.get('revenue_yesterday'))} ₽ "
                            f"({item.get('delta_units', 0)})\n"
                        )

        return text.strip()

    async def send_daily_report(
        self,
        chat_id: str,
        stats: Dict,
        *,
        title: Optional[str] = None,
        store_stats: Optional[List[Dict]] = None,
    ) -> bool:
        """Отправляет ежедневный отчет"""
        text = self.build_daily_report_text(stats, title=title, store_stats=store_stats)

        return await self.send_message(chat_id, text)

    def build_monthly_closed_month_report_text(
        self,
        stats: Dict,
        *,
        title: Optional[str] = None,
    ) -> str:
        month_label = str(stats.get("month_label") or stats.get("month") or "")
        if not month_label:
            month_label = datetime.now().strftime("%m.%Y")

        def fmt_money(value: float | int | None) -> str:
            number = float(value or 0)
            return f"{number:,.2f}".replace(",", " ").replace(".00", "")

        def fmt_units(value: float | int | None) -> str:
            number = int(round(float(value or 0)))
            return f"{number:,}".replace(",", " ")

        text = f"🗓️ <b>{title or f'Ежемесячный отчет за {month_label}'}</b>\n\n"
        text += "<b>Общая сводка по закрытому месяцу:</b>\n"
        text += f"• Магазинов в отчете: {int(stats.get('stores_included') or 0)} из {int(stats.get('stores_total') or 0)}\n"
        text += f"• Продано: {fmt_units(stats.get('sold_units'))} шт.\n"
        text += f"• Возвраты: {fmt_units(stats.get('returned_units'))} шт. на {fmt_money(stats.get('returned_amount'))} ₽\n"
        text += f"• Выручка: {fmt_money(stats.get('revenue_amount'))} ₽\n"
        text += f"• Себестоимость: {fmt_money(stats.get('cogs'))} ₽\n"
        text += f"• Чистая прибыль: {fmt_money(stats.get('net_profit'))} ₽\n"

        if stats.get("stores_pending"):
            text += f"• Еще без закрытого месяца: {int(stats.get('stores_pending') or 0)}\n"
        if stats.get("stores_with_warning"):
            text += f"• С ограничениями Ozon: {int(stats.get('stores_with_warning') or 0)}\n"
        if stats.get("stores_need_cost"):
            text += f"• Требуют себестоимость: {int(stats.get('stores_need_cost') or 0)}\n"

        store_rows = stats.get("stores") or []
        if store_rows:
            text += "\n<b>По магазинам:</b>\n"
            sorted_rows = sorted(store_rows, key=lambda item: float(item.get("revenue_amount") or 0), reverse=True)
            for item in sorted_rows[:10]:
                status = str(item.get("status_label") or item.get("status") or "").strip()
                text += (
                    f"\n🏪 <b>{item.get('store_name') or 'Без названия'}</b>\n"
                    f"• Статус: {status or '-'}\n"
                    f"• Продано: {fmt_units(item.get('sold_units'))} шт.\n"
                    f"• Выручка: {fmt_money(item.get('revenue_amount'))} ₽\n"
                    f"• Чистая прибыль: {fmt_money(item.get('net_profit'))} ₽\n"
                )

        return text.strip()

    async def send_monthly_closed_month_report(
        self,
        chat_id: str,
        stats: Dict,
        *,
        title: Optional[str] = None,
    ) -> bool:
        text = self.build_monthly_closed_month_report_text(stats, title=title)
        return await self.send_message(chat_id, text)

    # ---------------------------------------------------------
    # supplies message
    # ---------------------------------------------------------

    def build_today_supplies_grouped_text(
        self,
        supplies: List[Dict],
        *,
        title: Optional[str] = None,
    ) -> str:
        if not supplies:
            return "📅 На сегодня поставок нет."

        today = datetime.now().strftime("%d.%m.%Y")
        text = f"📦 <b>{title or f'Поставки на {today}'}</b>\n\n"
        supplies_count_label = (
            "Поставок"
            if title and "Ближайшие поставки" in title
            else "Поставок сегодня"
        )

        supplies_by_store: Dict[str, List[Dict]] = defaultdict(list)
        for supply in supplies:
            supplies_by_store[supply.get("store_name") or "Без названия"].append(supply)

        for store_name, store_supplies in supplies_by_store.items():
            safe_store_name = escape(str(store_name or "Без названия"))
            total_items = sum(supply.get("total_items", 0) for supply in store_supplies)
            total_quantity = sum(supply.get("total_quantity", 0) for supply in store_supplies)
            text += (
                f"🏪 <b>{safe_store_name}</b>\n"
                f"• {supplies_count_label}: {len(store_supplies)}\n"
                f"• Всего позиций: {total_items}\n"
                f"• Всего штук: {total_quantity}\n\n"
            )

            for supply in store_supplies:
                safe_order_number = escape(str(supply.get("order_number") or "—"))
                safe_status = escape(str(supply.get("status_ru") or supply.get("status") or "—"))
                text += f"🔹 <b>Заявка №{safe_order_number}</b>\n"
                text += f"📊 Статус: {safe_status}\n"

                if supply.get("timeslot_from") and supply.get("timeslot_to"):
                    time_from = supply["timeslot_from"].strftime("%H:%M")
                    time_to = supply["timeslot_to"].strftime("%H:%M")
                    text += f"⏰ Таймслот: {time_from} - {time_to}\n"

                if supply.get("products"):
                    text += "📦 Товары:\n"

                    for _product_id, variants in supply["products"].items():
                        if not variants:
                            continue

                        product_name = escape(str(variants[0].get("product_name") or "Без названия"))
                        text += f"\n<b>{product_name}</b>\n"

                        for item in variants:
                            variant = item["variant"]
                            attrs = []
                            offer_id = escape(str(variant.get("offer_id") or "").strip())

                            if variant.get("color"):
                                attrs.append(f"цвет: {escape(str(variant['color']))}")
                            if variant.get("size"):
                                attrs.append(f"размер: {escape(str(variant['size']))}")
                            if variant.get("pack_size", 1) > 1:
                                attrs.append(f"упаковка: {variant['pack_size']} шт")

                            details_text = f"({', '.join(attrs)})" if attrs else ""
                            variant_label = offer_id or "Без артикула"
                            text += f"  • {variant_label}{details_text} - {variant['quantity']} шт.\n"

                    text += (
                        f"\n<b>Итого по заявке:</b> "
                        f"{supply['total_items']} позиций, {supply['total_quantity']} шт.\n"
                    )

                text += "\n"

        return text

    async def send_today_supplies_grouped(
        self,
        chat_id: str,
        supplies: List[Dict],
        *,
        title: Optional[str] = None,
    ):
        """Отправляет группированное сообщение о поставках на сегодня"""
        text = self.build_today_supplies_grouped_text(supplies, title=title)
        return await self.send_message(chat_id, text)

    async def close(self):
        """Закрывает соединение с ботом"""
        if self.bot:
            await self.bot.session.close()
