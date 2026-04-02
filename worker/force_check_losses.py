import asyncio
import sys
from datetime import datetime, timedelta

sys.path.insert(0, '/app')
sys.path.insert(0, '/app/backend')

from app.database import SessionLocal
from app.models.supply import Supply, SupplyItem
from app.models.variant import Variant
from app.models.product import Product
from app.services.telegram_service import TelegramService
from sqlalchemy import select
import os


async def force_check_losses():
    """Принудительная проверка потерь за последние 3 дня"""
    print("🔍 Force checking losses for last 3 days...")

    async with SessionLocal() as db:
        # Ищем за последние 3 дня
        three_days_ago = datetime.now() - timedelta(days=3)

        stmt = select(Supply).where(
            Supply.status == "COMPLETED",
            Supply.completed_at >= three_days_ago
        )
        result = await db.execute(stmt)
        supplies = result.scalars().all()

        print(f"📊 Found {len(supplies)} completed supplies in last 3 days")

        telegram = TelegramService()
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not chat_id:
            print("❌ TELEGRAM_CHAT_ID not set")
            return

        for supply in supplies:
            # Получаем товары с потерями
            items_stmt = select(SupplyItem).where(
                SupplyItem.supply_id == supply.id,
                SupplyItem.accepted_quantity.isnot(None),
                SupplyItem.accepted_quantity < SupplyItem.quantity
            )
            items_result = await db.execute(items_stmt)
            items = items_result.scalars().all()

            if items:
                print(f"⚠️ Found losses in supply {supply.id} ({supply.order_number})")
                print(f"   Completed at: {supply.completed_at}")

                losses = []
                for item in items:
                    variant = await db.get(Variant, item.variant_id)
                    product = await db.get(Product, variant.product_id) if variant else None

                    loss = {
                        'variant_id': item.variant_id,
                        'sku': variant.sku if variant else 'Unknown',
                        'product_name': product.name if product else 'Unknown',
                        'quantity': item.quantity,
                        'accepted_quantity': item.accepted_quantity,
                        'loss': item.quantity - item.accepted_quantity
                    }
                    losses.append(loss)
                    print(f"   - {loss['product_name']} ({loss['sku']}): "
                          f"отправлено {loss['quantity']}, принято {loss['accepted_quantity']}, "
                          f"потеря {loss['loss']}")

                # Отправляем уведомление
                print(f"📤 Sending notification for supply {supply.id}...")
                result = await telegram.send_loss_notification(
                    chat_id,
                    supply.id,
                    supply.order_number,
                    losses
                )
                print(f"   Result: {result}")
            else:
                print(f"✅ No losses in supply {supply.id}")

        await telegram.close()
        print("✅ Force check completed")


if __name__ == "__main__":
    asyncio.run(force_check_losses())