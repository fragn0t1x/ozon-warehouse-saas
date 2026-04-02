from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.warehouse import Warehouse, WarehouseStock
from app.models.variant import Variant
from app.models.product import Product
from app.models.variant_attribute import VariantAttribute
from app.models.inventory_transaction import InventoryTransaction, TransactionType
from app.models.supply import Supply
from datetime import datetime, timezone
from loguru import logger


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WarehouseService:
    async def _get_variant_attributes(self, db: AsyncSession, variant_id: int) -> dict[str, str]:
        attrs_stmt = select(VariantAttribute).where(VariantAttribute.variant_id == variant_id)
        attrs_result = await db.execute(attrs_stmt)
        return {attr.name: attr.value for attr in attrs_result.scalars().all()}

    async def _format_variant_label(self, db: AsyncSession, variant: Variant) -> str:
        attributes = await self._get_variant_attributes(db, variant.id)
        color = attributes.get("Цвет")
        size = attributes.get("Размер")

        parts = [f'артикул "{variant.offer_id or variant.sku}"']
        if color:
            parts.append(f"цвет {color}")
        if size:
            parts.append(f"размер {size}")

        return ", ".join(parts)

    async def _get_simple_group_stocks(
        self,
        db: AsyncSession,
        warehouse_id: int,
        variant_id: int,
    ) -> list[WarehouseStock]:
        variant = await db.get(Variant, variant_id)
        if not variant:
            raise ValueError(f"Variant {variant_id} not found")

        product = await db.get(Product, variant.product_id)
        if not product:
            raise ValueError(f"Product for variant {variant_id} not found")

        group_variants_stmt = select(Variant, Product).join(Product, Product.id == Variant.product_id)
        if product.warehouse_product_id:
            group_variants_stmt = group_variants_stmt.where(
                Product.warehouse_product_id == product.warehouse_product_id
            )
        else:
            group_variants_stmt = group_variants_stmt.where(
                Product.id == product.id
            )
        group_variants_result = await db.execute(group_variants_stmt)
        variant_rows = group_variants_result.all()
        variant_ids = [row.Variant.id for row in variant_rows]

        attrs_map: dict[int, dict[str, str]] = {}
        if variant_ids:
            attrs_stmt = select(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids))
            attrs_result = await db.execute(attrs_stmt)
            for attr in attrs_result.scalars().all():
                attrs_map.setdefault(attr.variant_id, {})[attr.name] = attr.value

        target_attrs = attrs_map.get(variant_id, {})
        target_color = target_attrs.get("Цвет", "").strip().lower()
        target_size = target_attrs.get("Размер", "").strip().lower()

        matching_variants: list[Variant] = []
        for row in variant_rows:
            row_product = row.Product
            row_variant = row.Variant
            if product.warehouse_product_id and row_product.warehouse_product_id != product.warehouse_product_id:
                continue

            row_attrs = attrs_map.get(row_variant.id, {})
            row_color = row_attrs.get("Цвет", "").strip().lower()
            row_size = row_attrs.get("Размер", "").strip().lower()
            if row_color == target_color and row_size == target_size:
                matching_variants.append(row_variant)

        if not matching_variants:
            matching_variants = [variant]

        matching_variant_ids = [
            current_variant.id
            for current_variant in sorted(
                matching_variants,
                key=lambda current_variant: ((current_variant.pack_size or 1), current_variant.id)
            )
        ]

        stocks_stmt = select(WarehouseStock).where(
            WarehouseStock.warehouse_id == warehouse_id,
            WarehouseStock.variant_id.in_(matching_variant_ids)
        )
        stocks_result = await db.execute(stocks_stmt)
        stocks = stocks_result.scalars().all()

        stocks_by_variant = {stock.variant_id: stock for stock in stocks}
        ordered_stocks: list[WarehouseStock] = []
        for current_variant_id in matching_variant_ids:
            stock = stocks_by_variant.get(current_variant_id)
            if not stock:
                stock = WarehouseStock(
                    warehouse_id=warehouse_id,
                    variant_id=current_variant_id,
                    unpacked_quantity=0,
                    packed_quantity=0,
                    reserved_quantity=0
                )
                db.add(stock)
                await db.flush()
            ordered_stocks.append(stock)

        return ordered_stocks

    async def get_pack_availability(
        self,
        db: AsyncSession,
        warehouse_id: int,
        variant_id: int,
    ) -> dict[str, int | str]:
        variant = await db.get(Variant, variant_id)
        if not variant:
            raise ValueError(f"Variant {variant_id} not found")

        group_stocks = await self._get_simple_group_stocks(db, warehouse_id, variant_id)
        unpacked_stock = group_stocks[0]

        return {
            "pack_size": variant.pack_size or 1,
            "unpacked_quantity": unpacked_stock.unpacked_quantity,
            "variant_label": await self._format_variant_label(db, variant),
        }

    async def income(
        self,
        db: AsyncSession,
        warehouse_id: int,
        variant_id: int,
        quantity: int,
        reference_type: str = "MANUAL",
        reference_id: int | None = None,
        commit: bool = True
    ):
        try:
            warehouse = await db.get(Warehouse, warehouse_id)
            if not warehouse:
                raise ValueError(f"Warehouse {warehouse_id} not found")

            variant = await db.get(Variant, variant_id)
            if not variant:
                raise ValueError(f"Variant {variant_id} not found")

            group_stocks = await self._get_simple_group_stocks(db, warehouse_id, variant_id)
            stock = group_stocks[0]
            stock.unpacked_quantity += quantity

            tx = InventoryTransaction(
                warehouse_id=warehouse_id,
                variant_id=variant_id,
                type=TransactionType.INCOME,
                quantity=quantity,
                reference_type=reference_type,
                reference_id=reference_id,
            )
            db.add(tx)

            await db.flush()
            if commit:
                await db.commit()
                await db.refresh(stock)

            logger.info(f"✅ Income: +{quantity} units for variant {variant_id} in warehouse {warehouse_id}")

            return {"status": "ok", "new_unpacked": stock.unpacked_quantity}

        except Exception as e:
            logger.error(f"❌ Error in income: {e}")
            await db.rollback()
            raise

    async def pack(
        self,
        db: AsyncSession,
        warehouse_id: int,
        variant_id: int,
        boxes: int,
        packing_mode: str | None = None,
        reference_type: str = "PACKING",
        reference_id: int | None = None,
        commit: bool = True
    ):
        try:
            if packing_mode == "simple":
                raise ValueError("Упаковка доступна только в режиме «Расширенный — учитываем упакованные коробки»")

            warehouse = await db.get(Warehouse, warehouse_id)
            if not warehouse:
                raise ValueError(f"Warehouse {warehouse_id} not found")

            variant = await db.get(Variant, variant_id)
            if not variant:
                raise ValueError(f"Variant {variant_id} not found")

            pack_size = variant.pack_size or 1
            total_items = boxes * pack_size

            group_stocks = await self._get_simple_group_stocks(db, warehouse_id, variant_id)
            unpacked_stock = group_stocks[0]
            stock = await self._get_stock(db, warehouse_id, variant_id)

            if unpacked_stock.unpacked_quantity < total_items:
                variant_label = await self._format_variant_label(db, variant)
                raise ValueError(
                    f"Недостаточно неупакованного остатка для {variant_label}. "
                    f"Доступно: {unpacked_stock.unpacked_quantity} шт, нужно: {total_items} шт."
                )

            unpacked_stock.unpacked_quantity -= total_items
            stock.packed_quantity += boxes

            tx = InventoryTransaction(
                warehouse_id=warehouse_id,
                variant_id=variant_id,
                type=TransactionType.PACK,
                quantity=boxes,
                reference_type=reference_type,
                reference_id=reference_id,
            )
            db.add(tx)

            await db.flush()
            if commit:
                await db.commit()
                await db.refresh(unpacked_stock)
                await db.refresh(stock)

            logger.info(f"📦 Packed: {boxes} boxes ({total_items} units) of variant {variant_id}")

            return {
                "status": "ok",
                "unpacked": unpacked_stock.unpacked_quantity,
                "packed": stock.packed_quantity
            }

        except Exception as e:
            logger.error(f"❌ Error in pack: {e}")
            await db.rollback()
            raise

    async def reserve(
        self,
        db: AsyncSession,
        warehouse_id: int,
        variant_id: int,
        quantity: int,
        supply_id: int,
        packing_mode: str | None = None,
        commit: bool = True
    ):
        try:
            stmt = select(InventoryTransaction).where(
                InventoryTransaction.reference_type == "SUPPLY",
                InventoryTransaction.reference_id == supply_id,
                InventoryTransaction.variant_id == variant_id,
                InventoryTransaction.type == TransactionType.RESERVE
            )
            result = await db.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                logger.warning(f"⚠️ Supply {supply_id} already has reservation for variant {variant_id}")
                return {"status": "already_reserved", "reserved": existing.quantity}

            group_stocks = await self._get_simple_group_stocks(db, warehouse_id, variant_id)
            unpacked_stock = group_stocks[0]
            stock = unpacked_stock
            variant = await db.get(Variant, variant_id)
            if not variant:
                raise ValueError(f"Variant {variant_id} not found")
            pack_size = variant.pack_size or 1

            if packing_mode == "simple":
                available = unpacked_stock.unpacked_quantity - unpacked_stock.reserved_quantity
            else:
                packed_units = stock.packed_quantity * pack_size
                available = unpacked_stock.unpacked_quantity + packed_units - unpacked_stock.reserved_quantity
            if available < quantity:
                raise ValueError(f"Not enough available stock. Have {available}, need {quantity}")

            unpacked_stock.reserved_quantity += quantity

            tx = InventoryTransaction(
                warehouse_id=warehouse_id,
                variant_id=variant_id,
                type=TransactionType.RESERVE,
                quantity=quantity,
                reference_type="SUPPLY",
                reference_id=supply_id
            )
            db.add(tx)

            supply = await db.get(Supply, supply_id)
            if supply:
                supply.reserved_at = utcnow()

            await db.flush()
            if commit:
                await db.commit()
                await db.refresh(unpacked_stock)

            logger.info(f"🔒 Reserved {quantity} units for supply {supply_id}")

            return {"status": "ok", "reserved": unpacked_stock.reserved_quantity}

        except ValueError as e:
            if str(e).startswith("Not enough available stock."):
                logger.warning(f"⏸️ Reserve is waiting for stock income: {e}")
            else:
                logger.error(f"❌ Error in reserve: {e}")
            await db.rollback()
            raise
        except Exception as e:
            logger.error(f"❌ Error in reserve: {e}")
            await db.rollback()
            raise

    async def ship(
        self,
        db: AsyncSession,
        warehouse_id: int,
        variant_id: int,
        quantity: int,
        supply_id: int,
        packing_mode: str | None = None,
        commit: bool = True
    ):
        try:
            group_stocks = await self._get_simple_group_stocks(db, warehouse_id, variant_id)
            unpacked_stock = group_stocks[0]
            stock = await self._get_stock(db, warehouse_id, variant_id)
            variant = await db.get(Variant, variant_id)
            if not variant:
                raise ValueError(f"Variant {variant_id} not found")
            pack_size = variant.pack_size or 1

            if unpacked_stock.reserved_quantity < quantity:
                raise ValueError(f"Not enough reserved stock. Have {unpacked_stock.reserved_quantity}, need {quantity}")

            if packing_mode == "simple":
                if unpacked_stock.unpacked_quantity < quantity:
                    raise ValueError(f"Not enough unpacked stock. Have {unpacked_stock.unpacked_quantity}, need {quantity}")
                unpacked_stock.unpacked_quantity -= quantity
            else:
                remaining = quantity
                packed_units = stock.packed_quantity * pack_size

                if packed_units >= remaining:
                    boxes_to_ship = remaining // pack_size if pack_size else remaining
                    if boxes_to_ship * pack_size != remaining:
                        boxes_to_ship = (remaining + pack_size - 1) // pack_size
                    stock.packed_quantity -= boxes_to_ship
                    shipped_from_packed = boxes_to_ship * pack_size
                    leftover_units = shipped_from_packed - remaining
                    if leftover_units > 0:
                        unpacked_stock.unpacked_quantity += leftover_units
                    remaining = 0
                else:
                    stock.packed_quantity = 0
                    remaining -= packed_units
                    if unpacked_stock.unpacked_quantity < remaining:
                        raise ValueError(
                            f"Not enough unpacked stock. Have {unpacked_stock.unpacked_quantity}, need {remaining}"
                        )
                    unpacked_stock.unpacked_quantity -= remaining
                    remaining = 0

            unpacked_stock.reserved_quantity -= quantity

            tx = InventoryTransaction(
                warehouse_id=warehouse_id,
                variant_id=variant_id,
                type=TransactionType.SHIP,
                quantity=-quantity,
                reference_type="SUPPLY",
                reference_id=supply_id
            )
            db.add(tx)

            await db.flush()
            if commit:
                await db.commit()
                await db.refresh(unpacked_stock)
                await db.refresh(stock)

            logger.info(f"🚚 Shipped {quantity} units for supply {supply_id}")

            return {"status": "ok", "remaining_reserved": unpacked_stock.reserved_quantity}

        except Exception as e:
            logger.error(f"❌ Error in ship: {e}")
            await db.rollback()
            raise

    async def cancel_reserve(
        self,
        db: AsyncSession,
        warehouse_id: int,
        variant_id: int,
        quantity: int,
        supply_id: int,
        commit: bool = True
    ):
        try:
            group_stocks = await self._get_simple_group_stocks(db, warehouse_id, variant_id)
            stock = group_stocks[0]

            if stock.reserved_quantity < quantity:
                raise ValueError(f"Not enough reserved stock to cancel. Have {stock.reserved_quantity}, need {quantity}")

            stock.reserved_quantity -= quantity

            tx = InventoryTransaction(
                warehouse_id=warehouse_id,
                variant_id=variant_id,
                type=TransactionType.UNRESERVE,
                quantity=-quantity,
                reference_type="SUPPLY",
                reference_id=supply_id
            )
            db.add(tx)

            await db.flush()
            if commit:
                await db.commit()
                await db.refresh(stock)

            logger.info(f"🔓 Cancelled reserve of {quantity} units for supply {supply_id}")

            return {"status": "ok", "reserved": stock.reserved_quantity}

        except Exception as e:
            logger.error(f"❌ Error in cancel_reserve: {e}")
            await db.rollback()
            raise

    async def _get_stock(self, db: AsyncSession, warehouse_id: int, variant_id: int) -> WarehouseStock:
        stmt = select(WarehouseStock).where(
            WarehouseStock.warehouse_id == warehouse_id,
            WarehouseStock.variant_id == variant_id
        )
        result = await db.execute(stmt)
        stock = result.scalar_one_or_none()

        if not stock:
            stock = WarehouseStock(
                warehouse_id=warehouse_id,
                variant_id=variant_id,
                unpacked_quantity=0,
                packed_quantity=0,
                reserved_quantity=0
            )
            db.add(stock)
            await db.flush()
            logger.info(f"✨ Created new stock record for warehouse {warehouse_id}, variant {variant_id}")

        return stock

    async def return_from_shipment(self, db: AsyncSession, warehouse_id: int,
                                   variant_id: int, quantity: int,
                                   supply_id: int, reason: str = None,
                                   commit: bool = True):
        """
        Возврат товара после отгрузки (например, при расхождении приемки)
        Используется только в режиме "correction" при статусе COMPLETED
        """
        try:
            # Получаем запись о стоке
            group_stocks = await self._get_simple_group_stocks(db, warehouse_id, variant_id)
            stock = group_stocks[0]

            # Возвращаем товар на склад (в unpacked, т.к. он "как новый")
            stock.unpacked_quantity += quantity

            # Создаем транзакцию возврата
            tx = InventoryTransaction(
                warehouse_id=warehouse_id,
                variant_id=variant_id,
                type=TransactionType.RETURN,
                quantity=quantity,
                reference_type="SUPPLY",
                reference_id=supply_id,
                # metadata={"reason": reason} if reason else None  # Убрать metadata, если его нет в модели
            )
            db.add(tx)

            await db.flush()
            if commit:
                await db.commit()

            logger.info(f"🔄 Returned {quantity} units from supply {supply_id} to stock (reason: {reason})")

            return {"status": "ok", "returned": quantity}

        except Exception as e:
            logger.error(f"❌ Error in return_from_shipment: {e}")
            await db.rollback()
            raise

    async def delete_manual_transaction(
        self,
        db: AsyncSession,
        transaction: InventoryTransaction,
        packing_mode: str | None = None,
        commit: bool = True,
    ):
        try:
            if transaction.type == TransactionType.INCOME:
                group_stocks = await self._get_simple_group_stocks(db, transaction.warehouse_id, transaction.variant_id)
                stock = group_stocks[0]
                if stock.unpacked_quantity < transaction.quantity:
                    raise ValueError("Недостаточно неупакованного остатка для удаления прихода")
                stock.unpacked_quantity -= transaction.quantity

            elif transaction.type == TransactionType.PACK:
                stock = await self._get_stock(db, transaction.warehouse_id, transaction.variant_id)
                variant = await db.get(Variant, transaction.variant_id)
                if not variant:
                    raise ValueError(f"Variant {transaction.variant_id} not found")
                if stock.packed_quantity < transaction.quantity:
                    raise ValueError("Недостаточно упакованных коробок для отмены упаковки")

                group_stocks = await self._get_simple_group_stocks(db, transaction.warehouse_id, transaction.variant_id)
                unpacked_stock = group_stocks[0]
                stock.packed_quantity -= transaction.quantity
                unpacked_stock.unpacked_quantity += transaction.quantity * (variant.pack_size or 1)

            else:
                raise ValueError("Удаление поддерживается только для прихода и упаковки")

            await db.delete(transaction)
            await db.flush()

            if commit:
                await db.commit()

            return {"status": "ok"}
        except Exception as e:
            logger.error(f"❌ Error deleting manual transaction: {e}")
            await db.rollback()
            raise
