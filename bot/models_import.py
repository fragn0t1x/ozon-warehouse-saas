import sys
import os

# Добавляем путь к backend
sys.path.append("/app/backend")
sys.path.append("/app")

# Импортируем все модели в правильном порядке
from app.models.user import User
from app.models.store import Store
from app.models.ozon_warehouse import OzonWarehouse, Cluster, OzonStock
from app.models.product import Product
from app.models.variant import Variant
from app.models.warehouse import Warehouse, WarehouseStock
from app.models.supply import Supply, SupplyItem
from app.models.inventory_transaction import InventoryTransaction, TransactionType
from app.database import SessionLocal

# Экспортируем всё, что нужно боту
__all__ = [
    'Supply', 'Store', 'SessionLocal',
    'User', 'OzonWarehouse', 'Cluster', 'Product', 'Variant',
    'Warehouse', 'WarehouseStock', 'SupplyItem',
    'InventoryTransaction', 'TransactionType', 'OzonStock'
]

# Для отладки
print(f"✅ Models imported. TransactionType: {TransactionType}")