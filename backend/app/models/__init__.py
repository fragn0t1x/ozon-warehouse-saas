"""
Инициализация всех моделей в правильном порядке
"""

from app.models.user import User
from app.models.ozon_warehouse import Cluster, OzonWarehouse
from app.models.store import Store
from app.models.warehouse_product import WarehouseProduct
from app.models.product import Product
from app.models.variant import Variant
from app.models.category_attribute import CategoryPackAttribute
from app.models.variant_attribute import VariantAttribute
from app.models.user_settings import UserSettings
from app.models.user_notification import UserNotification
from app.models.web_push_subscription import WebPushSubscription
from app.models.store_economics_history import StoreEconomicsHistory
from app.models.variant_cost_history import VariantCostHistory
from app.models.store_month_finance import StoreMonthFinance, StoreMonthOfferFinance
from app.models.supply_notification_event import SupplyNotificationEvent
from app.models.supply_processing import SupplyProcessing
from app.models.warehouse import Warehouse, WarehouseStock
from app.models.supply import Supply, SupplyItem
from app.models.inventory_transaction import InventoryTransaction, TransactionType
from app.models.ozon_warehouse import OzonStock
from app.models.base_models import BaseProduct, BaseVariant, ProductMatch, VariantMatch

__all__ = [
    'User',
    'Cluster',
    'OzonWarehouse',
    'OzonStock',
    'Store',
    'WarehouseProduct',
    'Product',
    'Variant',
    'VariantAttribute',
    'Warehouse',
    'WarehouseStock',
    'Supply',
    'SupplyItem',
    'InventoryTransaction',
    'TransactionType',
    'CategoryPackAttribute',
    'UserSettings',
    'UserNotification',
    'WebPushSubscription',
    'StoreEconomicsHistory',
    'VariantCostHistory',
    'StoreMonthFinance',
    'StoreMonthOfferFinance',
    'SupplyNotificationEvent',
    'SupplyProcessing',
    'BaseProduct',
    'BaseVariant',
    'ProductMatch',
    'VariantMatch',
]
