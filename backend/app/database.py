from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from app.config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.SQL_ECHO,
    pool_pre_ping=True,
    pool_recycle=settings.DB_POOL_RECYCLE_SECONDS,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()

async def init_db():
    from app.models import (  # noqa
        User, Store, WarehouseProduct, Product, Variant,
        Warehouse, WarehouseStock,
        Supply, SupplyItem,
        InventoryTransaction, TransactionType,
        Cluster, OzonWarehouse, OzonStock,
        CategoryPackAttribute,
        VariantAttribute,
        UserSettings,
        UserNotification,
        WebPushSubscription,
        StoreEconomicsHistory,
        VariantCostHistory,
        SupplyNotificationEvent,
        SupplyProcessing,
        StoreMonthFinance, StoreMonthOfferFinance,
        BaseProduct, BaseVariant, ProductMatch, VariantMatch
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
