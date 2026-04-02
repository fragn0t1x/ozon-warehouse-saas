from app.services.ozon.client import OzonClient
from loguru import logger

class OzonValidationService:
    async def validate_store(self, client_id: str, api_key: str) -> dict:
        client = OzonClient(client_id, api_key, emit_notifications=False)

        try:
            logger.info(f"Validating store with client_id: {client_id}")
            result = await client.get_products_list(limit=1)

            if result and result.get("result"):
                items = result.get("result", {}).get("items", [])

                return {
                    "valid": True,
                    "message": "✓ Подключение успешно!",
                    "data": {
                        "products_count": result.get("result", {}).get("total", 0),
                        "has_products": len(items) > 0
                    }
                }
            else:
                return {
                    "valid": False,
                    "message": "❌ Не удалось получить данные. Проверьте Client-ID и API ключ"
                }

        except Exception as e:
            logger.error(f"Validation error: {e}")
            return {
                "valid": False,
                "message": f"❌ Ошибка подключения: {str(e)[:100]}"
            }
        finally:
            await client.close()

    async def get_store_info(self, client_id: str, api_key: str) -> dict:
        client = OzonClient(client_id, api_key, emit_notifications=False)

        try:
            result = await client.get_products_list(limit=1)

            return {
                "name": f"Магазин OZON ({client_id[-4:]})",
                "client_id": client_id,
                "is_valid": True
            }
        except Exception as e:
            logger.error(f"Error getting store info: {e}")
            return None
        finally:
            await client.close()
