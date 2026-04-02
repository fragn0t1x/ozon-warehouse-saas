from app.models.user import User


def get_cabinet_owner_id(user: User) -> int:
    return int(getattr(user, "owner_user_id", None) or getattr(user, "id"))


def is_cabinet_owner(user: User) -> bool:
    return not bool(user.is_admin) and (user.role or "owner") == "owner"


def can_manage_business_settings(user: User) -> bool:
    return bool(user.is_admin) or is_cabinet_owner(user)


def ensure_cabinet_owner(user: User) -> None:
    if can_manage_business_settings(user):
        return
    raise PermissionError("Only the owner can change shared cabinet settings")
