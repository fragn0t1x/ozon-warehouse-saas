"""add product base_name

Revision ID: 2f6b6b1b0e6d
Revises: f7f8c79fb80a
Create Date: 2026-03-17 22:10:00.000000
"""
from __future__ import annotations

import re

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2f6b6b1b0e6d'
down_revision = 'f7f8c79fb80a'
branch_labels = None
depends_on = None


def extract_base_product_name(name: str) -> str:
    if not name:
        return "Без названия"

    characteristic_words = [
        'чёрн', 'черн', 'бел', 'син', 'красн', 'зелен', 'желт',
        'сер', 'мужск', 'женск', 'пар', 'упак', 'размер', 'темно',
        'набор', 'шт', 'штук',
    ]

    cleaned = re.sub(r'\s+', ' ', name).strip()
    words = re.split(r'[\s,()/+-]+', cleaned)

    base_words: list[str] = []
    for word in words:
        word_lower = word.lower()
        if not word_lower:
            continue

        if re.fullmatch(r'\d+', word_lower):
            break

        if any(char_word in word_lower for char_word in characteristic_words):
            if base_words:
                break
            continue

        base_words.append(word)

    if base_words:
        return " ".join(base_words)

    return cleaned


def upgrade() -> None:
    op.add_column('products', sa.Column('base_name', sa.String(), nullable=True))

    connection = op.get_bind()
    products = connection.execute(sa.text("SELECT id, name FROM products")).fetchall()

    for product_id, name in products:
        base_name = extract_base_product_name(name)
        connection.execute(
            sa.text("UPDATE products SET base_name = :base_name WHERE id = :product_id"),
            {"base_name": base_name, "product_id": product_id},
        )

    op.alter_column('products', 'base_name', existing_type=sa.String(), nullable=False)
    op.create_index('ix_products_store_base_name', 'products', ['store_id', 'base_name'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_products_store_base_name', table_name='products')
    op.drop_column('products', 'base_name')
