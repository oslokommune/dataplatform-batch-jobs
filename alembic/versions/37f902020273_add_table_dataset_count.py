"""Add dataset count table.

Revision ID: 37f902020273
Revises: e7c69542157f
Create Date: 2020-03-26 14:06:54.782026

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "37f902020273"
down_revision = "e7c69542157f"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dataset_count",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("count", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("date"),
    )


def downgrade():
    op.drop_table("dataset_count")
