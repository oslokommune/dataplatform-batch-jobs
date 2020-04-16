"""Add table for registering datasets on a date instead of just the count.

Revision ID: b35b6f01b439
Revises: 37f902020273
Create Date: 2020-04-14 13:17:36.722627

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b35b6f01b439"
down_revision = "37f902020273"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dataset_on_date",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("dataset_id", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset_id", "date"),
    )
    op.drop_table("dataset_count")


def downgrade():
    op.create_table(
        "dataset_count",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("date", sa.DATE(), autoincrement=False, nullable=True),
        sa.Column("count", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint("id", name="dataset_count_pkey"),
        sa.UniqueConstraint("date", name="dataset_count_date_key"),
    )
    op.drop_table("dataset_on_date")
