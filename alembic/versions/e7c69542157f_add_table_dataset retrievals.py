"""Add dataset retrievals table.

Revision ID: e7c69542157f
Revises: 091026a652b2
Create Date: 2020-03-02 13:24:00.263529

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e7c69542157f"
down_revision = "091026a652b2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dataset_retrievals",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dataset_id", sa.String(), nullable=True),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("count", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset_id", "date"),
    )


def downgrade():
    op.drop_table("dataset_retrievals")
