"""Analytics extensions: geography, order channel/fraud outcome, user events, fraud alerts

Revision ID: a1c3e5f7b9d2
Revises: 70e4ba23d616
Create Date: 2026-10-31 14:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1c3e5f7b9d2"
down_revision: Union[str, Sequence[str], None] = "70e4ba23d616"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    # Customers: denormalised geography for regional reporting
    op.add_column("customers", sa.Column("country", sa.String(length=2), nullable=True))
    op.add_column("customers", sa.Column("region", sa.String(length=30), nullable=True))
    op.create_index("idx_customers_region", "customers", ["region"])

    # Orders: sales channel and the known fraud outcome
    op.add_column(
        "orders",
        sa.Column("channel", sa.String(length=20), nullable=False, server_default="online"),
    )
    op.add_column(
        "orders",
        sa.Column("is_fraudulent", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index("idx_orders_channel", "orders", ["channel"])

    # Clickstream events (funnels, traffic, sessions)
    op.create_table(
        "user_events",
        sa.Column("event_id", sa.String(length=50), nullable=False),
        sa.Column("user_id", sa.String(length=50), nullable=True),
        sa.Column("session_id", sa.String(length=50), nullable=False),
        sa.Column("event_type", sa.String(length=30), nullable=False),
        sa.Column("product_id", sa.String(length=50), nullable=True),
        sa.Column("device_type", sa.String(length=20), nullable=False, server_default="desktop"),
        sa.Column("traffic_source", sa.String(length=20), nullable=False, server_default="direct"),
        sa.Column("event_timestamp", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("event_id"),
        sa.ForeignKeyConstraint(["user_id"], ["customers.user_id"]),
        sa.CheckConstraint(
            "event_type IN ('page_view', 'product_view', 'add_to_cart', 'checkout_start', 'purchase')",
            name="check_event_type",
        ),
    )
    op.create_index("idx_user_events_ts", "user_events", ["event_timestamp"])
    op.create_index("idx_user_events_type", "user_events", ["event_type"])
    op.create_index("idx_user_events_session", "user_events", ["session_id"])
    op.create_index("idx_user_events_user", "user_events", ["user_id"])

    # Fraud alerts raised by the rules engine
    op.create_table(
        "fraud_alerts",
        sa.Column("alert_id", sa.String(length=50), nullable=False),
        sa.Column("order_id", sa.String(length=50), nullable=True),
        sa.Column("user_id", sa.String(length=50), nullable=False),
        sa.Column("alert_type", sa.String(length=30), nullable=False),
        sa.Column("priority", sa.String(length=10), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="open"),
        sa.Column("risk_score", sa.DECIMAL(4, 3), nullable=False),
        sa.Column("amount", sa.DECIMAL(12, 2), nullable=False, server_default="0"),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("rules_triggered", sa.JSON(), nullable=True),
        sa.Column("assigned_to", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("alert_id"),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"]),
        sa.ForeignKeyConstraint(["user_id"], ["customers.user_id"]),
        sa.CheckConstraint("priority IN ('critical', 'high', 'medium', 'low')", name="check_alert_priority"),
        sa.CheckConstraint(
            "status IN ('open', 'investigating', 'resolved', 'escalated', 'false_positive')",
            name="check_alert_status",
        ),
    )
    op.create_index("idx_fraud_alerts_created", "fraud_alerts", ["created_at"])
    op.create_index("idx_fraud_alerts_status", "fraud_alerts", ["status"])
    op.create_index("idx_fraud_alerts_priority", "fraud_alerts", ["priority"])
    op.create_index("idx_fraud_alerts_user", "fraud_alerts", ["user_id"])
    op.create_index("idx_fraud_alerts_type", "fraud_alerts", ["alert_type"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("fraud_alerts")
    op.drop_table("user_events")
    op.drop_index("idx_orders_channel", table_name="orders")
    op.drop_column("orders", "is_fraudulent")
    op.drop_column("orders", "channel")
    op.drop_index("idx_customers_region", table_name="customers")
    op.drop_column("customers", "region")
    op.drop_column("customers", "country")
