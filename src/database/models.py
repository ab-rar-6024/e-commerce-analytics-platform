"""Database models for e-commerce analytics platform."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    DECIMAL,
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column, relationship

Base = declarative_base()


class Customer(Base):
    """Customer master data table."""

    __tablename__ = "customers"

    # Primary key
    user_id: Mapped[str] = mapped_column(String(50), primary_key=True)

    # Basic information
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))
    phone: Mapped[Optional[str]] = mapped_column(String(20))
    date_of_birth: Mapped[Optional[datetime]] = mapped_column(DateTime)
    gender: Mapped[Optional[str]] = mapped_column(String(10))

    # Account information
    registration_date: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    last_login: Mapped[Optional[datetime]] = mapped_column(DateTime)
    account_status: Mapped[str] = mapped_column(String(20), default="active")
    customer_tier: Mapped[str] = mapped_column(String(20), default="bronze")

    # Business metrics
    lifetime_value: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), default=0.00)
    total_orders: Mapped[int] = mapped_column(Integer, default=0)

    # Geography (denormalised from address for fast regional reporting)
    country: Mapped[Optional[str]] = mapped_column(String(2))
    region: Mapped[Optional[str]] = mapped_column(String(30))

    # Preferences
    preferred_language: Mapped[str] = mapped_column(String(10), default="en")
    marketing_consent: Mapped[bool] = mapped_column(Boolean, default=False)

    # JSON fields for flexible data
    address: Mapped[Optional[dict]] = mapped_column(JSON)
    preferences: Mapped[Optional[dict]] = mapped_column(JSON)

    # Relationships
    orders: Mapped[List["Order"]] = relationship("Order", back_populates="customer")

    # Indexes
    __table_args__ = (
        Index("idx_customers_email", "email"),
        Index("idx_customers_status", "account_status"),
        Index("idx_customers_tier", "customer_tier"),
        Index("idx_customers_registration", "registration_date"),
        Index("idx_customers_region", "region"),
        CheckConstraint(
            "account_status IN ('active', 'inactive', 'suspended')",
            name="check_account_status",
        ),
        CheckConstraint(
            "customer_tier IN ('bronze', 'silver', 'gold', 'platinum')",
            name="check_customer_tier",
        ),
    )


class Product(Base):
    """Product catalog table."""

    __tablename__ = "products"

    # Primary key
    product_id: Mapped[str] = mapped_column(String(50), primary_key=True)

    # Basic information
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    sku: Mapped[Optional[str]] = mapped_column(String(100), unique=True)

    # Category information
    category_id: Mapped[str] = mapped_column(String(50), nullable=False)
    category_name: Mapped[str] = mapped_column(String(100), nullable=False)
    subcategory: Mapped[Optional[str]] = mapped_column(String(100))
    brand: Mapped[str] = mapped_column(String(100), nullable=False)

    # Pricing and inventory
    price: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=False)
    cost: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=False)
    inventory_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Physical properties
    weight_kg: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(8, 2))
    dimensions_cm: Mapped[Optional[str]] = mapped_column(String(50))

    # Status and metadata
    status: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), onupdate=func.now()
    )

    # Additional fields
    tags: Mapped[Optional[List[str]]] = mapped_column(JSON)
    supplier_id: Mapped[Optional[str]] = mapped_column(String(50))

    # Rating information
    rating_avg: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(3, 2))
    review_count: Mapped[int] = mapped_column(Integer, default=0)

    # Relationships
    order_items: Mapped[List["OrderItem"]] = relationship(
        "OrderItem", back_populates="product"
    )

    # Indexes
    __table_args__ = (
        Index("idx_products_category", "category_id"),
        Index("idx_products_brand", "brand"),
        Index("idx_products_status", "status"),
        Index("idx_products_price", "price"),
        Index("idx_products_created", "created_at"),
        Index("idx_products_sku", "sku"),
        CheckConstraint(
            "status IN ('active', 'inactive', 'discontinued')",
            name="check_product_status",
        ),
        CheckConstraint("price >= 0", name="check_positive_price"),
        CheckConstraint("cost >= 0", name="check_positive_cost"),
        CheckConstraint("inventory_count >= 0", name="check_positive_inventory"),
    )


class Order(Base):
    """Order header table."""

    __tablename__ = "orders"

    # Primary key
    order_id: Mapped[str] = mapped_column(String(50), primary_key=True)

    # Customer reference
    user_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("customers.user_id"), nullable=False
    )

    # Order information
    order_date: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    order_status: Mapped[str] = mapped_column(String(20), default="pending")

    # Financial information
    subtotal: Mapped[Decimal] = mapped_column(
        DECIMAL(12, 2), nullable=False, default=0.00
    )
    tax_amount: Mapped[Decimal] = mapped_column(
        DECIMAL(12, 2), nullable=False, default=0.00
    )
    shipping_cost: Mapped[Decimal] = mapped_column(
        DECIMAL(12, 2), nullable=False, default=0.00
    )
    discount_amount: Mapped[Decimal] = mapped_column(
        DECIMAL(12, 2), nullable=False, default=0.00
    )
    total_amount: Mapped[Decimal] = mapped_column(
        DECIMAL(12, 2), nullable=False, default=0.00
    )

    # Payment information
    payment_method: Mapped[Optional[str]] = mapped_column(String(50))
    payment_status: Mapped[str] = mapped_column(String(20), default="pending")
    payment_date: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Shipping information
    shipping_address: Mapped[Optional[dict]] = mapped_column(JSON)
    billing_address: Mapped[Optional[dict]] = mapped_column(JSON)
    shipping_method: Mapped[Optional[str]] = mapped_column(String(50))
    tracking_number: Mapped[Optional[str]] = mapped_column(String(100))

    # Fulfillment information
    shipped_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    delivered_date: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Additional information
    notes: Mapped[Optional[str]] = mapped_column(Text)
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    channel: Mapped[str] = mapped_column(String(20), default="online")

    # Known fraud outcome (e.g. a chargeback); used to evaluate fraud rules
    is_fraudulent: Mapped[bool] = mapped_column(Boolean, default=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), onupdate=func.now()
    )

    # Relationships
    customer: Mapped["Customer"] = relationship("Customer", back_populates="orders")
    order_items: Mapped[List["OrderItem"]] = relationship(
        "OrderItem", back_populates="order", cascade="all, delete-orphan"
    )

    # Indexes
    __table_args__ = (
        Index("idx_orders_user_id", "user_id"),
        Index("idx_orders_status", "order_status"),
        Index("idx_orders_date", "order_date"),
        Index("idx_orders_payment_status", "payment_status"),
        Index("idx_orders_total", "total_amount"),
        Index("idx_orders_channel", "channel"),
        CheckConstraint(
            "order_status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')",
            name="check_order_status",
        ),
        CheckConstraint(
            "payment_status IN ('pending', 'paid', 'failed', 'refunded', 'cancelled')",
            name="check_payment_status",
        ),
        CheckConstraint("subtotal >= 0", name="check_positive_subtotal"),
        CheckConstraint("tax_amount >= 0", name="check_positive_tax"),
        CheckConstraint("shipping_cost >= 0", name="check_positive_shipping"),
        CheckConstraint("discount_amount >= 0", name="check_positive_discount"),
        CheckConstraint("total_amount >= 0", name="check_positive_total"),
    )


class OrderItem(Base):
    """Order line items table."""

    __tablename__ = "order_items"

    # Primary key
    order_item_id: Mapped[str] = mapped_column(
        String(50), primary_key=True, default=lambda: str(uuid.uuid4())
    )

    # Foreign keys
    order_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("orders.order_id"), nullable=False
    )
    product_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("products.product_id"), nullable=False
    )

    # Item information
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=False)
    discount_amount: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), default=0.00)
    line_total: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False)

    # Product snapshot at time of order
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    product_sku: Mapped[Optional[str]] = mapped_column(String(100))
    product_category: Mapped[str] = mapped_column(String(100), nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    order: Mapped["Order"] = relationship("Order", back_populates="order_items")
    product: Mapped["Product"] = relationship("Product", back_populates="order_items")

    # Indexes
    __table_args__ = (
        Index("idx_order_items_order_id", "order_id"),
        Index("idx_order_items_product_id", "product_id"),
        Index("idx_order_items_created", "created_at"),
        CheckConstraint("quantity > 0", name="check_positive_quantity"),
        CheckConstraint("unit_price >= 0", name="check_positive_unit_price"),
        CheckConstraint("discount_amount >= 0", name="check_positive_discount"),
        CheckConstraint("line_total >= 0", name="check_positive_line_total"),
    )


class Category(Base):
    """Product category table."""

    __tablename__ = "categories"

    # Primary key
    category_id: Mapped[str] = mapped_column(String(50), primary_key=True)

    # Category information
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    parent_category_id: Mapped[Optional[str]] = mapped_column(
        String(50), ForeignKey("categories.category_id")
    )

    # Hierarchy and ordering
    level: Mapped[int] = mapped_column(Integer, default=1)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)

    # Status and metadata
    status: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), onupdate=func.now()
    )

    # Self-referential relationship for hierarchy
    parent: Mapped[Optional["Category"]] = relationship(
        "Category", remote_side="Category.category_id"
    )

    # Indexes
    __table_args__ = (
        Index("idx_categories_parent", "parent_category_id"),
        Index("idx_categories_status", "status"),
        Index("idx_categories_level", "level"),
        Index("idx_categories_sort_order", "sort_order"),
        CheckConstraint(
            "status IN ('active', 'inactive')", name="check_category_status"
        ),
        CheckConstraint("level > 0", name="check_positive_level"),
    )


class Supplier(Base):
    """Supplier master data table."""

    __tablename__ = "suppliers"

    # Primary key
    supplier_id: Mapped[str] = mapped_column(String(50), primary_key=True)

    # Basic information
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    contact_person: Mapped[Optional[str]] = mapped_column(String(255))
    email: Mapped[Optional[str]] = mapped_column(String(255))
    phone: Mapped[Optional[str]] = mapped_column(String(20))

    # Address information
    address: Mapped[Optional[dict]] = mapped_column(JSON)

    # Business information
    tax_id: Mapped[Optional[str]] = mapped_column(String(50))
    registration_number: Mapped[Optional[str]] = mapped_column(String(100))

    # Performance metrics
    rating: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(3, 2))
    total_orders: Mapped[int] = mapped_column(Integer, default=0)

    # Status and metadata
    status: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), onupdate=func.now()
    )

    # Indexes
    __table_args__ = (
        Index("idx_suppliers_status", "status"),
        Index("idx_suppliers_name", "name"),
        Index("idx_suppliers_email", "email"),
        CheckConstraint(
            "status IN ('active', 'inactive', 'suspended')",
            name="check_supplier_status",
        ),
    )


class UserEvent(Base):
    """Clickstream events (page views, cart actions, purchases) for funnels."""

    __tablename__ = "user_events"

    event_id: Mapped[str] = mapped_column(
        String(50), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    user_id: Mapped[Optional[str]] = mapped_column(
        String(50), ForeignKey("customers.user_id")
    )
    session_id: Mapped[str] = mapped_column(String(50), nullable=False)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    product_id: Mapped[Optional[str]] = mapped_column(String(50))
    device_type: Mapped[str] = mapped_column(String(20), default="desktop")
    traffic_source: Mapped[str] = mapped_column(String(20), default="direct")
    event_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        Index("idx_user_events_ts", "event_timestamp"),
        Index("idx_user_events_type", "event_type"),
        Index("idx_user_events_session", "session_id"),
        Index("idx_user_events_user", "user_id"),
        CheckConstraint(
            "event_type IN ('page_view', 'product_view', 'add_to_cart', "
            "'checkout_start', 'purchase')",
            name="check_event_type",
        ),
    )


class FraudAlert(Base):
    """Fraud alerts raised by the rules engine over stored orders."""

    __tablename__ = "fraud_alerts"

    alert_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    order_id: Mapped[Optional[str]] = mapped_column(
        String(50), ForeignKey("orders.order_id")
    )
    user_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("customers.user_id"), nullable=False
    )
    alert_type: Mapped[str] = mapped_column(String(30), nullable=False)
    priority: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="open")
    risk_score: Mapped[float] = mapped_column(DECIMAL(4, 3), nullable=False)
    amount: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), default=0.00)
    description: Mapped[Optional[str]] = mapped_column(Text)
    rules_triggered: Mapped[Optional[list]] = mapped_column(JSON)
    assigned_to: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    __table_args__ = (
        Index("idx_fraud_alerts_created", "created_at"),
        Index("idx_fraud_alerts_status", "status"),
        Index("idx_fraud_alerts_priority", "priority"),
        Index("idx_fraud_alerts_user", "user_id"),
        Index("idx_fraud_alerts_type", "alert_type"),
        CheckConstraint(
            "priority IN ('critical', 'high', 'medium', 'low')",
            name="check_alert_priority",
        ),
        CheckConstraint(
            "status IN ('open', 'investigating', 'resolved', 'escalated', "
            "'false_positive')",
            name="check_alert_status",
        ),
    )
