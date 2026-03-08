"""Pydantic validation model for CRM order data."""

from typing import Optional

from pydantic import BaseModel, field_validator

from src.helpers.date_parser import parse_date


class CrmOrderRow(BaseModel):
    """A single row from the CRM revenue CSV."""

    order_id: str
    customer_id: Optional[str] = None
    order_date: str
    revenue: Optional[float] = None
    channel_attributed: str
    campaign_source: str = ""
    product_category: str
    region: str

    @field_validator("order_date")
    @classmethod
    def normalize_date(cls, v):
        return parse_date(v)

    @field_validator("channel_attributed")
    @classmethod
    def normalize_channel(cls, v):
        return v.strip().lower()

    @field_validator("customer_id", mode="before")
    @classmethod
    def coerce_empty_customer_id(cls, v):
        if v is None or v == "":
            return None
        return v.strip()

    @field_validator("revenue", mode="before")
    @classmethod
    def coerce_empty_revenue(cls, v):
        if v is None or v == "":
            return None
        return float(v)

    @field_validator("campaign_source", mode="before")
    @classmethod
    def allow_empty_campaign_source(cls, v):
        if v is None or v == "":
            return ""
        return v.strip()

    def get_quarantine_reason(self):
        """Return the quarantine reason if this row should be quarantined, else None.

        Business rules checked here rather than in validators so the model
        parses successfully and the transformation Lambda can inspect the row
        before routing it to quarantine.
        """
        if self.revenue is None:
            return "null_revenue"
        if self.customer_id is None:
            return "null_customer_id"
        return None
