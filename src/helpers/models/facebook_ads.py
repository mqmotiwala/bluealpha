"""Pydantic validation model for Facebook Ads data."""

from pydantic import BaseModel, field_validator

from src.helpers.date_parser import parse_date


class FacebookAdRow(BaseModel):
    """A single row from the Facebook CSV export."""

    campaign_id: str
    campaign_name: str
    date: str
    impressions: int
    clicks: int
    spend: float
    purchases: int = 0  # Missing purchases treated as 0
    purchase_value: float = 0.0
    reach: int
    frequency: float

    @field_validator("date")
    @classmethod
    def normalize_date(cls, v):
        return parse_date(v)

    @field_validator("purchases", mode="before")
    @classmethod
    def fill_missing_purchases(cls, v):
        if v is None or v == "":
            return 0
        return int(v)

    @field_validator("purchase_value", mode="before")
    @classmethod
    def fill_missing_purchase_value(cls, v):
        if v is None or v == "":
            return 0.0
        return float(v)
