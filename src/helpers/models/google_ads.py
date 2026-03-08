"""Pydantic validation model for Google Ads data."""

from pydantic import BaseModel, field_validator

from src.helpers.date_parser import parse_date


class GoogleAdMetric(BaseModel):
    """A single day of metrics for one Google Ads campaign."""

    campaign_id: str
    campaign_name: str
    date: str
    impressions: int
    clicks: int
    cost_micros: int
    conversions: int
    conversion_value: float

    @field_validator("date")
    @classmethod
    def normalize_date(cls, v):
        return parse_date(v)
