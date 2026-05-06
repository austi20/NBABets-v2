from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.services.insights import InjuryStatusBadge, ProviderStatus


class ProviderStatusModel(BaseModel):
    provider_type: str
    provider_name: str
    endpoint: str
    fetched_at: datetime | None
    freshness_label: str
    status_label: str
    detail: str

    @classmethod
    def from_dataclass(cls, value: ProviderStatus) -> ProviderStatusModel:
        return cls(
            provider_type=value.provider_type,
            provider_name=value.provider_name,
            endpoint=value.endpoint,
            fetched_at=value.fetched_at,
            freshness_label=value.freshness_label,
            status_label=value.status_label,
            detail=value.detail,
        )


class InjuryStatusBadgeModel(BaseModel):
    label: str
    detail: str
    updated_at: datetime | None
    severity: int

    @classmethod
    def from_dataclass(cls, value: InjuryStatusBadge) -> InjuryStatusBadgeModel:
        return cls(
            label=value.label,
            detail=value.detail,
            updated_at=value.updated_at,
            severity=value.severity,
        )

