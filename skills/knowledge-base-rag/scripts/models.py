"""
models.py — Pydantic request and config models for the KB skill.
Mirrors the analytics bridge model patterns exactly.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "1.0"


class BaseRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ClientConfig(BaseRequestModel):
    base_url: str = Field(..., min_length=1, max_length=500)
    api_key: str = Field(..., min_length=1, max_length=500)
    timeout: float = Field(35.0, gt=0.1, le=120.0)
    max_retries: int = Field(2, ge=0, le=6)
    backoff_base: float = Field(0.5, gt=0.0, le=10.0)


class AskKbRequest(BaseRequestModel):
    question: str = Field(..., min_length=3, max_length=500)
    collection: str = Field("default", min_length=1, max_length=80)

    @field_validator("question", mode="before")
    @classmethod
    def _clean_question(cls, v: str) -> str:
        return " ".join(str(v).split()).strip()


class AddUrlRequest(BaseRequestModel):
    url: str = Field(..., min_length=10, max_length=2048)
    collection: str = Field("default", min_length=1, max_length=80)
    doc_title: str = Field("", max_length=200)

    @field_validator("url", mode="before")
    @classmethod
    def _clean_url(cls, v: str) -> str:
        return str(v).strip()


class SearchKbRequest(BaseRequestModel):
    query: str = Field(..., min_length=2, max_length=300)
    collection: str = Field("default", min_length=1, max_length=80)
    top_k: int = Field(5, ge=1, le=20)

    @field_validator("query", mode="before")
    @classmethod
    def _clean_query(cls, v: str) -> str:
        return " ".join(str(v).split()).strip()


class ListCollectionsRequest(BaseRequestModel):
    pass
