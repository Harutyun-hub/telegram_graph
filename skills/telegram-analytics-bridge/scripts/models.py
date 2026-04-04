from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


SCHEMA_VERSION = "1.0"
WindowLiteral = Literal["24h", "7d", "30d", "90d"]


class BaseRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class WindowedRequest(BaseRequestModel):
    window: WindowLiteral = "7d"


class LimitedWindowedRequest(WindowedRequest):
    limit: int = Field(default=5, ge=1, le=20)


class GetTopTopicsRequest(LimitedWindowedRequest):
    pass


class GetDecliningTopicsRequest(LimitedWindowedRequest):
    pass


class GetProblemSpikesRequest(WindowedRequest):
    pass


class GetQuestionClustersRequest(WindowedRequest):
    topic: Optional[str] = Field(default=None, max_length=120)

    @field_validator("topic")
    @classmethod
    def normalize_topic(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = " ".join(value.split()).strip()
        return text or None


class GetSentimentOverviewRequest(WindowedRequest):
    pass


class GetActiveAlertsRequest(BaseRequestModel):
    pass


class AskInsightsRequest(WindowedRequest):
    question: str = Field(..., min_length=3, max_length=300)

    @field_validator("question")
    @classmethod
    def normalize_question(cls, value: str) -> str:
        text = " ".join(value.split()).strip()
        if len(text) < 3:
            raise ValueError("question must be at least 3 characters after trimming")
        return text


class SearchEntitiesRequest(BaseRequestModel):
    query: str = Field(..., min_length=2, max_length=200)
    limit: int = Field(default=5, ge=1, le=10)

    @field_validator("query")
    @classmethod
    def normalize_query(cls, value: str) -> str:
        text = " ".join(value.split()).strip()
        if len(text) < 2:
            raise ValueError("query must be at least 2 characters after trimming")
        return text


class GetTopicDetailRequest(WindowedRequest):
    topic: str = Field(..., min_length=1, max_length=160)
    category: Optional[str] = Field(default=None, max_length=120)

    @field_validator("topic")
    @classmethod
    def normalize_topic(cls, value: str) -> str:
        text = " ".join(value.split()).strip()
        if not text:
            raise ValueError("topic is required")
        return text

    @field_validator("category")
    @classmethod
    def normalize_category(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = " ".join(value.split()).strip()
        return text or None


class GetTopicEvidenceRequest(GetTopicDetailRequest):
    view: Literal["all", "questions"] = "all"
    limit: int = Field(default=5, ge=1, le=10)
    focus_id: Optional[str] = Field(default=None, max_length=200)

    @field_validator("focus_id")
    @classmethod
    def normalize_focus_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = " ".join(value.split()).strip()
        return text or None


class GetFreshnessStatusRequest(BaseRequestModel):
    force: bool = False


class InvestigateTopicRequest(GetTopicDetailRequest):
    pass


class InvestigateQuestionRequest(WindowedRequest):
    question: str = Field(..., min_length=3, max_length=300)

    @field_validator("question")
    @classmethod
    def normalize_question(cls, value: str) -> str:
        text = " ".join(value.split()).strip()
        if len(text) < 3:
            raise ValueError("question must be at least 3 characters after trimming")
        return text


class ClientConfig(BaseRequestModel):
    base_url: str = Field(..., min_length=1, max_length=500)
    api_key: str = Field(..., min_length=1, max_length=500)
    timeout: float = Field(default=35.0, gt=0.1, le=120.0)
    max_retries: int = Field(default=2, ge=0, le=6)
    backoff_base: float = Field(default=0.5, gt=0.0, le=10.0)

    @field_validator("base_url", "api_key")
    @classmethod
    def normalize_required_strings(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("value is required")
        return text
