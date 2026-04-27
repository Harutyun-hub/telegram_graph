from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, get_args


SCHEMA_VERSION = "1.0"
WindowLiteral = Literal["24h", "7d", "30d", "90d"]
SignalFocusLiteral = Literal["all", "asks", "needs", "fear"]
EntityTypeLiteral = Literal["auto", "topic", "category", "channel"]
SourceTypeLiteral = Literal["auto", "telegram", "facebook_page", "instagram_profile", "google_domain"]
DEFAULT_TIMEOUT = 40.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 0.75


class ValidationError(ValueError):
    pass


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _require_text(value: object, field_name: str, *, min_length: int = 1, max_length: int | None = None) -> str:
    text = _clean_text(value)
    if len(text) < min_length:
        if min_length > 1:
            raise ValidationError(f"{field_name} must be at least {min_length} characters after trimming")
        raise ValidationError(f"{field_name} is required")
    if max_length is not None and len(text) > max_length:
        raise ValidationError(f"{field_name} must be at most {max_length} characters")
    return text


def _optional_text(value: object, field_name: str, *, max_length: int | None = None) -> str | None:
    if value is None:
        return None
    text = _clean_text(value)
    if not text:
        return None
    if max_length is not None and len(text) > max_length:
        raise ValidationError(f"{field_name} must be at most {max_length} characters")
    return text


def _validate_literal(value: str, literal_type: object, field_name: str) -> str:
    allowed = set(get_args(literal_type))
    if value not in allowed:
        allowed_text = ", ".join(str(item) for item in sorted(allowed))
        raise ValidationError(f"{field_name} must be one of: {allowed_text}")
    return value


def _validate_int_range(value: object, field_name: str, *, ge: int | None = None, le: int | None = None) -> int:
    if isinstance(value, bool):
        raise ValidationError(f"{field_name} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer") from exc
    if ge is not None and number < ge:
        raise ValidationError(f"{field_name} must be greater than or equal to {ge}")
    if le is not None and number > le:
        raise ValidationError(f"{field_name} must be less than or equal to {le}")
    return number


def _validate_float_range(value: object, field_name: str, *, gt: float | None = None, le: float | None = None) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be a number") from exc
    if gt is not None and number <= gt:
        raise ValidationError(f"{field_name} must be greater than {gt}")
    if le is not None and number > le:
        raise ValidationError(f"{field_name} must be less than or equal to {le}")
    return number


@dataclass
class BaseRequestModel:
    pass


@dataclass
class WindowedRequest(BaseRequestModel):
    window: WindowLiteral = "7d"

    def __post_init__(self) -> None:
        self.window = _validate_literal(_clean_text(self.window), WindowLiteral, "window")  # type: ignore[assignment]


@dataclass
class LimitedWindowedRequest(WindowedRequest):
    limit: int = 5

    def __post_init__(self) -> None:
        super().__post_init__()
        self.limit = _validate_int_range(self.limit, "limit", ge=1, le=20)


@dataclass
class GetTopTopicsRequest(LimitedWindowedRequest):
    pass


@dataclass
class GetDecliningTopicsRequest(LimitedWindowedRequest):
    pass


@dataclass
class GetProblemSpikesRequest(WindowedRequest):
    pass


@dataclass
class GetQuestionClustersRequest(WindowedRequest):
    topic: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self.topic = _optional_text(self.topic, "topic", max_length=120)


@dataclass
class GetSentimentOverviewRequest(WindowedRequest):
    pass


@dataclass
class GetActiveAlertsRequest(BaseRequestModel):
    pass


@dataclass
class AskInsightsRequest(WindowedRequest):
    question: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.question = _require_text(self.question, "question", min_length=3, max_length=300)


@dataclass
class SearchEntitiesRequest(BaseRequestModel):
    query: str
    limit: int = 5

    def __post_init__(self) -> None:
        self.query = _require_text(self.query, "query", min_length=2, max_length=200)
        self.limit = _validate_int_range(self.limit, "limit", ge=1, le=10)


@dataclass
class AddSourceRequest(BaseRequestModel):
    value: str
    source_type: SourceTypeLiteral = "auto"
    title: Optional[str] = None

    def __post_init__(self) -> None:
        self.value = _require_text(self.value, "value", min_length=2, max_length=2048)
        self.source_type = _validate_literal(_clean_text(self.source_type), SourceTypeLiteral, "source_type")  # type: ignore[assignment]
        self.title = _optional_text(self.title, "title", max_length=256)


@dataclass
class GetTopicDetailRequest(WindowedRequest):
    topic: str = ""
    category: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self.topic = _require_text(self.topic, "topic", min_length=1, max_length=160)
        self.category = _optional_text(self.category, "category", max_length=120)


@dataclass
class GetTopicEvidenceRequest(GetTopicDetailRequest):
    view: Literal["all", "questions"] = "all"
    limit: int = 5
    focus_id: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self.view = _validate_literal(_clean_text(self.view), Literal["all", "questions"], "view")  # type: ignore[assignment]
        self.limit = _validate_int_range(self.limit, "limit", ge=1, le=10)
        self.focus_id = _optional_text(self.focus_id, "focus_id", max_length=200)


@dataclass
class GetFreshnessStatusRequest(BaseRequestModel):
    force: bool = False

    def __post_init__(self) -> None:
        self.force = bool(self.force)


@dataclass
class GetGraphSnapshotRequest(WindowedRequest):
    category: Optional[str] = None
    signal_focus: SignalFocusLiteral = "all"
    max_nodes: int = 12

    def __post_init__(self) -> None:
        super().__post_init__()
        self.category = _optional_text(self.category, "category", max_length=120)
        self.signal_focus = _validate_literal(_clean_text(self.signal_focus), SignalFocusLiteral, "signal_focus")  # type: ignore[assignment]
        self.max_nodes = _validate_int_range(self.max_nodes, "max_nodes", ge=10, le=30)


@dataclass
class GetNodeContextRequest(WindowedRequest):
    entity: str = ""
    type: EntityTypeLiteral = "auto"

    def __post_init__(self) -> None:
        super().__post_init__()
        self.entity = _require_text(self.entity, "entity", min_length=1, max_length=200)
        self.type = _validate_literal(_clean_text(self.type), EntityTypeLiteral, "type")  # type: ignore[assignment]


@dataclass
class InvestigateTopicRequest(GetTopicDetailRequest):
    pass


@dataclass
class InvestigateChannelRequest(WindowedRequest):
    channel: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.channel = _require_text(self.channel, "channel", min_length=1, max_length=160)


@dataclass
class InvestigateQuestionRequest(WindowedRequest):
    question: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.question = _require_text(self.question, "question", min_length=3, max_length=300)


@dataclass
class CompareTopicsRequest(WindowedRequest):
    topic_a: str = ""
    topic_b: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.topic_a = _require_text(self.topic_a, "topic_a", min_length=1, max_length=160)
        self.topic_b = _require_text(self.topic_b, "topic_b", min_length=1, max_length=160)
        if self.topic_a.lower() == self.topic_b.lower():
            raise ValidationError("topic_a and topic_b must be different")


@dataclass
class CompareChannelsRequest(WindowedRequest):
    channel_a: str = ""
    channel_b: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.channel_a = _require_text(self.channel_a, "channel_a", min_length=1, max_length=160)
        self.channel_b = _require_text(self.channel_b, "channel_b", min_length=1, max_length=160)
        if self.channel_a.lower() == self.channel_b.lower():
            raise ValidationError("channel_a and channel_b must be different")


@dataclass
class ClientConfig(BaseRequestModel):
    base_url: str
    api_key: str
    timeout: float = DEFAULT_TIMEOUT
    max_retries: int = DEFAULT_MAX_RETRIES
    backoff_base: float = DEFAULT_BACKOFF_BASE

    def __post_init__(self) -> None:
        self.base_url = _require_text(self.base_url, "base_url", min_length=1, max_length=500)
        self.api_key = _require_text(self.api_key, "api_key", min_length=1, max_length=500)
        self.timeout = _validate_float_range(self.timeout, "timeout", gt=0.1, le=120.0)
        self.max_retries = _validate_int_range(self.max_retries, "max_retries", ge=0, le=6)
        self.backoff_base = _validate_float_range(self.backoff_base, "backoff_base", gt=0.0, le=10.0)
