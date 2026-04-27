from __future__ import annotations

import unittest

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from models import (
    AddSourceRequest,
    AskInsightsRequest,
    ClientConfig,
    CompareChannelsRequest,
    CompareTopicsRequest,
    GetGraphSnapshotRequest,
    GetNodeContextRequest,
    GetQuestionClustersRequest,
    GetTopTopicsRequest,
    GetTopicEvidenceRequest,
    GetTopicDetailRequest,
    InvestigateChannelRequest,
    InvestigateQuestionRequest,
    SearchEntitiesRequest,
    ValidationError,
)


class ModelValidationTests(unittest.TestCase):
    def test_accepts_valid_window_and_limit(self) -> None:
        request = GetTopTopicsRequest(window="30d", limit=7)
        self.assertEqual(request.window, "30d")
        self.assertEqual(request.limit, 7)

    def test_rejects_invalid_window(self) -> None:
        with self.assertRaises(ValidationError):
            GetTopTopicsRequest(window="14d", limit=5)

    def test_normalizes_optional_topic(self) -> None:
        request = GetQuestionClustersRequest(window="7d", topic="  Residency permits  ")
        self.assertEqual(request.topic, "Residency permits")

    def test_question_is_trimmed(self) -> None:
        request = AskInsightsRequest(window="7d", question="  What is driving residency delays?   ")
        self.assertEqual(request.question, "What is driving residency delays?")

    def test_search_entities_normalizes_query(self) -> None:
        request = SearchEntitiesRequest(query="  residency permit delays  ", limit=3)
        self.assertEqual(request.query, "residency permit delays")
        self.assertEqual(request.limit, 3)

    def test_add_source_accepts_known_source_type(self) -> None:
        request = AddSourceRequest(value=" @docschat ", source_type="telegram", title=" Docs Chat ")
        self.assertEqual(request.value, "@docschat")
        self.assertEqual(request.source_type, "telegram")
        self.assertEqual(request.title, "Docs Chat")

    def test_add_source_rejects_unknown_source_type(self) -> None:
        with self.assertRaises(ValidationError):
            AddSourceRequest(value="@docschat", source_type="twitter")

    def test_topic_detail_normalizes_optional_category(self) -> None:
        request = GetTopicDetailRequest(window="7d", topic=" Residency permits ", category=" Documents ")
        self.assertEqual(request.topic, "Residency permits")
        self.assertEqual(request.category, "Documents")

    def test_topic_evidence_accepts_valid_view(self) -> None:
        request = GetTopicEvidenceRequest(window="7d", topic="Residency permits", view="questions", limit=4)
        self.assertEqual(request.view, "questions")
        self.assertEqual(request.limit, 4)

    def test_topic_evidence_rejects_invalid_view(self) -> None:
        with self.assertRaises(ValidationError):
            GetTopicEvidenceRequest(window="7d", topic="Residency permits", view="invalid")

    def test_investigate_question_normalizes_question(self) -> None:
        request = InvestigateQuestionRequest(window="7d", question="  Why are permit delays spiking?  ")
        self.assertEqual(request.question, "Why are permit delays spiking?")

    def test_graph_snapshot_accepts_signal_focus(self) -> None:
        request = GetGraphSnapshotRequest(window="7d", category=" Documents ", signal_focus="needs", max_nodes=12)
        self.assertEqual(request.category, "Documents")
        self.assertEqual(request.signal_focus, "needs")

    def test_node_context_defaults_to_auto(self) -> None:
        request = GetNodeContextRequest(window="7d", entity=" Docs Chat ")
        self.assertEqual(request.entity, "Docs Chat")
        self.assertEqual(request.type, "auto")

    def test_investigate_channel_normalizes_channel(self) -> None:
        request = InvestigateChannelRequest(window="7d", channel=" Docs Chat ")
        self.assertEqual(request.channel, "Docs Chat")

    def test_compare_topics_requires_distinct_topics(self) -> None:
        with self.assertRaises(ValidationError):
            CompareTopicsRequest(window="7d", topic_a="Residency permits", topic_b="Residency permits")

    def test_compare_channels_requires_distinct_channels(self) -> None:
        with self.assertRaises(ValidationError):
            CompareChannelsRequest(window="7d", channel_a="Docs Chat", channel_b="Docs Chat")

    def test_client_config_defaults_use_warmer_runtime_tuning(self) -> None:
        request = ClientConfig(base_url="https://analytics.example.com", api_key="sk_test")
        self.assertEqual(request.timeout, 40.0)
        self.assertEqual(request.max_retries, 3)
        self.assertEqual(request.backoff_base, 0.75)


if __name__ == "__main__":
    unittest.main()
