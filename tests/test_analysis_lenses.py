from __future__ import annotations

import json
from pathlib import Path
import unittest

from api.analysis_lenses import (
    DEFAULT_ANALYSIS_LENS_IDS,
    SOCIAL_SYSTEM_PROMPT,
    analysis_lens_signature,
    build_lens_prompt_template,
    build_lens_system_prompt,
    filter_topics_by_confidence,
    get_analysis_lens_catalog,
    normalize_analysis_lens_ids,
    normalize_lens_metadata,
    render_active_lenses_block,
    resolve_analysis_lenses,
)
from processor import intent_extractor


class AnalysisLensTests(unittest.TestCase):
    def test_catalog_has_required_lenses_and_few_shots(self) -> None:
        catalog = get_analysis_lens_catalog()
        self.assertEqual({lens["id"] for lens in catalog}, {"finance_markets", "competitor_analysis", "business_analysis"})
        for lens in catalog:
            self.assertEqual(lens["confidence_threshold"], 0.70)
            self.assertGreaterEqual(len(lens["few_shot_examples"]), 3)
            for example in lens["few_shot_examples"]:
                self.assertTrue(example["input_excerpt"])
                self.assertTrue(example["bad_output_example"])
                self.assertTrue(example["good_output_example"])
                self.assertTrue(example["reason"])

    def test_default_and_unknown_lens_validation(self) -> None:
        self.assertEqual(normalize_analysis_lens_ids(None), list(DEFAULT_ANALYSIS_LENS_IDS))
        with self.assertRaises(ValueError):
            normalize_analysis_lens_ids(["finance_markets", "unknown_lens"])

    def test_signature_includes_versions(self) -> None:
        v1 = analysis_lens_signature([{"id": "finance_markets", "version": 1}])
        v2 = analysis_lens_signature([{"id": "finance_markets", "version": 2}])
        self.assertNotEqual(v1, v2)

    def test_prompt_rendering_is_stable_and_ordered(self) -> None:
        prompt_a = build_lens_system_prompt("BASE PROMPT", include_directive=True, suffix="FIXED SCHEMA")
        prompt_b = build_lens_system_prompt("BASE PROMPT", include_directive=True, suffix="FIXED SCHEMA")
        self.assertEqual(prompt_a, prompt_b)
        self.assertLess(prompt_a.index("BASE PROMPT"), prompt_a.index("ACTIVE_ANALYSIS_LENSES"))
        self.assertLess(prompt_a.index("ACTIVE_ANALYSIS_LENSES"), prompt_a.index("FIXED SCHEMA"))
        self.assertIn("```json", render_active_lenses_block(resolve_analysis_lenses(["finance_markets"])))
        self.assertIn("topics: objects with name, evidence, confidence", SOCIAL_SYSTEM_PROMPT)
        self.assertIn('"confidence": 0.0', intent_extractor.SYSTEM_PROMPT)

    def test_admin_prompt_template_shows_directive_without_lens_json(self) -> None:
        prompt = build_lens_prompt_template("BASE PROMPT", include_directive=True, suffix="FIXED SCHEMA")
        self.assertIn("BASE PROMPT", prompt)
        self.assertIn("LENS DIRECTIVE", prompt)
        self.assertIn("FIXED SCHEMA", prompt)
        self.assertNotIn("ACTIVE_ANALYSIS_LENSES\n```json", prompt)

    def test_matched_lenses_are_intersected_with_active_selection(self) -> None:
        active = resolve_analysis_lenses(["finance_markets"])
        metadata = normalize_lens_metadata(
            {
                "lens_relevance": "high",
                "matched_lenses": ["finance_markets", "competitor_analysis", "made_up"],
                "lens_signals": ["macro event interpretation"],
            },
            active,
        )
        self.assertEqual(metadata["matched_lenses"], ["finance_markets"])

    def test_confidence_filter_drops_low_topics_when_others_remain(self) -> None:
        active = resolve_analysis_lenses(["finance_markets"])
        topics, quality, stats = filter_topics_by_confidence(
            [
                {"name": "Trading", "confidence": 0.40},
                {"name": "Oil volatility from Middle East risk", "confidence": 0.82},
            ],
            matched_lenses=["finance_markets"],
            active_lenses=active,
            log_label="test",
        )
        self.assertEqual([topic["name"] for topic in topics], ["Oil volatility from Middle East risk"])
        self.assertEqual(quality, "accepted")
        self.assertEqual(stats["count"], 2)
        self.assertEqual(stats["threshold"], 0.70)

    def test_confidence_filter_keeps_all_topics_when_filter_would_empty_list(self) -> None:
        active = resolve_analysis_lenses(["finance_markets"])
        topics, quality, _stats = filter_topics_by_confidence(
            [{"name": "Borderline market theme", "confidence": 0.52}],
            matched_lenses=["finance_markets"],
            active_lenses=active,
            log_label="test",
        )
        self.assertEqual([topic["name"] for topic in topics], ["Borderline market theme"])
        self.assertEqual(quality, "low")

    def test_lens_eval_fixture_is_informational(self) -> None:
        fixture = Path(__file__).parent / "fixtures" / "lens_eval_v1.json"
        payload = json.loads(fixture.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(payload["items"]), 3)
        self.assertEqual(payload["ci_policy"], "informational_only")


if __name__ == "__main__":
    unittest.main()
