from __future__ import annotations

import unittest

from social.text_cleaning import (
    clean_social_text_content,
    extract_readable_social_text,
    looks_like_raw_social_payload,
)


class SocialTextCleaningTests(unittest.TestCase):
    def test_extracts_text_from_python_dict_string(self) -> None:
        raw = "{'strong_id__': '123', 'bit_flags': 0, 'text': 'Readable post text\\n\\nSecond line', 'pk': '456'}"

        self.assertTrue(looks_like_raw_social_payload(raw))
        self.assertEqual(extract_readable_social_text(raw), "Readable post text Second line")

    def test_extracts_text_from_nested_snapshot(self) -> None:
        payload = {
            "id": "ad-1",
            "snapshot": {
                "body": {"text": "Main ad body"},
                "title": "Fallback title",
            },
        }

        self.assertEqual(clean_social_text_content(None, provider_payload=payload), "Main ad body")

    def test_uses_analysis_summary_as_final_fallback(self) -> None:
        self.assertEqual(
            clean_social_text_content(
                "{'id': 'raw-only', 'pk': '1'}",
                analysis={"summary": "Model summary"},
            ),
            "Model summary",
        )

    def test_does_not_return_whole_provider_payload(self) -> None:
        payload = {"id": "ad-1", "url": "https://example.com", "pk": "123"}

        self.assertIsNone(clean_social_text_content(None, provider_payload=payload))


if __name__ == "__main__":
    unittest.main()
