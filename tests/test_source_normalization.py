from __future__ import annotations

import unittest

from utils.source_normalization import normalize_channel_username


class SourceNormalizationTests(unittest.TestCase):
    def test_public_handle_message_link_normalizes_to_handle(self) -> None:
        self.assertEqual(
            normalize_channel_username("https://t.me/dogfriendly_yerevan/7062"),
            "dogfriendly_yerevan",
        )

    def test_public_c_style_message_link_normalizes_to_handle(self) -> None:
        self.assertEqual(
            normalize_channel_username("https://t.me/c/dogfriendly_yerevan/7062"),
            "dogfriendly_yerevan",
        )

    def test_private_numeric_c_link_is_rejected(self) -> None:
        self.assertEqual(
            normalize_channel_username("https://t.me/c/1941234567/7062"),
            "",
        )


if __name__ == "__main__":
    unittest.main()
