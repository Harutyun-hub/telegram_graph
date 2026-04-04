from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from utils.source_bulk_import import (
    SourceImportRow,
    SourceImportApiClient,
    build_preflight_rows,
    evaluate_wave_gate,
    verify_final_state,
)


def _write_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "Channel Name,Telegram URL,Username",
                "New Channel,https://t.me/new_channel,@new_channel",
                "Existing Active,https://t.me/existing_active,@existing_active",
                "Existing Inactive,https://t.me/existing_inactive,@existing_inactive",
                "Duplicate New,https://t.me/new_channel,@new_channel",
                "URL Only,https://t.me/url_only_channel/42,",
                "Bad Username,https://t.me/backup_handle,12345bad",
            ]
        ),
        encoding="utf-8",
    )


def _row(
    *,
    csv_row_number: int,
    handle: str,
    wave_number: int,
    final_id: str,
    is_active: bool,
) -> SourceImportRow:
    return SourceImportRow(
        csv_row_number=csv_row_number,
        channel_name=handle,
        raw_username=f"@{handle}",
        raw_telegram_url=f"https://t.me/{handle}",
        input_source="username",
        normalized_handle=handle,
        canonical_username=f"@{handle}",
        fallback_title=handle,
        preflight_status="new",
        wave_number=wave_number,
        final_id=final_id,
        final_is_active=is_active,
    )


class SourceBulkImportTests(unittest.TestCase):
    def test_build_preflight_rows_assigns_statuses_and_waves(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "sources.csv"
            _write_csv(csv_path)

            rows = build_preflight_rows(
                csv_path,
                [
                    {"id": "src-1", "channel_username": "@existing_active", "is_active": True},
                    {"id": "src-2", "channel_username": "existing_inactive", "is_active": False},
                ],
                wave_size=2,
            )

        by_csv_row = {row.csv_row_number: row for row in rows}

        self.assertEqual(by_csv_row[2].preflight_status, "new")
        self.assertEqual(by_csv_row[2].wave_number, 1)
        self.assertEqual(by_csv_row[3].preflight_status, "existing-active")
        self.assertEqual(by_csv_row[3].existing_id, "src-1")
        self.assertEqual(by_csv_row[3].wave_number, 1)
        self.assertEqual(by_csv_row[4].preflight_status, "existing-inactive")
        self.assertEqual(by_csv_row[4].existing_id, "src-2")
        self.assertEqual(by_csv_row[4].wave_number, 2)
        self.assertEqual(by_csv_row[5].preflight_status, "invalid")
        self.assertEqual(by_csv_row[5].duplicate_of_row, 2)
        self.assertEqual(by_csv_row[6].normalized_handle, "url_only_channel")
        self.assertEqual(by_csv_row[6].input_source, "telegram_url")
        self.assertEqual(by_csv_row[6].wave_number, 2)
        self.assertEqual(by_csv_row[7].preflight_status, "invalid")
        self.assertEqual(by_csv_row[7].note, "invalid_username")

    def test_evaluate_wave_gate_uses_cycle_and_backlog_thresholds(self) -> None:
        healthy = evaluate_wave_gate(
            {"backlog": {"unprocessed_posts": 12, "unprocessed_comments": 18}},
            {"last_error": None},
            clean_cycle_completed=True,
            post_threshold=250,
            comment_threshold=120,
        )

        self.assertTrue(healthy.healthy)
        self.assertEqual(healthy.reason, "healthy")

        blocked = evaluate_wave_gate(
            {"backlog": {"unprocessed_posts": 250, "unprocessed_comments": 121}},
            {"last_error": "scheduler failed"},
            clean_cycle_completed=False,
            post_threshold=250,
            comment_threshold=120,
        )

        self.assertFalse(blocked.healthy)
        self.assertEqual(blocked.scheduler_error, "scheduler failed")
        self.assertEqual(
            blocked.reason,
            "clean_cycle_not_completed, scheduler_error, "
            "post_backlog_threshold_exceeded, comment_backlog_threshold_exceeded",
        )

    def test_verify_final_state_flags_duplicates_and_activation_mismatches(self) -> None:
        rows = [
            _row(csv_row_number=2, handle="wave_one", wave_number=1, final_id="src-1", is_active=True),
            _row(csv_row_number=3, handle="wave_two", wave_number=2, final_id="src-2", is_active=False),
        ]

        verification = verify_final_state(
            rows,
            [
                {"id": "src-1", "channel_username": "@wave_one", "is_active": True, "source_type": "channel"},
                {"id": "src-2", "channel_username": "@wave_two", "is_active": True, "source_type": "channel"},
                {"id": "dup-1", "channel_username": "@duplicate_handle", "is_active": True},
                {"id": "dup-2", "channel_username": "duplicate_handle", "is_active": False},
            ],
            highest_activated_wave=1,
        )

        self.assertEqual(verification["duplicate_handles"], ["duplicate_handle"])
        self.assertEqual(verification["unexpected_active_handles"], ["wave_two"])
        self.assertEqual(verification["missing_handles"], [])

    def test_api_client_omits_auth_header_when_token_missing(self) -> None:
        client = SourceImportApiClient(api_base="http://127.0.0.1:8005", auth_token="")
        self.assertEqual(client._headers(), {})


if __name__ == "__main__":
    unittest.main()
