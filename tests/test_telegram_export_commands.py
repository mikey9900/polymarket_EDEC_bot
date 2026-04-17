import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.telegram_bot import TelegramBot


class _FakeSentMessage:
    def __init__(self, text: str = ""):
        self.text = text
        self.message_id = 1


class _FakeMessage:
    def __init__(self):
        self.message_id = 123
        self.texts: list[str] = []

    async def reply_text(self, text: str, parse_mode=None):
        self.texts.append(text)
        return _FakeSentMessage(text=text)

    async def reply_document(self, document, filename=None, caption=None):
        raise AssertionError("reply_document should not be used in export command flows")


class TelegramExportCommandTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"edec_export_cmd_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    def _build_bot(self, **kwargs) -> TelegramBot:
        bot = TelegramBot(
            SimpleNamespace(telegram_chat_id="1"),
            tracker=object(),
            risk_manager=object(),
            **kwargs,
        )
        bot._track = lambda msg: msg
        return bot

    async def test_export_command_uses_shared_file_sender(self):
        sent_files: list[dict] = []
        export_path = self.tmpdir / "export.xlsx"
        export_path.write_text("xlsx", encoding="utf-8")

        def export_fn(today_only: bool = False):
            self.assertTrue(today_only)
            return str(export_path)

        bot = self._build_bot(export_fn=export_fn)

        async def _send_file(path: str, caption: str):
            sent_files.append({"path": path, "caption": caption})
            return True, None

        bot._send_file_path = _send_file
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            message=_FakeMessage(),
        )
        context = SimpleNamespace(args=["today"])

        await bot._cmd_export(update, context)

        self.assertEqual([Path(item["path"]).name for item in sent_files], [export_path.name])
        self.assertTrue(any("Generating Excel export" in text for text in update.message.texts))

    async def test_sync_repo_latest_command_sends_synced_files(self):
        sent_files: list[dict] = []
        excel_path = self.tmpdir / "latest.xlsx"
        trades_path = self.tmpdir / "latest_trades.csv.gz"
        index_path = self.tmpdir / "latest_index.json"
        for path in (excel_path, trades_path, index_path):
            path.write_text("x", encoding="utf-8")

        def repo_sync_fn():
            return {
                "ok": True,
                "output_dir": str(self.tmpdir),
                "expanded_trades_csv": None,
                "expanded_signals_csv": None,
                "downloads": {
                    "latest_last24h_xlsx": {"ok": True, "status": 200, "path": str(excel_path)},
                    "latest_trades_csv_gz": {"ok": True, "status": 200, "path": str(trades_path)},
                    "latest_signals_csv_gz": {"ok": False, "optional_missing": True, "path": None},
                    "latest_index_json": {"ok": True, "status": 200, "path": str(index_path)},
                },
            }

        bot = self._build_bot(repo_sync_fn=repo_sync_fn)

        async def _send_file(path: str, caption: str):
            sent_files.append({"path": path, "caption": caption})
            return True, None

        bot._send_file_path = _send_file
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            message=_FakeMessage(),
        )

        await bot._cmd_sync_repo_latest(update, context=None)

        self.assertEqual(
            [Path(item["path"]).name for item in sent_files],
            [excel_path.name, trades_path.name, index_path.name],
        )
        self.assertTrue(any("Repo Sync Complete" in text for text in update.message.texts))


if __name__ == "__main__":
    unittest.main()
