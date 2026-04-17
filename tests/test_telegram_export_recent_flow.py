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
        self.texts: list[str] = []

    async def reply_text(self, text: str, parse_mode=None):
        self.texts.append(text)
        return _FakeSentMessage(text=text)

    async def reply_document(self, document, filename=None, caption=None):
        raise AssertionError("reply_document should not be used in export workflows")


class _FakeCallbackQuery:
    def __init__(self, data: str):
        self.data = data
        self.message = _FakeMessage()

    async def answer(self):
        return None


class ExportRecentFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"edec_export_recent_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    async def _build_bot(self, **kwargs) -> TelegramBot:
        config = SimpleNamespace(telegram_chat_id="1")
        bot = TelegramBot(config, tracker=object(), risk_manager=object(), **kwargs)
        bot._track = lambda msg: msg

        async def _noop():
            return None

        bot._repost_dashboard = _noop
        return bot

    async def test_export_recent_prefers_repo_sync_after_archive(self):
        call_order: list[str] = []
        sent_files: list[dict] = []
        archive_trades = self.tmpdir / "archive_trades.csv.gz"
        archive_signals = self.tmpdir / "archive_signals.csv.gz"
        sync_trades = self.tmpdir / "sync_trades.csv"
        sync_signals = self.tmpdir / "sync_signals.csv"
        local_trades = self.tmpdir / "local_trades.csv"
        for p in (archive_trades, archive_signals, sync_trades, sync_signals, local_trades):
            p.write_text("x", encoding="utf-8")

        def archive_fn():
            call_order.append("archive")
            return {"latest_trades": str(archive_trades), "latest_signals": str(archive_signals)}

        def repo_sync_fn():
            call_order.append("sync")
            return {"expanded_trades_csv": str(sync_trades), "expanded_signals_csv": str(sync_signals)}

        def export_recent_fn():
            call_order.append("local")
            return str(local_trades)

        bot = await self._build_bot(
            archive_fn=archive_fn,
            repo_sync_fn=repo_sync_fn,
            export_recent_fn=export_recent_fn,
        )
        async def _send_file(path: str, caption: str):
            sent_files.append({"path": path, "caption": caption})
            return True, None
        bot._send_file_path = _send_file
        query = _FakeCallbackQuery(data="export_recent")
        update = SimpleNamespace(
            callback_query=query,
            effective_chat=SimpleNamespace(id="1"),
        )

        await bot._handle_button(update, context=None)

        self.assertEqual(call_order, ["archive", "sync"])
        sent_names = [Path(item["path"]).name for item in sent_files]
        self.assertIn(sync_trades.name, sent_names)
        self.assertIn(sync_signals.name, sent_names)

    async def test_export_recent_warns_and_falls_back_if_archive_fails(self):
        sent_files: list[dict] = []
        local_trades = self.tmpdir / "local_trades.csv"
        local_trades.write_text("x", encoding="utf-8")

        def archive_fn():
            raise RuntimeError("dropbox unavailable")

        def repo_sync_fn():
            return {}

        def export_recent_fn():
            return str(local_trades)

        bot = await self._build_bot(
            archive_fn=archive_fn,
            repo_sync_fn=repo_sync_fn,
            export_recent_fn=export_recent_fn,
        )
        async def _send_file(path: str, caption: str):
            sent_files.append({"path": path, "caption": caption})
            return True, None
        bot._send_file_path = _send_file
        query = _FakeCallbackQuery(data="export_recent")
        update = SimpleNamespace(
            callback_query=query,
            effective_chat=SimpleNamespace(id="1"),
        )

        await bot._handle_button(update, context=None)

        sent_names = [Path(item["path"]).name for item in sent_files]
        self.assertIn(local_trades.name, sent_names)
        warning_lines = [t for t in query.message.texts if "Dropbox archive refresh failed" in t]
        self.assertTrue(warning_lines)

    async def test_export_recent_warns_if_repo_sync_fails_and_uses_local_fallback(self):
        sent_files: list[dict] = []
        local_trades = self.tmpdir / "local_trades.csv"
        local_trades.write_text("x", encoding="utf-8")

        def repo_sync_fn():
            raise RuntimeError("repo sync exploded")

        def export_recent_fn():
            return str(local_trades)

        bot = await self._build_bot(
            repo_sync_fn=repo_sync_fn,
            export_recent_fn=export_recent_fn,
        )
        async def _send_file(path: str, caption: str):
            sent_files.append({"path": path, "caption": caption})
            return True, None
        bot._send_file_path = _send_file
        query = _FakeCallbackQuery(data="export_recent")
        update = SimpleNamespace(
            callback_query=query,
            effective_chat=SimpleNamespace(id="1"),
        )

        await bot._handle_button(update, context=None)

        sent_names = [Path(item["path"]).name for item in sent_files]
        self.assertIn(local_trades.name, sent_names)
        warning_lines = [t for t in query.message.texts if "Dropbox repo sync failed" in t]
        self.assertTrue(warning_lines)


if __name__ == "__main__":
    unittest.main()
