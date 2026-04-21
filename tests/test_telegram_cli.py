import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.polymarket_cli import (  # noqa: E402
    AccountStatusInfo,
    BalanceInfo,
    CancelAllResult,
    OrdersInfo,
    TradesInfo,
    WalletInfo,
)
from bot.telegram_bot import TelegramBot  # noqa: E402


class DummyTracker:
    def get_paper_capital(self):
        return (50.0, 50.0)


class DummyRiskManager:
    pass


def make_config(*, allow_mutating=False):
    return SimpleNamespace(
        telegram_chat_id="123",
        telegram_bot_token="",
        cli=SimpleNamespace(
            allow_mutating_commands=allow_mutating,
            signature_type="proxy",
        ),
    )


def make_message():
    message = SimpleNamespace(message_id=99)
    message.reply_text = AsyncMock(return_value=SimpleNamespace(message_id=100))
    return message


class TelegramCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_pmaccount_reports_unavailable_when_cli_missing(self):
        bot = TelegramBot(make_config(), DummyTracker(), DummyRiskManager(), polymarket_cli=None)
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="123"),
            message=make_message(),
        )

        await bot._cmd_pmaccount(update, SimpleNamespace(args=[]))

        update.message.reply_text.assert_awaited_once()
        sent_text = update.message.reply_text.await_args.args[0]
        self.assertIn("Polymarket CLI", sent_text)

    async def test_pmaccount_formats_wallet_status_and_balance(self):
        cli = SimpleNamespace(
            get_wallet_info=AsyncMock(
                return_value=WalletInfo(
                    configured=True,
                    address="0xabc123",
                    proxy_address="0xproxy456",
                    signature_type="proxy",
                    source="env",
                )
            ),
            get_account_status=AsyncMock(return_value=AccountStatusInfo(closed_only=False)),
            get_collateral_balance=AsyncMock(
                return_value=BalanceInfo(balance="42.50", allowances={"0xallow": "42.50"})
            ),
        )
        bot = TelegramBot(make_config(), DummyTracker(), DummyRiskManager(), polymarket_cli=cli)
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="123"),
            message=make_message(),
        )

        await bot._cmd_pmaccount(update, SimpleNamespace(args=[]))

        sent_text = update.message.reply_text.await_args.args[0]
        self.assertIn("*Polymarket Account*", sent_text)
        self.assertIn("Address: `0xabc123`", sent_text)
        self.assertIn("Mode: `Active`", sent_text)
        self.assertIn("Collateral: `42.50` USDC", sent_text)

    async def test_pmorders_formats_open_orders(self):
        cli = SimpleNamespace(
            get_open_orders=AsyncMock(
                return_value=OrdersInfo(
                    data=[
                        {
                            "id": "order-1",
                            "side": "buy",
                            "price": "0.44",
                            "original_size": "25",
                            "status": "live",
                            "size_matched": "5",
                        }
                    ]
                )
            )
        )
        bot = TelegramBot(make_config(), DummyTracker(), DummyRiskManager(), polymarket_cli=cli)
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="123"),
            message=make_message(),
        )

        await bot._cmd_pmorders(update, SimpleNamespace(args=[]))

        sent_text = update.message.reply_text.await_args.args[0]
        self.assertIn("*Polymarket Open Orders*", sent_text)
        self.assertIn("BUY", sent_text)
        self.assertIn("@ 0.44 x 25", sent_text)

    async def test_pmorders_reports_unavailable_when_cli_binary_missing(self):
        cli = SimpleNamespace(
            is_available=False,
            unavailable_reason=lambda: "Polymarket CLI not installed in this runtime.",
        )
        bot = TelegramBot(make_config(), DummyTracker(), DummyRiskManager(), polymarket_cli=cli)
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="123"),
            message=make_message(),
        )

        await bot._cmd_pmorders(update, SimpleNamespace(args=[]))

        sent_text = update.message.reply_text.await_args.args[0]
        self.assertIn("not installed in this runtime", sent_text)

    async def test_pmtrades_formats_recent_trades(self):
        cli = SimpleNamespace(
            get_trades=AsyncMock(
                return_value=TradesInfo(
                    data=[
                        {
                            "id": "trade-1",
                            "side": "sell",
                            "price": "0.61",
                            "size": "10",
                            "status": "matched",
                            "match_time": "2026-04-21T12:34:56Z",
                        }
                    ]
                )
            )
        )
        bot = TelegramBot(make_config(), DummyTracker(), DummyRiskManager(), polymarket_cli=cli)
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="123"),
            message=make_message(),
        )

        await bot._cmd_pmtrades(update, SimpleNamespace(args=[]))

        sent_text = update.message.reply_text.await_args.args[0]
        self.assertIn("*Polymarket Recent Trades*", sent_text)
        self.assertIn("SELL", sent_text)
        self.assertIn("@ 0.61 x 10", sent_text)

    async def test_pmcancelall_command_returns_confirmation_keyboard(self):
        cli = SimpleNamespace(unavailable_reason=lambda: "n/a")
        bot = TelegramBot(
            make_config(allow_mutating=True),
            DummyTracker(),
            DummyRiskManager(),
            polymarket_cli=cli,
        )
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="123"),
            message=make_message(),
        )

        await bot._cmd_pmcancelall(update, SimpleNamespace(args=[]))

        kwargs = update.message.reply_text.await_args.kwargs
        keyboard = kwargs["reply_markup"]
        self.assertEqual(keyboard.inline_keyboard[0][0].callback_data, "pmcancelall_confirm")
        self.assertEqual(keyboard.inline_keyboard[0][1].callback_data, "pmcancelall_abort")

    async def test_pmcancelall_confirm_button_executes_cancel(self):
        cli = SimpleNamespace(
            unavailable_reason=lambda: "n/a",
            cancel_all_orders=AsyncMock(return_value=CancelAllResult(canceled=["order-1"])),
        )
        bot = TelegramBot(
            make_config(allow_mutating=True),
            DummyTracker(),
            DummyRiskManager(),
            polymarket_cli=cli,
        )
        query = SimpleNamespace(
            data="pmcancelall_confirm",
            answer=AsyncMock(),
            edit_message_text=AsyncMock(),
        )
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="123"),
            callback_query=query,
        )

        await bot._handle_button(update, SimpleNamespace())

        cli.cancel_all_orders.assert_awaited_once()
        sent_text = query.edit_message_text.await_args.args[0]
        self.assertIn("Canceled: `1`", sent_text)
