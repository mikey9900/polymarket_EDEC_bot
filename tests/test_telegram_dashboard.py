import sys
import unittest
from types import SimpleNamespace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot import telegram_dashboard as dashboard_ui


class _FakeAggregator:
    def get_aggregated_price(self, coin: str):
        if coin == "btc":
            return SimpleNamespace(price=64321.12)
        return None


class _FakeScanner:
    def get_status_snapshot(self):
        return {
            "btc": {"up_ask": 0.41, "down_ask": 0.38},
        }


class _FakeTracker:
    def get_paper_stats(self):
        return {
            "total_pnl": 12.5,
            "wins": 3,
            "losses": 1,
            "open_positions": 2,
            "current_balance": 112.5,
            "total_capital": 100.0,
            "win_rate": 75.0,
            "buys": 4,
            "sells": 3,
            "avg_buy_price": 0.422,
            "avg_sell_price": 0.611,
        }

    def get_coin_recent_outcomes(self, coin: str, limit: int = 4):
        return ["UP", "DOWN"]


class _FakeStrategyEngine:
    is_active = True
    mode = "both"


class TelegramDashboardUiTests(unittest.TestCase):
    def setUp(self):
        self.config = SimpleNamespace(
            coins=["btc", "eth"],
            execution=SimpleNamespace(dry_run=True),
            dual_leg=SimpleNamespace(price_threshold=0.46, max_combined_cost=0.82),
            single_leg=SimpleNamespace(entry_max=0.44, opposite_min=0.52),
            lead_lag=SimpleNamespace(min_entry=0.35, max_entry=0.45),
            swing_leg=SimpleNamespace(first_leg_max=0.30),
        )

    def test_build_dashboard_text_renders_state_coin_rows_and_pnl(self):
        text = dashboard_ui.build_dashboard_text(
            version="1.2.3",
            config=self.config,
            tracker=_FakeTracker(),
            scanner=_FakeScanner(),
            aggregator=_FakeAggregator(),
            strategy_engine=_FakeStrategyEngine(),
        )

        self.assertIn("EDEC Bot v1.2.3", text)
        self.assertIn("SCANNING", text)
        self.assertIn("BTC", text)
        self.assertIn("$64,321", text)
        self.assertIn("P&L", text)

    def test_build_main_keyboard_contains_expected_control_callbacks(self):
        keyboard = dashboard_ui.build_main_keyboard(
            is_running=True,
            is_dry=True,
            order_size=10,
            capital_balance=250,
        )

        callbacks = [
            button.callback_data
            for row in keyboard.inline_keyboard
            for button in row
        ]
        self.assertIn("stop", callbacks)
        self.assertIn("kill", callbacks)
        self.assertIn("budget", callbacks)
        self.assertIn("capital", callbacks)
        self.assertIn("sync_repo_latest", callbacks)

    def test_build_status_command_text_includes_book_snapshot_and_signal_hint(self):
        risk_status = {
            "kill_switch": False,
            "paused": False,
            "daily_pnl": 4.5,
            "session_pnl": 6.25,
            "open_positions": 1,
            "trades_this_hour": 2,
        }

        text = dashboard_ui.build_status_command_text(
            config=self.config,
            risk_status=risk_status,
            scanner=_FakeScanner(),
            strategy_engine=_FakeStrategyEngine(),
        )

        self.assertIn("EDEC Bot Status", text)
        self.assertIn("BTC", text)
        self.assertIn("UP@0.410 DN@0.380", text)
        self.assertIn("DUAL?", text)

    def test_recent_trades_panel_handles_empty_and_populated_states(self):
        self.assertEqual(dashboard_ui.build_recent_trades_panel_text([]), "No trades yet.")
        populated = dashboard_ui.build_recent_trades_panel_text(
            [{"timestamp": "2026-04-16T10:00:00", "coin": "btc", "status": "success", "actual_profit": 1.25}]
        )
        self.assertIn("BTC", populated)
        self.assertIn("$+1.2500", populated)


if __name__ == "__main__":
    unittest.main()
