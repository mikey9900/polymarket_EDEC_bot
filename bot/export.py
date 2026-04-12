"""Export SQLite data to Excel (.xlsx) with multiple analysis sheets."""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, numbers
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)

GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)


def export_to_excel(db_path: str = "data/decisions.db",
                    output_dir: str = "data",
                    today_only: bool = False) -> str:
    """Generate an Excel workbook from the SQLite database. Returns the file path."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    wb = Workbook()

    date_filter = ""
    if today_only:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        date_filter = f" WHERE d.timestamp LIKE '{today}%'"

    # Sheet 1: Trades
    _build_trades_sheet(wb, conn, date_filter)

    # Sheet 2: All Decisions
    _build_decisions_sheet(wb, conn, date_filter)

    # Sheet 3: Skipped Winners
    _build_skipped_winners_sheet(wb, conn, date_filter)

    # Sheet 4: Daily Summary
    _build_daily_summary_sheet(wb, conn)

    # Sheet 5: Filter Performance
    _build_filter_performance_sheet(wb, conn, date_filter)

    # Remove default empty sheet if we created others
    if "Sheet" in wb.sheetnames and len(wb.sheetnames) > 1:
        del wb["Sheet"]

    # Save
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"edec_export_{timestamp}.xlsx"
    filepath = str(Path(output_dir) / filename)
    wb.save(filepath)
    conn.close()

    logger.info(f"Excel export saved: {filepath}")
    return filepath


def _style_header(ws, num_cols: int):
    """Apply header styling to the first row."""
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=1, column=col)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center")


def _auto_width(ws):
    """Auto-fit column widths."""
    for col_cells in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col_cells[0].column)
        for cell in col_cells:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max_len + 3, 40)


def _color_pnl_column(ws, col_idx: int, start_row: int = 2):
    """Color P&L cells green/red."""
    for row in ws.iter_rows(min_row=start_row, min_col=col_idx, max_col=col_idx):
        cell = row[0]
        if cell.value is not None:
            try:
                val = float(cell.value)
                cell.fill = GREEN_FILL if val > 0 else RED_FILL if val < 0 else PatternFill()
            except (ValueError, TypeError):
                pass


def _build_trades_sheet(wb: Workbook, conn: sqlite3.Connection, date_filter: str):
    ws = wb.create_sheet("Trades")
    headers = [
        "Timestamp", "Market", "UP Price", "DOWN Price", "Combined Cost",
        "Fees", "Shares", "Status", "Abort Cost", "Actual P&L"
    ]
    ws.append(headers)

    query = f"""
        SELECT t.timestamp, t.market_slug, t.up_price, t.down_price,
               t.combined_cost, t.fee_total, t.shares, t.status, t.abort_cost,
               do.actual_profit
        FROM trades t
        LEFT JOIN decision_outcomes do ON do.decision_id = t.decision_id
        {'WHERE t.timestamp LIKE ' + repr(datetime.utcnow().strftime("%Y-%m-%d") + "%") if date_filter else ''}
        ORDER BY t.id DESC
    """
    for row in conn.execute(query):
        ws.append(list(row))

    _style_header(ws, len(headers))
    _auto_width(ws)
    _color_pnl_column(ws, 10)  # Actual P&L column
    ws.auto_filter.ref = ws.dimensions


def _build_decisions_sheet(wb: Workbook, conn: sqlite3.Connection, date_filter: str):
    ws = wb.create_sheet("Decisions")
    headers = [
        "Timestamp", "Market", "UP Ask", "DOWN Ask", "Combined",
        "BTC Price", "Velocity 30s", "Velocity 60s",
        "UP Depth", "DOWN Depth", "Time Left (s)",
        "Feeds", "Passed Filters", "Failed Filters",
        "Action", "Reason", "Would Have Profited", "Hypothetical P&L"
    ]
    ws.append(headers)

    where = date_filter.replace("d.", "decisions.")
    query = f"""
        SELECT d.timestamp, d.market_slug, d.up_best_ask, d.down_best_ask,
               d.combined_cost, d.btc_price, d.coin_velocity_30s, d.coin_velocity_60s,
               d.up_depth_usd, d.down_depth_usd, d.time_remaining_s,
               d.feed_count, d.filter_passed, d.filter_failed,
               d.action, d.reason,
               do.would_have_profited, do.hypothetical_profit
        FROM decisions d
        LEFT JOIN decision_outcomes do ON do.decision_id = d.id
        {where}
        ORDER BY d.id DESC
        LIMIT 10000
    """
    for row in conn.execute(query):
        ws.append(list(row))

    _style_header(ws, len(headers))
    _auto_width(ws)
    _color_pnl_column(ws, 18)  # Hypothetical P&L column
    ws.auto_filter.ref = ws.dimensions


def _build_skipped_winners_sheet(wb: Workbook, conn: sqlite3.Connection, date_filter: str):
    ws = wb.create_sheet("Skipped Winners")
    headers = [
        "Timestamp", "Market", "UP Ask", "DOWN Ask", "Combined",
        "Failed Filters", "Reason", "Hypothetical P&L", "Winner"
    ]
    ws.append(headers)

    where = date_filter.replace("d.", "decisions.")
    if where:
        where += " AND"
    else:
        where = " WHERE"

    query = f"""
        SELECT d.timestamp, d.market_slug, d.up_best_ask, d.down_best_ask,
               d.combined_cost, d.filter_failed, d.reason,
               do.hypothetical_profit, o.winner
        FROM decisions d
        JOIN decision_outcomes do ON do.decision_id = d.id
        JOIN outcomes o ON do.outcome_id = o.id
        {where} d.action = 'SKIP' AND do.would_have_profited = 1
        ORDER BY do.hypothetical_profit DESC
        LIMIT 5000
    """
    for row in conn.execute(query):
        ws.append(list(row))

    _style_header(ws, len(headers))
    _auto_width(ws)
    _color_pnl_column(ws, 8)  # Hypothetical P&L
    ws.auto_filter.ref = ws.dimensions


def _build_daily_summary_sheet(wb: Workbook, conn: sqlite3.Connection):
    ws = wb.create_sheet("Daily Summary")
    headers = [
        "Date", "Evaluations", "Signals", "Skips", "Trades",
        "Successful", "Aborted", "Total P&L", "Missed Profit (Skipped Winners)"
    ]
    ws.append(headers)

    query = """
        SELECT
            DATE(d.timestamp) as date,
            COUNT(*) as evaluations,
            SUM(CASE WHEN d.action IN ('TRADE', 'DRY_RUN_SIGNAL') THEN 1 ELSE 0 END) as signals,
            SUM(CASE WHEN d.action = 'SKIP' THEN 1 ELSE 0 END) as skips,
            (SELECT COUNT(*) FROM trades t WHERE DATE(t.timestamp) = DATE(d.timestamp)) as trades,
            (SELECT COUNT(*) FROM trades t WHERE DATE(t.timestamp) = DATE(d.timestamp) AND t.status = 'success') as successful,
            (SELECT COUNT(*) FROM trades t WHERE DATE(t.timestamp) = DATE(d.timestamp) AND t.status IN ('aborted', 'partial_abort')) as aborted,
            (SELECT COALESCE(SUM(do2.actual_profit), 0) FROM decision_outcomes do2
             JOIN decisions d2 ON do2.decision_id = d2.id
             WHERE DATE(d2.timestamp) = DATE(d.timestamp) AND do2.actual_profit IS NOT NULL) as total_pnl,
            (SELECT COALESCE(SUM(do3.hypothetical_profit), 0) FROM decision_outcomes do3
             JOIN decisions d3 ON do3.decision_id = d3.id
             WHERE DATE(d3.timestamp) = DATE(d.timestamp) AND d3.action = 'SKIP' AND do3.would_have_profited = 1) as missed
        FROM decisions d
        GROUP BY DATE(d.timestamp)
        ORDER BY date DESC
    """
    for row in conn.execute(query):
        ws.append(list(row))

    _style_header(ws, len(headers))
    _auto_width(ws)
    _color_pnl_column(ws, 8)  # Total P&L
    ws.auto_filter.ref = ws.dimensions


def _build_filter_performance_sheet(wb: Workbook, conn: sqlite3.Connection, date_filter: str):
    ws = wb.create_sheet("Filter Performance")
    headers = [
        "Filter", "Times Passed", "Times Failed", "Reject Rate %",
        "Correct Rejections", "Missed Winners", "Accuracy %"
    ]
    ws.append(headers)

    # Get all filter data
    where = date_filter if date_filter else ""
    rows = conn.execute(
        f"SELECT filter_passed, filter_failed, action FROM decisions {where}"
    ).fetchall()

    filter_counts: dict[str, dict] = {}
    for passed_str, failed_str, action in rows:
        for name in (passed_str or "").split(","):
            name = name.strip()
            if name:
                filter_counts.setdefault(name, {"passed": 0, "failed": 0})
                filter_counts[name]["passed"] += 1
        for name in (failed_str or "").split(","):
            name = name.strip()
            if name:
                filter_counts.setdefault(name, {"passed": 0, "failed": 0})
                filter_counts[name]["failed"] += 1

    # Get accuracy data (how many rejections were correct)
    for fname in filter_counts:
        # A "correct rejection" is when the filter failed AND the hypothetical profit was <= 0
        query = f"""
            SELECT
                SUM(CASE WHEN do.would_have_profited = 0 THEN 1 ELSE 0 END) as correct,
                SUM(CASE WHEN do.would_have_profited = 1 THEN 1 ELSE 0 END) as missed
            FROM decisions d
            JOIN decision_outcomes do ON do.decision_id = d.id
            {where} {"AND" if where else "WHERE"} d.filter_failed LIKE ?
        """
        result = conn.execute(query, (f"%{fname}%",)).fetchone()
        if result:
            filter_counts[fname]["correct"] = result[0] or 0
            filter_counts[fname]["missed"] = result[1] or 0
        else:
            filter_counts[fname]["correct"] = 0
            filter_counts[fname]["missed"] = 0

    for name, counts in sorted(filter_counts.items()):
        total = counts["passed"] + counts["failed"]
        reject_pct = (counts["failed"] / total * 100) if total > 0 else 0
        correct = counts.get("correct", 0)
        missed = counts.get("missed", 0)
        accuracy_total = correct + missed
        accuracy = (correct / accuracy_total * 100) if accuracy_total > 0 else 0
        ws.append([name, counts["passed"], counts["failed"], round(reject_pct, 1),
                    correct, missed, round(accuracy, 1)])

    _style_header(ws, len(headers))
    _auto_width(ws)
    ws.auto_filter.ref = ws.dimensions
