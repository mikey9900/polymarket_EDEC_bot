import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.archive import _dropbox_latest_remote_candidates


class DropboxPathCandidateTests(unittest.TestCase):
    def test_includes_label_subfolder_fallback_when_root_is_slash(self):
        latest_filenames = {
            "latest_trades_csv_gz": "EDEC-BOT_latest_trades.csv.gz",
        }
        candidates = _dropbox_latest_remote_candidates("/", latest_filenames, label="EDEC-BOT")
        paths = candidates["latest_trades_csv_gz"]
        self.assertIn("/latest/EDEC-BOT_latest_trades.csv.gz", paths)
        self.assertIn("/EDEC-BOT/latest/EDEC-BOT_latest_trades.csv.gz", paths)


if __name__ == "__main__":
    unittest.main()
