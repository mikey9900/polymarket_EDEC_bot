import logging
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

import main as app_main


class LoggingSetupTests(unittest.TestCase):
    def test_setup_logging_writes_unicode_to_log_file(self):
        root_logger = logging.getLogger()
        prior_handlers = list(root_logger.handlers)
        prior_level = root_logger.level
        tmp_dir_handle = TemporaryDirectory()
        self.addCleanup(tmp_dir_handle.cleanup)

        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)

        try:
            log_path = Path(tmp_dir_handle.name) / "unicode.log"
            config = SimpleNamespace(
                logging=SimpleNamespace(level="INFO", file=str(log_path))
            )

            app_main.setup_logging(config)
            logging.getLogger("unicode-smoke").warning("arrow → emoji 🤖")

            for handler in logging.getLogger().handlers:
                handler.flush()
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass

            for handler in prior_handlers:
                root_logger.addHandler(handler)
            root_logger.setLevel(prior_level)

        contents = log_path.read_text(encoding="utf-8")
        self.assertIn("arrow → emoji 🤖", contents)


if __name__ == "__main__":
    unittest.main()
