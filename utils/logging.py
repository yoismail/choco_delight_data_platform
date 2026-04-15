import logging


class ColorFormatter(logging.Formatter):
    COLORS = {
        "INFO": "\033[94m",     # Blue
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",    # Red
        "SUCCESS": "\033[92m",  # Green
        "DEBUG": "\033[90m",    # Grey
        "RESET": "\033[0m",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        reset = self.COLORS["RESET"]
        record.msg = f"{color}{record.msg}{reset}"
        return super().format(record)


def setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter("%(levelname)s - %(message)s"))

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
        force=True  # ensures our handler overrides defaults
    )
