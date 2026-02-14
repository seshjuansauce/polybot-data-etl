from __future__ import annotations

from pathlib import Path
import os
import yaml
from dotenv import load_dotenv


class Config:
    def __init__(self, config_path: str | Path | None = None):
        # Load .env from the current working directory (or parents) if present
        load_dotenv()

        # Expose env vars as attributes for downstream code
        self.reddit_user_id = os.getenv("reddit_user_id")
        self.reddit_client_secret = os.getenv("reddit_client_secret")

        # Resolve config.yaml relative to this module so it works regardless of where you run python from
        if config_path is None:
            config_path = Path(__file__).resolve().parent / "config.yaml"
        else:
            config_path = Path(config_path).expanduser().resolve()

        if not config_path.exists():
            raise FileNotFoundError(
                f"Config YAML not found at: {config_path}\n"
                f"Tip: put config.yaml next to src/etl/config/config.py, or pass Config(config_path=...)\n"
                f"cwd: {Path.cwd()}"
            )

        self.data = yaml.safe_load(config_path.read_text()) or {}
    