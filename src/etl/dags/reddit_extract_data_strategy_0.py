from src.etl.config.config import Config
from src.etl.pipelines.reddit.extract import Extract as reddit_extract


config = Config()
reddit_extract(config)