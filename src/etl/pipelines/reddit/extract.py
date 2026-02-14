from src.etl.processor.reddit_processor import RedditProcessor
from src.etl.config.config import Config


class Extract: 

    def __init__(self, config: Config): 
        self.reddit_processor = RedditProcessor(config)
        
    def extraction_strategy_0(self): 
        pass
        
