from src.etl.config.config import Config


class RedditProcessor: 
    
    def __init__(self, config: Config): 
        print(config.reddit_user_id)
        pass 


    def _get_reddit_access_token(self): 
        pass 