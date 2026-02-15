from src.etl.utilities.logger_utils import LoggerUtils
from src.etl.config.config import Config
from src.etl.utilities.requests_utils import RequestsUtils
from src.etl.processor.polymarket_processor import PolymarketProcessor


class Extract: 

    def __init__(self, config: Config, requests_util: RequestsUtils, logger_utils: LoggerUtils): 
        PolymarketProcessor(config, requests_util, logger_utils)
        pass 


    def fetch_markets_strategy_0(self): 
        pass
    