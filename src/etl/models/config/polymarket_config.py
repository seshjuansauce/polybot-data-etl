
from src.etl.processor.r2_processor import R2Processor
from src.etl.utilities.logger_utils import LoggerUtils
from src.etl.utilities.requests_utils import RequestsUtils


class PolymarketConfig: 
    requests_util : RequestsUtils
    polymarket_gamma_url : str 
    logger_util : LoggerUtils
    r2_processor : R2Processor