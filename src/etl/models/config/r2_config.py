from dataclasses import dataclass

from src.etl.utilities.logger_utils import LoggerUtils


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    region_name: str = "auto"
    endpoint_template: str = "https://{account_id}.r2.cloudflarestorage.com"
    logger_utils: LoggerUtils 