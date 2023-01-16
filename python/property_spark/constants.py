import os
from datetime import datetime, timezone

from typing_extensions import Final

DATE_ID_COLUMN_NAME: Final[str] = "date_id"
DATA_DIRECTORY: Final[str] = "property_data"
SEC_PER_HOUR = 3600  # pragma: no cover
TIME_GAP_BY_TIME_ZONE = SEC_PER_HOUR * 9  # pragma: no cover
JDBC_MYSQL_DRIVER: Final[str] = "com.mysql.cj.jdbc.Driver"

DEPLOY_PHASE: Final[str] = os.environ.get("DEPLOY_PHASE", "dev")
S3_BUCKET_NAME: Final[str] = (
    "data-property-prod" if DEPLOY_PHASE == "prod" else "data-property-dev"
)

BIRTHDAY_OF_INTERNET: Final[datetime] = datetime(
    year=1983, month=1, day=1, tzinfo=timezone.utc
)
