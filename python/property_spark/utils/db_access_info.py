from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DBAccessInfo:
    driver: str
    url: str
    user: str
    password: str
    dbtable: str
    rewrite_batched_statements: bool
    num_partitions: Optional[int] = None
