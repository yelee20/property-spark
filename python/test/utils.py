import math
from abc import abstractmethod
from datetime import datetime, timedelta
from random import randrange, uniform
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, overload

from property_spark.constants import DATE_ID_COLUMN_NAME
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_sub


def recursive_delete_s3_key(s3, bucket_name: str, key: Optional[str] = None):
    if key is None:
        list_objects_response = s3.list_objects(Bucket=bucket_name)
    else:
        list_objects_response = s3.list_objects(Bucket=bucket_name, Prefix=key)

    object_info_list = list_objects_response.get("Contents", [])
    if object_info_list:
        objects = [{"Key": object_info["Key"]} for object_info in object_info_list]

        s3.delete_objects(
            Bucket=bucket_name,
            Delete={
                "Objects": objects,
            },
        )


def assert_df_schema_eq(
    left: DataFrame, right: DataFrame, skip_nullablility_check: bool = False
) -> None:
    if not skip_nullablility_check:
        assert left.schema == right.schema
    else:
        assert len(left.schema) == len(right.schema)
        for ls, rs in zip(left.schema, right.schema):
            assert ls.name == rs.name
            assert ls.dataType == rs.dataType


def df_eq(left: DataFrame, right: DataFrame) -> bool:
    if left.schema != right.schema:
        return False

    collected_from_left = left.collect()
    collected_from_right = right.collect()

    return all(row in collected_from_right for row in collected_from_left) and all(
        row in collected_from_left for row in collected_from_right
    )


def assert_df_eq(left: DataFrame, right: DataFrame) -> None:
    assert df_eq(left, right)


def get_df_one_day_ago(df: DataFrame) -> DataFrame:
    return df.withColumn(DATE_ID_COLUMN_NAME, date_sub(col(DATE_ID_COLUMN_NAME), 1))


def random_datetime(
    start: datetime = datetime(year=2020, month=10, day=21),
    end: datetime = datetime(year=2022, month=10, day=21),
) -> datetime:
    delta = end - start
    random_second = randrange(math.floor(delta.total_seconds()))
    return start + timedelta(seconds=random_second)


def randrange_float(
    start: Union[int, float],
    stop: Union[int, float],
    step: Optional[int] = None,
) -> float:
    if stop is None and start == 0:
        stop = 99999
    elif stop is None:
        stop = start
        start = 0
    if isinstance(step, int):
        random_float = uniform(start, stop)
        return random_float - random_float % step
    return uniform(start, stop)


def random_int_str(start: int = 0, stop: Optional[int] = None) -> str:
    if stop is None and start == 0:
        stop = 99999
    elif stop is None:
        stop = start
        start = 0

    return str(randrange(start, stop))


def random_bool() -> bool:
    return randrange(0, 2) == 1


def get_quarter(month: int) -> int:
    if month <= 0 or month > 13:
        raise ValueError(f"Invalid month: {month}")

    return math.ceil(month / (12 // 4))


def sqlalchemy_row_eq_rdd_row(sqlalchemy_row, rdd_row):
    return all(s == r for s, r in zip(sqlalchemy_row, rdd_row))


def assert_sqlalchemy_rows_rdd_eq(all_from_sqlalchemy_cursor, collected):
    assert len(all_from_sqlalchemy_cursor) == len(collected)
    assert all(
        any(sqlalchemy_row_eq_rdd_row(s, r) for s in all_from_sqlalchemy_cursor)
        for r in collected
    )
    assert all(
        any(sqlalchemy_row_eq_rdd_row(s, r) for r in collected)
        for s in all_from_sqlalchemy_cursor
    )


def round_half_up(n: Union[int, float], decimals: int = 0) -> Union[int, float]:
    multiplier = 10**decimals
    return math.floor(n * multiplier + 0.5) / multiplier


def get_larger_datetime_in_same_week(smaller: datetime) -> datetime:
    monday = smaller.date() - timedelta(days=smaller.weekday())
    next_monday = datetime.combine(monday + timedelta(days=7), datetime.min.time())
    return random_datetime(smaller, next_monday)


def get_larger_datetime_in_same_month(smaller: datetime) -> datetime:
    if smaller.month == 12:
        return random_datetime(smaller, datetime(year=smaller.year + 1, month=1, day=1))
    else:
        return random_datetime(
            smaller, datetime(year=smaller.year, month=smaller.month + 1, day=1)
        )


def get_larger_datetime_in_same_quarter(smaller: datetime) -> datetime:
    quarter = get_quarter(smaller.month)
    quarter_last_month = quarter * 3
    if quarter_last_month == 12:
        return random_datetime(smaller, datetime(year=smaller.year + 1, month=1, day=1))
    else:
        return random_datetime(
            smaller, datetime(year=smaller.year, month=quarter_last_month + 1, day=1)
        )


def get_larger_datetime_in_same_year(smaller: datetime) -> datetime:
    return random_datetime(smaller, datetime(year=smaller.year + 1, month=1, day=1))


def percent_rank_int(values: List[int], target: int) -> float:
    no_max = [value for value in values if value != max(values)]
    values_lt = [value for value in no_max if value < target]
    return len(values_lt) / len(no_max)


class PercentRankInt(Sequence):
    _percent_rank_dict: Dict[int, float]

    def _get_idx_value_pairs_for_max_and_non_max(
        self,
        values: List[int],
    ) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
        i_v_pairs = [(i, v) for i, v in enumerate(values)]
        if self.desc:
            return [], i_v_pairs

        return [(i, v) for i, v in i_v_pairs if v == max(values)], [
            (i, v) for i, v in i_v_pairs if v != max(values)
        ]

    def _get_idx_p_rank_pairs_for_non_max_values(
        self,
        non_max_idx_value_pairs: List[Tuple[int, int]],
    ) -> List[Tuple[int, float]]:
        total_size = len(non_max_idx_value_pairs)
        if total_size == 0:
            return []

        sorted_pairs = sorted(
            non_max_idx_value_pairs, key=lambda p: p[1], reverse=self.desc
        )
        percent_rank_for_non_max_indices: List[Tuple[int, float]] = []
        num_smaller = 0
        std = sorted_pairs[0][1]
        for curr_pos, orig_idx_value_pair in enumerate(sorted_pairs):
            orig_idx, value = orig_idx_value_pair
            if std != value:
                num_smaller = curr_pos
                std = value
            percent_rank_for_non_max_indices.append(
                (orig_idx, num_smaller / total_size)
            )

        return percent_rank_for_non_max_indices

    def __init__(self, values: List[int], desc: bool = False):
        self.desc = desc
        if len(values) == 0:
            self._percent_rank_dict = {}
            return

        (
            i_v_pairs_max,
            i_v_pairs_non_max,
        ) = self._get_idx_value_pairs_for_max_and_non_max(values)
        p_rank_for_max = [(i, 1.0) for i, v in i_v_pairs_max]
        p_rank_for_non_max = self._get_idx_p_rank_pairs_for_non_max_values(
            i_v_pairs_non_max
        )
        self._percent_rank_dict = dict([*p_rank_for_max, *p_rank_for_non_max])

    @overload
    @abstractmethod
    def __getitem__(self, i: int) -> float:
        ...

    @overload
    @abstractmethod
    def __getitem__(self, s: slice) -> Sequence[float]:
        ...

    def __getitem__(self, key: Union[int, slice]) -> Union[float, Sequence[float]]:
        if isinstance(key, int):
            try:
                return self._percent_rank_dict[key]
            except KeyError:
                raise IndexError()
        elif isinstance(key, slice):
            start = key.start if key.start is not None else 0
            stop = key.stop if key.stop is not None else self.__len__()
            step = key.step if key.step is not None else 1
            return [self.__getitem__(i) for i in range(start, stop, step)]
        else:
            raise TypeError()

    def __contains__(self, item: Any) -> bool:
        return item in self._percent_rank_dict.values()

    def __len__(self) -> int:
        return len(self._percent_rank_dict)

    def __eq__(self, other) -> bool:
        if not isinstance(other, PercentRankInt):
            return False

        if self.__len__() != len(other):
            return False

        for i in range(len(other)):
            if self.__getitem__(i) != other[i]:
                return False

        return True

    def __repr__(self) -> str:
        self_list: List[float] = []
        for i in range(self.__len__()):
            self_list.append(self.__getitem__(i))
        return self_list.__repr__()
