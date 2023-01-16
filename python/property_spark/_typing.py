from typing import Callable, Tuple

from pyspark.sql import DataFrame

TransformFunc = Callable[[DataFrame], DataFrame]
Transformation = Tuple[TransformFunc, ...]

Enrichment = Callable[..., Transformation]
