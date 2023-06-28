from datetime import datetime
from typing import Optional

import patito as pt
import polars as pl
from patito import Model


class Input(Model):
	orderid: int = pt.Field(dtype=pl.Int64)
	pick: int = pt.Field(dtype=pl.Int64)
	first_deliver_attempt: float = pt.Field(dtype=pl.Float64)
	second_deliver_attempt: Optional [float] = pt.Field(dtype=pl.Float64)
	buyer_address: str = pt.Field(dtype=pl.Utf8)
	seller_address: str = pt.Field(dtype=pl.Utf8)


class Output(Model):
	orderid: int = pt.Field(dtype=pl.Int64)
	pick: int = pt.Field(dtype=pl.Datetime)
	first_deliver_attempt: float = pt.Field(dtype=pl.Datetime)
	second_deliver_attempt: Optional [datetime] = pt.Field(dtype=pl.Datetime)
	buyer_address: str = pt.Field(dtype=pl.Utf8)
	seller_address: str = pt.Field(dtype=pl.Utf8)
	sla: int = pt.Field(dtype=pl.Int32)
	num_days1: int = pt.Field(dtype=pl.Int32)
	num_days2: int = pt.Field(dtype=pl.Int32)
	is_late: int = pt.Field(dtype=pl.Int32)
