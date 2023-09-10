from __future__ import annotations

from typing import Iterable

import numpy as np
import polars as pl
import polars.selectors as cs
from polars import DataFrame, LazyFrame


def map_address(map_dict: dict [str, int]) -> pl.Expr:
	"""A function to slice from the end of a string with given offset
    then map string according to a location_to_index dict

    Args:
        map_dict (dict[str, int]): location to index dictionary

    Returns:
        pl.Expr:
    """
	if not map_dict:
		raise ValueError("The map_dict argument cannot be empty")
	if not isinstance(map_dict, dict):
		raise ValueError("The map_dict argument must be a dictionary")
	return (
		cs.string()
		.str.to_lowercase()
		.str.extract(r"(.{0,5})$")
		.map_dict(map_dict)
	)


def convert_time_date(column: str | Iterable [str]) -> pl.Expr:
	"""Convert unix time to second time in gmt+8

    Args:
            column (str): column name

    Returns:
            pl.Expr: _description_
    """
	gmt8_offset = 3600 * 8
	duration_1day = 3600 * 24

	return (
		pl.col(column).map(lambda x: (x + gmt8_offset) / duration_1day).cast(pl.Int32)
	)


def compute_working_days(df: pl.LazyFrame) -> tuple [np.ndarray, np.ndarray]:
	workdays = "1111110"
	holidays = ["2020-03-08", "2020-03-25", "2020-03-30", "2020-03-31"]

	t1 = (
		(df.select(convert_time_date("pick")))
		.collect()
		.to_numpy()
		.astype("datetime64[D]")
	)

	t2 = (
		(df.select(convert_time_date("first_deliver_attempt")))
		.collect()
		.to_numpy()
		.astype("datetime64[D]")
	)

	t3 = (
		(df.select(convert_time_date("second_deliver_attempt").fill_null(strategy="zero")))
		.collect()
		.to_numpy()
		.astype("datetime64[D]")
	)

	num_days1 = np.busday_count(t1, t2, weekmask=workdays, holidays=holidays).flatten()
	num_days2 = np.busday_count(t2, t3, weekmask=workdays, holidays=holidays).flatten()
	return num_days1, num_days2


def tweak_result(df: pl.LazyFrame) -> tuple [DataFrame, LazyFrame]:
	"""
	tweaking the input polars.LazyFame to fetch appropriate results
	:param df: pl.LazyFrame
	:return: output: DataFrame, result:LazyFrame
	"""
	sla_matrix_1st_attempt = [[3, 5, 7, 7], [5, 5, 7, 7], [7, 7, 7, 7], [7, 7, 7, 7]]

	locations = ["Metro Manila", "Luzon", "Visayas", "Mindanao"]
	locations = [loc.lower() for loc in locations]
	index_to_location = {loc: i for loc, i in enumerate(locations)}

	min_length = min(map(len, locations))
	trunc_location_to_index = {loc [-min_length:]: i for i, loc in enumerate(locations)}
	map_to_dict = dict(enumerate(np.array(sla_matrix_1st_attempt).flatten().tolist()))

	num_days1, num_days2 = compute_working_days(df)

	output = (df.with_columns(
			[
				map_address(trunc_location_to_index),
			]
	)
	.with_columns(
			(4 * pl.col("buyer_address") + pl.col("seller_address"))
			.alias("sla")
			.map_dict(map_to_dict)
			.cast(pl.Int32),
			pl.Series(name="num_days1", values=num_days1)
			.cast(pl.Int32),
			pl.Series(name="num_days2", values=num_days2)
			.clip_min(lower_bound=0)
			.cast(pl.Int32)
	)
	.collect()
	.with_columns(
			[
				pl.when(
						(pl.col("num_days1") > pl.col("sla")) | (pl.col("num_days2") > 3)
				)
				.then(pl.lit(1, pl.Int32))
				.otherwise(pl.lit(0, pl.Int32))
				.alias("is_late"),
				pl.col(['buyer_address', 'seller_address']).map_dict(index_to_location),
				# pl.when(pl.col("num_days2") < 0).then(pl.lit(0)).,
				pl.from_epoch(
						pl.col(["pick", "first_deliver_attempt", "second_deliver_attempt"]),
						time_unit="s",
				),

			]
	)
	)

	result = (
		output.lazy()
		.groupby("is_late")
		.agg(pl.count("is_late").alias("count_order"))
		.with_columns(
				(pl.col("count_order") / pl.col("count_order").sum())
				.alias("percent_late")
		)
	)

	return output, result
