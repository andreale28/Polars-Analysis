import os
from typing import Iterable

import dotenv
import duckdb
import polars as pl
from deltalake import DeltaTable, write_deltalake
from duckdb import DuckDBPyConnection


def load_s3_envvars(vars: list [str]):
	"""A function to check whether the s3 keys are set inside .env file
	Args:
		vars (list[str], optional): A list of required key.
		Defaults to REQUIRED_S3_KEYS.

	Raises:
		ValueError: Raise error when a key is missing

	Returns:
		_type_: None_
	"""
	dotenv.load_dotenv()

	for var in vars:
		if not os.getenv(var):
			raise ValueError(
					f"Required environment variables are not set correctly: {var}"
			)

	return None


def duckdb_connection( ) -> DuckDBPyConnection:
	"""Connect to DuckDb and set up some additional extensions

	Returns:
		DuckDBPyConnection: A DuckDB python connection
	"""

	# load .env file
	required_key = [
		"AWS_DEFAULT_REGION",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"LOCAL_FILE_NAME",
		"S3_BUCKET",
	]
	load_s3_envvars(vars=required_key)

	s3_region = os.getenv('AWS_DEFAULT_REGION')
	s3_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
	s3_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

	# connect to duckdb and setup extensions
	con = duckdb.connect(':memory:')
	con.sql(
			f"""
        INSTALL httpfs;
        LOAD httpfs;
        PRAGMA enable_optimizer;
        SET s3_region='{s3_region}';
		SET s3_access_key_id='{s3_access_key_id}';
		SET s3_secret_access_key='{s3_secret_access_key}';
        """
	)
	return con


def write_data_to_deltatable(
		con: DuckDBPyConnection,
		table: str
		):
	"""Load data and transform to deltatable.
	This function also create a table inside database just in case for other purposes

	Args:
		con (DuckDBPyConnection): a DuckDB Python connection
	"""
	bucket_name = os.getenv("S3_BUCKET")
	data_name = os.getenv("LOCAL_FILE_NAME")
	s3_table = 'march_delivery'
	con.sql(
			f"""
			CREATE OR REPLACE TABLE {s3_table} AS
			SELECT
				*
			FROM read_parquet('s3://{bucket_name}/data/{data_name}');
			""")

	arrow_table = con.sql(
			f"""
			SELECT * 
			FROM {s3_table};
			"""
	).arrow()

	write_deltalake(
			data=arrow_table,
			table_or_uri=table,
			mode="overwrite",
			overwrite_schema=True,
	)


def read_deltatable(
		table_name: str,
		columns: Iterable [str]
		) -> pl.LazyFrame:
	"""Load parquet format data to deltalake format, do compact and z-order optimization
	    then use scan delta method from polars to load it to lazyframe

	Args:
		table_name (str): name of table
		columns (Iterable[str]): name of columns for perform z-order

	Returns:
		DeltaTable: Delta Table
	"""
	dt = DeltaTable(table_name)

	print(f"Schema of our data is \n {dt.schema().to_pyarrow()}")

	try:
		dt.optimize.compact()
		dt.optimize.z_order(columns=columns)
	except Exception as e:
		print(f"Error when optimize table as {e}")

	rename_dict = {
		'1st_deliver_attempt': 'first_deliver_attempt',
		'2nd_deliver_attempt': 'second_deliver_attempt',
		'buyeraddress'       : 'buyer_address',
		'selleraddress'      : 'seller_address',
	}
	df = (
		pl
		.scan_delta(table_name)
		.rename(rename_dict)
	)

	return df
