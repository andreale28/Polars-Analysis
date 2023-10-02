import os
import time

import patito as pt
import s3fs

from duckdb_ingestion import (
	duckdb_connection,
	read_deltatable,
	write_data_to_deltatable,
)
from schemas import (
	Input, Output
)
from transforms import tweak_result


def check_file_exists(
		bucket_name: str,
		file_name: str,
		key: str,
		secret: str
		) -> bool:
	"""Check if a file exists in S3 storage.

	Args:
		bucket_name (str): The name of the S3 bucket.
		file_name (str): The name of the file to check.
		key (str): access key to s3
		secret (str): secret access key to s3

	Returns:
		bool: True if the file exists, False otherwise.
	"""

	fs = s3fs.S3FileSystem(anon=True,
	                       key=key,

	                       secret=secret)
	return fs.exists(f's3://{bucket_name}/data/{file_name}')


def main():

	start_time = time.perf_counter()

	table = "march_order"
	columns = ["orderid", "pick"]
	rename_dict = {
		'1st_deliver_attempt': 'first_deliver_attempt',
		'2nd_deliver_attempt': 'second_deliver_attempt',
		'buyeraddress': 'buyer_address',
		'selleraddress': 'seller_address',
	}
	# connect to duckdb
	conn = duckdb_connection()

	# write data to deltalake
	write_data_to_deltatable(conn, table=table)

	df = read_deltatable(table_name=table, columns=columns, rename_dict=rename_dict)

	try:
		Input.validate(df.collect())
	except pt.ValidationError as e:
		print(e)

	output, result = tweak_result(df)

	try:
		Output.validate(output)
	except pt.ValidationError as e:
		print(e)

	print(f"Percentage of late delivered orders {result.collect()}")

	conn.sql("select * from output;").to_table("output")
	conn.sql("SUMMARIZE output;").show()

	bucket_name = os.getenv("S3_BUCKET")
	output_name = os.getenv("S3_FILE_NAME")
	key = os.getenv("aws_access_key_id")
	secret_key = os.getenv("aws_secret_access_key")

	if not check_file_exists(bucket_name, output_name, key, secret_key):
		conn.sql(
				f"""
	        COPY
	            output
	        TO
	            's3://{bucket_name}/data/{output_name}'(FORMAT 'PARQUET');
	        """
		)

	print(f"Execution time: {time.perf_counter() - start_time} seconds")


if __name__ == "__main__":
	main()
