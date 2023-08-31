import os
import time

import dotenv
import patito as pt
import s3fs

from schemas import (
	Input, Output
)
from transforms import tweak_result
from write_deltatable import (
	duckdb_connection,
	read_deltatable,
	write_data_to_deltatable,
)


def check_file_exists(bucket_name, file_name, key, secret):
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
	try:
		fs.open(f's3://{bucket_name}/data/{file_name}')
		return True
	except FileNotFoundError:
		return False


def main():
	dotenv.load_dotenv()

	start_time = time.time()

	table = "march_order"
	columns = ["orderid", "pick"]
	# connect to duckdb
	conn = duckdb_connection()

	# write data to deltalake
	write_data_to_deltatable(conn, table=table)

	conn.sql("SUMMARIZE march_delivery").show()

	df = read_deltatable(table_name=table, columns=columns)

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

	conn.sql("SELECT * FROM output;").to_table("output")
	conn.sql("SUMMARIZE output;").show()

	bucket_name = os.getenv("S3_BUCKET")
	file_name = os.getenv("S3_FILE_NAME")
	key = os.getenv("aws_access_key_id")
	secret = os.getenv("aws_secret_access_key")

	if not check_file_exists(bucket_name, file_name, key, secret):
		conn.sql(
				f"""
	        COPY
	            output
	        TO
	            's3://{bucket_name}/data/{file_name}'(FORMAT 'PARQUET');
	        """
		)

	print(f"DuckDB execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
	main()
