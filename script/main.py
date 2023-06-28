import os
import time

import patito as pt

from analysis import tweak_result
from schemas import (
	Input, Output
)
from write_deltatable import (
	duckdb_connection,
	read_deltatable,
	write_data_to_deltatable,
)


def main():
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

	conn.sql(
			f"""
        COPY
            output
        TO
            's3://{os.getenv('S3_BUCKET')}/data/output.parquet'(FORMAT 'PARQUET');
        """
	)

	print(f"DuckDB execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
	main()
