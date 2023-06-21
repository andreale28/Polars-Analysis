import time

import polars as pl
from analysis import tweak_result
from write_deltatable import (
    duckdb_connection,
    load_deltatable,
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

    load_deltatable(table_name=table, columns=columns)

    df = pl.scan_delta("march_order")

    output, result = tweak_result(df)

    print(f"Percentage of late deliveried orders {result.collect()}")

    conn.sql("SELECT * FROM output;").to_table("output")
    conn.sql("SUMMARIZE output;").show()

    # conn.sql(
    #     f"""
    #     COPY
    #         output
    #     TO
    #         's3://{os.getenv('S3_BUCKET')}/data/output.parquet'(FORMAT 'PARQUET');
    #     """
    # )

    print(f"DuckDB execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()
