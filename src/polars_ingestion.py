import os
from typing import Iterable

import dotenv
import duckdb
import polars as pl
import pyarrow.parquet as parquet
import s3fs
from deltalake import write_deltalake, DeltaTable


def load_s3_env_file(
        envs: list[str]
) -> None:
    """
    Check environment variables for S3
    :param envs:
    :return:
    """
    dotenv.load_dotenv()

    for var in envs:
        if not os.getenv(var):
            raise ValueError(
                f"Required environment variables are not set correctly: {var}"
            )

    return None


def pyarrow_ingestion(table_name: str) -> None:
    """

    :param table_name: str, name of the table to ingest
    :return: None
    """
    required_key = [
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "LOCAL_FILE_NAME",
        "S3_BUCKET",
    ]
    load_s3_env_file(envs=required_key)
    s3_region = os.getenv('AWS_DEFAULT_REGION')
    s3_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    s3_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket_name = os.getenv("S3_BUCKET")
    s3_local_file_name = os.getenv("LOCAL_FILE_NAME")
    storage_options = {
        'aws_default_region': s3_region,
        'aws_access_key_id': s3_access_key_id,
        'aws_secret_access_key': s3_secret_access_key,
    }

    cloud_fs = s3fs.S3FileSystem(
        key=s3_access_key_id,
        secret=s3_secret_access_key,
    )
    s3_path = f"s3://{s3_bucket_name}/data/{s3_local_file_name}"

    with cloud_fs.open(s3_path, "rb") as f:
        arrow_table = parquet.read_table(f)

    write_deltalake(
        data=arrow_table,
        table_or_uri=table_name,
        mode='overwrite',
        overwrite_schema=True
    )

    return None


def read_delta(
        table_name: str,
        columns: Iterable[str],
        rename_dict: dict[str, str]
) -> pl.LazyFrame:
    """
    Lazily read deltalake table from disk to LazyFrame with rename
    :param table_name: name of Delta table to read
    :param columns: columns to optimize with z-orders
    :param rename_dict: dictionary to rename column names
    :return: LazyFrame
    """
    dt = DeltaTable(table_uri=table_name)
    schemas = dt.schema().to_pyarrow().names
    valid_columns = [col for col in columns if col in schemas]

    # print(f"Schema of our data is \n {dt.schema().to_pyarrow()}")
    try:
        dt.optimize.compact()
        dt.optimize.z_order(columns=valid_columns)
    except Exception as e:
        raise ValueError(f"Error when optimizing table: {e}")

    df = (
        pl
        .scan_delta(source=table_name)
        .rename(mapping=rename_dict)
    )

    return df


def export_s3_by_duckdb(df: pl.DataFrame,
                        s3_region: str,
                        s3_access_key_id: str,
                        s3_secret_access_key: str,
                        s3_bucket_name,
                        s3_file_name: str
                        ):
    """
    export polars.DataFrame to S3 bucket using DuckDB
    :param df:
    """

    output = df
    conn = duckdb.connect(':memory:')
    conn.sql(
        f"""
                INSTALL httpfs;
                LOAD httpfs;
                PRAGMA enable_optimizer;
                SET s3_region='{s3_region}';
                SET s3_access_key_id='{s3_access_key_id}';
                SET s3_secret_access_key='{s3_secret_access_key}';
            """
    )

    conn.sql("SELECT * FROM output;").to_table("output")
    conn.sql(
        f"""
            COPY
                output
            TO
                's3://{s3_bucket_name}/data/{s3_file_name}'(FORMAT 'PARQUET');
            """
    )

    return None
