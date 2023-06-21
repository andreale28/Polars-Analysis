import os
from typing import Iterable

import dotenv
import duckdb
from deltalake import DeltaTable, write_deltalake
from duckdb import DuckDBPyConnection


def load_s3_envvars(vars: list[str]):
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


def duckdb_connection() -> DuckDBPyConnection:
    """Connect to DuckDb and set up some additional extensions

    Returns:
        DuckDBPyConnection: A DuckDB python connection
    """

    # load .env file
    REQUIRED_S3_KEYS = [
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "LOCAL_FILE_PATH",
        "S3_BUCKET",
    ]
    load_s3_envvars(vars=REQUIRED_S3_KEYS)

    # connect to duckdb and setup extensions
    con = duckdb.connect()
    con.sql(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        PRAGMA enable_optimizer;
        SET s3_region='{os.getenv('AWS_DEFAULT_REGION')}';
        SET s3_access_key_id={os.getenv('AWS_ACCESS_KEY_ID')};
        SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}';
        """
    )
    return con


def write_data_to_deltatable(con: DuckDBPyConnection, table: str):
    """Load data and transform to deltatable.
    This function also create a table inside database just in case for other purposes

    Args:
        con (DuckDBPyConnection): a DuckDB Python connection
    """

    con.sql(
        """
        CREATE OR REPLACE TABLE march_delivery AS
        SELECT
            *
        FROM read_parquet('s3://sonlebucket/data/delivery_orders_march.parquet');
        """
    )

    arrow_table = con.sql(
        """
            SELECT * FROM march_delivery;
            """
    ).arrow()

    write_deltalake(
        data=arrow_table,
        table_or_uri=table,
        mode="overwrite",
        overwrite_schema=True,
    )


def load_deltatable(table_name: str, columns: Iterable[str]) -> None:
    """Load deltatable to variables and do optimization

    Args:
        table_name (str): name of table
        columns (Iterable[str]): name of columns for perform z-order

    Returns:
        DeltaTable: Delta Table
    """
    dt = DeltaTable(table_name)

    print(f"Schema of our data is {dt.schema().to_json()}")

    try:
        dt.optimize.compact()
        dt.optimize.z_order(columns=columns)
    except Exception:
        print("Error when optimize table as {e}")

    return None
