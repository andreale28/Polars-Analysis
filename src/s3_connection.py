# %%
import os
import time

import dotenv
import numpy as np
import polars as pl
from pyarrow.dataset import dataset
from s3fs import S3FileSystem

from analysis import tweak_result


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


def read_s3_parquet(s3_loc):
    # check .env files

    # setup cloud filesystem access

    cloudfs = S3FileSystem(
        profile="sonlebucket",
    )

    # reference multiple parquet files
    pyarrow_dataset = dataset(
        source=s3_loc,
        filesystem=cloudfs,
        format="parquet",
    )

    # load efficiently into polars
    df = pl.scan_pyarrow_dataset(pyarrow_dataset)

    return df


def polars_analysis(df: pl.LazyFrame):
    sla_matrix_1st_attempt = [[3, 5, 7, 7], [5, 5, 7, 7], [7, 7, 7, 7], [7, 7, 7, 7]]

    locations = ["Metro Manila", "Luzon", "Visayas", "Mindanao"]
    locations = [loc.lower() for loc in locations]
    {loc: i for i, loc in enumerate(locations)}
    min_length = min(map(len, locations))
    {loc[-min_length:]: i for i, loc in enumerate(locations)}

    # transform to native python type for easily mapping
    dict(enumerate(np.array(sla_matrix_1st_attempt).flatten().tolist()))


def main():
    start_time = time.time()

    REQUIRED_S3_KEYS = [
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "LOCAL_FILE_PATH",
        "S3_BUCKET",
    ]
    s3_loc = "s3://sonlebucket/delivery_orders_march.parquet"
    # check .env file
    load_s3_envvars(vars=REQUIRED_S3_KEYS)

    # read parquet from s3
    df = read_s3_parquet(s3_loc=s3_loc)

    output = tweak_result(df)

    print(output)
    print(f"Polars execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()
