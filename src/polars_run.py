import os
import time

import patito as pt
import s3fs

from polars_ingestion import pyarrow_ingestion, read_delta
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

    fs = s3fs.S3FileSystem(
        anon=True,
        key=key,

        secret=secret
    )
    return fs.exists(
        f's3://{bucket_name}/data/{file_name}'
    )


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

    pyarrow_ingestion(
        table_name=table
    )

    df = read_delta(
        table_name=table,
        columns=columns,
        rename_dict=rename_dict
    )

    try:
        Input.validate(
            df.collect()
        )
    except pt.ValidationError as e:
        print(e)

    output, result = tweak_result(df)

    try:
        Output.validate(output)
    except pt.ValidationError as e:
        print(e)

    print(f"Percentage of late delivered orders {result.collect()}")

    s3_region = os.getenv('AWS_DEFAULT_REGION')
    s3_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    s3_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket_name = os.getenv("S3_BUCKET")
    s3_file_name = os.getenv("S3_FILE_NAME")

    cloud_fs = s3fs.S3FileSystem(
        key=s3_access_key_id,
        secret=s3_secret_access_key,
    )

    if not check_file_exists(
            s3_bucket_name,
            s3_file_name,
            s3_access_key_id,
            s3_secret_access_key
    ):
        with cloud_fs.open(f"s3://{s3_bucket_name}/data/{s3_file_name}", "wb") as f:
            output.write_parquet(f)

    print(
        f"Execution time: {time.perf_counter() - start_time} seconds"
    )


if __name__ == "__main__":
    main()
