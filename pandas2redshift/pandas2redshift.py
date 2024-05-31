import io
import os
import uuid
from textwrap import dedent
from typing import Dict, List

import boto3
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Connection

DEFAULT_QUERY_ARGS = [
    "IGNOREHEADER 1",
    "FORMAT AS CSV",
    "DELIMITER ','",
    "EMPTYASNULL",
    "ACCEPTANYDATE",
    "DATEFORMAT 'auto'",
    "TIMEFORMAT 'auto'",
]


def upload_to_s3(
    data: pd.DataFrame,
    table_name: str,
    aws_access_key: str,
    aws_secret_key: str,
    aws_bucket_name: str,
    aws_bucket_root: str = None,
) -> str:
    """
    Uploads a DataFrame to an S3 bucket.

    Args:
        data (pd.DataFrame): The data to upload.
        table_name (str): The name of the table.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        aws_bucket_name (str): The name of the S3 bucket.
        aws_bucket_root (str, optional): The root path in the S3 bucket. Defaults to None.

    Returns:
        str: The path in the S3 bucket where the data was uploaded.
    """

    def _create_bucket_path(table_name: str, bucket_root: str = None) -> str:
        bucket_table_name = f"{table_name}-{uuid.uuid4().hex}"
        args = [bucket_table_name]
        if bucket_root:
            args.insert(0, bucket_root)
        return os.path.join(*args)

    client = boto3.client(
        "s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )

    bucket_path = _create_bucket_path(
        bucket_root=aws_bucket_root, table_name=table_name
    )

    dataframe_bytes = data.to_csv(index=False).encode("utf-8")
    dataframe_file_obj = io.BytesIO(dataframe_bytes)
    client.upload_fileobj(
        dataframe_file_obj,
        aws_bucket_name,
        bucket_path,
    )

    return bucket_path


def delete_from_s3(
    file_path: str,
    aws_access_key: str,
    aws_secret_key: str,
    aws_bucket_name: str,
) -> None:
    """
    Deletes a file from an S3 bucket.

    Args:
        file_path (str): The path to the file in the S3 bucket.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        aws_bucket_name (str): The name of the S3 bucket.
    """
    client = boto3.client(
        "s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )

    client.delete_objects(
        Bucket=aws_bucket_name, Delete={"Objects": [{"Key": file_path}]}
    )


def copy(
    data: pd.DataFrame,
    table_name: str,
    schema: str,
    conn: Connection,
    aws_access_key: str,
    aws_secret_key: str,
    aws_bucket_name: str,
    query_args: List[str] = DEFAULT_QUERY_ARGS,
    aws_bucket_root: str = None,
) -> None:
    """
    Copies data from a DataFrame to a Redshift table.

    Args:
        data (pd.DataFrame): The data to copy.
        table_name (str): The name of the Redshift table.
        schema (str): The schema of the Redshift table.
        conn (Connection): SQLAlchemy connection object.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        aws_bucket_name (str): The name of the S3 bucket.
        query_args (List[str], optional): Additional query arguments for the COPY command. Defaults to DEFAULT_QUERY_ARGS.
        aws_bucket_root (str, optional): The root path in the S3 bucket. Defaults to None.
    """
    aws_bucket_path = upload_to_s3(
        data=data,
        table_name=table_name,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        aws_bucket_name=aws_bucket_name,
        aws_bucket_root=aws_bucket_root,
    )

    BREAK_LINE = "\n" + (4 * " ")
    COPY_QUERY = dedent(
        f"""
            COPY {schema}.{table_name}
            FROM '{aws_bucket_name}/{aws_bucket_path}'
            {BREAK_LINE.join(query_args)}
            ACCESS_KEY_ID '{aws_access_key}'
            SECRET_ACCESS_KEY '{aws_secret_key}'
        """
    )

    conn.execute(COPY_QUERY)

    delete_from_s3(
        file_path=aws_bucket_path,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        aws_bucket_name=aws_bucket_name,
    )


def create_table(
    conn: Connection, table_name: str, schema: str, data_types: Dict
) -> None:
    """
    Creates the specified schema and table in Redshift.

    Args:
        conn (Connection): SQLAlchemy connection object.
        table_name (str): The name of the table.
        schema (str): The schema of the table.
        data_types (Dict): A dictionary of column names and their data types.
    """
    column_definitions = ",\n".join(f"{key} {val}" for key, val in data_types.items())
    CREATE_TABLE_QUERY = f"""
        CREATE TABLE {schema}.{table_name} (
            {column_definitions}
        )
    """

    conn.execute(f"""CREATE SCHEMA IF NOT EXISTS {schema}""")
    conn.execute(CREATE_TABLE_QUERY)


def pandas_to_redshift_datatypes(dataframe_schema: Dict) -> Dict:
    """
    Maps pandas data types to Redshift data types.

    Args:
        dataframe_schema (Dict): A dictionary of column names and pandas data types.

    Returns:
        Dict: A dictionary of column names and Redshift data types.
    """
    DTYPE_MAPPING = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "object": "VARCHAR(MAX)",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "timedelta[ns]": "INTERVAL",
        "category": "VARCHAR(MAX)",
        "datetime64[ns, UTC]": "TIMESTAMPTZ",
    }

    return {
        col: DTYPE_MAPPING.get(str(dtype), "VARCHAR(MAX)")
        for col, dtype in dataframe_schema.items()
    }


def insert(
    data: pd.DataFrame,
    table_name: str,
    schema: str,
    conn: Connection,
    aws_access_key: str,
    aws_secret_key: str,
    aws_bucket_name: str,
    query_args: List[str] = DEFAULT_QUERY_ARGS,
    aws_bucket_root: str = None,
    ensure_exists: bool = False,
    truncate_table: bool = False,
    table_data_types: Dict = None,
):
    """
    Inserts data from a DataFrame into a Redshift table.

    Args:
        data (pd.DataFrame): The data to insert.
        table_name (str): The name of the Redshift table.
        schema (str): The schema of the Redshift table.
        conn (Connection): SQLAlchemy connection object.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        aws_bucket_name (str): The name of the S3 bucket.
        query_args (List[str], optional): Additional query arguments for the COPY command. Defaults to DEFAULT_QUERY_ARGS.
        aws_bucket_root (str, optional): The root path in the S3 bucket. Defaults to None.
        ensure_exists (bool, optional): Whether to ensure the table exists before inserting data. Defaults to False.
        truncate_table (bool, optional): Whether to truncate the table before inserting data. Defaults to False.
        table_data_types (Dict, optional): A dictionary of column names and their data types for table creation. Defaults to None.
    """

    def _exists(table_name: str, schema: str):
        return sa.inspect(conn).has_table(table_name, schema=schema)

    if truncate_table:
        conn.execute(f"""TRUNCATE TABLE {schema}.{table_name}""")

    if ensure_exists:
        if not _exists(table_name=table_name, schema=schema):
            if not table_data_types:
                table_data_types = pandas_to_redshift_datatypes(
                    dataframe_schema=data.dtypes.to_dict()
                )

            create_table(
                conn=conn,
                table_name=table_name,
                schema=schema,
                data_types=table_data_types,
            )

    copy(
        data=data,
        table_name=table_name,
        schema=schema,
        conn=conn,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        aws_bucket_name=aws_bucket_name,
        query_args=query_args,
        aws_bucket_root=aws_bucket_root,
    )
