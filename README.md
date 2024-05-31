# Pandas2Redshift

This is a utility library for uploading a Pandas DataFrame to Amazon Redshift table, utilizing AWS S3 for temporary storage.

## Features

- Upload a Pandas DataFrame to a Redshift Table
- Uses the `COPY` command, using S3 as a middleware for fast inserts on Redshift
- Can create the table for you based on a Dict containing the datatypes or generates it automatically based on the pandas datatypes of the dataframe

## Installation

Install the package using pip:

```bash
pip install pandas2redshift
```

## Usage

### Insert Data into Redshift

Insert data from a DataFrame into a Redshift table:

```python
import pandas as pd
from sqlalchemy import create_engine
import pandas2redshift as p2r

data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
engine = create_engine('redshift+psycopg2://user:password@host:port/dbname')

with engine.connect() as conn:
    p2r.insert(
        data=data,
        table_name='my_table',
        schema='public',
        conn=conn,
        aws_access_key='YOUR_AWS_ACCESS_KEY',
        aws_secret_key='YOUR_AWS_SECRET_KEY',
        aws_bucket_name='YOUR_S3_BUCKET_NAME',
    )
```

**IMPORTANT**: If your data does not appear in the target table after executing the example above, it may be because SQLAlchemy is not committing your operations to Redshift. To resolve this issue, create the engine as follows:

```python
engine = (
    create_engine('redshift+psycopg2://user:password@host:port/dbname')
    .execution_options(
        autocommit=True, isolation_level="AUTOCOMMIT"
    )
)
```

You can enhance the functionality of the `insert` function with several optional arguments:

- `ensure_exists (bool, optional)`: Checks if the schema and table you are inserting data into exist in the database. If they do not exist, it creates them. Defaults to `False`.
- `truncate_table (bool, optional)`: When set to `True`, truncates the target table before inserting the data. Defaults to False.
- `table_data_types (Dict[str, str], optional)`: A dictionary specifying column names and their data types for table creation. If not provided, it infers the data types based on pandas dtypes and the mapping defined in the `pandas_to_redshift_datatypes` function. Defaults to `None`.
