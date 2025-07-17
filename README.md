# PysparkForMe
# Whitespace Validator for Spark Tables
```python
from pyspark.sql.functions import col, trim
from functools import reduce

def find_leading_trailing_spaces(table_name, join_key=None):
    """
    Checks all columns in the given Spark table/view for leading/trailing spaces.
    
    Args:
        table_name (str): Full table/view name as used in Spark SQL.
        join_key (str, optional): Column to use for join back to original data.
                                  If None, uses first column by default.
    
    Returns:
        DataFrame: Rows with leading/trailing spaces in any column.
    """
    df = spark.sql(f"SELECT * FROM {table_name}")
    all_cols = df.columns
    
    if join_key is None:
        join_key = all_cols[0]
    
    # Create boolean mask for columns where cast-to-string value != trimmed version
    space_conditions = [
        (col(c).cast("string") != trim(col(c).cast("string"))).alias(c) for c in all_cols
    ]
    
    mask_df = df.select(*space_conditions)
    
    if space_conditions:
        mask_df_with_flag = mask_df.withColumn(
            "has_spaces",
            reduce(lambda a, b: a | b, [col(c) for c in all_cols])
        )
        
        rows_with_spaces = df.join(
            mask_df_with_flag.filter(col("has_spaces")),
            on=join_key,
            how="inner"
        )
        print(f"Rows with leading/trailing spaces in table {table_name}:")
        rows_with_spaces.show(truncate=False)
        
        return rows_with_spaces
    else:
        print(f"No columns found in table {table_name}.")
        return None

# Example usage for different tables:
tables = [
    "schema1.table1",
    "schema2.table2",
    "schema3.table3",
    "schema4.table4",
]

for tbl in tables:
    find_leading_trailing_spaces(tbl)
```

This script scans Spark SQL tables for **leading or trailing whitespace in any column**, regardless of datatype.

## Features

- Works on any Spark-compatible table or view
- Casts all columns to string for whitespace check
- Detects and returns offending rows
- Supports multiple tables and custom join keys

## Usage

```python
from check_whitespace_in_tables import check_multiple_tables

tables = ["schema.table1", "schema.table2"]
join_keys = {"schema.table1": "id"}
check_multiple_tables(tables, join_keys)
