from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from functools import reduce

spark = SparkSession.builder.getOrCreate()

def find_leading_trailing_spaces(table_name, join_key=None):
    """
    Detects rows in a Spark SQL table/view where any column (any datatype)
    contains leading or trailing whitespace.

    Args:
        table_name (str): Fully qualified table or view name (e.g., 'schema.table')
        join_key (str): Column to use as join key (defaults to first column)

    Returns:
        DataFrame: Rows where any column has leading/trailing whitespace
    """
    df = spark.sql(f"SELECT * FROM {table_name}")
    all_cols = df.columns

    if not all_cols:
        print(f"⚠️ No columns found in table {table_name}.")
        return None

    if join_key is None:
        join_key = all_cols[0]

    # Build condition: cast everything to string and compare with trimmed value
    space_conditions = [
        (col(c).cast("string") != trim(col(c).cast("string"))).alias(c) for c in all_cols
    ]

    mask_df = df.select(*space_conditions)

    if not space_conditions:
        print(f"⚠️ No columns to evaluate in table {table_name}.")
        return None

    mask_df_with_flag = mask_df.withColumn(
        "has_spaces",
        reduce(lambda a, b: a | b, [col(c) for c in all_cols])
    )

    # Join back to get actual dirty rows
    rows_with_spaces = df.join(
        mask_df_with_flag.filter(col("has_spaces")),
        on=join_key,
        how="inner"
    )

    count = rows_with_spaces.count()
    print(f"✅ Table `{table_name}` — {count} row(s) with leading/trailing spaces.")

    return rows_with_spaces if count > 0 else None

def check_multiple_tables(tables, join_keys=None):
    """
    Runs whitespace validation for multiple tables.

    Args:
        tables (list): List of table names (e.g., ['schema.table1', 'schema.table2'])
        join_keys (dict, optional): Dict of table_name: join_key for custom join columns
    """
    results = {}

    for table in tables:
        key = join_keys.get(table) if join_keys else None
        print(f"\n--- Checking table: {table} ---")
        result = find_leading_trailing_spaces(table, join_key=key)
        results[table] = result

    return results

# Example usage:
if __name__ == "__main__":
    tables_to_check = [
        "schema.table1",
        "schema.table2",
        "schema.table3"
    ]

    # Optional: if join key is not the first column, define it here
    join_keys = {
        "schema.table1": "id",
        "schema.table2": "record_id"
    }

    check_multiple_tables(tables_to_check, join_keys)
