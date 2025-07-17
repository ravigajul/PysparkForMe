# PysparkForMe
# Whitespace Validator for Spark Tables

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
