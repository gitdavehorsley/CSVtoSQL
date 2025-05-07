# CSV to SQL Database Importer

A simple, powerful Python script that imports CSV data into SQL databases with automatic table creation and column type inference.

## Features

- Imports CSV files into SQL databases (SQLite, PostgreSQL, MySQL, etc.)
- Automatically creates tables based on CSV headers
- Infers appropriate column types from the data
- Handles large files by processing in chunks
- Options for handling existing tables (fail, replace, append)
- Supports custom delimiters and file encodings
- Detailed logging of the import process

## Requirements

- Python 3.6+
- pandas
- sqlalchemy
- Database-specific drivers as needed:
  - SQLite: Built into Python
  - PostgreSQL: `psycopg2-binary`
  - MySQL: `pymysql`
  - Others as needed

## Installation

1. Clone or download this repository
2. Install required packages:

```bash
pip install pandas sqlalchemy
```

3. Install database-specific drivers as needed:

```bash
# For PostgreSQL
pip install psycopg2-binary

# For MySQL
pip install pymysql
```

## Usage

Basic syntax:

```bash
python csv_to_sql.py --file <csv_file> --table <table_name> [options]
```

### Required Arguments

- `--file`: Path to the CSV file to import
- `--table`: Name of the table to create/update in the database

### Optional Arguments

- `--db`: Database connection string (default: `sqlite:///data.db`)
- `--schema`: Database schema (default: `public`)
- `--if-exists`: Action if table exists (`fail`, `replace`, `append`) (default: `fail`)
- `--chunksize`: Size of chunks to process at once (default: 1000)
- `--no-infer-types`: Don't infer column types from data (use string for all)
- `--encoding`: File encoding (default: `utf-8`)
- `--delimiter`: CSV delimiter (default: `,`)

## Database Connection Strings

The script uses SQLAlchemy, which supports many database systems. Here are some examples:

- SQLite: `sqlite:///path/to/database.db`
- PostgreSQL: `postgresql://username:password@localhost:5432/database`
- MySQL: `mysql+pymysql://username:password@localhost/database`
- Oracle: `oracle://username:password@localhost:1521/database`
- Microsoft SQL Server: `mssql+pyodbc://username:password@server/database?driver=SQL+Server`

## Examples

### Basic usage with SQLite (default)

```bash
python csv_to_sql.py --file customer_data.csv --table customers
```

### Using PostgreSQL with schema and table replacement

```bash
python csv_to_sql.py \
  --file sales_data.csv \
  --table sales \
  --db postgresql://username:password@localhost:5432/mydatabase \
  --schema analytics \
  --if-exists replace
```

### MySQL with custom chunk size and delimiter

```bash
python csv_to_sql.py \
  --file large_dataset.csv \
  --table large_data \
  --db mysql+pymysql://user:pass@localhost/mydb \
  --chunksize 5000 \
  --delimiter ";" \
  --encoding latin1
```

### Append to existing table

```bash
python csv_to_sql.py \
  --file new_transactions.csv \
  --table transactions \
  --if-exists append
```

## How It Works

1. The script reads the CSV file in chunks to handle large files efficiently
2. For the first chunk, it analyzes the data to infer appropriate SQL column types
3. It creates a new table (or modifies an existing one based on your options)
4. It imports all data in chunks, showing progress
5. It provides a summary of the import upon completion

## Type Inference

The script attempts to infer the most appropriate SQL column type for each column:

- Integer types (SmallInteger, Integer, BigInteger) based on range
- Float for decimal numbers
- DateTime for date/time values
- String(length) for text (with appropriate length)
- Text for very long strings

If type inference is disabled with `--no-infer-types`, all columns will default to string/text types.

## Troubleshooting

### Common Issues

1. **File not found**: Make sure the path to your CSV file is correct
2. **Connection errors**: Check your database connection string and credentials
3. **Permission issues**: Ensure your database user has sufficient privileges
4. **Memory errors**: Try reducing the chunk size with `--chunksize`
5. **Encoding errors**: Try specifying the correct file encoding with `--encoding`

## License

This project is licensed under the MIT License - see the LICENSE file for details.
