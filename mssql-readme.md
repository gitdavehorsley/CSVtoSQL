# CSV to Microsoft SQL Server Importer

A specialized Python script for importing CSV data into Microsoft SQL Server databases with optimal performance, SQL Server-specific data type mapping, and support for both Windows and SQL Server authentication.

## Features

- **SQL Server Optimized**: Designed specifically for Microsoft SQL Server
- **Intelligent Type Mapping**: Maps CSV data to appropriate SQL Server data types (VARCHAR, INT, DECIMAL, etc.)
- **High-Performance Bulk Import**: Uses fast_executemany for efficient batch imports
- **Authentication Options**: Supports both Windows Authentication and SQL Server Authentication
- **Schema Support**: Imports data to specified schema (default: dbo)
- **Safe Identifier Handling**: Sanitizes column names to prevent SQL errors and injection risks
- **Flexible Table Handling**: Options to fail, replace, or append when table exists
- **Detailed Logging**: Shows progress, performance metrics, and column type mapping

## Requirements

- Python 3.6+
- Required Python packages:
  - pandas
  - pyodbc
  - sqlalchemy
- SQL Server ODBC Driver (default: ODBC Driver 17 for SQL Server)

## Installation

1. Install required Python packages:

```bash
pip install pandas pyodbc sqlalchemy
```

2. Ensure you have the appropriate SQL Server ODBC driver installed:
   - For Windows: Install the Microsoft ODBC Driver for SQL Server
   - For Linux/macOS: Follow Microsoft's instructions for installing the ODBC driver

## Usage

Basic syntax:

```bash
python csv_to_mssql.py --file <csv_file> --table <table_name> --server <server> --database <database> [options]
```

### Required Arguments

- `--file`: Path to the CSV file to import
- `--table`: Name of the table to create/update in SQL Server
- `--server`: SQL Server hostname or IP address
- `--database`: Database name

### Authentication Options

- For Windows Authentication (recommended when possible):
  ```bash
  python csv_to_mssql.py --file data.csv --table employees --server SQLSERVER01 --database HRData --trusted-connection
  ```

- For SQL Server Authentication:
  ```bash
  python csv_to_mssql.py --file data.csv --table employees --server SQLSERVER01 --database HRData --username sa --password YourPassword123
  ```

### Optional Arguments

- `--schema`: Database schema (default: `dbo`)
- `--if-exists`: Action if table exists (`fail`, `replace`, `append`) (default: `fail`)
- `--batch-size`: Size of batches for bulk insert (default: 1000)
- `--driver`: ODBC Driver name (default: `ODBC Driver 17 for SQL Server`)
- `--infer-types`: Infer column types from data (default)
- `--no-infer-types`: Don't infer column types from data (use NVARCHAR for all)
- `--encoding`: File encoding (default: `utf-8`)
- `--delimiter`: CSV delimiter (default: `,`)

## SQL Server Data Type Mapping

The script intelligently maps CSV data to appropriate SQL Server data types:

| Data in CSV | SQL Server Type |
|-------------|----------------|
| Integers (small range) | SMALLINT |
| Integers (medium range) | INT |
| Integers (large range) | BIGINT |
| Decimal numbers (standard) | FLOAT |
| Decimal numbers (high precision) | DECIMAL(p,s) with appropriate precision and scale |
| Dates only | DATE |
| Dates with time | DATETIME |
| Dates with microseconds | DATETIME2 |
| Boolean values | BIT |
| Short text (<= 4000 chars) | NVARCHAR(size) |
| Long text (> 4000 chars) | NVARCHAR(MAX) |

## Examples

### Basic Usage with Windows Authentication

```bash
python csv_to_mssql.py --file customer_data.csv --table customers --server SQLSERVER01 --database CRM --trusted-connection
```

### Using SQL Server Authentication with Schema

```bash
python csv_to_mssql.py --file sales_data.csv --table sales --server 192.168.1.100 --database Analytics --schema reports --username dbuser --password P@ssw0rd
```

### Replacing Existing Table

```bash
python csv_to_mssql.py --file updated_products.csv --table products --server SQLSERVER01 --database Inventory --trusted-connection --if-exists replace
```

### Handling Large Files with Custom Batch Size

```bash
python csv_to_mssql.py --file large_dataset.csv --table transactions --server SQLSERVER01 --database Finance --trusted-connection --batch-size 5000
```

### Using a Different SQL Server Driver

```bash
python csv_to_mssql.py --file export.csv --table audit --server SQLSERVER01 --database Compliance --trusted-connection --driver "ODBC Driver 18 for SQL Server"
```

### Importing a European CSV with Different Delimiter and Encoding

```bash
python csv_to_mssql.py --file european_data.csv --table eu_sales --server SQLSERVER01 --database International --trusted-connection --delimiter ";" --encoding "latin1"
```

## Performance Optimization

The script includes several optimizations for SQL Server:

1. **fast_executemany**: Uses pyodbc's fast_executemany feature for bulk inserts
2. **Batched Inserts**: Processes data in configurable batches to minimize memory usage
3. **Connection Pooling**: Maintains efficient database connections
4. **Progress Tracking**: Reports performance metrics (rows/second) for each batch

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Verify your SQL Server credentials
   - Ensure the server allows the authentication method you're using
   - For Windows Authentication, verify your domain permissions

2. **ODBC Driver Issues**:
   - Verify the ODBC driver is correctly installed
   - Try specifying the exact driver name with `--driver`
   - Common driver names:
     - `ODBC Driver 17 for SQL Server` (most common)
     - `ODBC Driver 18 for SQL Server` (newer)
     - `SQL Server Native Client 11.0` (older)

3. **Permission Issues**:
   - Ensure your user has CREATE TABLE permission in the database
   - Verify you have INSERT permission on the table
   - Check schema permissions

4. **Large File Issues**:
   - Try decreasing the batch size with `--batch-size`
   - Ensure adequate disk space for temporary storage
   - Consider splitting very large files

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Microsoft SQL Server Documentation
- pyodbc and SQLAlchemy projects
