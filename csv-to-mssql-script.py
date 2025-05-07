#!/usr/bin/env python3
"""
CSV to Microsoft SQL Server Importer

This script imports data from a CSV file into a Microsoft SQL Server database.
It automatically creates the table structure based on the CSV headers
and intelligently maps column types to appropriate SQL Server data types.

Usage:
    python csv_to_mssql.py --file path/to/file.csv --table table_name [options]

Options:
    --file FILE           Path to CSV file (required)
    --table TABLE         Name of the table to create/insert into (required)
    --server SERVER       SQL Server hostname or IP (required)
    --database DATABASE   Database name (required)
    --schema SCHEMA       Database schema (default: dbo)
    --username USERNAME   SQL Server username (will use Windows auth if omitted)
    --password PASSWORD   SQL Server password
    --trusted-connection  Use Windows Authentication (default if no username/password)
    --driver DRIVER       ODBC Driver (default: ODBC Driver 17 for SQL Server)
    --if-exists {fail,replace,append}
                          Action if table exists (default: fail)
    --batch-size BATCHSIZE Size of batches for bulk insert (default: 1000)
    --infer-types         Infer column types from data (default)
    --no-infer-types      Don't infer column types from data (use NVARCHAR for all)
    --encoding ENCODING   File encoding (default: utf-8)
    --delimiter DELIMITER CSV delimiter (default: ,)
"""

import os
import sys
import argparse
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, inspect, text
import urllib.parse
import logging
from datetime import datetime
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def infer_sql_server_type(column_values):
    """
    Infer SQL Server data type from a pandas Series of values.
    Returns SQL Server type as string.
    """
    # Drop NA values for type inference
    sample = column_values.dropna()
    if len(sample) == 0:
        return "NVARCHAR(255)"
    
    # Check if all values are numeric
    try:
        if pd.to_numeric(sample, errors='coerce').notna().all():
            # Check if all values are integers
            if pd.Series([float(x).is_integer() for x in sample if pd.notna(x)]).all():
                # Check range for appropriate integer size
                min_val = min(sample.astype(float))
                max_val = max(sample.astype(float))
                
                if min_val >= -32768 and max_val <= 32767:
                    return "SMALLINT"
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return "INT"
                else:
                    return "BIGINT"
            else:
                # Decimal values
                # Determine precision and scale
                max_str_len = max(sample.astype(str).str.len())
                if max_str_len <= 15:  # Standard float range
                    return "FLOAT"
                else:
                    # Use DECIMAL with appropriate precision
                    max_before_decimal = max(sample.astype(str).str.split('.').str[0].str.len())
                    
                    # Get max digits after decimal
                    max_after_decimal = 0
                    for val in sample:
                        if pd.notna(val) and '.' in str(val):
                            after_decimal = len(str(val).split('.')[1])
                            max_after_decimal = max(max_after_decimal, after_decimal)
                    
                    # SQL Server's DECIMAL has max precision of 38
                    precision = min(38, max_before_decimal + max_after_decimal)
                    scale = min(max_after_decimal, precision - max_before_decimal)
                    
                    return f"DECIMAL({precision}, {scale})"
    except:
        pass
    
    # Check if all values are dates
    try:
        date_series = pd.to_datetime(sample, errors='coerce')
        if date_series.notna().all():
            # Check if any times are non-midnight
            if any(date_series.dt.time != pd.Timestamp('00:00:00').time()):
                # Check for microseconds
                if any(date_series.dt.microsecond > 0):
                    return "DATETIME2"
                else:
                    return "DATETIME"
            else:
                return "DATE"
    except:
        pass
    
    # Check if all values are booleans
    if set(sample.astype(str).str.lower()) <= {'true', 'false', '1', '0', 'yes', 'no'}:
        return "BIT"
    
    # Default to string types, with appropriate length
    max_len = sample.astype(str).str.len().max()
    
    # For very short strings
    if max_len <= 1:
        return "CHAR(1)"
    
    # For strings that fit within NVARCHAR limits
    if max_len <= 4000:
        # Add some buffer for potential longer values
        recommended_len = int(max_len * 1.2)
        return f"NVARCHAR({min(4000, recommended_len)})"
    else:
        return "NVARCHAR(MAX)"

def create_connection_string(args):
    """Create SQLAlchemy connection string for SQL Server."""
    params = {}
    
    # Use Trusted Connection (Windows Auth) if no username/password provided or explicitly requested
    if args.trusted_connection or (not args.username and not args.password):
        auth = "Trusted_Connection=yes"
    else:
        auth = f"UID={args.username};PWD={args.password}"
    
    # Create connection string
    conn_str = (
        f"DRIVER={{{args.driver}}};"
        f"SERVER={args.server};"
        f"DATABASE={args.database};"
        f"{auth}"
    )
    
    # URL encode the connection string for SQLAlchemy
    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    
    # Create the SQLAlchemy connection string
    return f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}"

def sanitize_identifier(name):
    """
    Sanitize SQL Server identifiers to prevent SQL injection and errors.
    """
    # Replace invalid characters with underscores
    sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = 'col_' + sanitized
    
    # Handle reserved keywords by adding brackets if needed
    reserved_keywords = {
        'add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'authorization',
        'backup', 'begin', 'between', 'break', 'browse', 'bulk', 'by',
        'cascade', 'case', 'check', 'checkpoint', 'close', 'clustered',
        'coalesce', 'collate', 'column', 'commit', 'compute', 'constraint',
        'contains', 'containstable', 'continue', 'convert', 'create', 'cross',
        'current', 'current_date', 'current_time', 'current_timestamp',
        'current_user', 'cursor', 'database', 'dbcc', 'deallocate',
        'declare', 'default', 'delete', 'deny', 'desc', 'disk', 'distinct',
        'distributed', 'double', 'drop', 'dump', 'else', 'end', 'errlvl',
        'escape', 'except', 'exec', 'execute', 'exists', 'exit', 'external',
        'fetch', 'file', 'fillfactor', 'for', 'foreign', 'freetext',
        'freetexttable', 'from', 'full', 'function', 'goto', 'grant',
        'group', 'having', 'holdlock', 'identity', 'identity_insert',
        'identitycol', 'if', 'in', 'index', 'inner', 'insert', 'intersect',
        'into', 'is', 'join', 'key', 'kill', 'left', 'like', 'lineno',
        'load', 'merge', 'national', 'nocheck', 'nonclustered', 'not',
        'null', 'nullif', 'of', 'off', 'offsets', 'on', 'open',
        'opendatasource', 'openquery', 'openrowset', 'openxml', 'option',
        'or', 'order', 'outer', 'over', 'percent', 'pivot', 'plan',
        'precision', 'primary', 'print', 'proc', 'procedure', 'public',
        'raiserror', 'read', 'readtext', 'reconfigure', 'references',
        'replication', 'restore', 'restrict', 'return', 'revert', 'revoke',
        'right', 'rollback', 'rowcount', 'rowguidcol', 'rule', 'save',
        'schema', 'securityaudit', 'select', 'semantickeyphrasetable',
        'semanticsimilaritydetailstable', 'semanticsimilaritytable',
        'session_user', 'set', 'setuser', 'shutdown', 'some', 'statistics',
        'system_user', 'table', 'tablesample', 'textsize', 'then', 'to',
        'top', 'tran', 'transaction', 'trigger', 'truncate', 'try_convert',
        'tsequal', 'union', 'unique', 'unpivot', 'update', 'updatetext',
        'use', 'user', 'values', 'varying', 'view', 'waitfor', 'when',
        'where', 'while', 'with', 'within', 'writetext'
    }
    
    if sanitized.lower() in reserved_keywords:
        return f"[{sanitized}]"
    
    return sanitized

def create_table_from_csv(engine, csv_path, table_name, schema="dbo", 
                         if_exists="fail", batch_size=1000, infer_types=True,
                         encoding="utf-8", delimiter=","):
    """
    Create a SQL Server table from a CSV file and import data.
    """
    start_time = datetime.now()
    
    # Create a connection directly with pyodbc for more control
    connection_url = engine.url
    connection_string = connection_url.query['odbc_connect']
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Read the first chunk to get column names and sample data for type inference
    logger.info(f"Reading CSV file: {csv_path}")
    first_chunk = pd.read_csv(csv_path, nrows=batch_size, encoding=encoding, delimiter=delimiter, low_memory=False)
    
    # Clean column names
    original_columns = first_chunk.columns
    cleaned_columns = [sanitize_identifier(col.strip()) for col in original_columns]
    column_mapping = dict(zip(original_columns, cleaned_columns))
    
    # Rename the columns in the dataframe
    first_chunk.rename(columns=column_mapping, inplace=True)
    
    # Set up SQL column types
    column_types = {}
    if infer_types:
        logger.info("Inferring SQL Server column types from data")
        for col in cleaned_columns:
            column_types[col] = infer_sql_server_type(first_chunk[col])
            logger.info(f"Column '{col}' inferred as {column_types[col]}")
    else:
        # Default all columns to NVARCHAR(255)
        for col in cleaned_columns:
            column_types[col] = "NVARCHAR(255)"
    
    # Check if table exists
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
    """)
    table_exists = cursor.fetchone()[0] > 0
    
    if table_exists and if_exists == "fail":
        logger.error(f"Table '{schema}.{table_name}' already exists. Use --if-exists option.")
        conn.close()
        sys.exit(1)
    elif table_exists and if_exists == "replace":
        logger.info(f"Dropping existing table '{schema}.{table_name}'")
        cursor.execute(f"DROP TABLE [{schema}].[{table_name}]")
        conn.commit()
        table_exists = False
    
    # Create table if it doesn't exist or we're replacing it
    if not table_exists or if_exists == "replace":
        # Generate the CREATE TABLE statement
        create_table_sql = f"CREATE TABLE [{schema}].[{table_name}] (\n"
        
        # Add a numeric identity column as primary key
        create_table_sql += "    [Id] INT IDENTITY(1,1) PRIMARY KEY,\n"
        
        # Add columns based on inferred or default types
        for i, col in enumerate(cleaned_columns):
            create_table_sql += f"    [{col}] {column_types[col]}"
            if i < len(cleaned_columns) - 1:
                create_table_sql += ",\n"
            else:
                create_table_sql += "\n"
        
        create_table_sql += ")"
        
        # Create the table
        logger.info(f"Creating table with SQL:\n{create_table_sql}")
        cursor.execute(create_table_sql)
        conn.commit()
    
    # Import data in chunks
    total_rows = 0
    
    # Prepare the INSERT statement template
    placeholders = ", ".join(["?" for _ in cleaned_columns])
    columns_str = ", ".join([f"[{col}]" for col in cleaned_columns])
    insert_sql = f"INSERT INTO [{schema}].[{table_name}] ({columns_str}) VALUES ({placeholders})"
    
    # Read and insert the data in chunks
    logger.info("Starting data import...")
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=batch_size, encoding=encoding, delimiter=delimiter, low_memory=False)):
        # Apply column name mapping
        chunk.rename(columns=column_mapping, inplace=True)
        
        # Convert chunk to list of tuples for fast insertion
        rows = [tuple(row) for row in chunk.values]
        
        # Insert batch of rows
        start_batch = time.time()
        cursor.fast_executemany = True  # Enable fast_executemany for better performance
        cursor.executemany(insert_sql, rows)
        conn.commit()
        
        batch_rows = len(rows)
        total_rows += batch_rows
        end_batch = time.time()
        batch_time = end_batch - start_batch
        
        logger.info(f"Batch {i+1}: Inserted {batch_rows} rows in {batch_time:.2f} seconds ({batch_rows/batch_time:.2f} rows/sec)")
        logger.info(f"Total progress: {total_rows} rows imported so far...")
    
    # Close the connection
    conn.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Import completed: {total_rows} rows imported in {duration}")
    
    return total_rows

def main():
    parser = argparse.ArgumentParser(description="Import CSV data into Microsoft SQL Server")
    parser.add_argument("--file", required=True, help="Path to CSV file")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--server", required=True, help="SQL Server hostname or IP")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--schema", default="dbo", help="Database schema (default: dbo)")
    parser.add_argument("--username", help="SQL Server username (omit for Windows auth)")
    parser.add_argument("--password", help="SQL Server password")
    parser.add_argument("--trusted-connection", action="store_true", help="Use Windows Authentication")
    parser.add_argument("--driver", default="ODBC Driver 17 for SQL Server", 
                        help="ODBC Driver (default: ODBC Driver 17 for SQL Server)")
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="fail",
                        help="Action if table exists (default: fail)")
    parser.add_argument("--batch-size", type=int, default=1000, 
                        help="Size of batches for bulk insert (default: 1000)")
    parser.add_argument("--infer-types", action="store_true", default=True,
                        help="Infer column types from data (default)")
    parser.add_argument("--no-infer-types", action="store_false", dest="infer_types",
                        help="Don't infer column types from data (use NVARCHAR for all)")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    
    args = parser.parse_args()
    
    # Check if file exists
    if not os.path.isfile(args.file):
        logger.error(f"File not found: {args.file}")
        sys.exit(1)
    
    try:
        # Create connection string and engine
        connection_string = create_connection_string(args)
        logger.info(f"Connecting to SQL Server: {args.server}, Database: {args.database}")
        engine = create_engine(connection_string, fast_executemany=True)
        
        # Test connection
        connection = engine.connect()
        connection.close()
        logger.info("Connection to SQL Server successful")
        
        # Import data
        rows_imported = create_table_from_csv(
            engine=engine,
            csv_path=args.file,
            table_name=args.table,
            schema=args.schema,
            if_exists=args.if_exists,
            batch_size=args.batch_size,
            infer_types=args.infer_types,
            encoding=args.encoding,
            delimiter=args.delimiter
        )
        
        logger.info(f"Successfully imported {rows_imported} rows into table '{args.schema}.{args.table}'")
        
    except Exception as e:
        logger.error(f"Error importing data: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
