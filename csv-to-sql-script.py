#!/usr/bin/env python3
"""
CSV to SQL Database Importer

This script imports data from a CSV file into a SQL database.
It automatically creates the table structure based on the CSV headers
and infers column types from the data.

Usage:
    python csv_to_sql.py --file path/to/file.csv --table table_name [options]

Options:
    --file FILE           Path to CSV file (required)
    --table TABLE         Name of the table to create/insert into (required)
    --db DB               Database connection string (default: sqlite:///data.db)
    --schema SCHEMA       Database schema (default: public)
    --if-exists {fail,replace,append}
                          Action if table exists (default: fail)
    --chunksize CHUNKSIZE Size of chunks to process at once (default: 1000)
    --no-infer-types      Don't infer column types from data (use string for all)
    --encoding ENCODING   File encoding (default: utf-8)
    --delimiter DELIMITER CSV delimiter (default: ,)
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, inspect
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def infer_sql_column_type(column_values):
    """
    Infer SQL column type from a pandas Series of values.
    Returns SQLAlchemy type.
    """
    # Drop NA values for type inference
    sample = column_values.dropna()
    if len(sample) == 0:
        return sqlalchemy.String
    
    # Check if all values are numeric
    try:
        if pd.to_numeric(sample, errors='coerce').notna().all():
            # Check if all values are integers
            if pd.Series([float(x).is_integer() for x in sample if pd.notna(x)]).all():
                # Check range for appropriate integer size
                min_val = min(sample.astype(float))
                max_val = max(sample.astype(float))
                if min_val >= -32768 and max_val <= 32767:
                    return sqlalchemy.SmallInteger
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return sqlalchemy.Integer
                else:
                    return sqlalchemy.BigInteger
            else:
                return sqlalchemy.Float
    except:
        pass
    
    # Check if all values are dates
    try:
        if pd.to_datetime(sample, errors='coerce').notna().all():
            return sqlalchemy.DateTime
    except:
        pass
    
    # Default to string, with appropriate length
    max_len = sample.astype(str).str.len().max()
    if max_len <= 255:
        return sqlalchemy.String(max_len)
    else:
        return sqlalchemy.Text

def create_table_from_csv(engine, csv_path, table_name, schema="public", 
                         if_exists="fail", chunksize=1000, infer_types=True,
                         encoding="utf-8", delimiter=","):
    """
    Create a SQL table from a CSV file and import data.
    """
    start_time = datetime.now()
    
    # Read the first chunk to get column names and sample data for type inference
    logger.info(f"Reading CSV file: {csv_path}")
    first_chunk = pd.read_csv(csv_path, nrows=chunksize, encoding=encoding, delimiter=delimiter)
    
    # Clean column names (replace spaces with underscores, etc.)
    first_chunk.columns = [col.strip().lower().replace(' ', '_') for col in first_chunk.columns]
    
    # Set up SQL column types
    if infer_types:
        logger.info("Inferring column types from data")
        dtype_dict = {col: infer_sql_column_type(first_chunk[col]) for col in first_chunk.columns}
    else:
        dtype_dict = None  # Will default to string/text types
    
    # Check if table exists
    inspector = inspect(engine)
    table_exists = table_name in inspector.get_table_names(schema=schema)
    
    if table_exists and if_exists == "fail":
        logger.error(f"Table '{table_name}' already exists. Use --if-exists option.")
        sys.exit(1)
    elif table_exists and if_exists == "replace":
        logger.info(f"Dropping existing table '{table_name}'")
        engine.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    
    # Import data in chunks
    total_rows = 0
    
    if if_exists == "append" and table_exists:
        # Just append to existing table
        logger.info(f"Appending data to existing table '{table_name}'")
        for chunk in pd.read_csv(csv_path, chunksize=chunksize, encoding=encoding, delimiter=delimiter):
            chunk.columns = [col.strip().lower().replace(' ', '_') for col in chunk.columns]
            chunk.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
            total_rows += len(chunk)
            logger.info(f"Imported {total_rows} rows so far...")
    else:
        # Create new table
        logger.info(f"Creating table '{table_name}' and importing data")
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize, encoding=encoding, delimiter=delimiter)):
            chunk.columns = [col.strip().lower().replace(' ', '_') for col in chunk.columns]
            
            if i == 0:
                # Create table with first chunk
                chunk.to_sql(table_name, engine, schema=schema, if_exists="replace", 
                            index=False, dtype=dtype_dict)
            else:
                # Append remaining chunks
                chunk.to_sql(table_name, engine, schema=schema, if_exists="append", 
                            index=False)
            
            total_rows += len(chunk)
            logger.info(f"Imported {total_rows} rows so far...")
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Import completed: {total_rows} rows imported in {duration}")
    
    return total_rows

def main():
    parser = argparse.ArgumentParser(description="Import CSV data into SQL database")
    parser.add_argument("--file", required=True, help="Path to CSV file")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--db", default="sqlite:///data.db", 
                        help="Database connection string (default: sqlite:///data.db)")
    parser.add_argument("--schema", default="public", help="Database schema (default: public)")
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="fail",
                        help="Action if table exists (default: fail)")
    parser.add_argument("--chunksize", type=int, default=1000, 
                        help="Size of chunks to process at once (default: 1000)")
    parser.add_argument("--no-infer-types", action="store_true", 
                        help="Don't infer column types from data")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    
    args = parser.parse_args()
    
    # Check if file exists
    if not os.path.isfile(args.file):
        logger.error(f"File not found: {args.file}")
        sys.exit(1)
    
    try:
        # Create database connection
        logger.info(f"Connecting to database: {args.db}")
        engine = create_engine(args.db)
        
        # Import data
        rows_imported = create_table_from_csv(
            engine=engine,
            csv_path=args.file,
            table_name=args.table,
            schema=args.schema,
            if_exists=args.if_exists,
            chunksize=args.chunksize,
            infer_types=not args.no_infer_types,
            encoding=args.encoding,
            delimiter=args.delimiter
        )
        
        logger.info(f"Successfully imported {rows_imported} rows into table '{args.table}'")
        
    except Exception as e:
        logger.error(f"Error importing data: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
