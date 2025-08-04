#!/usr/bin/env python3
"""
Database setup and data loading script for NQ trading analytics

This script:
1. Sets up PostgreSQL database and tables
2. Loads the converted CSV data into PostgreSQL
3. Prepares the data for dbt transformations
"""

import os
import sys
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
from pathlib import Path
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

data_dir = Path("Data/processed/")


def create_database_connection(
    host='localhost',
    user='postgres',
    password='password',
    database='trading_analytics',
    port=5432
):
    """Create database connection."""
    try:
        # Use psycopg2 for database creation
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        
        conn = psycopg2.connect(
            host=host, user=user, password=password, 
            database='postgres', port=port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cur:
            try:
                cur.execute(f"CREATE DATABASE {database}")
                logger.info(f"Created database: {database}")
            except psycopg2.errors.DuplicateDatabase:
                logger.info(f"Database {database} already exists")
        
        conn.close()
        
        # Connect to the new database using SQLAlchemy
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(conn_str)
        logger.info(f"Connected to database: {database}")
        return engine
        
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def create_raw_data_schema(engine):
    """Create raw_data schema and tables."""
    try:
        with engine.begin() as conn:  # Use begin() for transaction
            # Create raw_data schema
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data"))
            logger.info("Created raw_data schema")
            
            # Create OHLCV table
            ohlcv_ddl = """
            CREATE TABLE IF NOT EXISTS raw_data.ohlcv_1m (
                ts_event VARCHAR(50),
                rtype INTEGER,
                publisher_id INTEGER,
                instrument_id BIGINT,
                open DECIMAL(12,2),
                high DECIMAL(12,2),
                low DECIMAL(12,2),
                close DECIMAL(12,2),
                volume BIGINT,
                symbol VARCHAR(20)
            )
            """
            conn.execute(text(ohlcv_ddl))
            
            # Create symbology table
            symbology_ddl = """
            CREATE TABLE IF NOT EXISTS raw_data.symbology (
                raw_symbol VARCHAR(50),
                instrument_id BIGINT,
                date VARCHAR(20)
            )
            """
            conn.execute(text(symbology_ddl))
            
            logger.info("Created raw data tables")
            
    except Exception as e:
        logger.error(f"Failed to create schema: {e}")
        raise


def load_csv_data(engine, csv_file_path, table_name, schema='raw_data', chunksize=10000):
    """Load CSV data into PostgreSQL table."""
    try:
        csv_path = Path(csv_file_path)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_file_path}")
            return False
            
        logger.info(f"Loading {csv_file_path} into {schema}.{table_name}")
        
        # Load data in chunks for memory efficiency
        chunk_count = 0
        total_rows = 0
        
        for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
            chunk.to_sql(
                table_name, 
                engine, 
                schema=schema,
                if_exists='append' if chunk_count > 0 else 'replace',
                index=False,
                method='multi'
            )
            chunk_count += 1
            total_rows += len(chunk)
            
            if chunk_count % 10 == 0:
                logger.info(f"Loaded {chunk_count} chunks, {total_rows:,} rows so far...")
        
        logger.info(f"Successfully loaded {total_rows:,} rows into {schema}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return False


def setup_database():
    """Main setup function."""
    logger.info("Starting database setup...")
    
    # Database connection parameters (can be overridden by environment variables)
    db_params = {
        'host': os.getenv('DBT_POSTGRES_HOST', 'localhost'),
        'user': os.getenv('DBT_POSTGRES_USER', 'postgres'),  # Changed to match profiles.yml
        'password': os.getenv('DBT_POSTGRES_PASSWORD', 'password'),  # Changed to match profiles.yml
        'database': os.getenv('DBT_POSTGRES_DB', 'trading_analytics'),
        'port': int(os.getenv('DBT_POSTGRES_PORT', '5432'))
    }
    
    try:
        # Create database connection
        engine = create_database_connection(**db_params)
        
        # Create schema and tables
        create_raw_data_schema(engine)
        
        # Load OHLCV data
        ohlcv_file = data_dir / "glbx-mdp3-20100606-20250603.ohlcv-1m.csv"
        if Path(ohlcv_file).exists():
            load_csv_data(engine, ohlcv_file, 'ohlcv_1m')
        else:
            logger.warning(f"OHLCV file not found: {ohlcv_file}")
        
        # Load symbology data
        symbology_file = data_dir / "symbology.csv"
        if Path(symbology_file).exists():
            load_csv_data(engine, symbology_file, 'symbology')
        else:
            logger.warning(f"Symbology file not found: {symbology_file}")
        
        logger.info("Database setup completed successfully!")
        
        # Print some basic stats
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw_data.ohlcv_1m"))
            ohlcv_count = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM raw_data.symbology"))
            symbology_count = result.fetchone()[0]
            
            logger.info(f"Data loaded - OHLCV: {ohlcv_count:,} rows, Symbology: {symbology_count:,} rows")
        
        return True
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False


if __name__ == "__main__":
    success = setup_database()
    sys.exit(0 if success else 1)
