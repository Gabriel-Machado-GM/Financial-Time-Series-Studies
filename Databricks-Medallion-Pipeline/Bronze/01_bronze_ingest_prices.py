"""
Bronze Layer - Price Data Ingestion
====================================

This module ingests historical price data (OHLCV) from Yahoo Finance using yfinance
and stores it in Delta Lake format in the Bronze layer of the Medallion architecture.

The Bronze layer preserves raw data "as-is" with minimal processing, focusing on:
- Fast data ingestion
- Data lineage tracking
- Preservation of original data for auditing
- Support for both batch and incremental loading

Schema: bronze_prices_raw
- ticker: STRING - Stock symbol
- date: DATE - Trading date  
- open: DOUBLE - Opening price
- high: DOUBLE - Highest price of the day
- low: DOUBLE - Lowest price of the day
- close: DOUBLE - Closing price
- volume: LONG - Trading volume
- adj_close: DOUBLE - Adjusted closing price
- ingestion_timestamp: TIMESTAMP - When data was ingested
- ingestion_id: STRING - Unique identifier for ingestion job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, LongType, TimestampType
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import uuid


def create_spark_session(app_name="Bronze_Price_Ingestion"):
    """Create and configure Spark session for Delta Lake operations."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_bronze_prices_schema():
    """Define the schema for the bronze_prices_raw table."""
    return StructType([
        StructField("ticker", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("ingestion_id", StringType(), False)
    ])


def fetch_price_data_from_yfinance(tickers, start_date, end_date):
    """
    Fetch price data from Yahoo Finance for multiple tickers.
    
    Args:
        tickers: List of stock symbols
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format or None for current date)
    
    Returns:
        pandas.DataFrame with price data
    """
    try:
        # Download data for all tickers at once for efficiency
        data = yf.download(tickers, start=start_date, end=end_date, 
                          progress=False, group_by='ticker', auto_adjust=False)
        
        # Handle single ticker case
        if len(tickers) == 1:
            ticker = tickers[0]
            df_ticker = data.copy()
            df_ticker['ticker'] = ticker
            df_ticker = df_ticker.reset_index()
            return df_ticker
        
        # Handle multiple tickers
        result_dfs = []
        for ticker in tickers:
            try:
                if ticker in data.columns.levels[0]:
                    df_ticker = data[ticker].copy()
                    df_ticker['ticker'] = ticker
                    df_ticker = df_ticker.reset_index()
                    result_dfs.append(df_ticker)
                else:
                    print(f"Warning: No data found for ticker {ticker}")
            except Exception as e:
                print(f"Error processing ticker {ticker}: {e}")
                continue
        
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error fetching data from yfinance: {e}")
        return pd.DataFrame()


def clean_and_standardize_price_data(df_pandas, job_id):
    """
    Clean and standardize the price data from yfinance.
    
    Args:
        df_pandas: Raw pandas DataFrame from yfinance
        job_id: Unique identifier for this ingestion job
    
    Returns:
        pandas.DataFrame with standardized column names and data types
    """
    if df_pandas.empty:
        return df_pandas
    
    # Standardize column names (yfinance uses title case)
    column_mapping = {
        'Date': 'date',
        'Open': 'open', 
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',
        'Adj Close': 'adj_close'
    }
    
    # Rename columns if they exist
    for old_name, new_name in column_mapping.items():
        if old_name in df_pandas.columns:
            df_pandas = df_pandas.rename(columns={old_name: new_name})
    
    # Ensure date column is properly formatted
    if 'Date' in df_pandas.columns:
        df_pandas = df_pandas.rename(columns={'Date': 'date'})
    
    # Convert date index to column if necessary
    if df_pandas.index.name == 'Date':
        df_pandas = df_pandas.reset_index()
        df_pandas = df_pandas.rename(columns={'Date': 'date'})
    
    # Add metadata columns
    df_pandas['ingestion_timestamp'] = datetime.now()
    df_pandas['ingestion_id'] = job_id
    
    # Handle missing values
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'adj_close']
    for col in numeric_columns:
        if col in df_pandas.columns:
            df_pandas[col] = pd.to_numeric(df_pandas[col], errors='coerce')
    
    # Remove rows with all null price data
    price_cols = ['open', 'high', 'low', 'close']
    df_pandas = df_pandas.dropna(subset=price_cols, how='all')
    
    return df_pandas


def write_to_bronze_layer(spark, df_pandas, output_path, mode="append"):
    """
    Write the cleaned price data to Delta Lake in the Bronze layer.
    
    Args:
        spark: SparkSession
        df_pandas: Cleaned pandas DataFrame
        output_path: Path to write the Delta table
        mode: Write mode ('append', 'overwrite', 'ignore')
    """
    if df_pandas.empty:
        print("Warning: No data to write to Bronze layer")
        return
    
    # Convert to Spark DataFrame with proper schema
    schema = get_bronze_prices_schema()
    
    try:
        # Ensure date column is properly formatted
        df_pandas['date'] = pd.to_datetime(df_pandas['date']).dt.date
        
        # Create Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas, schema=schema)
        
        # Write to Delta Lake
        df_spark.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .save(output_path)
            
        print(f"Successfully wrote {df_spark.count()} records to {output_path}")
        
    except Exception as e:
        print(f"Error writing to Bronze layer: {e}")
        raise


def run_price_ingestion(spark, tickers, start_date, end_date, output_path, mode="append"):
    """
    Main function to run the complete price data ingestion pipeline.
    
    Args:
        spark: SparkSession
        tickers: List of stock symbols to ingest
        start_date: Start date for data collection
        end_date: End date for data collection (None for current date)
        output_path: Path to store the Bronze layer Delta table
        mode: Write mode for Delta table
    
    Returns:
        str: Job ID for tracking
    """
    job_id = f"price_ingestion_{uuid.uuid4().hex[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Starting price data ingestion job: {job_id}")
    print(f"Tickers: {tickers}")
    print(f"Date range: {start_date} to {end_date or 'current'}")
    
    try:
        # Fetch data from yfinance
        print("Fetching data from Yahoo Finance...")
        df_pandas = fetch_price_data_from_yfinance(tickers, start_date, end_date)
        
        if df_pandas.empty:
            print("No data fetched from Yahoo Finance")
            return job_id
        
        print(f"Fetched {len(df_pandas)} records")
        
        # Clean and standardize data
        print("Cleaning and standardizing data...")
        df_cleaned = clean_and_standardize_price_data(df_pandas, job_id)
        
        # Write to Bronze layer
        print("Writing to Bronze layer...")
        write_to_bronze_layer(spark, df_cleaned, output_path, mode)
        
        print(f"Price ingestion job {job_id} completed successfully")
        return job_id
        
    except Exception as e:
        print(f"Price ingestion job {job_id} failed: {e}")
        raise


# Example usage and testing
if __name__ == "__main__":
    # This section is for testing the module independently
    from config import DEFAULT_TICKERS, DEFAULT_START_DATE, BRONZE_PRICES_PATH
    
    # Create Spark session
    spark = create_spark_session()
    
    # Test with a small subset of tickers
    test_tickers = DEFAULT_TICKERS[:3]  # First 3 tickers for testing
    
    try:
        job_id = run_price_ingestion(
            spark=spark,
            tickers=test_tickers,
            start_date=DEFAULT_START_DATE,
            end_date=None,
            output_path=BRONZE_PRICES_PATH,
            mode="overwrite"  # Use overwrite for testing
        )
        
        # Verify the data was written correctly
        df_result = spark.read.format("delta").load(BRONZE_PRICES_PATH)
        print(f"\nVerification: Bronze table contains {df_result.count()} records")
        print("Sample data:")
        df_result.show(5)
        
    except Exception as e:
        print(f"Test execution failed: {e}")
    finally:
        spark.stop()