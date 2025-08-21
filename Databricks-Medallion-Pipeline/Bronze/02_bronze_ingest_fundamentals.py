"""
Bronze Layer - Fundamental Data Ingestion
==========================================

This module ingests fundamental data (P/E ratio, sector, industry, etc.) from Yahoo Finance
using yfinance and stores it in Delta Lake format in the Bronze layer.

The fundamental data has lower frequency updates compared to price data, typically 
quarterly or annually. This script handles the temporal mismatch between high-frequency
price data and low-frequency fundamental data.

Schema: bronze_fundamentals_raw
- ticker: STRING - Stock symbol
- date: DATE - Date of fundamental data (typically fiscal year end)
- pe_ratio: DOUBLE - Price-to-Earnings ratio
- eps: DOUBLE - Earnings Per Share
- sector: STRING - Business sector
- industry: STRING - Industry classification
- beta: DOUBLE - Beta coefficient (volatility relative to market)
- market_cap: DOUBLE - Market capitalization
- enterprise_value: DOUBLE - Enterprise value
- ingestion_timestamp: TIMESTAMP - When data was ingested
- ingestion_id: STRING - Unique identifier for ingestion job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import uuid
import time


def create_spark_session(app_name="Bronze_Fundamentals_Ingestion"):
    """Create and configure Spark session for Delta Lake operations."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_bronze_fundamentals_schema():
    """Define the schema for the bronze_fundamentals_raw table."""
    return StructType([
        StructField("ticker", StringType(), False),
        StructField("date", DateType(), True),
        StructField("pe_ratio", DoubleType(), True),
        StructField("eps", DoubleType(), True),
        StructField("sector", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("beta", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("enterprise_value", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("ingestion_id", StringType(), False)
    ])


def fetch_fundamental_data_from_yfinance(tickers, delay_between_requests=0.1):
    """
    Fetch fundamental data from Yahoo Finance for multiple tickers.
    
    Args:
        tickers: List of stock symbols
        delay_between_requests: Delay between API calls to avoid rate limiting
    
    Returns:
        pandas.DataFrame with fundamental data
    """
    fundamentals_list = []
    failed_tickers = []
    
    print(f"Fetching fundamental data for {len(tickers)} tickers...")
    
    for i, ticker in enumerate(tickers):
        try:
            # Add delay to avoid rate limiting
            if i > 0 and delay_between_requests > 0:
                time.sleep(delay_between_requests)
            
            print(f"Processing {ticker} ({i+1}/{len(tickers)})")
            
            # Create yfinance Ticker object
            ticker_obj = yf.Ticker(ticker)
            info = ticker_obj.info
            
            # Extract fundamental data with fallback values
            fundamental_data = {
                "ticker": ticker,
                "date": extract_date_from_info(info),
                "pe_ratio": safe_get_numeric(info, "trailingPE"),
                "eps": safe_get_numeric(info, "trailingEps"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "beta": safe_get_numeric(info, "beta"),
                "market_cap": safe_get_numeric(info, "marketCap"),
                "enterprise_value": safe_get_numeric(info, "enterpriseValue")
            }
            
            fundamentals_list.append(fundamental_data)
            
        except Exception as e:
            print(f"Error fetching fundamentals for {ticker}: {e}")
            failed_tickers.append(ticker)
            continue
    
    if failed_tickers:
        print(f"Failed to fetch data for tickers: {failed_tickers}")
    
    if fundamentals_list:
        df = pd.DataFrame(fundamentals_list)
        print(f"Successfully fetched fundamental data for {len(df)} tickers")
        return df
    else:
        print("No fundamental data was successfully fetched")
        return pd.DataFrame()


def extract_date_from_info(info):
    """
    Extract the most relevant date from ticker info.
    Tries to get fiscal year end, last fiscal year end, or current date.
    """
    # Try different date fields in order of preference
    date_fields = [
        "lastFiscalYearEnd",
        "mostRecentQuarter", 
        "nextFiscalYearEnd"
    ]
    
    for field in date_fields:
        if field in info and info[field]:
            try:
                # yfinance returns timestamps as epoch seconds
                return pd.to_datetime(info[field], unit='s').date()
            except:
                continue
    
    # Fallback to current date
    return datetime.now().date()


def safe_get_numeric(info_dict, key, default_value=None):
    """
    Safely extract numeric values from yfinance info dictionary.
    
    Args:
        info_dict: Dictionary from yfinance ticker.info
        key: Key to extract
        default_value: Value to return if key doesn't exist or is invalid
    
    Returns:
        float or None
    """
    try:
        value = info_dict.get(key, default_value)
        if value is None or value == "":
            return None
        
        # Handle various problematic values
        if isinstance(value, str):
            if value.lower() in ['n/a', 'nan', 'null', '']:
                return None
        
        # Convert to float
        return float(value)
        
    except (ValueError, TypeError):
        return None


def clean_and_standardize_fundamental_data(df_pandas, job_id):
    """
    Clean and standardize the fundamental data.
    
    Args:
        df_pandas: Raw pandas DataFrame with fundamental data
        job_id: Unique identifier for this ingestion job
    
    Returns:
        pandas.DataFrame with cleaned and standardized data
    """
    if df_pandas.empty:
        return df_pandas
    
    # Add metadata columns
    df_pandas['ingestion_timestamp'] = datetime.now()
    df_pandas['ingestion_id'] = job_id
    
    # Ensure date column is properly formatted
    if 'date' in df_pandas.columns:
        df_pandas['date'] = pd.to_datetime(df_pandas['date'], errors='coerce').dt.date
    
    # Clean numeric columns - remove infinite values and extreme outliers
    numeric_columns = ['pe_ratio', 'eps', 'beta', 'market_cap', 'enterprise_value']
    
    for col in numeric_columns:
        if col in df_pandas.columns:
            # Replace infinite values with None
            df_pandas[col] = df_pandas[col].replace([float('inf'), float('-inf')], None)
            
            # Handle extreme outliers (optional - can be configured)
            if col == 'pe_ratio':
                # Remove P/E ratios above 1000 or below -1000 (likely errors)
                df_pandas.loc[
                    (df_pandas[col] > 1000) | (df_pandas[col] < -1000), col
                ] = None
            elif col == 'beta':
                # Beta typically ranges from -3 to 3 for most stocks
                df_pandas.loc[
                    (df_pandas[col] > 5) | (df_pandas[col] < -5), col
                ] = None
    
    # Clean string columns - standardize capitalization and handle nulls
    string_columns = ['sector', 'industry']
    for col in string_columns:
        if col in df_pandas.columns:
            # Standardize to title case and handle empty strings
            df_pandas[col] = df_pandas[col].astype(str)
            df_pandas[col] = df_pandas[col].replace(['nan', 'None', ''], None)
            df_pandas.loc[df_pandas[col].notna(), col] = df_pandas.loc[df_pandas[col].notna(), col].str.title()
    
    print(f"Cleaned fundamental data: {len(df_pandas)} records")
    return df_pandas


def write_to_bronze_fundamentals(spark, df_pandas, output_path, mode="overwrite"):
    """
    Write the cleaned fundamental data to Delta Lake in the Bronze layer.
    
    Args:
        spark: SparkSession
        df_pandas: Cleaned pandas DataFrame
        output_path: Path to write the Delta table
        mode: Write mode ('append', 'overwrite', 'ignore')
    """
    if df_pandas.empty:
        print("Warning: No fundamental data to write to Bronze layer")
        return
    
    # Get the schema
    schema = get_bronze_fundamentals_schema()
    
    try:
        # Create Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas, schema=schema)
        
        # Write to Delta Lake
        df_spark.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .save(output_path)
            
        print(f"Successfully wrote {df_spark.count()} fundamental records to {output_path}")
        
    except Exception as e:
        print(f"Error writing fundamental data to Bronze layer: {e}")
        raise


def run_fundamentals_ingestion(spark, tickers, output_path, mode="overwrite", delay_between_requests=0.1):
    """
    Main function to run the complete fundamental data ingestion pipeline.
    
    Args:
        spark: SparkSession
        tickers: List of stock symbols to ingest
        output_path: Path to store the Bronze layer Delta table
        mode: Write mode for Delta table (typically 'overwrite' for fundamentals)
        delay_between_requests: Delay between API requests to avoid rate limiting
    
    Returns:
        str: Job ID for tracking
    """
    job_id = f"fundamentals_ingestion_{uuid.uuid4().hex[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Starting fundamental data ingestion job: {job_id}")
    print(f"Tickers: {tickers}")
    print(f"Output path: {output_path}")
    
    try:
        # Fetch data from yfinance
        print("Fetching fundamental data from Yahoo Finance...")
        df_pandas = fetch_fundamental_data_from_yfinance(tickers, delay_between_requests)
        
        if df_pandas.empty:
            print("No fundamental data fetched from Yahoo Finance")
            return job_id
        
        # Clean and standardize data
        print("Cleaning and standardizing fundamental data...")
        df_cleaned = clean_and_standardize_fundamental_data(df_pandas, job_id)
        
        # Write to Bronze layer
        print("Writing fundamental data to Bronze layer...")
        write_to_bronze_fundamentals(spark, df_cleaned, output_path, mode)
        
        print(f"Fundamental data ingestion job {job_id} completed successfully")
        return job_id
        
    except Exception as e:
        print(f"Fundamental data ingestion job {job_id} failed: {e}")
        raise


# Example usage and testing
if __name__ == "__main__":
    # This section is for testing the module independently
    from config import DEFAULT_TICKERS, BRONZE_FUNDAMENTALS_PATH
    
    # Create Spark session
    spark = create_spark_session()
    
    # Test with a small subset of tickers
    test_tickers = DEFAULT_TICKERS[:5]  # First 5 tickers for testing
    
    try:
        job_id = run_fundamentals_ingestion(
            spark=spark,
            tickers=test_tickers,
            output_path=BRONZE_FUNDAMENTALS_PATH,
            mode="overwrite",  # Use overwrite for testing
            delay_between_requests=0.2  # Slightly longer delay for testing
        )
        
        # Verify the data was written correctly
        df_result = spark.read.format("delta").load(BRONZE_FUNDAMENTALS_PATH)
        print(f"\nVerification: Bronze fundamentals table contains {df_result.count()} records")
        print("Sample data:")
        df_result.show(5, truncate=False)
        
        # Show summary statistics
        print("\nSummary of fundamental data:")
        df_result.select("ticker", "sector", "pe_ratio", "beta").describe().show()
        
    except Exception as e:
        print(f"Test execution failed: {e}")
    finally:
        spark.stop()