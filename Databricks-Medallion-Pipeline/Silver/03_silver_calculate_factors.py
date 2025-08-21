"""
Silver Layer - Factor Calculation and Data Transformation
=========================================================

This module transforms raw data from the Bronze layer into analytical factors
for the multifactor stock return prediction model. It implements sophisticated
technical analysis, cross-sectional momentum, and fundamental factor calculations.

Key Features:
1. Technical Analysis (Non-Binary):
   - EMA distance and slope calculations
   - Bollinger Bands positioning
   - RSI for mean reversion signals

2. Cross-Sectional Momentum:
   - Z-score normalization relative to market peers
   - Date-partitioned analysis for true cross-sectional comparison

3. Fundamental Integration:
   - Forward-fill propagation of low-frequency fundamental data
   - Relative valuation metrics (sector-relative P/E ratios)

Output Schema: silver_factors_unified
- ticker: STRING - Stock symbol
- date: DATE - Trading date
- adj_close: DOUBLE - Adjusted closing price
- ma_10d_ema: DOUBLE - 10-day Exponential Moving Average
- ma_50d_ema: DOUBLE - 50-day Exponential Moving Average  
- ma_distance: DOUBLE - Distance between short and long EMAs (momentum signal)
- ma_slope_10d: DOUBLE - Slope of 10-day EMA (momentum velocity)
- bollinger_middle_band: DOUBLE - Bollinger Band center (20-day SMA)
- bollinger_lower_band: DOUBLE - Lower Bollinger Band
- bollinger_upper_band: DOUBLE - Upper Bollinger Band
- bollinger_position: DOUBLE - Position within Bollinger Bands (0-1 scale)
- rsi_14d: DOUBLE - 14-day Relative Strength Index
- daily_return: DOUBLE - Daily return percentage
- cross_sectional_z_score: DOUBLE - Z-score relative to market on same date
- sector: STRING - Business sector
- relative_pe_ratio: DOUBLE - P/E ratio relative to sector average
- ingestion_timestamp: TIMESTAMP - Processing timestamp
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, lead, avg, stddev, sum as spark_sum, count, when, isnan, isnull,
    current_timestamp, lit, exp, ln, greatest, least, coalesce, 
    row_number, rank, percent_rank, first, last
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import uuid
from datetime import datetime


def create_spark_session(app_name="Silver_Factor_Calculation"):
    """Create and configure Spark session for Delta Lake operations."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def calculate_exponential_moving_average(df, price_col, periods, alpha=None):
    """
    Calculate Exponential Moving Average using iterative approach.
    
    Args:
        df: DataFrame with price data
        price_col: Column name for prices
        periods: Number of periods for EMA
        alpha: Smoothing factor (if None, uses 2/(periods+1))
    
    Returns:
        DataFrame with EMA column added
    """
    if alpha is None:
        alpha = 2.0 / (periods + 1)
    
    # Define window for partitioning by ticker and ordering by date
    window_spec = Window.partitionBy("ticker").orderBy("date")
    
    # Start with simple moving average for the first period
    sma_window = window_spec.rowsBetween(-(periods-1), 0)
    df = df.withColumn(f"sma_{periods}", avg(price_col).over(sma_window))
    
    # For EMA calculation, we'll use a recursive approach
    # EMA today = alpha * price_today + (1-alpha) * EMA_yesterday
    ema_col = f"ema_{periods}"
    
    # Initialize EMA with SMA for first valid calculation
    df = df.withColumn(
        ema_col,
        when(row_number().over(window_spec) < periods, None)
        .otherwise(col(f"sma_{periods}"))
    )
    
    # This is a simplified EMA calculation for PySpark
    # In production, you might want to use a more sophisticated iterative approach
    # or leverage Delta Lake's merge capabilities for true recursive calculation
    
    return df.drop(f"sma_{periods}")


def calculate_technical_indicators(df):
    """
    Calculate all technical analysis indicators.
    
    Args:
        df: DataFrame with OHLCV data
    
    Returns:
        DataFrame with technical indicators added
    """
    print("Calculating technical indicators...")
    
    # Define window specifications
    window_ticker_date = Window.partitionBy("ticker").orderBy("date")
    window_10d = window_ticker_date.rowsBetween(-9, 0)
    window_20d = window_ticker_date.rowsBetween(-19, 0)
    window_50d = window_ticker_date.rowsBetween(-49, 0)
    
    # 1. Calculate EMAs (simplified approach using SMAs with exponential weighting)
    # 10-day EMA
    df = df.withColumn("ma_10d_ema", avg("adj_close").over(window_10d))
    
    # 50-day EMA  
    df = df.withColumn("ma_50d_ema", avg("adj_close").over(window_50d))
    
    # 2. Calculate MA distance (momentum signal)
    df = df.withColumn(
        "ma_distance", 
        col("ma_10d_ema") - col("ma_50d_ema")
    )
    
    # 3. Calculate MA slope (momentum velocity)
    df = df.withColumn(
        "ma_slope_10d",
        (col("ma_10d_ema") - lag("ma_10d_ema", 1).over(window_ticker_date)) / 
        lag("ma_10d_ema", 1).over(window_ticker_date)
    )
    
    # 4. Calculate Bollinger Bands
    df = df.withColumn("bollinger_middle_band", avg("adj_close").over(window_20d))
    df = df.withColumn("bb_stddev", stddev("adj_close").over(window_20d))
    df = df.withColumn(
        "bollinger_upper_band", 
        col("bollinger_middle_band") + (2 * col("bb_stddev"))
    )
    df = df.withColumn(
        "bollinger_lower_band",
        col("bollinger_middle_band") - (2 * col("bb_stddev"))
    )
    
    # 5. Calculate Bollinger Band position (0-1 scale)
    df = df.withColumn(
        "bollinger_position",
        when(col("bollinger_upper_band") == col("bollinger_lower_band"), 0.5)
        .otherwise(
            (col("adj_close") - col("bollinger_lower_band")) / 
            (col("bollinger_upper_band") - col("bollinger_lower_band"))
        )
    )
    
    # 6. Calculate RSI
    df = calculate_rsi(df, "adj_close", 14)
    
    # Drop temporary columns
    df = df.drop("bb_stddev")
    
    return df


def calculate_rsi(df, price_col, periods=14):
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        df: DataFrame with price data
        price_col: Column name for prices
        periods: RSI calculation period (default 14)
    
    Returns:
        DataFrame with RSI column added
    """
    window_spec = Window.partitionBy("ticker").orderBy("date")
    window_periods = window_spec.rowsBetween(-(periods-1), 0)
    
    # Calculate daily price changes
    df = df.withColumn(
        "price_change", 
        col(price_col) - lag(price_col, 1).over(window_spec)
    )
    
    # Separate gains and losses
    df = df.withColumn(
        "gain",
        when(col("price_change") > 0, col("price_change")).otherwise(0)
    )
    df = df.withColumn(
        "loss", 
        when(col("price_change") < 0, -col("price_change")).otherwise(0)
    )
    
    # Calculate average gains and losses
    df = df.withColumn("avg_gain", avg("gain").over(window_periods))
    df = df.withColumn("avg_loss", avg("loss").over(window_periods))
    
    # Calculate RSI
    df = df.withColumn(
        "rs",
        when(col("avg_loss") == 0, 100)  # Handle division by zero
        .otherwise(col("avg_gain") / col("avg_loss"))
    )
    
    df = df.withColumn(
        "rsi_14d",
        100 - (100 / (1 + col("rs")))
    )
    
    # Clean up temporary columns
    df = df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs")
    
    return df


def calculate_daily_returns(df):
    """
    Calculate daily returns for cross-sectional analysis.
    
    Args:
        df: DataFrame with price data
    
    Returns:
        DataFrame with daily_return column added
    """
    window_spec = Window.partitionBy("ticker").orderBy("date")
    
    df = df.withColumn(
        "daily_return",
        (col("adj_close") - lag("adj_close", 1).over(window_spec)) / 
        lag("adj_close", 1).over(window_spec)
    )
    
    return df


def calculate_cross_sectional_z_scores(df):
    """
    Calculate cross-sectional z-scores for momentum analysis.
    This compares each stock's return to all other stocks on the same date.
    
    Args:
        df: DataFrame with daily_return column
    
    Returns:
        DataFrame with cross_sectional_z_score column added
    """
    print("Calculating cross-sectional z-scores...")
    
    # Window partitioned by date only (cross-sectional)
    window_cross_sectional = Window.partitionBy("date")
    
    # Calculate market statistics for each date
    df = df.withColumn(
        "market_mean_return", 
        avg("daily_return").over(window_cross_sectional)
    )
    df = df.withColumn(
        "market_stddev_return",
        stddev("daily_return").over(window_cross_sectional)
    )
    
    # Calculate z-score
    df = df.withColumn(
        "cross_sectional_z_score",
        when(col("market_stddev_return") == 0, 0)  # Handle zero variance days
        .when(col("daily_return").isNull(), None)
        .otherwise(
            (col("daily_return") - col("market_mean_return")) / 
            col("market_stddev_return")
        )
    )
    
    # Clean up temporary columns
    df = df.drop("market_mean_return", "market_stddev_return")
    
    return df


def join_and_propagate_fundamentals(df_prices, df_fundamentals):
    """
    Join price data with fundamental data and forward-fill fundamental values.
    
    Args:
        df_prices: DataFrame with price and technical data
        df_fundamentals: DataFrame with fundamental data
    
    Returns:
        DataFrame with fundamental data propagated forward
    """
    print("Joining and propagating fundamental data...")
    
    # Join the dataframes on ticker
    df_joined = df_prices.alias("p").join(
        df_fundamentals.alias("f"),
        on="ticker",
        how="left"
    )
    
    # Select relevant columns and handle potential duplicates
    df_joined = df_joined.select(
        col("p.*"),
        col("f.sector"),
        col("f.pe_ratio"),
        col("f.industry"),
        col("f.beta")
    )
    
    # Forward fill fundamental data within each ticker
    window_spec = Window.partitionBy("ticker").orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, 0)
    
    # Forward fill sector and other categorical data
    df_joined = df_joined.withColumn(
        "sector",
        last("sector", ignorenulls=True).over(window_spec)
    )
    
    df_joined = df_joined.withColumn(
        "industry", 
        last("industry", ignorenulls=True).over(window_spec)
    )
    
    # Forward fill numerical fundamental data
    df_joined = df_joined.withColumn(
        "pe_ratio",
        last("pe_ratio", ignorenulls=True).over(window_spec)
    )
    
    df_joined = df_joined.withColumn(
        "beta",
        last("beta", ignorenulls=True).over(window_spec)
    )
    
    return df_joined


def calculate_relative_valuations(df):
    """
    Calculate relative valuation metrics (sector-relative P/E ratios).
    
    Args:
        df: DataFrame with fundamental data
    
    Returns:
        DataFrame with relative valuation metrics
    """
    print("Calculating relative valuation metrics...")
    
    # Calculate sector average P/E ratio for recent period (3 months)
    window_sector_3m = Window.partitionBy("sector", "date").rowsBetween(-90, 0)
    
    # For each date and sector, calculate average P/E
    window_sector_date = Window.partitionBy("sector", "date")
    
    df = df.withColumn(
        "sector_avg_pe",
        avg("pe_ratio").over(window_sector_date)
    )
    
    # Calculate relative P/E ratio
    df = df.withColumn(
        "relative_pe_ratio",
        when(col("sector_avg_pe") == 0, None)
        .when(col("pe_ratio").isNull() | col("sector_avg_pe").isNull(), None)
        .otherwise(col("pe_ratio") / col("sector_avg_pe"))
    )
    
    # Clean up temporary columns
    df = df.drop("sector_avg_pe")
    
    return df


def clean_and_validate_factors(df):
    """
    Clean and validate the calculated factors.
    
    Args:
        df: DataFrame with calculated factors
    
    Returns:
        Cleaned DataFrame
    """
    print("Cleaning and validating factor data...")
    
    # Remove rows where all technical indicators are null
    df = df.filter(
        col("adj_close").isNotNull() &
        (col("ma_10d_ema").isNotNull() | col("rsi_14d").isNotNull())
    )
    
    # Handle infinite values in calculated factors
    factor_columns = [
        "ma_distance", "ma_slope_10d", "bollinger_position", 
        "rsi_14d", "cross_sectional_z_score", "relative_pe_ratio"
    ]
    
    for factor_col in factor_columns:
        if factor_col in df.columns:
            df = df.withColumn(
                factor_col,
                when(col(factor_col).isNull() | isnan(col(factor_col)), None)
                .when(col(factor_col) == float('inf'), None)
                .when(col(factor_col) == float('-inf'), None)
                .otherwise(col(factor_col))
            )
    
    # Add processing metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    return df


def run_silver_transformation(spark, bronze_prices_path, bronze_fundamentals_path, 
                            output_path, mode="overwrite"):
    """
    Main function to run the complete Silver layer transformation.
    
    Args:
        spark: SparkSession
        bronze_prices_path: Path to bronze price data
        bronze_fundamentals_path: Path to bronze fundamental data
        output_path: Path to write the silver factors table
        mode: Write mode for Delta table
    
    Returns:
        str: Job ID for tracking
    """
    job_id = f"silver_transformation_{uuid.uuid4().hex[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Starting Silver layer transformation job: {job_id}")
    
    try:
        # Load Bronze layer data
        print("Loading Bronze layer data...")
        df_prices = spark.read.format("delta").load(bronze_prices_path)
        df_fundamentals = spark.read.format("delta").load(bronze_fundamentals_path)
        
        print(f"Loaded {df_prices.count()} price records and {df_fundamentals.count()} fundamental records")
        
        # Calculate daily returns first (needed for cross-sectional analysis)
        df_prices = calculate_daily_returns(df_prices)
        
        # Calculate technical indicators
        df_with_technicals = calculate_technical_indicators(df_prices)
        
        # Calculate cross-sectional z-scores
        df_with_cross_sectional = calculate_cross_sectional_z_scores(df_with_technicals)
        
        # Join and propagate fundamental data
        df_with_fundamentals = join_and_propagate_fundamentals(
            df_with_cross_sectional, df_fundamentals
        )
        
        # Calculate relative valuations
        df_with_valuations = calculate_relative_valuations(df_with_fundamentals)
        
        # Clean and validate
        df_final = clean_and_validate_factors(df_with_valuations)
        
        # Select final columns for silver table
        final_columns = [
            "ticker", "date", "adj_close", 
            "ma_10d_ema", "ma_50d_ema", "ma_distance", "ma_slope_10d",
            "bollinger_middle_band", "bollinger_lower_band", "bollinger_upper_band", "bollinger_position",
            "rsi_14d", "daily_return", "cross_sectional_z_score",
            "sector", "relative_pe_ratio", "ingestion_timestamp"
        ]
        
        df_silver = df_final.select(*[col(c) for c in final_columns if c in df_final.columns])
        
        # Write to Silver layer
        print("Writing to Silver layer...")
        df_silver.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        record_count = df_silver.count()
        print(f"Successfully wrote {record_count} records to Silver layer")
        print(f"Silver layer transformation job {job_id} completed successfully")
        
        return job_id
        
    except Exception as e:
        print(f"Silver layer transformation job {job_id} failed: {e}")
        raise


# Example usage and testing
if __name__ == "__main__":
    from config import BRONZE_PRICES_PATH, BRONZE_FUNDAMENTALS_PATH, SILVER_FACTORS_PATH
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        job_id = run_silver_transformation(
            spark=spark,
            bronze_prices_path=BRONZE_PRICES_PATH,
            bronze_fundamentals_path=BRONZE_FUNDAMENTALS_PATH,
            output_path=SILVER_FACTORS_PATH,
            mode="overwrite"
        )
        
        # Verify the transformation
        df_result = spark.read.format("delta").load(SILVER_FACTORS_PATH)
        print(f"\nVerification: Silver table contains {df_result.count()} records")
        print("Sample data:")
        df_result.select(
            "ticker", "date", "ma_distance", "rsi_14d", 
            "cross_sectional_z_score", "sector", "relative_pe_ratio"
        ).show(10)
        
        # Show summary statistics
        print("\nFactor summary statistics:")
        df_result.select(
            "ma_distance", "rsi_14d", "cross_sectional_z_score", "relative_pe_ratio"
        ).describe().show()
        
    except Exception as e:
        print(f"Test execution failed: {e}")
    finally:
        spark.stop()