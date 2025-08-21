"""
Configuration file for the Databricks Medallion Stock Prediction Pipeline.
Contains all configurable parameters for data sources, paths, and model settings.
"""

# Data Configuration
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "V", "JPM", "JNJ",
    "UNH", "WMT", "PG", "MA", "HD", "BAC", "ABBV", "KO", "PFE", "TMO"
]

# Date ranges for data collection
DEFAULT_START_DATE = "2020-01-01"
DEFAULT_END_DATE = None  # None means current date

# Data Storage Paths (Delta Lake format)
BRONZE_PRICES_PATH = "/tmp/bronze_prices_raw"
BRONZE_FUNDAMENTALS_PATH = "/tmp/bronze_fundamentals_raw" 
SILVER_FACTORS_PATH = "/tmp/silver_factors_unified"
GOLD_PREDICTIONS_PATH = "/tmp/gold_predictions"
MODEL_PATH = "/tmp/models/return_prediction_model"

# Technical Analysis Parameters
EMA_SHORT_PERIOD = 10
EMA_LONG_PERIOD = 50
BOLLINGER_PERIOD = 20
BOLLINGER_STD_DEV = 2
RSI_PERIOD = 14

# Cross-sectional Analysis Parameters
MIN_STOCKS_FOR_CROSS_SECTIONAL = 10  # Minimum stocks needed for cross-sectional analysis

# Model Parameters
TRAIN_TEST_SPLIT = 0.7
MODEL_MAX_ITER = 10
MODEL_FEATURES = [
    "ma_distance", 
    "ma_slope_10d", 
    "bollinger_position",
    "rsi_14d", 
    "cross_sectional_z_score", 
    "relative_pe_ratio"
]

# Pipeline Configuration
BATCH_SIZE = 1000
JOB_TIMEOUT_MINUTES = 30

# Databricks Configuration (for production deployment)
DATABRICKS_CLUSTER_CONFIG = {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
}

# Logging Configuration
LOG_LEVEL = "INFO"
ENABLE_DETAILED_LOGGING = False