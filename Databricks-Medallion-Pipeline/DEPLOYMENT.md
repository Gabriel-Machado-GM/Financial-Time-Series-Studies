# Databricks Deployment Guide

This document provides instructions for deploying the Medallion Pipeline to Databricks.

## Prerequisites

1. **Databricks Workspace**: Access to a Databricks workspace
2. **Cluster Configuration**: Cluster with PySpark 3.4+ and Delta Lake support
3. **Libraries**: Install required dependencies listed in requirements.txt

## Cluster Setup

### Recommended Cluster Configuration

```
Databricks Runtime: 13.3.x-scala2.12 (includes Apache Spark 3.4.1, Delta Lake)
Node Type: Standard_DS3_v2 (or equivalent)
Workers: 2-4 nodes (depending on data volume)
```

### Required Libraries

Install the following libraries on your cluster:

1. **PyPI Libraries**:
   - yfinance>=0.2.0
   - pandas>=1.5.0
   - numpy>=1.21.0

2. **Maven Libraries** (already included in Databricks Runtime):
   - io.delta:delta-core_2.12:2.4.0

## Deployment Steps

### 1. Upload Files to Databricks

```bash
# Create directory structure in Databricks workspace
/Workspace/Users/your-email/databricks-medallion-pipeline/
├── Bronze/
│   ├── 01_bronze_ingest_prices.py
│   └── 02_bronze_ingest_fundamentals.py
├── Silver/
│   └── 03_silver_calculate_factors.py
├── Gold/
│   └── 04_gold_predict_returns.py
├── config.py
└── Examples/
    ├── example_pipeline.py
    └── test_pipeline.py
```

### 2. Configure Storage Paths

Update the paths in `config.py` for your Databricks environment:

```python
# Data Storage Paths (Delta Lake format)
BRONZE_PRICES_PATH = "/mnt/your-storage/bronze_prices_raw"
BRONZE_FUNDAMENTALS_PATH = "/mnt/your-storage/bronze_fundamentals_raw"
SILVER_FACTORS_PATH = "/mnt/your-storage/silver_factors_unified"
GOLD_PREDICTIONS_PATH = "/mnt/your-storage/gold_predictions"
MODEL_PATH = "/mnt/your-storage/models/return_prediction_model"
```

### 3. Set Up Delta Lake Storage

If using external storage (recommended for production):

```python
# Mount Azure Blob Storage or S3
dbutils.fs.mount(
    source="abfss://your-container@your-account.dfs.core.windows.net/",
    mount_point="/mnt/your-storage",
    extra_configs={
        "fs.azure.account.auth.type.your-account.dfs.core.windows.net": "OAuth",
        "fs.azure.account.oauth.provider.type.your-account.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id.your-account.dfs.core.windows.net": "your-client-id",
        "fs.azure.account.oauth2.client.secret.your-account.dfs.core.windows.net": "your-client-secret",
        "fs.azure.account.oauth2.client.endpoint.your-account.dfs.core.windows.net": "https://login.microsoftonline.com/your-tenant-id/oauth2/token"
    }
)
```

## Running the Pipeline

### Option 1: Notebooks

Create separate notebooks for each layer:

**Bronze Layer Notebook**:
```python
%run "./Bronze/01_bronze_ingest_prices"
%run "./Bronze/02_bronze_ingest_fundamentals"

# Run ingestion
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
run_price_ingestion(spark, tickers, "2020-01-01", None, "/mnt/storage/bronze_prices")
run_fundamentals_ingestion(spark, tickers, "/mnt/storage/bronze_fundamentals")
```

**Silver Layer Notebook**:
```python
%run "./Silver/03_silver_calculate_factors"

# Run transformation
run_silver_transformation(
    spark, 
    "/mnt/storage/bronze_prices",
    "/mnt/storage/bronze_fundamentals", 
    "/mnt/storage/silver_factors"
)
```

**Gold Layer Notebook**:
```python
%run "./Gold/04_gold_predict_returns"

# Run prediction model
run_gold_prediction_pipeline(
    spark,
    "/mnt/storage/silver_factors",
    "/mnt/storage/gold_predictions",
    "/mnt/storage/models/prediction_model"
)
```

### Option 2: Jobs

Create Databricks Jobs for automated execution:

1. **Bronze Job**: Schedule daily after market close
2. **Silver Job**: Trigger after Bronze job completion
3. **Gold Job**: Trigger after Silver job completion

### Option 3: Workflows

Use Databricks Workflows for orchestration:

```yaml
name: "Stock Prediction Pipeline"
triggers:
  - schedule:
      cron: "0 22 * * 1-5"  # 10 PM Monday-Friday
      timezone: "America/New_York"

tasks:
  - task_key: "bronze_ingestion"
    notebook_path: "/path/to/bronze_notebook"
    cluster:
      existing_cluster_id: "your-cluster-id"
    
  - task_key: "silver_transformation"
    depends_on:
      - task_key: "bronze_ingestion"
    notebook_path: "/path/to/silver_notebook"
    cluster:
      existing_cluster_id: "your-cluster-id"
    
  - task_key: "gold_prediction"
    depends_on:
      - task_key: "silver_transformation"
    notebook_path: "/path/to/gold_notebook"
    cluster:
      existing_cluster_id: "your-cluster-id"
```

## Performance Optimization

### 1. Cluster Configuration

For large datasets (>1M records):
```
Node Type: Standard_DS4_v2 or Standard_DS5_v2
Workers: 4-8 nodes
Enable autoscaling: 2-8 workers
```

### 2. Spark Configuration

Add to cluster configuration:
```
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.adaptive.skewJoin.enabled true
spark.databricks.delta.preview.enabled true
spark.databricks.delta.optimizeWrite.enabled true
spark.databricks.delta.autoCompact.enabled true
```

### 3. Delta Lake Optimization

Optimize tables periodically:
```python
# Optimize Delta tables
spark.sql("OPTIMIZE delta.`/mnt/storage/bronze_prices`")
spark.sql("OPTIMIZE delta.`/mnt/storage/silver_factors`")
spark.sql("OPTIMIZE delta.`/mnt/storage/gold_predictions`")

# Z-order by frequently queried columns
spark.sql("OPTIMIZE delta.`/mnt/storage/silver_factors` ZORDER BY (ticker, date)")
```

## Monitoring and Maintenance

### 1. Data Quality Checks

Implement data quality validations:
```python
def validate_bronze_data(df):
    # Check for null values in critical columns
    assert df.filter(col("ticker").isNull()).count() == 0
    assert df.filter(col("date").isNull()).count() == 0
    assert df.filter(col("adj_close") <= 0).count() == 0

def validate_predictions(df):
    # Check prediction ranges
    assert df.filter(col("predicted_return") > 1.0).count() == 0  # >100% return
    assert df.filter(col("predicted_return") < -1.0).count() == 0  # <-100% return
```

### 2. Model Performance Monitoring

Track model metrics over time:
```python
def log_model_metrics(metrics, model_version):
    metrics_df = spark.createDataFrame([{
        "model_version": model_version,
        "rmse": metrics["rmse"],
        "mae": metrics["mae"],
        "r2": metrics["r2"],
        "timestamp": datetime.now()
    }])
    
    metrics_df.write.format("delta").mode("append").save("/mnt/storage/model_metrics")
```

### 3. Alerting

Set up alerts for:
- Data ingestion failures
- Model performance degradation
- Prediction anomalies

## Security Considerations

1. **Access Control**: Use Databricks access control to restrict access to sensitive data
2. **Secrets Management**: Store API keys and credentials in Databricks Secrets
3. **Network Security**: Configure network access rules if needed
4. **Data Encryption**: Enable encryption at rest and in transit

## Troubleshooting

### Common Issues

1. **Memory Errors**: Increase cluster size or optimize data partitioning
2. **yfinance Rate Limits**: Implement retry logic and delays
3. **Delta Lake Conflicts**: Handle concurrent writes properly
4. **Feature Drift**: Monitor feature distributions over time

### Debugging Tips

1. Enable detailed logging in cluster configuration
2. Use Spark UI to analyze job performance
3. Check Delta Lake transaction logs for write conflicts
4. Monitor cluster metrics for resource utilization