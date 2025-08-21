# Databricks Medallion Pipeline - Integrated Stock Return Prediction Model

This directory implements a comprehensive multifactor stock return prediction model using the Databricks Lakehouse architecture with the Medallion pattern (Bronze, Silver, Gold layers).

## Architecture Overview

### Bronze Layer (Data Ingestion)
- **bronze_ingest_prices.py**: Ingests historical price data (OHLCV) from yfinance
- **bronze_ingest_fundamentals.py**: Ingests fundamental data (P/E, sector, industry) from yfinance

### Silver Layer (Data Transformation)
- **silver_calculate_factors.py**: Transforms raw data into analytical factors
  - Technical indicators (EMA distance, Bollinger Bands, RSI)
  - Cross-sectional momentum analysis
  - Fundamental data propagation and relative valuations

### Gold Layer (ML Model)
- **gold_predict_returns.py**: Multifactor prediction model using Gradient Boosted Trees
  - Feature engineering and model training
  - Return predictions with model versioning

## Key Features

1. **Non-Binary Technical Analysis**: Uses continuous signals instead of binary crossovers
2. **Cross-Sectional Momentum**: Normalizes returns relative to market peers using z-scores
3. **Fundamental Integration**: Combines high-frequency technical data with low-frequency fundamental data
4. **Scalable Architecture**: Designed for both batch and streaming processing
5. **Model Versioning**: Tracks model versions for reproducibility and auditing

## Data Flow

```
Raw Data (yfinance) → Bronze Tables → Silver Factors → Gold Predictions
```

## Usage

1. Configure the pipeline parameters in `config.py`
2. Run Bronze layer scripts to ingest data
3. Execute Silver layer transformation
4. Train and apply the Gold layer prediction model

## Dependencies

- PySpark 3.4+
- yfinance
- pandas
- numpy
- scikit-learn (for compatibility with PySpark ML)

## File Structure

```
Databricks-Medallion-Pipeline/
├── README.md
├── requirements.txt
├── config.py
├── Bronze/
│   ├── 01_bronze_ingest_prices.py
│   └── 02_bronze_ingest_fundamentals.py
├── Silver/
│   └── 03_silver_calculate_factors.py
├── Gold/
│   └── 04_gold_predict_returns.py
└── Examples/
    └── example_pipeline.py
```