"""
Example Pipeline Execution
==========================

This script demonstrates how to run the complete Databricks Medallion Pipeline
for stock return prediction. It orchestrates the Bronze, Silver, and Gold layers
to create a comprehensive multifactor prediction model.

Usage:
    python example_pipeline.py [--tickers AAPL,MSFT,GOOGL] [--start-date 2020-01-01]
"""

import sys
import os
import argparse
from datetime import datetime, timedelta

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Bronze.bronze_ingest_prices import run_price_ingestion, create_spark_session as create_bronze_spark
from Bronze.bronze_ingest_fundamentals import run_fundamentals_ingestion
from Silver.silver_calculate_factors import run_silver_transformation, create_spark_session as create_silver_spark
from Gold.gold_predict_returns import run_gold_prediction_pipeline, create_spark_session as create_gold_spark
import config


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run Databricks Medallion Stock Prediction Pipeline")
    
    parser.add_argument(
        "--tickers",
        type=str,
        default=",".join(config.DEFAULT_TICKERS[:10]),  # Use first 10 for demo
        help="Comma-separated list of stock tickers"
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        default=config.DEFAULT_START_DATE,
        help="Start date for data collection (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for data collection (YYYY-MM-DD, default: current date)"
    )
    
    parser.add_argument(
        "--model-type",
        type=str,
        choices=["gbt", "linear"],
        default="gbt",
        help="Type of ML model to use"
    )
    
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip Bronze layer data ingestion (use existing data)"
    )
    
    parser.add_argument(
        "--skip-silver",
        action="store_true", 
        help="Skip Silver layer transformation (use existing factors)"
    )
    
    return parser.parse_args()


def run_bronze_layer(tickers, start_date, end_date):
    """
    Run Bronze layer data ingestion.
    
    Args:
        tickers: List of stock tickers
        start_date: Start date for data collection
        end_date: End date for data collection
    
    Returns:
        Tuple of (price_job_id, fundamentals_job_id)
    """
    print("=" * 60)
    print("BRONZE LAYER - DATA INGESTION")
    print("=" * 60)
    
    spark = create_bronze_spark("Pipeline_Bronze_Layer")
    
    try:
        # Ingest price data
        print("\nStep 1: Ingesting price data...")
        price_job_id = run_price_ingestion(
            spark=spark,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            output_path=config.BRONZE_PRICES_PATH,
            mode="overwrite"
        )
        
        # Ingest fundamental data
        print("\nStep 2: Ingesting fundamental data...")
        fundamentals_job_id = run_fundamentals_ingestion(
            spark=spark,
            tickers=tickers,
            output_path=config.BRONZE_FUNDAMENTALS_PATH,
            mode="overwrite",
            delay_between_requests=0.1
        )
        
        print("\n✅ Bronze layer completed successfully")
        return price_job_id, fundamentals_job_id
        
    except Exception as e:
        print(f"\n❌ Bronze layer failed: {e}")
        raise
    finally:
        spark.stop()


def run_silver_layer():
    """
    Run Silver layer data transformation.
    
    Returns:
        Job ID for the Silver transformation
    """
    print("\n" + "=" * 60)
    print("SILVER LAYER - DATA TRANSFORMATION") 
    print("=" * 60)
    
    spark = create_silver_spark("Pipeline_Silver_Layer")
    
    try:
        print("\nStep 3: Calculating technical and fundamental factors...")
        silver_job_id = run_silver_transformation(
            spark=spark,
            bronze_prices_path=config.BRONZE_PRICES_PATH,
            bronze_fundamentals_path=config.BRONZE_FUNDAMENTALS_PATH,
            output_path=config.SILVER_FACTORS_PATH,
            mode="overwrite"
        )
        
        print("\n✅ Silver layer completed successfully")
        return silver_job_id
        
    except Exception as e:
        print(f"\n❌ Silver layer failed: {e}")
        raise
    finally:
        spark.stop()


def run_gold_layer(model_type="gbt"):
    """
    Run Gold layer ML prediction model.
    
    Args:
        model_type: Type of ML model to use
    
    Returns:
        Tuple of (job_id, model_metrics)
    """
    print("\n" + "=" * 60)
    print("GOLD LAYER - ML PREDICTION MODEL")
    print("=" * 60)
    
    spark = create_gold_spark("Pipeline_Gold_Layer")
    
    try:
        print(f"\nStep 4: Training {model_type.upper()} model and generating predictions...")
        gold_job_id, metrics = run_gold_prediction_pipeline(
            spark=spark,
            silver_path=config.SILVER_FACTORS_PATH,
            output_path=config.GOLD_PREDICTIONS_PATH,
            model_path=config.MODEL_PATH,
            train_test_split=0.8,
            model_type=model_type
        )
        
        print("\n✅ Gold layer completed successfully")
        return gold_job_id, metrics
        
    except Exception as e:
        print(f"\n❌ Gold layer failed: {e}")
        raise
    finally:
        spark.stop()


def display_pipeline_summary(args, bronze_jobs, silver_job, gold_job, metrics, execution_time):
    """Display a summary of the pipeline execution."""
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    
    print(f"\n📊 Configuration:")
    print(f"   Tickers: {', '.join(args.tickers.split(',')[:5])}{'...' if len(args.tickers.split(',')) > 5 else ''}")
    print(f"   Date Range: {args.start_date} to {args.end_date or 'current'}")
    print(f"   Model Type: {args.model_type.upper()}")
    print(f"   Execution Time: {execution_time:.2f} seconds")
    
    print(f"\n🔗 Job IDs:")
    if bronze_jobs:
        print(f"   Bronze Prices: {bronze_jobs[0]}")
        print(f"   Bronze Fundamentals: {bronze_jobs[1]}")
    if silver_job:
        print(f"   Silver Factors: {silver_job}")
    if gold_job:
        print(f"   Gold Predictions: {gold_job}")
    
    if metrics:
        print(f"\n📈 Model Performance:")
        print(f"   RMSE: {metrics['rmse']:.6f}")
        print(f"   MAE: {metrics['mae']:.6f}")
        print(f"   R²: {metrics['r2']:.6f}")
        print(f"   Predictions: {metrics['num_predictions']}")
    
    print(f"\n💾 Output Locations:")
    print(f"   Bronze Prices: {config.BRONZE_PRICES_PATH}")
    print(f"   Bronze Fundamentals: {config.BRONZE_FUNDAMENTALS_PATH}")
    print(f"   Silver Factors: {config.SILVER_FACTORS_PATH}")
    print(f"   Gold Predictions: {config.GOLD_PREDICTIONS_PATH}")
    print(f"   Trained Model: {config.MODEL_PATH}")
    
    print(f"\n🎯 Next Steps:")
    print(f"   - Review predictions in {config.GOLD_PREDICTIONS_PATH}")
    print(f"   - Analyze feature importance from model output")
    print(f"   - Set up automated pipeline scheduling")
    print(f"   - Monitor model performance over time")


def main():
    """Main pipeline execution function."""
    start_time = datetime.now()
    
    # Parse arguments
    args = parse_arguments()
    tickers = [t.strip().upper() for t in args.tickers.split(",")]
    
    print("🚀 Starting Databricks Medallion Stock Prediction Pipeline")
    print(f"⏰ Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    bronze_jobs = None
    silver_job = None
    gold_job = None
    metrics = None
    
    try:
        # Bronze Layer - Data Ingestion
        if not args.skip_bronze:
            bronze_jobs = run_bronze_layer(tickers, args.start_date, args.end_date)
        else:
            print("\n⏭️  Skipping Bronze layer (using existing data)")
        
        # Silver Layer - Data Transformation
        if not args.skip_silver:
            silver_job = run_silver_layer()
        else:
            print("\n⏭️  Skipping Silver layer (using existing factors)")
        
        # Gold Layer - ML Prediction
        gold_job, metrics = run_gold_layer(args.model_type)
        
        # Calculate execution time
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Display summary
        display_pipeline_summary(args, bronze_jobs, silver_job, gold_job, metrics, execution_time)
        
        print(f"\n🎉 Pipeline completed successfully!")
        print(f"⏰ End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n💥 Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()