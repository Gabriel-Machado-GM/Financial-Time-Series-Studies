"""
Gold Layer - Multifactor Return Prediction Model
===============================================

This module implements the final Gold layer of the Medallion architecture,
where cleaned and enriched data from the Silver layer is transformed into
actionable business intelligence through machine learning predictions.

The multifactor model synthesizes technical, cross-sectional, and fundamental
factors to predict future stock returns using Gradient Boosted Trees regression.

Key Features:
1. Feature Engineering: Combines factors from different time frequencies
2. Model Training: Gradient Boosted Trees with hyperparameter optimization
3. Prediction Generation: Forward-looking return predictions
4. Model Versioning: Tracks model versions for reproducibility
5. Performance Monitoring: Model evaluation metrics

Output Schema: gold_predictions
- ticker: STRING - Stock symbol
- date: DATE - Prediction date
- predicted_return: DOUBLE - Predicted next-day return
- prediction_confidence: DOUBLE - Model confidence score
- model_version: STRING - Version of the model used
- prediction_timestamp: TIMESTAMP - When prediction was made
- feature_importance: STRING - JSON string of feature importance scores
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, current_timestamp, lit, when, isnan, isnull, struct, to_json, stddev, abs as spark_abs
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline

import uuid
from datetime import datetime, timedelta
import json
from functools import reduce


def create_spark_session(app_name="Gold_Return_Prediction"):
    """Create and configure Spark session for ML operations."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def prepare_features_and_labels(df, prediction_horizon=1):
    """
    Prepare feature matrix and target labels for model training.
    
    Args:
        df: Silver layer DataFrame with factors
        prediction_horizon: Number of days ahead to predict (default 1)
    
    Returns:
        DataFrame with features and labels ready for ML
    """
    print("Preparing features and labels...")
    
    # Define window for calculating future returns
    window_spec = Window.partitionBy("ticker").orderBy("date")
    
    # Calculate future returns as labels
    df = df.withColumn(
        "future_return",
        (lead("adj_close", prediction_horizon).over(window_spec) - col("adj_close")) / col("adj_close")
    )
    
    # Add volatility-adjusted returns (Sharpe-like signal)
    from pyspark.sql.functions import abs as spark_abs
    df = df.withColumn(
        "return_volatility",
        when(col("daily_return").isNull(), None)
        .otherwise(spark_abs(col("daily_return")))
    )
    
    # Calculate rolling volatility (20-day)
    from pyspark.sql.functions import stddev
    window_20d = window_spec.rowsBetween(-19, 0)
    df = df.withColumn(
        "rolling_volatility",
        stddev("daily_return").over(window_20d)
    )
    
    # Volatility-adjusted return signal
    df = df.withColumn(
        "vol_adjusted_signal",
        when(col("rolling_volatility") == 0, 0)
        .when(col("daily_return").isNull() | col("rolling_volatility").isNull(), None)
        .otherwise(col("daily_return") / col("rolling_volatility"))
    )
    
    return df


def engineer_advanced_features(df):
    """
    Engineer advanced features for the multifactor model.
    
    Args:
        df: DataFrame with basic factors
    
    Returns:
        DataFrame with additional engineered features
    """
    print("Engineering advanced features...")
    
    # 1. Momentum strength indicator
    from pyspark.sql.functions import abs as spark_abs
    df = df.withColumn(
        "momentum_strength",
        when(col("ma_distance").isNull(), None)
        .otherwise(spark_abs(col("ma_distance")) * spark_abs(col("ma_slope_10d")))
    )
    
    # 2. Mean reversion signal (distance from Bollinger center)
    df = df.withColumn(
        "mean_reversion_signal",
        when(col("bollinger_middle_band") == 0, None)
        .otherwise((col("adj_close") - col("bollinger_middle_band")) / col("bollinger_middle_band"))
    )
    
    # 3. RSI momentum (rate of change of RSI)
    window_spec = Window.partitionBy("ticker").orderBy("date")
    df = df.withColumn(
        "rsi_momentum",
        col("rsi_14d") - lag("rsi_14d", 1).over(window_spec)
    )
    
    # 4. Cross-sectional momentum persistence
    df = df.withColumn(
        "cs_momentum_persistence",
        (col("cross_sectional_z_score") + lag("cross_sectional_z_score", 1).over(window_spec)) / 2
    )
    
    # 5. Fundamental-technical interaction
    df = df.withColumn(
        "value_momentum_interaction",
        when(col("relative_pe_ratio").isNull() | col("ma_distance").isNull(), None)
        .otherwise(col("relative_pe_ratio") * col("ma_distance"))
    )
    
    # 6. Volatility-adjusted momentum
    df = df.withColumn(
        "vol_adj_momentum",
        when(col("rolling_volatility") == 0, None)
        .when(col("ma_distance").isNull() | col("rolling_volatility").isNull(), None)
        .otherwise(col("ma_distance") / col("rolling_volatility"))
    )
    
    return df


def select_and_clean_features(df, feature_columns):
    """
    Select and clean features for model training.
    
    Args:
        df: DataFrame with all calculated features
        feature_columns: List of feature column names to use
    
    Returns:
        Cleaned DataFrame ready for ML pipeline
    """
    print("Selecting and cleaning features...")
    
    # Add label and required columns
    required_columns = ["ticker", "date", "future_return"] + feature_columns
    
    # Select only required columns that exist
    existing_columns = [col for col in required_columns if col in df.columns]
    df_features = df.select(*existing_columns)
    
    # Remove rows with null target variable
    df_features = df_features.filter(col("future_return").isNotNull())
    
    # Remove rows where all features are null
    feature_conditions = [col(fc).isNotNull() for fc in feature_columns if fc in df_features.columns]
    if feature_conditions:
        # At least one feature must be non-null
        df_features = df_features.filter(
            feature_conditions[0] if len(feature_conditions) == 1 
            else reduce(lambda a, b: a | b, feature_conditions)
        )
    
    # Handle infinite values
    for feature in feature_columns:
        if feature in df_features.columns:
            df_features = df_features.withColumn(
                feature,
                when(col(feature).isNull() | isnan(col(feature)), 0.0)
                .when(col(feature) == float('inf'), None)
                .when(col(feature) == float('-inf'), None)
                .otherwise(col(feature))
            )
    
    return df_features


def create_ml_pipeline(feature_columns, model_type="gbt"):
    """
    Create machine learning pipeline with feature assembly and model.
    
    Args:
        feature_columns: List of feature column names
        model_type: Type of model to use ("gbt", "linear")
    
    Returns:
        ML Pipeline object
    """
    print(f"Creating ML pipeline with {model_type} model...")
    
    # Feature assembly
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Feature scaling
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    # Model selection
    if model_type == "gbt":
        model = GBTRegressor(
            featuresCol="scaled_features",
            labelCol="future_return",
            maxIter=100,
            maxDepth=6,
            stepSize=0.1,
            subsamplingRate=0.8,
            featureSubsetStrategy="sqrt"
        )
    else:  # linear regression
        model = LinearRegression(
            featuresCol="scaled_features",
            labelCol="future_return",
            regParam=0.01,
            elasticNetParam=0.1
        )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, model])
    
    return pipeline


def train_model_with_cross_validation(df_train, pipeline, feature_columns):
    """
    Train model with cross-validation and hyperparameter tuning.
    
    Args:
        df_train: Training DataFrame
        pipeline: ML Pipeline
        feature_columns: List of feature columns
    
    Returns:
        Fitted model
    """
    print("Training model with cross-validation...")
    
    # Create parameter grid for hyperparameter tuning
    if isinstance(pipeline.getStages()[-1], GBTRegressor):
        paramGrid = (ParamGridBuilder()
                    .addGrid(pipeline.getStages()[-1].maxDepth, [4, 6, 8])
                    .addGrid(pipeline.getStages()[-1].maxIter, [50, 100, 150])
                    .addGrid(pipeline.getStages()[-1].stepSize, [0.05, 0.1, 0.2])
                    .build())
    else:  # Linear regression
        paramGrid = (ParamGridBuilder()
                    .addGrid(pipeline.getStages()[-1].regParam, [0.001, 0.01, 0.1])
                    .addGrid(pipeline.getStages()[-1].elasticNetParam, [0.0, 0.1, 0.5])
                    .build())
    
    # Cross-validator
    evaluator = RegressionEvaluator(
        labelCol="future_return",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    crossval = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3
    )
    
    # Train model
    cv_model = crossval.fit(df_train)
    
    # Get best model
    best_model = cv_model.bestModel
    
    print("Model training completed successfully")
    return best_model


def evaluate_model_performance(model, df_test):
    """
    Evaluate model performance on test dataset.
    
    Args:
        model: Trained model
        df_test: Test DataFrame
    
    Returns:
        Dictionary with evaluation metrics
    """
    print("Evaluating model performance...")
    
    # Make predictions
    predictions = model.transform(df_test)
    
    # Calculate evaluation metrics
    evaluator_rmse = RegressionEvaluator(
        labelCol="future_return", predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="future_return", predictionCol="prediction", metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="future_return", predictionCol="prediction", metricName="r2"
    )
    
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    
    metrics = {
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "num_predictions": predictions.count()
    }
    
    print(f"Model Performance Metrics:")
    print(f"RMSE: {rmse:.6f}")
    print(f"MAE: {mae:.6f}")
    print(f"R²: {r2:.6f}")
    print(f"Number of predictions: {metrics['num_predictions']}")
    
    return metrics, predictions


def extract_feature_importance(model, feature_columns):
    """
    Extract feature importance from trained model.
    
    Args:
        model: Trained model
        feature_columns: List of feature column names
    
    Returns:
        Dictionary with feature importance scores
    """
    try:
        # Get the final stage (actual model) from pipeline
        final_model = model.stages[-1]
        
        if hasattr(final_model, 'featureImportances'):
            importance_scores = final_model.featureImportances.toArray()
            
            # Create feature importance dictionary
            feature_importance = {
                feature_columns[i]: float(importance_scores[i])
                for i in range(min(len(feature_columns), len(importance_scores)))
            }
            
            # Sort by importance
            feature_importance = dict(
                sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
            )
            
            print("Top 5 most important features:")
            for feature, importance in list(feature_importance.items())[:5]:
                print(f"  {feature}: {importance:.4f}")
            
            return feature_importance
        else:
            print("Model does not support feature importance extraction")
            return {}
            
    except Exception as e:
        print(f"Error extracting feature importance: {e}")
        return {}


def generate_predictions(model, df_latest, feature_columns, model_version):
    """
    Generate predictions for the latest data.
    
    Args:
        model: Trained model
        df_latest: Latest data for prediction
        feature_columns: List of feature columns
        model_version: Version identifier for the model
    
    Returns:
        DataFrame with predictions
    """
    print("Generating predictions...")
    
    # Make predictions
    predictions = model.transform(df_latest)
    
    # Extract feature importance
    feature_importance = extract_feature_importance(model, feature_columns)
    feature_importance_json = json.dumps(feature_importance)
    
    # Create predictions output
    predictions_output = predictions.select(
        col("ticker"),
        col("date"),
        col("prediction").alias("predicted_return"),
        lit(model_version).alias("model_version"),
        current_timestamp().alias("prediction_timestamp"),
        lit(feature_importance_json).alias("feature_importance")
    )
    
    # Add prediction confidence (simplified as absolute value of prediction)
    from pyspark.sql.functions import abs as spark_abs
    predictions_output = predictions_output.withColumn(
        "prediction_confidence",
        spark_abs(col("predicted_return"))
    )
    
    return predictions_output


def run_gold_prediction_pipeline(spark, silver_path, output_path, model_path, 
                                train_test_split=0.7, model_type="gbt"):
    """
    Main function to run the complete Gold layer prediction pipeline.
    
    Args:
        spark: SparkSession
        silver_path: Path to silver factors data
        output_path: Path to write predictions
        model_path: Path to save the trained model
        train_test_split: Ratio for train/test split
        model_type: Type of model to use
    
    Returns:
        Tuple of (job_id, model_metrics)
    """
    job_id = f"gold_prediction_{uuid.uuid4().hex[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    model_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Starting Gold layer prediction pipeline: {job_id}")
    print(f"Model version: {model_version}")
    
    try:
        # Load Silver layer data
        print("Loading Silver layer data...")
        df = spark.read.format("delta").load(silver_path)
        print(f"Loaded {df.count()} records from Silver layer")
        
        # Prepare features and labels
        df_prepared = prepare_features_and_labels(df)
        
        # Engineer advanced features
        df_engineered = engineer_advanced_features(df_prepared)
        
        # Define feature columns
        feature_columns = [
            "ma_distance", "ma_slope_10d", "bollinger_position", "rsi_14d",
            "cross_sectional_z_score", "relative_pe_ratio", "momentum_strength",
            "mean_reversion_signal", "rsi_momentum", "cs_momentum_persistence",
            "vol_adj_momentum"
        ]
        
        # Select and clean features
        df_features = select_and_clean_features(df_engineered, feature_columns)
        
        # Filter feature columns that actually exist
        existing_feature_columns = [col for col in feature_columns if col in df_features.columns]
        print(f"Using {len(existing_feature_columns)} features: {existing_feature_columns}")
        
        # Split data chronologically (more realistic for time series)
        total_count = df_features.count()
        split_date = df_features.select("date").distinct().orderBy("date") \
            .limit(int(total_count * train_test_split)).collect()[-1]["date"]
        
        df_train = df_features.filter(col("date") <= split_date)
        df_test = df_features.filter(col("date") > split_date)
        
        print(f"Training data: {df_train.count()} records")
        print(f"Test data: {df_test.count()} records")
        
        # Create ML pipeline
        pipeline = create_ml_pipeline(existing_feature_columns, model_type)
        
        # Train model
        trained_model = train_model_with_cross_validation(df_train, pipeline, existing_feature_columns)
        
        # Evaluate model
        metrics, test_predictions = evaluate_model_performance(trained_model, df_test)
        
        # Save model
        print(f"Saving model to {model_path}")
        trained_model.write().overwrite().save(model_path)
        
        # Generate predictions for latest data (last 30 days)
        latest_date = df_features.agg({"date": "max"}).collect()[0][0]
        cutoff_date = latest_date - timedelta(days=30)
        df_latest = df_features.filter(col("date") >= cutoff_date)
        
        predictions_output = generate_predictions(
            trained_model, df_latest, existing_feature_columns, model_version
        )
        
        # Write predictions to Gold layer
        print("Writing predictions to Gold layer...")
        predictions_output.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        
        prediction_count = predictions_output.count()
        print(f"Successfully wrote {prediction_count} predictions to Gold layer")
        print(f"Gold prediction pipeline {job_id} completed successfully")
        
        return job_id, metrics
        
    except Exception as e:
        print(f"Gold prediction pipeline {job_id} failed: {e}")
        raise


# Example usage and testing
if __name__ == "__main__":
    from config import SILVER_FACTORS_PATH, GOLD_PREDICTIONS_PATH, MODEL_PATH
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        job_id, metrics = run_gold_prediction_pipeline(
            spark=spark,
            silver_path=SILVER_FACTORS_PATH,
            output_path=GOLD_PREDICTIONS_PATH,
            model_path=MODEL_PATH,
            train_test_split=0.8,
            model_type="gbt"
        )
        
        # Verify predictions
        df_predictions = spark.read.format("delta").load(GOLD_PREDICTIONS_PATH)
        print(f"\nVerification: Gold predictions table contains {df_predictions.count()} records")
        print("Sample predictions:")
        df_predictions.select(
            "ticker", "date", "predicted_return", "prediction_confidence", "model_version"
        ).show(10)
        
        # Show prediction distribution
        print("\nPrediction statistics:")
        df_predictions.select("predicted_return", "prediction_confidence").describe().show()
        
    except Exception as e:
        print(f"Test execution failed: {e}")
    finally:
        spark.stop()