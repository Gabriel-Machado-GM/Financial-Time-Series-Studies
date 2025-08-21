"""
Standalone Demo - Simplified Pipeline Demonstration
==================================================

This script demonstrates the core concepts of the Databricks Medallion Pipeline
using pandas (without PySpark) for educational purposes. It shows the data flow
and transformations that would occur in the full pipeline.

This is NOT the production implementation - use the PySpark versions for actual deployment.
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def demo_bronze_layer():
    """Simulate Bronze layer data ingestion."""
    print("🥉 BRONZE LAYER - Data Ingestion")
    print("=" * 40)
    
    # Demo tickers (small set for quick testing)
    tickers = ["AAPL", "MSFT", "GOOGL"]
    
    try:
        print(f"Fetching price data for {tickers}...")
        
        # Fetch price data (last 100 days to have enough for technical analysis)
        price_data = {}
        for ticker in tickers:
            try:
                data = yf.download(ticker, period="100d", progress=False)
                if not data.empty:
                    data['ticker'] = ticker
                    data['date'] = data.index
                    price_data[ticker] = data.reset_index()
                    print(f"✅ {ticker}: {len(data)} records")
                else:
                    print(f"❌ {ticker}: No data")
            except Exception as e:
                print(f"❌ {ticker}: Error - {e}")
        
        if not price_data:
            print("No price data available (likely network restrictions)")
            return None, None
        
        # Combine all ticker data
        df_prices = pd.concat([data for data in price_data.values()], ignore_index=True)
        df_prices['ingestion_timestamp'] = datetime.now()
        
        print(f"✅ Bronze prices: {len(df_prices)} total records")
        
        # Simulate fundamental data
        print("\nFetching fundamental data...")
        fundamentals = []
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                fundamentals.append({
                    'ticker': ticker,
                    'pe_ratio': info.get('trailingPE', np.nan),
                    'sector': info.get('sector', 'Unknown'),
                    'beta': info.get('beta', np.nan),
                    'ingestion_timestamp': datetime.now()
                })
                print(f"✅ {ticker}: PE={info.get('trailingPE', 'N/A')}, Sector={info.get('sector', 'Unknown')}")
            except Exception as e:
                print(f"❌ {ticker}: Error - {e}")
                fundamentals.append({
                    'ticker': ticker,
                    'pe_ratio': np.nan,
                    'sector': 'Unknown',
                    'beta': np.nan,
                    'ingestion_timestamp': datetime.now()
                })
        
        df_fundamentals = pd.DataFrame(fundamentals)
        print(f"✅ Bronze fundamentals: {len(df_fundamentals)} records")
        
        return df_prices, df_fundamentals
        
    except Exception as e:
        print(f"❌ Bronze layer failed: {e}")
        return None, None


def demo_silver_layer(df_prices, df_fundamentals):
    """Simulate Silver layer data transformation."""
    print("\n🥈 SILVER LAYER - Data Transformation")
    print("=" * 40)
    
    if df_prices is None:
        print("❌ No price data available for Silver layer")
        return None
    
    try:
        # Work with each ticker separately for calculations
        silver_data = []
        
        for ticker in df_prices['ticker'].unique():
            ticker_data = df_prices[df_prices['ticker'] == ticker].copy()
            ticker_data = ticker_data.sort_values('Date').reset_index(drop=True)
            
            print(f"\nProcessing {ticker}...")
            
            # 1. Technical Indicators
            print("  Calculating technical indicators...")
            
            # EMAs (simplified as moving averages)
            ticker_data['ma_10d'] = ticker_data['Adj Close'].rolling(10).mean()
            ticker_data['ma_50d'] = ticker_data['Adj Close'].rolling(50).mean()
            ticker_data['ma_distance'] = ticker_data['ma_10d'] - ticker_data['ma_50d']
            
            # Bollinger Bands
            ticker_data['bb_middle'] = ticker_data['Adj Close'].rolling(20).mean()
            ticker_data['bb_std'] = ticker_data['Adj Close'].rolling(20).std()
            ticker_data['bb_upper'] = ticker_data['bb_middle'] + (2 * ticker_data['bb_std'])
            ticker_data['bb_lower'] = ticker_data['bb_middle'] - (2 * ticker_data['bb_std'])
            ticker_data['bb_position'] = (ticker_data['Adj Close'] - ticker_data['bb_lower']) / (ticker_data['bb_upper'] - ticker_data['bb_lower'])
            
            # RSI (simplified)
            delta = ticker_data['Adj Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            ticker_data['rsi_14d'] = 100 - (100 / (1 + rs))
            
            # Daily returns
            ticker_data['daily_return'] = ticker_data['Adj Close'].pct_change()
            
            silver_data.append(ticker_data)
        
        # Combine all processed data
        df_silver = pd.concat(silver_data, ignore_index=True)
        
        # 2. Cross-sectional Analysis
        print("\nCalculating cross-sectional z-scores...")
        df_silver['cross_sectional_z_score'] = df_silver.groupby('Date')['daily_return'].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
        )
        
        # 3. Join with fundamentals
        print("Joining with fundamental data...")
        df_silver = df_silver.merge(df_fundamentals[['ticker', 'pe_ratio', 'sector', 'beta']], 
                                   on='ticker', how='left')
        
        # 4. Relative valuations
        df_silver['sector_avg_pe'] = df_silver.groupby('sector')['pe_ratio'].transform('mean')
        df_silver['relative_pe_ratio'] = df_silver['pe_ratio'] / df_silver['sector_avg_pe']
        
        # Select key columns for final silver table
        silver_columns = [
            'ticker', 'Date', 'Adj Close', 'ma_distance', 'bb_position', 
            'rsi_14d', 'daily_return', 'cross_sectional_z_score', 
            'sector', 'relative_pe_ratio'
        ]
        
        df_silver_final = df_silver[silver_columns].copy()
        df_silver_final['ingestion_timestamp'] = datetime.now()
        
        # Remove rows with insufficient data
        df_silver_final = df_silver_final.dropna(subset=['Adj Close', 'daily_return'])
        
        print(f"✅ Silver layer: {len(df_silver_final)} records with calculated factors")
        
        # Show sample of calculated factors
        print("\nSample factor values:")
        sample_data = df_silver_final.tail(5)[['ticker', 'Date', 'ma_distance', 'rsi_14d', 'cross_sectional_z_score']]
        print(sample_data.to_string(index=False))
        
        return df_silver_final
        
    except Exception as e:
        print(f"❌ Silver layer failed: {e}")
        return None


def demo_gold_layer(df_silver):
    """Simulate Gold layer ML prediction."""
    print("\n🥇 GOLD LAYER - ML Prediction Model")
    print("=" * 40)
    
    if df_silver is None:
        print("❌ No silver data available for Gold layer")
        return None
    
    try:
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_squared_error, r2_score
        from sklearn.preprocessing import StandardScaler
        
        print("Preparing features and labels...")
        
        # Prepare features and target
        feature_columns = ['ma_distance', 'bb_position', 'rsi_14d', 'cross_sectional_z_score', 'relative_pe_ratio']
        
        # Create future returns as target (1-day ahead)
        df_ml = df_silver.copy()
        df_ml = df_ml.sort_values(['ticker', 'Date']).reset_index(drop=True)
        df_ml['future_return'] = df_ml.groupby('ticker')['Adj Close'].shift(-1) / df_ml['Adj Close'] - 1
        
        # Remove rows with missing data
        df_ml = df_ml.dropna(subset=feature_columns + ['future_return'])
        
        if len(df_ml) < 20:
            print("❌ Insufficient data for ML model (need at least 20 samples)")
            return None
        
        X = df_ml[feature_columns]
        y = df_ml['future_return']
        
        print(f"Dataset: {len(X)} samples with {len(feature_columns)} features")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        print("Training Gradient Boosting model...")
        model = GradientBoostingRegressor(
            n_estimators=50,  # Reduced for demo
            max_depth=4,
            learning_rate=0.1,
            random_state=42
        )
        
        model.fit(X_train_scaled, y_train)
        
        # Make predictions
        y_pred = model.predict(X_test_scaled)
        
        # Calculate metrics
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        
        print(f"✅ Model Performance:")
        print(f"   RMSE: {rmse:.6f}")
        print(f"   R²: {r2:.6f}")
        print(f"   Test samples: {len(y_test)}")
        
        # Feature importance
        print(f"\n📊 Feature Importance:")
        importance = model.feature_importances_
        for feature, imp in zip(feature_columns, importance):
            print(f"   {feature:25s}: {imp:.4f}")
        
        # Generate predictions for latest data
        latest_data = df_ml.groupby('ticker').tail(1)
        if not latest_data.empty:
            X_latest = latest_data[feature_columns]
            X_latest_scaled = scaler.transform(X_latest)
            predictions = model.predict(X_latest_scaled)
            
            print(f"\n🔮 Latest Predictions:")
            for i, (ticker, pred) in enumerate(zip(latest_data['ticker'], predictions)):
                print(f"   {ticker}: {pred:+.4f} ({pred*100:+.2f}%)")
        
        # Create predictions DataFrame
        df_predictions = latest_data[['ticker', 'Date']].copy()
        df_predictions['predicted_return'] = predictions
        df_predictions['model_version'] = 'demo_v1'
        df_predictions['prediction_timestamp'] = datetime.now()
        
        return df_predictions
        
    except ImportError:
        print("❌ scikit-learn not available for ML demo")
        return None
    except Exception as e:
        print(f"❌ Gold layer failed: {e}")
        return None


def main():
    """Run the complete demo pipeline."""
    print("🚀 Databricks Medallion Pipeline - Demo")
    print("=" * 50)
    print("This demo shows the data flow and transformations")
    print("that would occur in the full PySpark implementation.\n")
    
    start_time = datetime.now()
    
    try:
        # Bronze Layer
        df_prices, df_fundamentals = demo_bronze_layer()
        
        # Silver Layer
        df_silver = demo_silver_layer(df_prices, df_fundamentals)
        
        # Gold Layer
        df_predictions = demo_gold_layer(df_silver)
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n" + "=" * 50)
        print("🎯 DEMO SUMMARY")
        print("=" * 50)
        print(f"Execution time: {duration:.2f} seconds")
        
        if df_predictions is not None:
            print(f"✅ Pipeline completed successfully!")
            print(f"   Bronze → Silver → Gold data flow demonstrated")
            print(f"   Final predictions generated for {len(df_predictions)} stocks")
        else:
            print(f"⚠️  Pipeline completed with limitations")
            print(f"   Network restrictions may have limited data access")
        
        print(f"\n📋 Next Steps:")
        print(f"   1. Deploy PySpark versions to Databricks")
        print(f"   2. Configure Delta Lake storage")
        print(f"   3. Set up automated scheduling")
        print(f"   4. Implement production monitoring")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()