"""
Simple Test Script for Databricks Medallion Pipeline
===================================================

This script provides a simplified way to test the pipeline components
without complex import dependencies. It can be run directly to validate
that the basic structure works.
"""

import os
import sys
from datetime import datetime

def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")
    
    try:
        # Test basic imports that should work
        import pandas as pd
        print("✅ pandas imported successfully")
        
        import yfinance as yf
        print("✅ yfinance imported successfully")
        
        # Test if we can create a simple yfinance request
        ticker = yf.Ticker("AAPL")
        print("✅ yfinance Ticker object created successfully")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"⚠️  Warning during import test: {e}")
    
    return True


def test_data_fetch():
    """Test basic data fetching capability."""
    print("\nTesting basic data fetch...")
    
    try:
        import yfinance as yf
        import pandas as pd
        
        # Test fetching a small amount of data
        ticker = "AAPL"
        print(f"Fetching 5 days of data for {ticker}...")
        
        data = yf.download(ticker, period="5d", progress=False)
        
        if not data.empty:
            print(f"✅ Successfully fetched {len(data)} records")
            print(f"Columns: {list(data.columns)}")
            print(f"Date range: {data.index[0]} to {data.index[-1]}")
            return True
        else:
            print("❌ No data returned")
            return False
            
    except Exception as e:
        print(f"❌ Data fetch error: {e}")
        return False


def test_configuration():
    """Test configuration loading."""
    print("\nTesting configuration...")
    
    try:
        # Add current directory to Python path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        if parent_dir not in sys.path:
            sys.path.append(parent_dir)
        
        import config
        
        print(f"✅ Configuration loaded successfully")
        print(f"Default tickers: {config.DEFAULT_TICKERS[:5]}...")
        print(f"Start date: {config.DEFAULT_START_DATE}")
        print(f"Bronze prices path: {config.BRONZE_PRICES_PATH}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False


def test_directory_structure():
    """Test that all required directories exist."""
    print("\nTesting directory structure...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    
    required_dirs = [
        "Bronze",
        "Silver", 
        "Gold",
        "Examples"
    ]
    
    required_files = [
        "README.md",
        "requirements.txt",
        "config.py",
        "Bronze/01_bronze_ingest_prices.py",
        "Bronze/02_bronze_ingest_fundamentals.py",
        "Silver/03_silver_calculate_factors.py",
        "Gold/04_gold_predict_returns.py"
    ]
    
    all_good = True
    
    # Check directories
    for dir_name in required_dirs:
        dir_path = os.path.join(parent_dir, dir_name)
        if os.path.exists(dir_path):
            print(f"✅ Directory {dir_name} exists")
        else:
            print(f"❌ Directory {dir_name} missing")
            all_good = False
    
    # Check files
    for file_name in required_files:
        file_path = os.path.join(parent_dir, file_name)
        if os.path.exists(file_path):
            print(f"✅ File {file_name} exists")
        else:
            print(f"❌ File {file_name} missing")
            all_good = False
    
    return all_good


def test_bronze_layer_basic():
    """Test basic Bronze layer functionality without PySpark."""
    print("\nTesting Bronze layer basic functionality...")
    
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        if parent_dir not in sys.path:
            sys.path.append(parent_dir)
        
        # Test importing Bronze modules
        from Bronze.bronze_ingest_prices import fetch_price_data_from_yfinance, clean_and_standardize_price_data
        from Bronze.bronze_ingest_fundamentals import fetch_fundamental_data_from_yfinance, clean_and_standardize_fundamental_data
        
        print("✅ Bronze layer modules imported successfully")
        
        # Test basic data fetching
        test_tickers = ["AAPL"]
        start_date = "2024-01-01"
        end_date = "2024-01-05"
        
        print("Testing price data fetch...")
        df_prices = fetch_price_data_from_yfinance(test_tickers, start_date, end_date)
        
        if not df_prices.empty:
            print(f"✅ Price data fetch successful: {len(df_prices)} records")
        else:
            print("⚠️  Price data fetch returned empty DataFrame")
        
        print("Testing fundamental data fetch...")
        df_fundamentals = fetch_fundamental_data_from_yfinance(test_tickers)
        
        if not df_fundamentals.empty:
            print(f"✅ Fundamental data fetch successful: {len(df_fundamentals)} records")
        else:
            print("⚠️  Fundamental data fetch returned empty DataFrame")
        
        return True
        
    except Exception as e:
        print(f"❌ Bronze layer test error: {e}")
        return False


def main():
    """Run all tests."""
    print("🧪 Databricks Medallion Pipeline - Test Suite")
    print("=" * 50)
    
    tests = [
        ("Import Dependencies", test_imports),
        ("Data Fetch Capability", test_data_fetch),
        ("Configuration Loading", test_configuration),
        ("Directory Structure", test_directory_structure),
        ("Bronze Layer Basic", test_bronze_layer_basic)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🔍 Running: {test_name}")
        print("-" * 30)
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ Test {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The pipeline structure is ready.")
        print("\n📋 Next steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Set up PySpark/Delta Lake environment")
        print("3. Run the example pipeline: python Examples/example_pipeline.py")
    else:
        print("⚠️  Some tests failed. Please check the issues above.")
        
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)