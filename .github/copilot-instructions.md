# Financial Time Series Studies

This repository contains a comprehensive portfolio of financial time series analysis applications built with Python. The codebase focuses on technical analysis, quantitative trading strategies, and econometric modeling using various Python libraries and data sources.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Environment Setup (MANDATORY - 5 minutes total)
- Install Python 3.10+ (tested with Python 3.12.3):
  - Ubuntu/Debian: `sudo apt install python3 python3-pip`
  - Check version: `python3 --version`
- Install core packages - NEVER CANCEL, allow up to 10 minutes for complete installation:
  ```bash
  # Core data science packages (2-3 minutes)
  pip install pandas numpy matplotlib seaborn scipy
  
  # Financial and time series analysis packages (5-7 minutes)
  pip install statsmodels scikit-learn yfinance arch plotly
  
  # Jupyter environment (2-3 minutes)  
  pip install jupyter notebook ipykernel
  ```
  - **TIMEOUT WARNINGS**: Set pip timeout to 300+ seconds. Network issues are common.
  - If pip fails due to network timeouts, retry individual packages: `pip install --retries 3 --timeout 60 <package>`

### Quick Validation (30 seconds)
Run this validation script to ensure environment is working:
```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Test data loading
df = pd.read_csv('Codes/Data/AirPassengers.csv')
print(f"Data loaded successfully: {df.shape}")

# Test basic time series operations
df['Month'] = pd.to_datetime(df['Month'])
rolling_mean = df['#Passengers'].rolling(12).mean()
print("✓ Environment validation successful")
```

### Running Notebooks (1-2 minutes per notebook)
- Start Jupyter: `jupyter notebook` or `jupyter lab`
- **NEVER CANCEL** notebook cells that appear to hang - time series computations can take 30+ seconds
- Navigate to:
  - `Codes/` - Main time series analysis notebooks
  - `Trading Quantitativo/` - Trading and market data analysis

### Data Sources and Requirements
- **Built-in Data**: Repository includes CSV files in `Codes/Data/` for immediate use
  - AirPassengers.csv, stock_prices.csv, macro_monthly.csv, yield_curve.csv
  - No external API keys required for basic analysis
- **MetaTrader5 Integration**: 
  - **Windows-only** - Will not work on Linux/macOS
  - Requires XP Investimentos account and MetaTrader5 installation
  - Credentials file needed: `credentials.json` with login/password/server
  - Skip MetaTrader5 notebooks if not on Windows
- **TradingView Data**: Requires credentials in separate password file
- **Yahoo Finance**: Works out-of-box with yfinance package

## Validation Scenarios

Always test these scenarios after making changes to ensure the codebase remains functional:

### Scenario 1: Basic Time Series Analysis (2 minutes)
```python
# Load and analyze Air Passengers data
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('Codes/Data/AirPassengers.csv')
df['Month'] = pd.to_datetime(df['Month'])
df.set_index('Month', inplace=True)

# Basic analysis
mean_passengers = df['#Passengers'].mean()
rolling_mean = df['#Passengers'].rolling(12).mean()

# Visualization
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['#Passengers'], label='Original')
plt.plot(df.index, rolling_mean, label='12-Month Rolling Mean')
plt.legend()
plt.title('Air Passengers Time Series Analysis')
plt.show()
```

### Scenario 2: Stock Market Analysis (1 minute)
```python
# Multi-asset correlation analysis
stock_data = pd.read_csv('Codes/Data/stock_prices.csv')
stock_data['Date'] = pd.to_datetime(stock_data['Date'])

# Calculate returns
numeric_cols = stock_data.select_dtypes(include=['float64']).columns
returns = stock_data[numeric_cols].pct_change().dropna()

# Correlation heatmap
import seaborn as sns
plt.figure(figsize=(10, 8))
sns.heatmap(returns.corr(), annot=True, cmap='coolwarm')
plt.title('Stock Index Correlations')
plt.show()
```

### Scenario 3: Alternative Analysis Without Advanced Packages
```python
# Time series decomposition using basic pandas operations
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Load temperature data
temp_df = pd.read_csv('Codes/Data/temperatures.csv')
temp_df['Date'] = pd.to_datetime(temp_df['Date'])
temp_df.set_index('Date', inplace=True)

# Basic decomposition using rolling windows
ts = temp_df['Austria Average Temperature C'].dropna()
trend = ts.rolling(window=12, center=True).mean()
seasonal = ts.groupby(ts.index.month).transform('mean')
residual = ts - trend - seasonal

# Visualization
fig, axes = plt.subplots(4, 1, figsize=(12, 10))
ts.plot(ax=axes[0], title='Original')
trend.plot(ax=axes[1], title='Trend') 
seasonal.plot(ax=axes[2], title='Seasonal')
residual.plot(ax=axes[3], title='Residual')
plt.tight_layout()
plt.show()
```

## Repository Structure

### Key Directories
- **`Codes/`** - Main analysis notebooks
  - `Time Series - VAR - Inflation.ipynb` - Vector autoregression models
  - `Time Series - LSTM.ipynb` - Deep learning for time series
  - `TS1 - ARMA, GARCH, VaR.ipynb` - Classic econometric models
  - `Data/` - CSV datasets for analysis
- **`Trading Quantitativo/`** - Trading applications
  - `Extraindo dados da bolsa com MetaTrader5/` - Brazilian market data (Windows-only)
  - `Obtencao de dados/` - General data acquisition scripts
  - `Backtesting/` - Strategy backtesting framework

### Common Analysis Workflows
1. **Time Series Decomposition**: Load data → Apply seasonal decomposition → Visualize trends
2. **ARMA/GARCH Modeling**: Data preprocessing → Stationarity tests → Model fitting → Forecasting
3. **VAR Analysis**: Multi-variate data → Lag selection → Model estimation → Impulse response
4. **LSTM Forecasting**: Data scaling → Sequence creation → Model training → Prediction

## Limitations and Workarounds

### Network Connectivity Issues
- **pip install may timeout** - Use `--retries 3 --timeout 60` flags
- If persistent failures: Download packages manually or use conda as alternative
- Document actual installation time in your validation notes

### Windows-Specific Components
- **MetaTrader5**: Only works on Windows with proper broker setup
- **Workaround**: Use yfinance or CSV data for market analysis on Linux/macOS
- Skip or comment out MetaTrader5 import statements when not available

### Missing Advanced Packages
If specialized packages fail to install (common with network timeouts):
- **statsmodels**: Use basic pandas rolling operations for trend analysis
- **arch (GARCH models)**: Focus on basic volatility analysis with pandas
- **tensorflow**: Skip LSTM notebooks, focus on classical methods  
- **plotly**: Use matplotlib for static visualizations
- **scikit-learn**: Use pandas/numpy for basic statistical operations

### Data Dependencies
- Some notebooks expect specific credential files - create dummy files or skip those sections
- Market data requires internet connection - use built-in CSV files for offline analysis

## Performance Expectations
- **Environment setup**: 5-10 minutes (including package installation)
- **Basic notebook execution**: 30 seconds - 2 minutes per notebook
- **Large dataset analysis**: 2-5 minutes (yield curve, macro data)
- **Model training (LSTM, GARCH)**: 1-10 minutes depending on data size
- **Visualization generation**: 10-30 seconds per complex plot

**NEVER CANCEL** operations that appear slow - time series analysis involves computationally intensive operations that legitimately require extended execution time.

## Troubleshooting

### Import Errors
```bash
# Check package availability
python3 -c "import pandas, numpy, matplotlib; print('Core packages OK')"

# Install missing packages individually
pip install --retries 3 package_name
```

### Jupyter Issues
```bash
# Reset Jupyter kernel if notebooks fail
jupyter kernelspec list
jupyter kernelspec remove python3
pip install --force-reinstall ipykernel
python3 -m ipykernel install --user
```

### Data Loading Errors
- Verify file paths are relative to repository root
- Check CSV encoding if non-ASCII characters present
- Use `pd.read_csv(file, encoding='utf-8')` for international data

### MetaTrader5 on Linux
```python
# Skip MetaTrader5 imports gracefully
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    print("MetaTrader5 not available (Windows-only)")
    MT5_AVAILABLE = False
```

## Quick Commands Reference

```bash
# Repository setup
git clone <repo-url>
cd Financial-Time-Series-Studies
pip install pandas numpy matplotlib seaborn scipy jupyter

# Start analysis environment
jupyter notebook

# Validate environment
python3 -c "import pandas as pd; df=pd.read_csv('Codes/Data/AirPassengers.csv'); print(f'✓ Ready: {df.shape}')"

# Run specific analysis
cd Codes/
python3 -c "exec(open('your_analysis_script.py').read())"
```