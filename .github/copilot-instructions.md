# Financial Time Series Studies - Copilot Instructions

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Repository Overview
Financial-Time-Series-Studies is a Python-based portfolio of financial technical analysis applications using Jupyter notebooks. The repository contains implementations of time series analysis techniques, quantitative trading strategies, and financial data visualization tools for both academic research and practical trading applications.

## Working Effectively

### Bootstrap and Setup
**NEVER CANCEL BUILDS OR INSTALLS** - Package installations may take 5-15 minutes. Set timeouts to 30+ minutes.

1. **Install Core Dependencies** (takes 5-10 minutes):
   ```bash
   pip3 install jupyter numpy pandas matplotlib seaborn statsmodels scikit-learn
   ```

2. **Install Financial Analysis Packages** (takes 3-8 minutes):
   ```bash
   pip3 install yfinance arch hmmlearn sktime
   ```

3. **Verify Installation** (takes 30 seconds):
   ```bash
   python3 -c "import pandas as pd, numpy as np, matplotlib.pyplot as plt, seaborn as sns, statsmodels.api as sm, sklearn, yfinance as yf, arch, hmmlearn, sktime; print('All core packages working')"
   ```

### Working with Notebooks
- **Start Jupyter Server** (immediate startup):
  ```bash
  jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir="."
  ```
  
- **Convert Notebooks to Scripts** (takes 10-30 seconds):
  ```bash
  jupyter nbconvert --to script "Codes/[NOTEBOOK_NAME].ipynb"
  ```

- **Run Notebooks via Command Line** (timing varies by notebook complexity):
  ```bash
  jupyter nbconvert --to notebook --execute "Codes/[NOTEBOOK_NAME].ipynb"
  ```

### Data Sources and Market Data
- **yfinance for Market Data** (primary data source, works globally):
  ```python
  import yfinance as yf
  # Brazilian stocks: use .SA suffix (e.g., "PETR4.SA")
  # US stocks: use ticker directly (e.g., "AAPL")
  ticker = yf.Ticker("PETR4.SA")
  ```

- **MetaTrader5 Integration**: 
  - **DOES NOT WORK on Linux/macOS** - Windows only with specific broker setup
  - Requires XP Investimentos account and MetaTrader5 terminal installation
  - Use yfinance as alternative for market data acquisition

## Repository Structure

### Key Directories
- **`Codes/`**: Primary time series analysis notebooks
  - VAR (Vector Autoregression) models
  - LSTM neural networks for time series
  - ARMA, GARCH, and VaR models
  - Hidden Markov Models
  - Time series decomposition
  - Sktime forecasting examples

- **`Trading Quantitativo/`**: Quantitative trading strategies
  - `Extraindo dados da bolsa com MetaTrader5/`: MetaTrader5 scripts (Windows-only)
  - `Obtencao de dados/`: Market data acquisition notebooks  
  - `Backtesting/`: Trading strategy backtesting

- **`Codes/Data/`**: Sample datasets
  - `macro_monthly.csv`: Macroeconomic indicators
  - `stock_prices.csv`: Stock price data
  - `yield_curve.csv`: Yield curve data
  - `AirPassengers.csv`: Classic time series dataset

### Key Files
- **Core Analysis**: `Codes/Time Series - VAR - Inflation.ipynb` (comprehensive VAR analysis)
- **Data Acquisition**: `Trading Quantitativo/Obtencao de dados/Manipulacao de MarketData.ipynb`
- **LSTM Example**: `Codes/Time Series - LSTM.ipynb`
- **MetaTrader5**: `Trading Quantitativo/Extraindo dados da bolsa com MetaTrader5/metatrader_intro.py`

## Validation and Testing

### Manual Validation Requirements
**ALWAYS** validate functionality after making changes by running these scenarios:

1. **Time Series Analysis Workflow** (2-5 minutes):
   ```bash
   cd "Codes"
   jupyter nbconvert --to script "Time Series - TS Decomposition.ipynb"
   ```
   
   **For execution testing** (optional - may take longer):
   ```bash
   cd "Codes"
   jupyter nbconvert --to notebook --execute "Time Series - TS Decomposition.ipynb" --ExecutePreprocessor.timeout=300
   ```

2. **Market Data Access Test** (30 seconds - may fail due to network restrictions):
   ```python
   # Test yfinance if network available
   try:
       import yfinance as yf
       ticker = yf.Ticker("AAPL")
       print("yfinance import successful")
   except Exception as e:
       print(f"yfinance test failed (expected in restricted networks): {e}")
   
   # Always test local data access
   import pandas as pd
   df = pd.read_csv('Codes/Data/AirPassengers.csv')
   print(f"Successfully loaded local data: {len(df)} rows")
   ```

3. **Statistical Analysis Test** (1 minute):
   ```python
   import pandas as pd
   import numpy as np
   from statsmodels.tsa.seasonal import seasonal_decompose
   
   # Generate sample data
   dates = pd.date_range('2020-01-01', periods=100, freq='D')
   ts = pd.Series(np.random.randn(100).cumsum(), index=dates)
   
   # Test decomposition
   decomposition = seasonal_decompose(ts, model='additive', period=7)
   print("Time series decomposition successful")
   ```

### Timing Expectations
- **Initial setup** (all dependencies): 15-20 minutes - NEVER CANCEL
- **Jupyter server startup**: Immediate (< 10 seconds)
- **Notebook execution**: 1-10 minutes depending on data size and model complexity
- **yfinance data fetch**: 5-30 seconds depending on date range
- **Package imports**: 2-5 seconds for all packages

## Common Tasks and Troubleshooting

### Dependency Issues
- **"ModuleNotFoundError" for financial packages**: Run the installation commands above
- **MetaTrader5 import fails**: Expected on Linux/macOS - use yfinance instead
- **Network timeouts during pip install**: Retry with `--timeout 60` flag
- **Package version conflicts**: Use `pip3 install --upgrade [package]` to update

### Data Access
- **Brazilian market data**: Always append `.SA` to ticker symbols (e.g., `PETR4.SA`)
- **US market data**: Use ticker symbols directly (e.g., `AAPL`, `MSFT`)
- **Historical data**: yfinance provides up to 10+ years of daily data (requires internet)
- **Local data**: Sample datasets available in `Codes/Data/` for offline analysis
- **Network restrictions**: If yfinance fails, use local CSV files for development and testing

### Notebook Execution
- **Kernel crashes**: Usually due to memory issues with large datasets - restart kernel
- **Plotting issues**: Ensure `%matplotlib inline` is set in notebook cells
- **Portuguese comments**: Repository contains mixed Portuguese/English - focus on code logic
- **Execution timeouts**: Add `--ExecutePreprocessor.timeout=300` for longer-running notebooks

### Quick Data Loading Examples
```python
# Load sample time series data
import pandas as pd

# AirPassengers dataset (classic time series)
air = pd.read_csv('Codes/Data/AirPassengers.csv', parse_dates=['Month'], index_col='Month')

# Stock prices (international indices)
stocks = pd.read_csv('Codes/Data/stock_prices.csv', parse_dates=['Date'], index_col='Date')

# Macroeconomic indicators (US data)
macro = pd.read_csv('Codes/Data/macro_monthly.csv', parse_dates=['DATE'], index_col='DATE')

# Temperature data
temps = pd.read_csv('Codes/Data/temperatures.csv')
```

## Development Guidelines

### Making Changes
- **Always test changes** with validation scenarios above
- **Run notebook conversions** to ensure syntax correctness
- **Verify data access** after modifying data acquisition code
- **Test plotting functionality** after visualization changes

### File Organization
- **New analysis notebooks**: Place in `Codes/` directory
- **Trading strategies**: Place in `Trading Quantitativo/` subdirectories
- **Data files**: Store in `Codes/Data/` or appropriate subdirectory
- **Helper scripts**: Create in appropriate subdirectory with descriptive names

### Best Practices
- **Use yfinance** instead of MetaTrader5 for cross-platform compatibility
- **Include data validation** steps in analysis notebooks
- **Add visualization** to support analysis findings
- **Document model parameters** and assumptions clearly
- **Save intermediate results** for long-running analyses

## CRITICAL Limitations
- **MetaTrader5**: Only works on Windows with XP Investimentos broker account
- **No build system**: Manual dependency management required
- **No automated testing**: Manual validation required for all changes
- **Mixed language**: Portuguese comments may require translation
- **No version control** for dependencies: Ensure consistent environment setup

## Command Reference

### Installation Commands
```bash
# Core dependencies (5-10 minutes, NEVER CANCEL)
pip3 install jupyter numpy pandas matplotlib seaborn statsmodels scikit-learn

# Financial packages (3-8 minutes, NEVER CANCEL)  
pip3 install yfinance arch hmmlearn sktime

# Verification
python3 -c "import pandas as pd, numpy as np, matplotlib.pyplot as plt, seaborn as sns, statsmodels.api as sm, sklearn, yfinance as yf, arch, hmmlearn, sktime; print('All packages working')"
```

### Jupyter Commands
```bash
# Start server
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir="."

# Convert to script
jupyter nbconvert --to script "path/to/notebook.ipynb"

# Execute notebook
jupyter nbconvert --to notebook --execute "path/to/notebook.ipynb" --ExecutePreprocessor.timeout=300
```

### Data Loading Commands
```python
import pandas as pd

# Sample datasets
air = pd.read_csv('Codes/Data/AirPassengers.csv', parse_dates=['Month'], index_col='Month')
stocks = pd.read_csv('Codes/Data/stock_prices.csv', parse_dates=['Date'], index_col='Date')
macro = pd.read_csv('Codes/Data/macro_monthly.csv', parse_dates=['DATE'], index_col='DATE')
```

Always reference the sample notebooks in `Codes/` for implementation patterns and the data files in `Codes/Data/` for testing your analyses.