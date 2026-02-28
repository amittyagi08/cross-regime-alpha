# Data Directory

Place your universe file at:

- `data/universe/sp500_tickers.csv`

Expected format (one of these):

1. Single-column CSV with header `ticker`
2. Single-column CSV without header (ticker per line)

Example:

```csv
ticker
AAPL
MSFT
NVDA
```

Optional override files:

- `data/universe/include_tickers.csv` (extra tickers to force-add)
- `data/universe/exclude_tickers.csv` (tickers to remove)
