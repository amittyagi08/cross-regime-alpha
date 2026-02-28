# Deep Pullback System Implementation Plan

Based on: `deep-pullback-system-spec.md`

## Step 1: Initialize Project Structure
- Create clear module boundaries for:
  - `data`
  - `brokers/ibkr`
  - `indicators`
  - `signals`
  - `portfolio`
  - `backtest`
  - `reporting`
  - `config`
  - `tests`
- Add one central config for strategy parameters (v1 defaults from spec section 12).
- Define naming conventions for columns, symbols, and output files.

## Step 2: Implement Universe Input (Ticker List)
- Accept universe from a user-provided ticker list file (CSV/TXT/JSON configurable).
- Validate ticker format and remove duplicates.
- Add optional include/exclude lists for quick overrides.
- Persist resolved universe used in each run for reproducibility.

## Step 3: Implement IBKR Connectivity Layer
- Build IBKR client wrapper for connection/session management.
- Read IBKR credentials/settings from environment variables or local secrets file.
- Ensure secrets are never committed (gitignore + sample env template only).
- Add connection health check and clear error messages for auth/permission failures.

## Step 4: Build Market Data Ingestion from IBKR
- Pull daily bars for all universe tickers plus `SPY`.
- Request required fields mapped to schema:
  - `date, open, high, low, close, adj_close, volume`
- If IBKR does not provide adjusted series directly, document and implement adjustment policy.
- Add batching/retry/throttling behavior for IBKR pacing limits.

## Step 5: Normalize and Validate Data
- Align all symbols to a common daily calendar.
- Handle missing bars and non-trading days explicitly.
- Add data quality checks:
  - duplicate rows
  - null/invalid prices
  - outlier jumps flagged for review
- Store cleaned dataset in local cache for faster reruns.

## Step 6: Implement Indicator Engine
- Implement indicators with strictly historical data (no lookahead):
  - `SMA200`
  - `SMA50`
  - `EMA20`
  - `RSI14` (Wilder)
  - `ATR14`
  - rolling 20-day high
  - optional volume `SMA50`
- Validate warm-up behavior (insufficient history handling).

## Step 7: Implement Market Regime Filter
- Compute SPY regime state per day:
  - `regime_on = SPY_adj_close > SPY_SMA200`
- Ensure regime state is known before evaluating entries.

## Step 8: Implement Stock Trend Eligibility Filter
- Mark stock as trend-eligible when:
  - `close > SMA200`
  - `SMA50 > SMA200`
- Make this reusable in signal generation.

## Step 9: Implement Deep Pullback Detector
- Mark pullback setup when both hold:
  - `RSI14 < 35` within last 5 trading days
  - current `close < EMA20`
- Persist setup state deterministically for entry logic.

## Step 10: Implement Entry Trigger Logic
- Entry signal requires:
  - pullback setup active
  - `RSI14 >= 45`
  - `close > EMA20`
  - regime filter ON
  - trend eligibility passes
- Execution rule:
  - enter at next trading day open.

## Step 11: Implement Position Sizing and Ranking
- Portfolio constraints:
  - max 10 concurrent positions
  - equal weight
  - no leverage
- If signals exceed capacity:
  - rank by `ROC_63`
  - pick highest ranked first
- Define deterministic tie-breaks.

## Step 12: Implement Exit and Risk Rules
- Exit on first trigger:
  - `close < EMA20`
  - hard stop at `-7%` from entry
  - 20 trading day time stop
- Keep optional 10% profit target implemented but disabled by default.
- Define precedence for same-day multi-trigger exits.

## Step 13: Add Trading Frictions
- Apply configurable costs:
  - slippage (default 2 bps)
  - commission
- Apply on both entries and exits consistently.

## Step 14: Build Backtest Execution Engine
- Implement day-by-day event loop:
  - update signals from available historical data
  - process exits
  - process entries with capacity and ranking
- Enforce no lookahead bias.
- Track cash, positions, and equity/NAV daily.

## Step 15: Generate Outputs
- Metrics:
  - CAGR
  - Total Return
  - Max Drawdown
  - Sharpe Ratio
  - Win Rate
  - Profit Factor
- Artifacts:
  - trade log
  - equity curve
  - run metadata (config + universe snapshot + data pull timestamp)
- Save in CSV/JSON and concise markdown summary.

## Step 16: Validation and Testing
- Unit tests:
  - indicators
  - regime/trend/pullback/entry/exit logic
  - ranking and capacity behavior
- Integration tests:
  - IBKR connection mock/stub
  - data pull and normalization path
- Backtest integrity tests:
  - no future leakage
  - no position cap violations
  - edge cases (missing bars, ties, gap-through stop)

## Step 17: v1 Runbook and Documentation
- Provide one-command run flow for:
  - fetch data from IBKR
  - run backtest
  - export reports
- Document required environment variables for IBKR (without storing secrets).
- Document assumptions and non-goals:
  - no options
  - no margin logic
  - no intraday execution
  - no ML
  - no portfolio optimization

## Review Checklist (Before Coding)
- Confirm ticker list input format you will provide.
- Confirm IBKR connection mode (Gateway or TWS), host, port, and client ID approach.
- Confirm historical date range for first backtest run.
- Confirm commission model and whether to keep default slippage at 2 bps.
- Confirm whether optional 10% profit target stays disabled for v1.
