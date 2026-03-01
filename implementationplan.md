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

## Step 2.5: Externalize IBKR Runtime Configuration (Env-First)
- Create/maintain local env template for IBKR keys:
  - `IBKR_HOST`
  - `IBKR_PORT`
  - `IBKR_CLIENT_ID`
  - `IBKR_READONLY`
  - `IBKR_TIMEOUT_SECONDS`
  - `IBKR_ACCOUNT`
- Ensure secrets safety:
  - keep `.env` and `.env.*` ignored
  - keep `.env.example` tracked for onboarding
- Define config source precedence for runtime settings:
  - environment variables (highest)
  - optional local env file
  - code defaults (fallback)
- Update IBKR connection flow to consume resolved settings object end-to-end.
- Add tests for precedence and connection behavior paths.
- Document local setup and Azure App Service configuration mapping for `IBKR_*` variables.

## Step 3: Implement IBKR Connectivity Layer
- Build IBKR client wrapper for connection/session management.
- Read IBKR credentials/settings from environment variables or local secrets file.
- Ensure secrets are never committed (gitignore + sample env template only).
- Add connection health check and clear error messages for auth/permission failures.

## Step 4: Build Market Data Ingestion from IBKR
- Pull daily bars for all universe tickers plus `SPY`.
- Request required fields mapped to schema:
  - `date, open, high, low, close, adj_close, volume`
- If IBKR does not provide adjusted series directly, use this adjustment policy:
  - pull OHLCV from `TRADES`
  - pull adjusted close from `ADJUSTED_LAST` when available
  - compute `adjustment_factor = adj_close / close`
  - if adjusted close is unavailable, set `adj_close = close`, `adjustment_factor = 1.0`, and record `adjustment_method = none` in metadata
- Add batching/retry/throttling behavior for IBKR pacing limits.
- Define storage format and layout (local + cloud-ready):
  - market bars stored as Parquet (primary)
  - run/ingestion metadata stored as JSON
  - local paths:
    - `data/cache/ibkr/raw/daily/symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-*.parquet`
    - `data/cache/ibkr/normalized/daily/symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-*.parquet`
    - `outputs/runs/<run_id>/metadata.json`
  - cloud target paths (future Azure):
    - Blob/ADLS: `market-data/raw/daily/...` and `market-data/normalized/daily/...` (Parquet)
    - Cosmos DB (or equivalent document store): run metadata, ingestion status, warnings (JSON)

## Step 5: Normalize and Validate Data
- Align all symbols to a common daily calendar.
- Handle missing bars and non-trading days explicitly.
- Add data quality checks:
  - duplicate rows
  - null/invalid prices
  - outlier jumps flagged for review
- Store cleaned dataset in local cache for faster reruns and keep path compatibility with cloud object storage layout.

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

## Step 18: Add Application Service Layer (Web-First)
- Keep strategy and backtest logic as pure domain modules (no UI framework coupling).
- Add API layer for web/mobile clients:
  - submit run request
  - check run status
  - fetch run results and artifacts
  - fetch model portfolio snapshot
- Add async job execution for long-running backtests/scans.
- Add persistence for run metadata, positions, and performance history.
- Containerize service and prepare Azure deployment profile (dev/stage/prod config separation).

## Step 19: Implement Model Portfolio Domain + UI Contracts
- Define model portfolio as first-class object:
  - portfolio id/name/version
  - holdings (symbol, target weight, current weight, entry date, status)
  - cash buffer and rebalance timestamp
  - source run id and generation metadata
- Add deterministic portfolio construction pipeline from approved signals/ranking.
- Persist portfolio snapshots and change history for auditability.
- Add API responses tailored for UI cards/tables/charts:
  - latest holdings
  - allocation breakdown
  - performance summary
  - rebalance/change log
- Define clear state model for UI: loading, ready, empty, stale, error.

## Step 20: Prepare Multi-Client Frontend Path (Web now, iOS/Android later)
- Build web frontend against the same versioned API contracts used by mobile.
- Keep auth/session model API-driven (token-based), not frontend-framework-specific.
- Add mobile-ready API pagination/filtering/sorting for portfolio and trade history views.
- Avoid web-only assumptions in response payloads (no HTML-formatted fields).
- Plan mobile app as a thin client consuming existing APIs with shared design tokens and DTOs.

## Review Checklist (Before Coding)
- Confirm ticker list input format you will provide.
- Confirm IBKR connection mode (Gateway or TWS), host, port, and client ID approach.
- Confirm historical date range for first backtest run.
- Confirm commission model and whether to keep default slippage at 2 bps.
- Confirm whether optional 10% profit target stays disabled for v1.
- Confirm initial model portfolio cadence (daily close, weekly, or event-driven rebalance).
- Confirm whether portfolio UI is read-only in v1 or includes manual override actions.
- Confirm Azure target runtime (App Service vs Container Apps) and managed services (Postgres/Redis/Blob).
