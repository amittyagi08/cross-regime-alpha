# Deep Pullback in Uptrend + Momentum Reclaim Trigger (S&P 500) --- Build Spec (for Codex)

Owner: Amit\
Goal: Implement a rule-based swing trading system that scans S&P 500
daily, finds deep pullbacks inside an uptrend, triggers entry on
momentum reclaim, and backtests results with strict risk controls.

------------------------------------------------------------------------

## 0) Non-Goals (for v1)

-   No options, no margin usage logic (cash equity only).
-   No ML in v1.
-   No intraday execution (use EOD daily bars).
-   No portfolio optimization; keep position sizing simple.

------------------------------------------------------------------------

## 1) Strategy Summary

Core idea: 1) Confirm the stock is in a primary uptrend\
2) Detect a deep pullback\
3) Enter only when price shows a reclaim / momentum shift\
4) Exit via trend-based stop + time stop\
5) Apply a market regime filter

------------------------------------------------------------------------

## 2) Universe & Data

Universe: - S&P 500 constituents (current list acceptable for v1)

Data Required: - date, open, high, low, close, adj_close, volume - SPY
daily data for regime filter

Use adjusted close consistently for indicators and returns.

------------------------------------------------------------------------

## 3) Indicators Needed

-   SMA200
-   SMA50
-   EMA20
-   RSI14 (Wilder)
-   ATR14
-   Rolling 20-day high
-   Optional: Volume SMA50

------------------------------------------------------------------------

## 4) Market Regime Filter

Regime ON when: - SPY_adj_close \> SPY_SMA200

------------------------------------------------------------------------

## 5) Stock Trend Filter

Eligible stock: - close \> SMA200 - SMA50 \> SMA200

------------------------------------------------------------------------

## 6) Deep Pullback Condition

Pullback detected when: - RSI14 \< 35 within last 5 days - AND close \<
EMA20

------------------------------------------------------------------------

## 7) Momentum Reclaim Entry Trigger

Enter when: - RSI14 \>= 45 - close \> EMA20 - Market regime ON - Trend
filter passes

Default execution: next day open

------------------------------------------------------------------------

## 8) Exit Rules

Exit when any occurs: - close \< EMA20 - 7% hard stop from entry - 20
trading day time stop

Optional: - 10% profit target (disabled by default)

------------------------------------------------------------------------

## 9) Position Sizing

-   Max 10 positions
-   Equal weight allocation
-   No leverage

------------------------------------------------------------------------

## 10) Ranking Logic

If more than 10 signals: - Rank by 63-day ROC - Select highest values
first

------------------------------------------------------------------------

## 11) Backtest Requirements

-   No lookahead bias
-   Indicators computed using historical data only
-   Include slippage (2 bps default)
-   Commission configurable

Output: - CAGR - Total Return - Max Drawdown - Sharpe Ratio - Win Rate -
Profit Factor - Trade Log - Equity Curve

------------------------------------------------------------------------

## 12) Default Parameter Set (v1)

-   Regime: SPY \> SMA200
-   Trend: close \> SMA200 AND SMA50 \> SMA200
-   Pullback: RSI \< 35 in last 5 days AND close \< EMA20
-   Entry: RSI \>= 45 AND close \> EMA20
-   Exit: EMA20 break OR -7% OR 20-day time stop
-   Ranking: ROC_63
-   Execution: next open

------------------------------------------------------------------------

End of Specification
