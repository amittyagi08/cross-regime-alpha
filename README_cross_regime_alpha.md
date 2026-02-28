# Cross Regime Alpha

Systematic equity strategies designed to generate robust returns across
market regimes using structured trend, pullback, and momentum
frameworks.

------------------------------------------------------------------------

## Overview

**Cross Regime Alpha** is a research-driven quantitative trading
framework focused on:

-   Regime-aware signal generation\
-   Structural trend filtering\
-   Deep pullback identification\
-   Momentum reclaim entries\
-   Risk-controlled systematic exits

The objective is not to predict markets, but to align with structural
flows and manage risk systematically.

This repository currently implements:

> Deep Pullback in Uptrend + Momentum Reclaim Strategy (S&P 500)

Designed for swing horizons (2--8 weeks), optimized for bull and neutral
regimes.

------------------------------------------------------------------------

## Philosophy

Markets behave differently across regimes.

Breakout systems outperform in expansion phases.\
Pullback systems outperform during rotational consolidation.\
Mean reversion works in compression environments.

This framework is built to:

-   Detect regime conditions\
-   Deploy appropriate structural models\
-   Maintain strict risk discipline\
-   Avoid indicator-only trading

Alpha comes from structure + regime alignment + risk control --- not
single indicators.

------------------------------------------------------------------------

## Strategy v1: Deep Pullback Momentum Reclaim

### Regime Filter

-   SPY \> 200-day SMA

### Trend Filter

-   Stock \> 200-day SMA\
-   50-day SMA \> 200-day SMA

### Pullback Condition

-   RSI(14) \< 35 within last 5 days\
-   Price below 20-day EMA

### Entry Trigger

-   RSI \>= 45\
-   Price closes back above 20-day EMA

### Exit Rules

-   Close below 20-day EMA\
-   7% hard stop\
-   20 trading day time stop

### Universe

-   S&P 500 constituents (current list for v1)

------------------------------------------------------------------------

## Architecture

src/ data_loader.py indicators.py signals.py backtester.py metrics.py
reporting.py cli.py

config.yaml outputs/

Core principles:

-   No lookahead bias\
-   Event-driven backtesting\
-   Clean separation of signals and execution\
-   Configurable parameters\
-   Reproducible results

------------------------------------------------------------------------

## Metrics Reported

-   CAGR\
-   Max Drawdown\
-   Sharpe Ratio\
-   Win Rate\
-   Profit Factor\
-   Average Holding Period\
-   Trades per Year\
-   Exposure %\
-   Trade Log\
-   Equity Curve

------------------------------------------------------------------------

## Roadmap

v1: - Deep pullback momentum system

v2: - Sector strength overlay\
- Relative strength ranking\
- ATR-based volatility sizing\
- Earnings filter\
- Walk-forward validation

v3: - Multi-strategy cross-regime deployment\
- Adaptive capital allocation\
- Portfolio volatility targeting

------------------------------------------------------------------------

## Disclaimer

This repository is for research and educational purposes only.\
Past performance does not guarantee future results.\
Trading involves risk of capital loss.

------------------------------------------------------------------------

Keywords: systematic trading, regime-based strategy, structural
momentum, S&P 500 pullback system, quantitative equity research
