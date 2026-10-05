# Financial Analytics Platform

A personal data engineering and applied statistics project built with **Python, SQL, and Databricks**. The platform brings together corporate filings, banking data, equity prices, and cryptocurrency candles to explore financial fundamentals, market risk, and volatility forecasting.

The project follows a **Bronze → Silver → Gold** architecture, with downstream notebooks for feature engineering, risk analysis, and model evaluation.

> **Project status:** Research prototype under active development. A code review identified issues in forecast evaluation and risk calculations that are being addressed. Existing forecasts, model rankings, and affected risk outputs are provisional until the corrections are validated and the results are recomputed. This repository is not yet a complete, independently reproducible deployment.

## Purpose

The project explores how to turn heterogeneous API data into reusable analytical datasets and evaluate whether more complex statistical models provide value beyond simple baselines.

Questions the platform is designed to support include:

- How do volatility, drawdowns, and tail losses differ across assets?
- Do GARCH-family models improve on EWMA and rolling-volatility baselines?
- Are risk-limit breaches occurring at the expected frequency, or clustering over time?
- How can SEC financial statements and FDIC bank data be organized into consistent analytical tables?

## Data sources

| Source | Data | Role in the project |
| --- | --- | --- |
| SEC EDGAR | XBRL company facts and filing metadata | Corporate financial analysis |
| FDIC BankFind | Institution records, quarterly financials, and bank failures | Banking analysis |
| Twelve Data | Daily equity prices | Return, volatility, and market-risk analysis |
| Coinbase | Cryptocurrency OHLCV candles | Crypto return and risk analysis |
| GDELT | News data | Sentiment ingestion experimentation |

These datasets support several analytical workflows; they are not all inputs to the forecasting models. The forecasting engine currently reads asset returns from Gold risk tables.

## Architecture

| Layer | Responsibility | Examples |
| --- | --- | --- |
| Bronze | Source ingestion and ingestion metadata | Market prices, crypto candles, SEC facts, FDIC records |
| Silver | Cleaning, deduplication, type normalization, and keys | Conformed market prices and FDIC datasets |
| Gold | Analytical facts, dimensions, and summaries | Market analytics, SEC summaries, FDIC trends |
| Features | Time-indexed modeling datasets | Lagged returns, rolling moments, volume features, cross-asset measures |
| Analytics | Forecasts, evaluation, diagnostics, and model selection | Forecast backtests, model performance, champion/challenger metadata |

The implementation uses Delta tables, SQL transformations, PySpark, and pandas-based statistical routines. Some notebooks reference tables created separately in the original Databricks workspace; their complete setup is still being consolidated into this repository.

## Implementation highlights

- **Equity ingestion:** Databricks secrets, API pacing, retries, explicit schemas, basic data validation, and Delta MERGE operations.
- **Data preparation:** Natural-key deduplication, surrogate keys, date parsing, OHLC checks, and analytical fact/dimension tables.
- **Forecasting:** GARCH, EGARCH, and GJR-GARCH candidates with multiple innovation distributions, compared against EWMA and rolling-volatility baselines.
- **Evaluation:** Walk-forward evaluation code, residual diagnostics, and champion/challenger selection logic.
- **Risk analysis:** Historical tail-risk measures, drawdown analysis, VaR backtests, and breach-clustering diagnostics.

These describe the implemented components and research approach, rather than verified claims of model superiority or production readiness.

## Repository structure

| Directory | Contents |
| --- | --- |
| `ingestion/` | API ingestion notebooks for SEC, FDIC, equities, crypto, and news |
| `transformations/` | Silver and Gold transformations and quality-check notebooks |
| `sql_queries/` | Exported SQL notebooks for dimensions, facts, and validation |
| `risk_analytics/` | Tail-risk and VaR extensions, plus export placeholders |
| `forecasting/` | Feature preparation, forecasting, evaluation, registry logic, and validation queries |

Files include Databricks source exports (`.py`), Jupyter notebook exports (`.ipynb`), and SQL scripts. Some `.py` files contain Databricks magic cells and are intended for notebook import rather than execution as ordinary Python scripts.

## Running in Databricks

The repository currently requires additional workspace setup. Importing the notebooks alone is not sufficient to recreate every dependency.

1. Import the notebooks and SQL scripts into a Databricks workspace with Spark and Delta support.
2. Review catalog and schema references. Code currently mixes two-part names such as `bronze.market_prices_daily` with three-part names beginning with `workspace`; align these to your environment.
3. Configure the required source credentials. The Twelve Data notebook currently reads the secret scope `market-data` and key `twelve-data-api-key`.
4. Replace the SEC notebook's placeholder contact email in its User-Agent configuration.
5. Review Python dependencies, including `requests`, `pandas`, `numpy`, `scipy`, `statsmodels`, and `arch`. A pinned environment specification is planned.
6. Resolve missing upstream table definitions and incomplete exports before executing downstream analytics.
7. Run ingestion, Silver transformations, Gold preparation, and risk calculations before dependent forecasting tasks. Inspect actual table dependencies rather than relying solely on notebook numbering.
8. Validate a small asset sample before attempting a full rebuild. In the forecasting engine, `MAX_ASSETS` provides a sample-size control.

Use a development catalog or schema when testing changes. Review existing write behavior: some notebooks rebuild or truncate tables, and the crypto ingest currently overwrites its destination.

## Known limitations and current priorities

The following items were identified in the exported code and remain open until fixes are verified:

- **Forecast scoring:** Correct the QLIKE formula and evaluate variance forecasts against appropriate variance proxies.
- **Temporal alignment:** Remove lookahead from the rolling-volatility baseline and audit forecast timestamps.
- **Drawdown calculation:** Correct trailing maximum drawdown to consider every peak-to-trough decline within a window.
- **Distribution and horizon consistency:** Align VaR and expected shortfall with the selected innovation distribution, and distinguish day-five volatility from cumulative five-day return risk.
- **Model selection:** Enforce the intended baseline-improvement rule and prevent duplicate registry entries from baseline joins.
- **Operational reliability:** Protect existing history from partial ingestion failures and replace truncate-then-append publication where appropriate.
- **Reproducibility:** Complete missing exports, include job dependencies and table setup, pin dependencies, and add enforced validation tests.
- **Experiment tracking:** Connect selection metadata to actual MLflow runs; current forecasting code leaves MLflow run IDs unpopulated.

Some files, including the exported SEC Silver transformation and market-risk validation notebook, currently contain only placeholders. Their presence should not be interpreted as completed functionality.

## Validation plan

The next milestone is a reproducible evaluation with:

- Deterministic tests for drawdown, scoring, distribution handling, and forecast alignment.
- Consistent evaluation dates across model candidates and baselines.
- Chronological model selection followed by a separate final test period.
- Recorded data cutoffs, optimizer failures, sample sizes, and runtime.
- Recomputed outputs and a documented comparison with the previous implementation.

No validated model-performance claims are made in this README.

## License and data usage

The project code is available under the [MIT License](LICENSE).

The code license does not grant rights to third-party datasets. Access, display, redistribution, and commercial use of source data remain subject to each provider's applicable terms and subscription permissions. Credentials should remain in a secrets manager and outside version control.

## Author

**Kyle McClelland** — personal portfolio project exploring data engineering, applied statistics, and financial analytics.
