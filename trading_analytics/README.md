# NQ Trading Analytics with dbt + PostgreSQL

A comprehensive data transformation pipeline for analyzing NQ futures trading data using dbt (data build tool) and PostgreSQL.

## 🎯 Project Overview

This project provides a complete analytics framework for NQ futures trading algorithms, including:

- **Data ingestion** from ZST compressed files
- **Data transformation** using dbt for cleaning and feature engineering  
- **Technical analysis** with moving averages, momentum, and volatility indicators
- **Trading signals** generation based on multiple algorithmic strategies
- **Performance tracking** and backtesting capabilities

## 📊 Data Architecture

```
Raw Data (PostgreSQL)
    ↓
Staging Models (dbt)
    ↓
Intermediate Models (dbt) 
    ↓
Mart Models (dbt)
    ↓
Algorithm Models (dbt)
```

## 🏗️ Project Structure

```
trading_analytics/
├── models/
│   ├── staging/          # Clean and standardize raw data
│   │   ├── stg_ohlcv_1m.sql
│   │   └── stg_symbology.sql
│   ├── intermediate/     # Feature engineering and technical indicators
│   │   └── int_technical_indicators.sql
│   ├── marts/           # Business-ready analytical tables
│   │   └── daily_market_summary.sql
│   ├── algorithms/      # Trading signal generation
│   │   └── nq_trading_signals.sql
│   └── schema.yml       # Documentation and tests
├── macros/              # Reusable SQL functions
├── seeds/               # Reference data (CSV files)
├── tests/               # Custom data quality tests
└── dbt_project.yml      # Project configuration
```

## 🚀 Getting Started

### 1. Prerequisites

- Python 3.7+
- PostgreSQL database
- Virtual environment (recommended)

### 2. Installation

```bash
# Install dependencies
pip install dbt-core dbt-postgres sqlalchemy psycopg2-binary pandas

# Or use requirements.txt
pip install -r requirements.txt
```

### 3. Database Setup

Set up your PostgreSQL database and environment variables:

```bash
export DBT_POSTGRES_USER="your_username"
export DBT_POSTGRES_PASSWORD="your_password"  
export DBT_POSTGRES_DB="trading_analytics"
export DBT_POSTGRES_HOST="localhost"
export DBT_POSTGRES_PORT="5432"
```

### 4. Load Raw Data

Convert your ZST files to CSV and load into PostgreSQL:

```bash
# Convert ZST to CSV (if needed)
python Data/prep/zst_to_csv_conversion.py your_data.csv.zst

# Setup database and load data
python setup_database.py
```

### 5. Run dbt Transformations

```bash
cd trading_analytics

# Test database connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## 📈 Key Models and Features

### Trading Signals (`nq_trading_signals`)

**Algorithm Strategies:**

1. **Moving Average Crossovers**: SMA 10 vs SMA 20 crossovers
2. **Breakout Trading**: Price breakouts with volume confirmation
3. **Mean Reversion**: Price deviations from moving averages
4. **Momentum Trading**: Strong directional moves with volume

**Signal Strength:**
- `STRONG_BUY/SELL`: 2+ confirming signals
- `BUY/SELL`: 1 confirming signal  
- `HOLD`: Conflicting or no signals
