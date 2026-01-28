# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EEL Slack Bot is a Python automation system for inventory management and advertising analytics:
- **Coupang Stock Recommender**: Analyzes inventory across multiple channels (main warehouse, Coupang Rocket, own mall) and recommends optimal shipment quantities to Coupang
- **Daily AD Reporter**: Fetches Facebook/Meta advertising metrics and posts to Slack

## Running the Project

```bash
# Install dependencies
pip install -r coupang_requirements.txt  # For recommender
pip install -r ads_requirements.txt       # For ad reporter

# Run Coupang stock recommender (Slack alert)
python coupang_stock_recommender/run_recommender_slack.py

# Run Coupang stock recommender (Local - outputs Excel files)
python coupang_stock_recommender/run_recommender_local.py

# Run ad reporter
python daily_ad_reporter/reporter.py
```

No test suite exists in this project.

## Architecture

### Coupang Stock Recommender Pipeline

```
Google Sheets (6 sheets) → data_loader.py → data_processor.py → recommender.py → Slack
```

1. **data_loader.py**: Loads inventory, Coupang stock, sales history, BOM (Bill of Materials), and product lists from Google Sheets
2. **data_processor.py**: Cleans data, merges sources, distributes set product sales to component SKUs
3. **recommender.py**: Core algorithm - runs 60-day inventory simulation to calculate optimal transfer quantities
4. **run_recommender_slack.py**: Entry point for Slack alerts (shows stockout products)
5. **run_recommender_local.py**: Entry point for local use (outputs two Excel files: full list + daily work top 100)

### Key Algorithm (recommender.py)

The recommendation algorithm has 4 steps:
1. **Sweep**: Move 100% of stock for Coupang-only or discontinued products
2. **Min Qty**: Ensure minimum 2-unit threshold on Coupang for low-stock items
3. **60-day Simulation**: Daily consumption simulation considering Coupang sales, own-mall defense (1.2x multiplier), and BOM relationships
4. **Final Defense**: Retain minimum 2 units per SKU in main warehouse (except Coupang-only)

Key constants in `config.py` and `recommender.py`:
- `coupang_safety_days`: 30 days safety stock
- `OWN_DEFENSE_DAYS`: 7 days own-mall coverage
- `MAX_DAYS`: 60 day simulation horizon
- `EXCLUDED_SKU_PREFIXES`: SKUs to exclude from analysis

### Google Sheets Data Sources

| Sheet | Purpose |
|-------|---------|
| 재고 시트 | Main warehouse inventory |
| 로켓그로스재고(매번입력) | Coupang Rocket live stock |
| 매출시트 | 30-day sales history |
| 세트구성품 | BOM - set product components |
| 품절상품 | Discontinued products |
| 쿠팡전용상품 | Coupang-only products |

## Environment Variables

**Coupang Recommender:**
- `SLACK_BOT_TOKEN` - Slack bot token
- `TARGET_CHANNEL` - Slack channel ID
- `SPREADSHEETCREDENTIALS_JSON` - Google Service Account JSON

**AD Reporter:**
- `FB_ACCESS_TOKEN` - Facebook Graph API token
- `FB_AD_ACCOUNT_ID` - Facebook Ad Account ID (without "act_" prefix)
- `SLACK_BOT_TOKEN` - Slack bot token
- `SLACK_CHANNEL_AD` - Slack channel ID

## GitHub Actions

- `run_coupang_recommender.yml`: Triggered manually or via repository_dispatch
- `daily_report.yml`: Runs daily at 00:00 UTC (09:00 KST)

## Important Notes

- All column names are in Korean (상품명, 쿠팡재고, etc.)
- BOM handling automatically distributes set product sales to component SKUs
- NaN values are converted to 0 during numeric processing
- Google Sheets credentials go in `coupang_stock_recommender/credentials/`
