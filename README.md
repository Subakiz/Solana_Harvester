# Solana Harvester v5.2

Solana Harvester is an advanced quantitative data harvesting and paper trading engine designed for the Solana meme coin market. It employs high-frequency tiered polling, sophisticated mathematical models for regime detection, and a robust risk-management framework to simulate trading strategies in real-time.

---

## 🚀 Core Architecture

### 1. Data Ingestion (Tiered Polling)
Replaces traditional flat-interval polling with a priority-based scheduler:
- **Tier 1 (OPEN_POSITION):** Polls every 2s for tokens with active trades.
- **Tier 2 (HOT_WATCHLIST):** Polls every 3s for pre-qualified entry candidates.
- **Tier 3 (WARM_SCANNER):** Polls every 12s for broader market monitoring (up to 200 tokens).
- **Tier 4 (DISCOVERY):** Polls every 25s for new token listings, boosts, and profiles via DexScreener.

### 2. Quantitative Math Engine
Stateless, high-performance implementations of key signals:
- **Hurst Exponent (H):** Detects market regimes (persistent vs. mean-reverting).
- **Micro-CVD (Cumulative Volume Delta):** Proxies order-flow delta from DexScreener tick data.
- **Gini Coefficient:** Measures holder concentration to identify rug-pull risks.
- **ATR (Average True Range):** Calibrates per-trade Take Profit (TP) and Stop Loss (SL) based on volatility.
- **Price Efficiency Ratio (PER):** Fallback for young tokens with insufficient Hurst data.

### 3. Paper Trading Engine
Realistic simulation with a comprehensive cost model:
- **Scaled Exits:** Partial TP at a target, followed by a trailing stop on the remaining position.
- **Cost Model:** Includes round-trip fees, liquidity-aware slippage, and SOL priority fees.
- **Dynamic Risk Management:** Daily loss limits, circuit breakers, and portfolio heat pauses.

---

## ✨ Key Features

- **Multi-Source Discovery:** Monitors trending tokens, latest pairs, and developer profiles.
- **Safety Enrichment:** Integrated **RugCheck** API for score-based filtering and **pump.fun** migration detection.
- **SOL Regime Filter:** Automatically pauses trading or force-closes positions during SOL market crashes.
- **Dynamic TP Optimizer:** Automatically re-calibrates Take Profit levels based on historical Maximum Favorable Excursion (MFE) data.
- **Real-time Dashboard:** Minimalist Streamlit interface for live monitoring of trades, portfolio equity, and signal distributions.

---

## 📂 Project Structure

```text
.
├── config/              # Centralized settings and .env loading
├── data/                # SQLite database storage (quant_harvest.db)
├── db/                  # Async database manager and schema definitions
├── ingestion/           # Tiered poller and data harvester logic
├── quant/               # Math engine (Hurst, CVD, Gini, ATR, etc.)
├── trading/             # Paper trading engine and exit logic
├── utils/               # Logger and rate-limiting utilities
├── dashboard.py         # Streamlit real-time dashboard
├── main.py              # Main execution entry point
├── setup.sh             # System and environment setup script
└── ecosystem.config.js  # PM2 process management configuration
```

---

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.10+
- Node.js & npm (for PM2 management)
- SQLite3

### 1. Automated Setup
Run the provided setup script to update the system, install dependencies, and create a virtual environment:
```bash
chmod +x setup.sh
./setup.sh
```

### 2. Manual Configuration
Copy the example environment file and add your Helius RPC API key:
```bash
cp .env.example .env
nano .env
```
*Note: A valid Helius API key is required for Gini coefficient analysis.*

---

## ⚙️ Configuration (.env)

| Variable | Description | Default |
|----------|-------------|---------|
| `SOLANA_RPC_URL` | Helius RPC endpoint | `Required` |
| `HURST_THRESHOLD` | Entry gate for trend persistence | `0.70` |
| `MIN_LIQUIDITY_USD` | Minimum pool liquidity for entry | `$75,000` |
| `MAX_OPEN_PAPER_TRADES` | Max concurrent open positions | `3` |
| `DAILY_LOSS_LIMIT_PCT` | Portfolio circuit breaker threshold | `0.15` |
| `RUGCHECK_ENABLED` | Toggle RugCheck safety integration | `true` |

---

## 📈 Usage

### Running the Harvester
Activate the virtual environment and start the engine:
```bash
source .venv/bin/activate
python main.py
```

### Launching the Dashboard
The dashboard provides a real-time view of market activity and trade performance:
```bash
streamlit run dashboard.py
```

### Production Management (PM2)
Manage both the harvester and dashboard as background processes:
```bash
pm2 start ecosystem.config.js
pm2 logs solana-harvester
pm2 monitor
```

---

## 📊 Analysis & Reporting
The engine automatically generates:
- **Hourly Performance Snapshots:** Detailed metrics stored in the database.
- **Analysis Reports:** See `ANALYSIS_REPORT.md` for deep dives into strategy profitability and recent fix logs.
- **Database Pruning:** Automatically manages DB size by pruning old tick data and rejections.

---

## ⚠️ Disclaimer
This software is for **educational and research purposes only**. It is a **paper trading simulation** and does not execute live trades on the Solana blockchain. Meme coin markets are extremely volatile and carry high risk. Use at your own discretion.
