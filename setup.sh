#!/usr/bin/env bash
set -euo pipefail

echo "========================================================"
echo "  Solana Data Harvester & Paper Trading Engine"
echo "  Setup Script"
echo "========================================================"

echo "[1/7] Updating system..."
sudo apt-get update -qq && sudo apt-get upgrade -y -qq

echo "[2/7] Installing Python and build tools..."
sudo apt-get install -y -qq python3 python3-venv python3-dev \
    build-essential curl git htop

echo "[3/7] Installing Node.js and pm2 for process management..."
if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y -qq nodejs
fi
sudo npm install -g pm2 2>/dev/null || true

echo "[4/7] Creating Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

echo "[5/7] Installing Python dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo "[6/7] Creating directories..."
mkdir -p data logs

echo "[7/7] Setting up environment file..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "  >>> IMPORTANT: Edit .env with your Helius RPC API key <<<"
    echo "      nano .env"
fi

chmod 600 .env

echo ""
echo "========================================================"
echo "  Setup complete."
echo "========================================================"
echo ""
echo "  Usage:"
echo "  1. Edit .env: nano .env"
echo "  2. Activate venv: source .venv/bin/activate"
echo "  3. Test run: python main.py"
echo "  4. Production (pm2): pm2 start ecosystem.config.js"
echo "  5. Monitor: pm2 logs solana-harvester"
echo "  6. Auto-start on boot: pm2 save && pm2 startup"
echo ""
echo "  Database will be at: data/quant_harvest.db"
echo "  Use any SQLite browser or Python to query results."
echo "========================================================"
