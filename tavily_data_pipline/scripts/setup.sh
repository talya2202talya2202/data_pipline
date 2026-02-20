#!/usr/bin/env bash
# One-time setup: create venv, install dependencies, create .env from template.
# Run from repo root: ./scripts/setup.sh   OR   bash scripts/setup.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Tavily Data Pipeline — Setup ==="

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
else
  echo "Virtual environment already exists."
fi

# Activate and install dependencies
echo "Installing dependencies..."
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Create .env from template if missing
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "Created .env from .env.example."
else
  echo ".env already exists."
fi

echo ""
echo "=== Setup complete ==="
echo "Next steps:"
echo "  1. Edit .env and add your secrets (at minimum: TAVILY_API_KEY for the agent)."
echo "  2. Activate the virtual environment (if not already):"
echo "       source venv/bin/activate"
echo "  3. Run the agent:"
echo "       python scripts/run_agent.py \"Nvidia\""
echo "  4. Run the dashboard (optional):"
echo "       streamlit run src/dashboard/app.py"
echo ""
