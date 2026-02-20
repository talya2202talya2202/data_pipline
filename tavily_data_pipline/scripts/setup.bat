@echo off
REM One-time setup: create venv, install dependencies, create .env from template.
REM Run from repo root: scripts\setup.bat

cd /d "%~dp0\.."

echo === Tavily Data Pipeline — Setup ===

if not exist "venv" (
  echo Creating virtual environment...
  python -m venv venv
) else (
  echo Virtual environment already exists.
)

echo Installing dependencies...
call venv\Scripts\activate.bat
pip install -q --upgrade pip
pip install -q -r requirements.txt

if not exist ".env" (
  copy .env.example .env
  echo Created .env from .env.example.
) else (
  echo .env already exists.
)

echo.
echo === Setup complete ===
echo Next steps:
echo   1. Edit .env and add your secrets (at minimum: TAVILY_API_KEY for the agent).
echo   2. Activate the virtual environment (if not already): venv\Scripts\activate
echo   3. Run the agent: python scripts\run_agent.py "Nvidia"
echo   4. Run the dashboard (optional): streamlit run src\dashboard\app.py
echo.
pause
