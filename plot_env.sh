#!/usr/bin/env bash
set -e

if [[ ! -x "$(command -v python3)" ]]; then
  echo "Python 3 is required. Install python3 and try again."
  exit 1
fi

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

echo "Python virtual environment created in .venv"
echo "Activate it with: source .venv/bin/activate"
echo "Then run: python3 plot_concurrency.py"
