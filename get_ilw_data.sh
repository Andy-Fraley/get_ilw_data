#!/bin/bash

# Capture the original working directory before changing directories
ORIGINAL_CWD="$(pwd)"
export ORIGINAL_CWD

# Activate the virtual environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"

if [ ! -d "$VENV_DIR" ]; then
  echo "Virtual environment not found in $VENV_DIR. Please set up the venv directory first."
  exit 1
fi

source "$VENV_DIR/bin/activate"

# Change to the script directory to ensure the module can be found
cd "$SCRIPT_DIR"

# Run the CLI module, passing all arguments
python -m get_ilw_data.cli "$@" 
