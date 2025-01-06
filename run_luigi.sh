#!/bin/bash

echo "========== Start dbt with Luigi Orcestration Process =========="

# Virtual Environment Path
VENV_PATH="/home/laode/pacmann/course/data-storage/week-6/.venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/home/laode/pacmann/course/data-storage/week-6/elt_pipeline.py"

# Run Python Script and Insert Log Process
python3 "$PYTHON_SCRIPT" >> /home/laode/pacmann/course/data-storage/week-6/logs/luigi_process.log 2>&1

# Luigi info simple log
dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi started at ${dt}" >> /home/laode/pacmann/course/data-storage/week-6/logs/luigi_info.log

echo "========== End of dbt with Luigi Orcestration Process =========="