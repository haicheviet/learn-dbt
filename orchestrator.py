
import os
import subprocess
import sys
from src.ai_engine import process_data

def run_command(command):
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, executable='/bin/bash')
    if result.returncode != 0:
        print(f"Command failed: {command}")
        sys.exit(1)

def main():
    print(">>> 0. Ingestion (Seeds & Bronze)")
    # Run seeds then create bronze views so snapshots can read them
    run_command("source .venv/bin/activate && dbt seed")
    run_command("source .venv/bin/activate && dbt run --select models/bronze")

    print(">>> 1. History Tracking (Snapshots)")
    run_command("source .venv/bin/activate && dbt snapshot")

    print(">>> 2. Transformation (Silver & Gold)")
    # Run Silver/Gold transformations and tests
    run_command("source .venv/bin/activate && dbt build --select models/silver models/gold")

    print(">>> 3. AI Engine [Shared Service]")
    # Python script processing
    process_data('learn_dbt.duckdb')

    print(">>> Pipeline Completed Successfully")

if __name__ == "__main__":
    main()
