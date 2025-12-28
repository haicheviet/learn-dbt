
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
    print(">>> 1. Ingestion (Bronze) & Transformation (Silver)")
    # Using 'dbt build' which includes seeds, run, and tests
    # Running seeds first to ensure source tables exist
    run_command("source .venv/bin/activate && dbt seed")
    
    # Run Bronze/Silver transformations and tests
    # Including bronze to ensure views are created
    run_command("source .venv/bin/activate && dbt build --select models/bronze models/silver")

    print(">>> 2. Transformation (Gold) [Gated]")
    # Only runs if Silver passing
    run_command("source .venv/bin/activate && dbt build --select models/gold")

    print(">>> 3. AI Engine [Shared Service]")
    # Python script processing
    process_data('learn_dbt.duckdb')

    print(">>> Pipeline Completed Successfully")

if __name__ == "__main__":
    main()
