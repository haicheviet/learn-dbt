
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
    # Use the dbt executable from the same environment as this python process
    dbt_path = os.path.join(os.path.dirname(sys.executable), "dbt")

    print(">>> 0. Ingestion (Seeds & Bronze)")
    # Run seeds then create bronze views so snapshots can read them
    run_command(f"{dbt_path} seed")
    run_command(f"{dbt_path} run --select models/bronze")

    print(">>> 1. History Tracking (Snapshots)")
    run_command(f"{dbt_path} snapshot")

    print(">>> 2. Transformation (Silver & Gold)")
    # Run Silver/Gold transformations and tests
    run_command(f"{dbt_path} build --select models/silver models/gold")

    print(">>> 3. AI Engine [Shared Service]")
    # Python script processing
    process_data('learn_dbt.duckdb')

    print(">>> Pipeline Completed Successfully")

if __name__ == "__main__":
    main()
