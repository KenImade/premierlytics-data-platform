import duckdb
import os

db_path = "/data/duckdb/premierlytics.duckdb"
os.makedirs("/data/duckdb", exist_ok=True)

# Create the file with open permissions
os.umask(0o000)
conn = duckdb.connect(db_path)
conn.close()

# Ensure permissions are open
os.chmod(db_path, 0o666)
print(f"DuckDB database created at {db_path}", flush=True)
