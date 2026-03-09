from pathlib import Path


def load_sql(filename: str, caller_file: str) -> str:
    sql_dir = Path(caller_file).parent / "sql"
    return (sql_dir / filename).read_text()
