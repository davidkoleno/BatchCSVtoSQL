import os
import glob
import re
import pyodbc

# =========================
# CONFIG (EDIT THESE)
# =========================
# Folder containing CSVs.
# If SQL Server is remote, this SHOULD be a UNC share SQL Server can access, e.g. r"\\MYPC\Share\BigCSVs"
CSV_ROOT = r"D:\BigCSVs"

RECURSIVE = True
CSV_PATTERN = "*.csv"

SCHEMA = "dbo"
TABLE = "BigCsvTable"

# SQL Server connection
SERVER = r"YOURSERVER\SQLEXPRESS"
DATABASE = "YourDB"

# Auth
USE_WINDOWS_AUTH = True
SQL_USERNAME = "your_user"
SQL_PASSWORD = "your_pass"

# CSV parsing
FIELDTERMINATOR = ","
# Common choices:
# - "0x0a" for \n
# - "0x0d0a" for Windows CRLF
ROWTERMINATOR = "0x0a"
FIRSTROW = 2  # skip header row

# Encoding (uncomment if needed)
# CODEPAGE = "65001"  # UTF-8
CODEPAGE = None

# Table column sizing strategy (fast, simple)
# This avoids the expensive full profiling step.
# If you need zero risk of truncation without VARCHAR(MAX), you must profile lengths.
DEFAULT_VARCHAR_LEN = 4000  # good performance; truncates if any value exceeds this

# BULK INSERT knobs
BATCHSIZE = 50_000
MAXERRORS = 0               # 0 = fail fast on any error
USE_TABLOCK = True


# =========================
# HELPERS
# =========================
def list_csv_files(root: str) -> list[str]:
    pattern = os.path.join(root, "**", CSV_PATTERN) if RECURSIVE else os.path.join(root, CSV_PATTERN)
    return sorted(glob.iglob(pattern, recursive=RECURSIVE))

def connect() -> pyodbc.Connection:
    if USE_WINDOWS_AUTH:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={SERVER};DATABASE={DATABASE};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={SERVER};DATABASE={DATABASE};"
            f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
            "TrustServerCertificate=yes;"
        )
    cn = pyodbc.connect(conn_str, autocommit=True)
    return cn

def qident(name: str) -> str:
    # SQL Server identifier quoting
    return "[" + name.replace("]", "]]") + "]"

def qstr(s: str) -> str:
    # SQL Server string literal quoting
    return "'" + s.replace("'", "''") + "'"

def table_exists(cur: pyodbc.Cursor, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (schema, table),
    )
    return cur.fetchone() is not None

def read_header_columns(first_csv: str) -> list[str]:
    # Read first line (header) safely. Assumes no embedded newlines in header.
    with open(first_csv, "r", encoding="utf-8-sig", errors="replace") as f:
        header = f.readline().rstrip("\r\n")
    # Basic CSV header split (works for simple headers; if your headers have quoted commas, tell me)
    cols = [c.strip() for c in header.split(FIELDTERMINATOR)]
    if not cols or any(c == "" for c in cols):
        raise ValueError(f"Could not parse header from: {first_csv}")
    return cols

def create_table_from_header(cur: pyodbc.Cursor, schema: str, table: str, columns: list[str]) -> None:
    # Make SQL-safe column names while preserving uniqueness
    # (If your headers are already clean, this just brackets them.)
    safe_cols = []
    seen = set()
    for c in columns:
        col = c
        # If you want to normalize column names, uncomment:
        # col = re.sub(r"\s+", " ", col).strip()
        if col in seen:
            i = 2
            while f"{col}_{i}" in seen:
                i += 1
            col = f"{col}_{i}"
        seen.add(col)
        safe_cols.append(col)

    col_defs = ",\n  ".join([f"{qident(c)} VARCHAR({DEFAULT_VARCHAR_LEN}) NULL" for c in safe_cols])
    sql = f"CREATE TABLE {qident(schema)}.{qident(table)} (\n  {col_defs}\n);"
    cur.execute(sql)

def truncate_table(cur: pyodbc.Cursor, schema: str, table: str) -> None:
    cur.execute(f"TRUNCATE TABLE {qident(schema)}.{qident(table)};")

def bulk_insert_file(cur: pyodbc.Cursor, schema: str, table: str, path: str) -> None:
    # BULK INSERT needs a literal path in dynamic SQL; parameters do not work reliably here.
    opts = [
        f"FIRSTROW = {FIRSTROW}",
        f"FIELDTERMINATOR = {qstr(FIELDTERMINATOR)}",
        f"ROWTERMINATOR = {qstr(ROWTERMINATOR)}",
        f"BATCHSIZE = {BATCHSIZE}",
        f"MAXERRORS = {MAXERRORS}",
        "KEEPNULLS",
    ]
    if USE_TABLOCK:
        opts.append("TABLOCK")
    if CODEPAGE:
        opts.append(f"CODEPAGE = {qstr(CODEPAGE)}")

    opt_block = ",\n    ".join(opts)

    sql = f"""
    BULK INSERT {qident(schema)}.{qident(table)}
    FROM {qstr(path)}
    WITH (
        {opt_block}
    );
    """
    cur.execute(sql)

def main():
    files = list_csv_files(CSV_ROOT)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {CSV_ROOT}")

    print(f"Found {len(files)} CSV files.")
    print("Connecting to SQL Server...")
    cn = connect()
    cur = cn.cursor()

    if table_exists(cur, SCHEMA, TABLE):
        print(f"Table {SCHEMA}.{TABLE} exists -> TRUNCATE")
        truncate_table(cur, SCHEMA, TABLE)
    else:
        print(f"Table {SCHEMA}.{TABLE} does not exist -> CREATE from header")
        cols = read_header_columns(files[0])
        create_table_from_header(cur, SCHEMA, TABLE, cols)

    print("Starting BULK INSERT...")
    for i, fpath in enumerate(files, start=1):
        # Show progress occasionally
        if i == 1 or i % 25 == 0:
            print(f"[{i}/{len(files)}] {fpath}")
        bulk_insert_file(cur, SCHEMA, TABLE, fpath)

    print("Done.")

if __name__ == "__main__":
    main()
