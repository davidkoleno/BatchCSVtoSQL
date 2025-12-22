import os
import glob
from urllib.parse import quote_plus

import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, event, text


# =========================
# CONFIG (EDIT THESE)
# =========================
CSV_FOLDER = r"D:\BigCSVs"          # <-- folder containing your .csv files (non-recursive)
CSV_PATTERN = "*.csv"               # usually fine
CSV_SEPARATOR = ","                # change if needed (e.g. "|", "\t")
CSV_ENCODING = None                # e.g. "utf-8" if needed

SCHEMA = "dbo"
TABLE = "BigCsvTable"

# SQL Server connection
SERVER = r"YOURSERVER\SQLEXPRESS"  # <-- e.g. "localhost" or "SERVER\INSTANCE"
DATABASE = "YourDB"
USE_WINDOWS_AUTH = True            # True = Trusted_Connection; False = SQL login below

SQL_USERNAME = "your_user"         # used only if USE_WINDOWS_AUTH = False
SQL_PASSWORD = "your_pass"         # used only if USE_WINDOWS_AUTH = False

# Performance / safety knobs
CHUNKSIZE = 200_000                # rows per chunk for both profiling and loading
VARCHAR_MAX_THRESHOLD = 4000       # >4000 => VARCHAR(MAX)
BUCKETS = [1, 10, 25, 50, 100, 255, 500, 1000, 2000, 4000]  # rounded sizes


# =========================
# INTERNALS
# =========================
def escape_ident(name: str) -> str:
    return name.replace("]", "]]")


def bucket_size(max_len: int) -> str:
    if max_len <= 0:
        return "1"
    if max_len > VARCHAR_MAX_THRESHOLD:
        return "MAX"
    for b in BUCKETS:
        if max_len <= b:
            return str(b)
    return str(VARCHAR_MAX_THRESHOLD)


def make_engine():
    if USE_WINDOWS_AUTH:
        odbc = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={SERVER};DATABASE={DATABASE};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        odbc = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={SERVER};DATABASE={DATABASE};"
            f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
            "TrustServerCertificate=yes;"
        )

    conn_str = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc)
    engine = create_engine(conn_str, pool_pre_ping=True, future=True)

    # Speed boost
    @event.listens_for(engine, "before_cursor_execute")
    def _fast_executemany(conn, cursor, statement, parameters, context, executemany):
        if executemany:
            try:
                cursor.fast_executemany = True
            except Exception:
                pass

    return engine


def get_csv_files() -> list[str]:
    files = sorted(glob.glob(os.path.join(CSV_FOLDER, CSV_PATTERN)))
    if not files:
        raise FileNotFoundError(f"No CSV files found in: {CSV_FOLDER} (pattern: {CSV_PATTERN})")
    return files


def read_header_columns(first_csv: str) -> list[str]:
    cols = list(pd.read_csv(first_csv, nrows=0, sep=CSV_SEPARATOR, encoding=CSV_ENCODING).columns)
    return [c.strip() for c in cols]


def profile_max_lengths(files: list[str], columns: list[str]) -> dict:
    stats = {c: {"max_len": 0, "nullable": False} for c in columns}

    for path in tqdm(files, desc="Profiling CSVs", unit="file"):
        reader = pd.read_csv(
            path,
            sep=CSV_SEPARATOR,
            encoding=CSV_ENCODING,
            chunksize=CHUNKSIZE,
            dtype="string",
            keep_default_na=True,
            low_memory=False,
        )

        for df in reader:
            df.columns = [c.strip() for c in df.columns]

            for col in columns:
                s = df[col]

                if s.isna().any():
                    stats[col]["nullable"] = True

                lengths = s.fillna("").astype(str).str.len()
                m = int(lengths.max()) if not lengths.empty else 0
                if m > stats[col]["max_len"]:
                    stats[col]["max_len"] = m

    return stats


def generate_create_table_sql(stats: dict) -> str:
    col_lines = []
    for col, info in stats.items():
        size = bucket_size(info["max_len"])
        null_sql = "NULL" if info["nullable"] else "NOT NULL"
        col_lines.append(f"  [{escape_ident(col)}] VARCHAR({size}) {null_sql}")

    cols_block = ",\n".join(col_lines)

    return (
        f"IF OBJECT_ID(N'[{escape_ident(SCHEMA)}].[{escape_ident(TABLE)}]', N'U') IS NOT NULL\n"
        f"    DROP TABLE [{escape_ident(SCHEMA)}].[{escape_ident(TABLE)}];\n"
        f"GO\n"
        f"CREATE TABLE [{escape_ident(SCHEMA)}].[{escape_ident(TABLE)}] (\n"
        f"{cols_block}\n"
        f");"
    )


def drop_and_create_table(engine, create_sql: str):
    # "GO" isn't recognized by SQLAlchemy; split it ourselves.
    batches = [b.strip() for b in create_sql.split("GO") if b.strip()]
    with engine.begin() as conn:
        for b in batches:
            conn.execute(text(b))


def load_all_csvs(engine, files: list[str], columns: list[str]):
    for path in tqdm(files, desc="Loading CSVs", unit="file"):
        reader = pd.read_csv(
            path,
            sep=CSV_SEPARATOR,
            encoding=CSV_ENCODING,
            chunksize=CHUNKSIZE,
            dtype="string",
            keep_default_na=True,
            low_memory=False,
        )

        for df in reader:
            df.columns = [c.strip() for c in df.columns]

            # Ensure consistent column order (and catch missing columns early)
            df = df[columns]

            df.to_sql(
                name=TABLE,
                con=engine,
                schema=SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
            )


def main():
    print("Finding CSV files...")
    files = get_csv_files()
    print(f"Found {len(files)} CSV file(s).")

    print("Reading header columns...")
    columns = read_header_columns(files[0])
    print(f"{len(columns)} columns detected.")

    print("Profiling max lengths (chunked)...")
    stats = profile_max_lengths(files, columns)

    print("Generating CREATE TABLE SQL...")
    create_sql = generate_create_table_sql(stats)

    # Print the CREATE TABLE so you can keep it / review it
    print("\n========== CREATE TABLE SQL ==========\n")
    print(create_sql)
    print("\n======================================\n")

    print("Connecting to SQL Server...")
    engine = make_engine()

    print("Dropping and creating table...")
    drop_and_create_table(engine, create_sql)

    print("Loading all CSV files into table...")
    load_all_csvs(engine, files, columns)

    print(f"Done. Loaded {len(files)} file(s) into {SCHEMA}.{TABLE}")


if __name__ == "__main__":
    main()
