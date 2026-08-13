from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

# --- CONFIG ---
HAMMER_URL = "https://jacobfilipp.com/hammerdata/hammer-5-csv.zip"
DATA_DIR = Path("data")
ZIP_PATH = DATA_DIR / "hammer-5-csv.zip"
MARKER_PATH = DATA_DIR / ".last_modified" # remembers last file version that was loaded
CHUNK_SIZE = 500_000

PRODUCT_TABLE = "PRODUCT"
PRICE_TABLE = "PRICE"
PRICE_WATERMARK_COL = "NOWTIME"

# --- Connecting to Snowflake (use credentials from .env) ---
load_dotenv()

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)

# --- DETECTING CHANGES ---
def remote_stamp(url: str) -> str:
    head = requests.head(url, timeout=30, allow_redirects=True)
    head.raise_for_status()
    return head.headers.get("ETag") or head.headers.get("Last-Modified") or ""

def read_marker() -> str:
    return MARKER_PATH.read_text() if MARKER_PATH.exists() else ""

# Stream the archive to disk (keep off RAM; deleted after loading)
def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0
        with open(dest, "wb") as f:
            for block in r.iter_content(chunk_size=1 << 20): # 1 MB blocks
                f.write(block)
                done += len(block)
                if total:
                    print(f"\r  {done/1e6:6.0f} / {total/1e6:.0f} MB "
                          f"({done/total*100:4.1f}%)", end="", flush=True)
                else:
                    print(f"\r  {done/1e6:6.0f} MB", end="", flush=True)
        print()
    print(f"  saved {dest} ({dest.stat().st_size / 1e6:.0f} MB)")

# Find the CSV file inside archive with name that contains `keyword`
def find_member(z: zipfile.ZipFile, keyword: str) -> str:
    for name in z.namelist():
        if name.lower().endswith(".csv") and keyword in name.lower():
            return name
    raise FileNotFoundError(f"No CSV containing '{keyword}'. Contents: {z.namelist()}")

# Yield uppercased-column string DataFrames read from archive
def read_chunks(z: zipfile.ZipFile, member: str):
    with z.open(member) as raw_f:
        text_f = io.TextIOWrapper(raw_f, encoding="utf-8-sig") # strips the BOM
        for chunk in pd.read_csv(text_f, dtype=str, chunksize=CHUNK_SIZE,
                                 skipinitialspace=True):
            chunk.columns = [c.strip().upper() for c in chunk.columns]
            yield chunk.reset_index(drop=True)

# Latest value of `col` already in RAW.<table>, or None if empty/not yet created
def get_watermark(table: str, col: str):
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(TRY_TO_TIMESTAMP({col})) FROM RAW.{table}")
        return cur.fetchone()[0]
    except snowflake.connector.errors.ProgrammingError:
        return None
    finally:
        cur.close()

# Overwrite RAW.<table> with the whole file
def load_full(z: zipfile.ZipFile, member: str, table: str) -> None:
    print(f"\n[full refresh] {member} -> RAW.{table}")
    total, created = 0, False
    for i, chunk in enumerate(read_chunks(z, member)):
        _, _, n, _ = write_pandas(conn, chunk, table_name=table,
                                  auto_create_table=not created,
                                  overwrite=not created, quote_identifiers=False)
        total += n
        created = True
        print(f"  chunk {i + 1}: +{n:,} rows (total {total:,})")
    print(f"  done: {total:,} rows")

# if `wcol` is newer than what's already in RAW.<table> -> append rows
def load_incremental(z: zipfile.ZipFile, member: str, table: str, wcol: str) -> None:
    watermark = get_watermark(table, wcol)
    mode = "first load (backfill)" if watermark is None else f"new rows where {wcol} > {watermark}"
    print(f"\n[incremental] {member} -> RAW.{table} ({mode})")
    total, created = 0, watermark is not None
    for i, chunk in enumerate(read_chunks(z, member)):
        if watermark is not None:
            ts = pd.to_datetime(chunk[wcol], errors="coerce")
            chunk = chunk[ts > watermark]
        if chunk.empty:
            continue
        _, _, n, _ = write_pandas(conn, chunk, table_name=table,
                                  auto_create_table=not created,
                                  overwrite=not created, quote_identifiers=False)
        total += n
        created = True
        print(f"  chunk {i + 1}: +{n:,} new rows (total {total:,})")
    print(f"  done: {total:,} new rows" + ("  (nothing new)" if total == 0 else ""))

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # no changes -> skip everything
    stamp = remote_stamp(HAMMER_URL)
    if stamp and stamp == read_marker():
        print("Source file unchanged since last run")
        return

    download(HAMMER_URL, ZIP_PATH)
    try:
        with zipfile.ZipFile(ZIP_PATH) as z:
            print("\nArchive contents:", z.namelist())
            load_full(z, find_member(z, "product"), PRODUCT_TABLE)
            load_full(z, find_member(z, "raw"), PRICE_TABLE)
    finally:
        ZIP_PATH.unlink(missing_ok=True)

    if stamp:
        MARKER_PATH.write_text(stamp)

    # summary
    print()
    cur = conn.cursor()
    for table in (PRODUCT_TABLE, PRICE_TABLE):
        cur.execute(f"SELECT COUNT(*) FROM RAW.{table}")
        print(f"RAW.{table}: {cur.fetchone()[0]:,} rows")
    cur.close()
    conn.close()
    print("\nIngestion done")

if __name__ == "__main__":
    main()
