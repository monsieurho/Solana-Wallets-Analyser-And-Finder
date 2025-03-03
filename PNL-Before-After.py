import os
import glob
import csv
import asyncio
import aiohttp
import logging
import random
from aiolimiter import AsyncLimiter

# ---------------------------- Configuration ---------------------------- #
API_KEY = "xxxxxxxxxxxxxxxxx"  # Replace with your actual API key
BASE_URL = "https://data.solanatracker.io"
RATE_LIMIT = 100             # 100 requests per second (as in the reference script)
CONCURRENCY_LIMIT = 100      # Maximum concurrent connections

# Retry configuration
MAX_RETRIES = 5              # Maximum number of retries per request
RETRY_BACKOFF_FACTOR = 5     # Base backoff in seconds

# ---------------------------- Logging Setup ---------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize the async rate limiter
limiter = AsyncLimiter(max_rate=RATE_LIMIT, time_period=1)

async def fetch_pnl_now(session: aiohttp.ClientSession, wallet: str, retry_count: int = 0) -> float:
    """
    Asynchronously fetches PnL Now from the Solanatracker API for a given wallet.
    Implements retry with exponential backoff if errors occur.
    Returns the total PnL (as a float) from the "summary" section.
    """
    url = f"{BASE_URL}/pnl/{wallet}"
    headers = {"x-api-key": API_KEY}
    
    try:
        async with limiter:
            async with session.get(url, headers=headers, timeout=60) as response:
                if response.status == 429:
                    # Handle rate limiting by checking Retry-After header if provided.
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        delay = float(retry_after)
                    else:
                        delay = RETRY_BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0, 1)
                    if retry_count < MAX_RETRIES:
                        logging.warning(f"[API] Wallet {wallet}: Received 429. Retrying in {delay:.2f} seconds (Retry {retry_count+1}/{MAX_RETRIES}).")
                        await asyncio.sleep(delay)
                        return await fetch_pnl_now(session, wallet, retry_count + 1)
                    else:
                        logging.error(f"[API] Wallet {wallet}: Exceeded max retries after 429 responses.")
                        return None
                elif response.status != 200:
                    # Retry for any non-200 status code.
                    if retry_count < MAX_RETRIES:
                        delay = RETRY_BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0, 1)
                        logging.warning(f"[API] Wallet {wallet}: Received status {response.status}. Retrying in {delay:.2f} seconds (Retry {retry_count+1}/{MAX_RETRIES}).")
                        await asyncio.sleep(delay)
                        return await fetch_pnl_now(session, wallet, retry_count + 1)
                    else:
                        logging.error(f"[API] Wallet {wallet}: Exceeded max retries for status {response.status}.")
                        return None

                data = await response.json()
                summary = data.get("summary", {})
                pnl_now = summary.get("total")
                if pnl_now is None:
                    logging.warning(f"[API] Wallet {wallet}: 'total' not found in API response summary.")
                    return None
                return float(pnl_now)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        if retry_count < MAX_RETRIES:
            delay = RETRY_BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0, 1)
            logging.warning(f"[API] Wallet {wallet}: Exception '{e}'. Retrying in {delay:.2f} seconds (Retry {retry_count+1}/{MAX_RETRIES}).")
            await asyncio.sleep(delay)
            return await fetch_pnl_now(session, wallet, retry_count + 1)
        else:
            logging.error(f"[API] Wallet {wallet}: Exceeded max retries after exception: {e}")
            return None

async def process_wallet(session: aiohttp.ClientSession, row: dict, index: int, total: int) -> dict:
    """
    Processes a single wallet row:
      - Calculates PnL Before (Realized_Gains + Unrealized_Gains)
      - Retrieves PnL Now from the API with retry/backoff
      - Calculates the percent change
    Returns a dictionary with the wallet data or None if processing fails.
    """
    wallet = row.get("wallet", "").strip()
    if not wallet:
        logging.warning(f"[Row {index}] Skipping row: no wallet address.")
        return None

    try:
        realized = float(row.get("Realized_Gains", 0))
        unrealized = float(row.get("Unrealized_Gains", 0))
    except ValueError:
        logging.error(f"[Row {index} - {wallet}] Could not convert gains to float. Skipping wallet.")
        return None

    pnl_before = realized + unrealized
    logging.info(f"[{index}/{total}] Processing wallet {wallet} | PnL Before: {pnl_before:.2f}")

    pnl_now = await fetch_pnl_now(session, wallet)
    if pnl_now is None:
        logging.warning(f"[{index}/{total}] Skipping wallet {wallet} due to API error.")
        return None

    # Calculate percent change; avoid division by zero
    percent_change = ((pnl_now - pnl_before) / pnl_before * 100) if pnl_before != 0 else 0.0
    logging.info(f"[{index}/{total}] Wallet {wallet} | PnL Now: {pnl_now:.2f} | Percent Change: {percent_change:.2f}%")

    return {
        "wallet": wallet,
        "pnl_before": pnl_before,
        "pnl_now": pnl_now,
        "percent_change": percent_change
    }

async def process_csv_file(csv_file: str):
    """
    Processes one CSV file:
      - Reads the CSV file
      - Creates asynchronous tasks to process each wallet
      - Sorts results by highest percent change
      - Writes the results into a Markdown file with the same base name
    """
    logging.info(f"[File] Processing CSV file: {csv_file}")

    # Read CSV rows (synchronously; file I/O is fast compared to API calls)
    with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    total_wallets = len(rows)
    logging.info(f"[File] Found {total_wallets} wallet entries in {csv_file}")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)  # Limit concurrent connections
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            process_wallet(session, row, i, total_wallets)
            for i, row in enumerate(rows, start=1)
        ]
        processed = await asyncio.gather(*tasks)
        # Filter out any None results
        results = [r for r in processed if r is not None]

    # Sort the results so that highest percent change is at the top
    results.sort(key=lambda x: x["percent_change"], reverse=True)

    # Build Markdown table content
    md_lines = [
        "| Wallet Address | PnL Before | PnL Now | Percent Change |",
        "| --- | --- | --- | --- |"
    ]
    for item in results:
        md_lines.append(
            f"| {item['wallet']} | ${item['pnl_before']:,.2f} | ${item['pnl_now']:,.2f} | {item['percent_change']:.2f}% |"
        )
    md_file = os.path.splitext(csv_file)[0] + ".md"
    with open(md_file, mode="w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    logging.info(f"[File] Finished processing {csv_file}. Markdown output written to {md_file}")

async def main():
    """
    Main entry point: processes all CSV files in the current directory.
    """
    csv_files = glob.glob("*.csv")
    if not csv_files:
        logging.error("No CSV files found in the current directory.")
        return

    logging.info(f"Found {len(csv_files)} CSV file(s) to process.\n")
    for csv_file in csv_files:
        await process_csv_file(csv_file)

if __name__ == "__main__":
    asyncio.run(main())
