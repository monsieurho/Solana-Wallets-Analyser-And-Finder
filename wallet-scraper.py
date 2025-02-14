import asyncio
import aiohttp
import aiofiles
import csv
import time
from datetime import datetime, timedelta
import logging
import sys
from aiolimiter import AsyncLimiter
import gc

# ----------------------- Common Configuration -----------------------

# Replace this with your actual Solscan API key
API_KEY = "YOUR_SOLSCAN_API_KEY"
if not API_KEY or API_KEY == "YOUR_SOLSCAN_API_KEY":
    print("Please replace 'YOUR_SOLSCAN_API_KEY' with your actual Solscan API key in the script.")
    sys.exit(1)

# Configure logging (shared by both functionalities)
LOG_FILENAME = f"solana_transfer_combined_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILENAME),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ----------------------- Exchange Outflows Configuration -----------------------

# List of exchanges and their wallet addresses
EXCHANGES = {
    "okx": "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD",
    "binance": "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9",
    "kraken": "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5",
    "kucoin": "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6",
    "coinbase": "9obNtb5GyUegcs3a1CbBkLuc5hEWynWfJC6gjz5uWQkE",
    "bybit": "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2",
    "bitget": "A77HErqtfN1hLLpvZ9pCtu66FEtM8BveoaKbbMoZ4RiR",
    "mexc": "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ",
    "FixedFloat": "5ndLnEYqSFiA5yUFHo6LVZ1eWc6Rhh11K5CfJNkoHEPs",
    "Backpack": "43DbAvKxhXh1oSxkJSqGosNw3HpBnmsWiak6tB5wpecN"
}

# Solana token address for native SOL
SOL_TOKEN_ADDRESS = "So11111111111111111111111111111111111111111"

# API Rate Limit Configurations for Exchange Outflows
EX_RATE_LIMIT = 1000  # requests per 60 seconds
EX_PER_SECOND_LIMIT = 16  # Approximate per-second limit

# Transfer Filtering Criteria for Exchange Outflows
TOTAL_PAGES = 500  # Default value; will be overridden by user input.
PAGE_SIZE = 100     # Number of transfers per page
AMOUNT_THRESHOLD_SOL = 2  # Minimum amount in SOL
AMOUNT_MAX_SOL = 99999999999  # Maximum amount in SOL

# Retry Configuration for Exchange Outflows
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds
BACKOFF_FACTOR = 2
MAX_BACKOFF = 60  # seconds

# Initialize Rate Limiters for Exchange Outflows
ex_global_rate_limiter = AsyncLimiter(max_rate=EX_RATE_LIMIT, time_period=60)
ex_per_second_rate_limiter = AsyncLimiter(max_rate=EX_PER_SECOND_LIMIT, time_period=1)

# ----------------------- Defi Exchanges Configuration -----------------------

# These will be set based on user input:
WALLETS = {}  # Dict to hold selected wallet names and addresses.
MAX_PAGES_DEF = 50000  # Default value; will be overridden by user input.

# API Rate Limit Configurations for Defi Exchanges
DF_RATE_LIMIT = 1000  # requests per 60 seconds
DF_PER_SECOND_LIMIT = 20  # Approximate per-second limit

# Transfer Filtering Criteria for Defi Exchanges
PAGE_SIZE_DEF = 100     # Number of activities per page
TIME_THRESHOLD = timedelta(hours=24)  # 24 hours cutoff

# Initialize Rate Limiters for Defi Exchanges
df_global_rate_limiter = AsyncLimiter(max_rate=DF_RATE_LIMIT, time_period=60)
df_per_second_rate_limiter = AsyncLimiter(max_rate=DF_PER_SECOND_LIMIT, time_period=1)

# Define maximum concurrent workers and semaphore for Defi Exchanges
MAX_CONCURRENT_WORKERS = 50
semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)

# ----------------------- Exchange Outflows Functions -----------------------

async def fetch_page_exchange(session, url, headers, exchange_name, page):
    """Fetch a single page of transfers for Exchange Outflows with retries."""
    attempt = 0
    backoff = INITIAL_BACKOFF

    while attempt <= MAX_RETRIES:
        async with ex_global_rate_limiter, ex_per_second_rate_limiter:
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if not data.get("success"):
                            logger.error(f"[{exchange_name}] API response unsuccessful for page {page}")
                            logger.debug(f"Response: {data}")
                            return None
                        transfers = data.get("data", [])
                        if not transfers:
                            logger.info(f"[{exchange_name}] No transfers found on page {page}. Ending pagination.")
                        return transfers
                    elif response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after else backoff
                        logger.warning(f"[{exchange_name}] Rate limited on page {page}. Retrying after {wait_time} seconds.")
                        await asyncio.sleep(wait_time)
                        attempt += 1
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                    elif 500 <= response.status < 600:
                        logger.warning(f"[{exchange_name}] Server error {response.status} on page {page}. Retrying in {backoff} seconds.")
                        await asyncio.sleep(backoff)
                        attempt += 1
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                    else:
                        text = await response.text()
                        logger.error(f"[{exchange_name}] Error {response.status} on page {page}. Response: {text}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"[{exchange_name}] Timeout on page {page}. Retrying in {backoff} seconds.")
                await asyncio.sleep(backoff)
                attempt += 1
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
            except aiohttp.ClientError as e:
                logger.warning(f"[{exchange_name}] Client error on page {page}: {e}. Retrying in {backoff} seconds.")
                await asyncio.sleep(backoff)
                attempt += 1
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
            except Exception as e:
                logger.error(f"[{exchange_name}] Unexpected error on page {page}: {e}")
                return None

    logger.error(f"[{exchange_name}] Exceeded maximum retries on page {page}. Skipping.")
    return None

def construct_url_exchange(wallet_address, page, amount_min, amount_max):
    """Construct the API URL for Exchange Outflows."""
    return (
        f"https://pro-api.solscan.io/v2.0/account/transfer"
        f"?address={wallet_address}"
        f"&activity_type[]=ACTIVITY_SPL_TRANSFER"
        f"&token={SOL_TOKEN_ADDRESS}"
        f"&flow=out"
        f"&page={page}"
        f"&page_size={PAGE_SIZE}"
        f"&sort_by=block_time"
        f"&sort_order=desc"
        f"&amount[]={amount_min}"
        f"&amount[]={amount_max}"
    )

async def process_exchange(session, exchange_name, wallet_address, global_set):
    """Process a single exchange and add unique destination addresses to a shared global set."""
    logger.info(f"Processing exchange: {exchange_name}")
    pages_queue = asyncio.Queue()

    # Enqueue pages from 1 to TOTAL_PAGES
    for page in range(1, TOTAL_PAGES + 1):
        await pages_queue.put(page)

    async def worker():
        while True:
            page = await pages_queue.get()
            if page is None:
                pages_queue.task_done()
                break
            url = construct_url_exchange(wallet_address, page, AMOUNT_THRESHOLD_SOL, AMOUNT_MAX_SOL)
            transfers = await fetch_page_exchange(session, url, {"token": API_KEY}, exchange_name, page)
            if transfers is None:
                logger.warning(f"[{exchange_name}] Skipping page {page} due to errors.")
                pages_queue.task_done()
                continue
            for transfer in transfers:
                to_address = transfer.get("to_address")
                # Use the global set for deduplication
                if to_address and to_address not in global_set:
                    global_set.add(to_address)
            if not transfers:
                logger.info(f"[{exchange_name}] No transfers on page {page}. Ending pagination.")
                # Drain the queue to stop further processing
                while not pages_queue.empty():
                    await pages_queue.get()
                    pages_queue.task_done()
                break
            logger.info(f"[{exchange_name}] Fetched page {page} (Total unique addresses so far: {len(global_set)})")
            pages_queue.task_done()

    # Start worker tasks based on the per-second limit
    tasks = [asyncio.create_task(worker()) for _ in range(EX_PER_SECOND_LIMIT)]
    await pages_queue.join()
    # Stop workers
    for _ in range(EX_PER_SECOND_LIMIT):
        await pages_queue.put(None)
    await asyncio.gather(*tasks)

async def main_exchange_outflows():
    start_time = time.time()
    # Use a global set for deduplication across all exchanges
    global_set = set()
    connector = aiohttp.TCPConnector(limit=None)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for exchange_name, wallet_address in EXCHANGES.items():
            task = asyncio.create_task(process_exchange(session, exchange_name, wallet_address, global_set))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(f"Total unique addresses collected across exchanges: {len(global_set)}")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"combined_wallets_{timestamp}.csv"
    try:
        with open(combined_filename, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["wallet"])
            for address in global_set:
                writer.writerow([address])
        logger.info(f"Successfully wrote {len(global_set)} unique addresses to {combined_filename}")
    except Exception as e:
        logger.error(f"Failed to write CSV: {e}")

    elapsed = time.time() - start_time
    logger.info(f"Exchange Outflows completed in {elapsed:.2f} seconds.")

# ----------------------- Defi Exchanges Functions -----------------------

def select_wallets():
    available_wallets = {
        "1": ("Pumpfun", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
        "2": ("Orca", "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"),
        "3": ("Raydium", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")
    }

    print("Select Exchange(s) To Scrape:")
    for key, (name, _) in available_wallets.items():
        print(f"{key}. {name}")

    while True:
        selection = input("Enter the numbers separated by commas (e.g., 1,3): ")
        selected_keys = [s.strip() for s in selection.split(",") if s.strip() in available_wallets]
        if selected_keys:
            break
        else:
            print("Invalid selection. Please try again.")

    selected_wallets = {}
    for key in selected_keys:
        name, address = available_wallets[key]
        selected_wallets[name.lower()] = address

    return selected_wallets

def get_max_pages():
    while True:
        user_input = input("Enter the maximum number of pages to scrape: ")
        if user_input.isdigit() and int(user_input) > 0:
            return int(user_input)
        else:
            print("Please enter a positive integer.")

async def fetch_page_defi(session, url, headers, wallet_name, page):
    """Fetch a single page of Defi activities with retries."""
    attempt = 0
    backoff = INITIAL_BACKOFF

    while attempt <= MAX_RETRIES:
        async with df_global_rate_limiter, df_per_second_rate_limiter:
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if not data.get("success"):
                            logger.warning(f"[{wallet_name}] API response unsuccessful for page {page}")
                            return None
                        activities = data.get("data", [])
                        if not activities:
                            logger.info(f"[{wallet_name}] No activities on page {page}. Ending pagination.")
                        return activities
                    elif response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after else backoff
                        logger.warning(f"[{wallet_name}] Rate limited on page {page}. Retrying after {wait_time} seconds.")
                        await asyncio.sleep(wait_time)
                        attempt += 1
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                    elif 500 <= response.status < 600:
                        logger.warning(f"[{wallet_name}] Server error {response.status} on page {page}. Retrying in {backoff} seconds.")
                        await asyncio.sleep(backoff)
                        attempt += 1
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                    else:
                        text = await response.text()
                        logger.error(f"[{wallet_name}] Error {response.status} on page {page}. Response: {text}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"[{wallet_name}] Timeout on page {page}. Retrying in {backoff} seconds.")
                await asyncio.sleep(backoff)
                attempt += 1
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
            except aiohttp.ClientError as e:
                logger.warning(f"[{wallet_name}] Client error on page {page}: {e}. Retrying in {backoff} seconds.")
                await asyncio.sleep(backoff)
                attempt += 1
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
            except Exception as e:
                logger.error(f"[{wallet_name}] Unexpected error on page {page}: {e}")
                return None

    logger.error(f"[{wallet_name}] Exceeded maximum retries on page {page}. Skipping.")
    return None

def construct_url_defi(program_address, page):
    """Construct the API URL for Defi Exchanges."""
    return (
        f"https://pro-api.solscan.io/v2.0/account/defi/activities"
        f"?address={program_address}"
        f"&activity_type[]=ACTIVITY_TOKEN_SWAP"
        f"&page={page}"
        f"&page_size={PAGE_SIZE_DEF}"
        f"&sort_by=block_time"
        f"&sort_order=desc"
    )

async def write_addresses_defi(file_handle, addresses):
    """Write a batch of addresses to the CSV file."""
    for address in addresses:
        await file_handle.write(f"{address}\n")

async def process_wallet_defi(session, wallet_name, program_address, cutoff_time, file_handle, global_set):
    logger.info(f"[{wallet_name}] Starting processing for wallet: {program_address}")
    pages_processed = 0

    async def process_page(page):
        nonlocal pages_processed
        url = construct_url_defi(program_address, page)
        activities = await fetch_page_defi(session, url, {"token": API_KEY}, wallet_name, page)
        if activities is None:
            logger.warning(f"[{wallet_name}] Skipping page {page} due to errors.")
            return
        if not activities:
            logger.info(f"[{wallet_name}] No activities on page {page}. Ending pagination.")
            raise StopAsyncIteration
        batch_addresses = []
        for activity in activities:
            activity_time_str = activity.get("time")
            if not activity_time_str:
                continue
            try:
                activity_time = datetime.strptime(activity_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                try:
                    activity_time = datetime.strptime(activity_time_str, "%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    logger.warning(f"[{wallet_name}] Unable to parse time: {activity_time_str}")
                    continue
            if activity_time < cutoff_time:
                logger.info(f"[{wallet_name}] Reached cutoff on page {page} at time {activity_time_str}. Stopping further requests.")
                raise StopAsyncIteration
            from_address = activity.get("from_address")
            if from_address and from_address not in global_set:
                batch_addresses.append(from_address)
                global_set.add(from_address)
        if batch_addresses:
            await write_addresses_defi(file_handle, batch_addresses)
            logger.info(f"[{wallet_name}] Page {page}: Collected {len(batch_addresses)} new addresses.")
        pages_processed += 1
        if pages_processed % 1000 == 0:
            gc.collect()
            logger.info(f"[{wallet_name}] Garbage collection triggered after {pages_processed} pages.")

    try:
        page_numbers = list(range(1, MAX_PAGES_DEF + 1))
        tasks = []
        for page in page_numbers:
            await semaphore.acquire()
            task = asyncio.create_task(process_page(page))
            tasks.append(task)
            def _callback(fut):
                semaphore.release()
                try:
                    fut.result()
                except StopAsyncIteration:
                    for t in tasks:
                        if not t.done():
                            t.cancel()
            task.add_done_callback(_callback)
        await asyncio.gather(*tasks, return_exceptions=True)
    except StopAsyncIteration:
        logger.info(f"[{wallet_name}] Pagination stopped as cutoff time was reached.")
    except Exception as e:
        logger.error(f"[{wallet_name}] Error: {e}")

    logger.info(f"[{wallet_name}] Finished processing. Total unique addresses collected: {len(global_set)}")

async def main_defi_exchanges():
    start_time = time.time()
    connector = aiohttp.TCPConnector(limit=1000)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"defi_from_addresses_{timestamp}.csv"
    async with aiofiles.open(combined_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        await csv_file.write("wallet\n")
        global_set = set()
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            current_time = datetime.utcnow()
            cutoff_time = current_time - TIME_THRESHOLD
            for wallet_name, program_address in WALLETS.items():
                task = asyncio.create_task(
                    process_wallet_defi(session, wallet_name, program_address, cutoff_time, csv_file, global_set)
                )
                tasks.append(task)
            await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Addresses have been written to {combined_filename}")
    elapsed = time.time() - start_time
    logger.info(f"Defi Exchanges completed in {elapsed:.2f} seconds.")

# ----------------------- Main Menu -----------------------

def main_menu():
    print("Select the script to run:")
    print("1. Exchange Outflows")
    print("2. Defi Exchanges")
    choice = input("Enter 1 or 2: ").strip()
    if choice == "1":
        try:
            pages = int(input("Enter the number of pages to scan for Exchange Outflows: "))
            global TOTAL_PAGES
            TOTAL_PAGES = pages
        except ValueError:
            print("Invalid input. Using default of 500 pages.")
            TOTAL_PAGES = 500
        try:
            asyncio.run(main_exchange_outflows())
        except KeyboardInterrupt:
            logger.info("Exchange Outflows terminated by user.")
        except Exception as e:
            logger.error(f"Unexpected error in Exchange Outflows: {e}")
    elif choice == "2":
        global WALLETS, MAX_PAGES_DEF
        WALLETS = select_wallets()
        MAX_PAGES_DEF = get_max_pages()
        try:
            asyncio.run(main_defi_exchanges())
        except KeyboardInterrupt:
            logger.info("Defi Exchanges terminated by user.")
        except Exception as e:
            logger.error(f"Unexpected error in Defi Exchanges: {e}")
    else:
        print("Invalid choice. Please run the script again and select either 1 or 2.")

if __name__ == "__main__":
    main_menu()
