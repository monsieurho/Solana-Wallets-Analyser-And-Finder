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

# ----------------------- Helper Functions for User Input -----------------------

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
            print("Invalid selection. Please enter valid numbers corresponding to the wallets.")

    selected_wallets = {}
    for key in selected_keys:
        name, address = available_wallets[key]
        selected_wallets[name.lower()] = address

    return selected_wallets

def get_max_pages():
    while True:
        user_input = input("Enter the number of max pages to scrape: ")
        if user_input.isdigit() and int(user_input) > 0:
            return int(user_input)
        else:
            print("Please enter a positive integer.")

# ----------------------- Configuration -----------------------

# These will be set based on user input
WALLETS = {}
MAX_PAGES = 50000  # Default value, will be overridden by user input

# API Rate Limit Configuration
RATE_LIMIT = 1000  # requests per 60 seconds (adjust as per Solscan's actual rate limits)
PER_SECOND_LIMIT = 20  # Approximate per-second limit (adjust as needed)

# Transfer Filtering Criteria
PAGE_SIZE = 100     # Number of activities per page
TIME_THRESHOLD = timedelta(hours=24)  # 24 hours

# Retry Configuration for Exponential Backoff
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # in seconds
BACKOFF_FACTOR = 2
MAX_BACKOFF = 60  # in seconds

# Logging Configuration
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(
    level=logging.INFO,  # Set to INFO or WARNING to balance verbosity and performance
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout)  # Logs will be printed to the terminal only
    ]
)
logger = logging.getLogger(__name__)

# ----------------------- Initialize Rate Limiters -----------------------

# Global rate limiter: 1000 requests per 60 seconds
global_rate_limiter = AsyncLimiter(max_rate=RATE_LIMIT, time_period=60)

# Per-second rate limiter: 20 requests per second
per_second_rate_limiter = AsyncLimiter(max_rate=PER_SECOND_LIMIT, time_period=1)

# Define maximum concurrent workers
MAX_CONCURRENT_WORKERS = 50  # Adjust based on testing and system capacity

# Initialize a semaphore to control concurrency
semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)

# ----------------------- Helper Functions -----------------------

async def fetch_page(session, url, headers, wallet_name, page):
    """Fetch a single page of DeFi activities with retries and exponential backoff."""
    attempt = 0
    backoff = INITIAL_BACKOFF

    while attempt <= MAX_RETRIES:
        async with global_rate_limiter, per_second_rate_limiter:
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if not data.get("success"):
                            logger.warning(f"[{wallet_name}] API response unsuccessful for page {page}")
                            return None
                        activities = data.get("data", [])
                        if not activities:
                            logger.info(f"[{wallet_name}] No activities found on page {page}. Ending pagination.")
                        return activities
                    elif response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after else backoff
                        logger.warning(f"[{wallet_name}] Rate limited. Retrying after {wait_time} seconds for page {page}.")
                        await asyncio.sleep(wait_time)
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                        attempt += 1
                    elif 500 <= response.status < 600:
                        logger.warning(f"[{wallet_name}] Server error {response.status} for page {page}. Retrying in {backoff} seconds.")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                        attempt += 1
                    else:
                        text = await response.text()
                        logger.error(f"[{wallet_name}] Received status code {response.status} for page {page}")
                        logger.debug(f"[{wallet_name}] Response: {text}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"[{wallet_name}] Timeout for page {page}. Retrying in {backoff} seconds.")
                await asyncio.sleep(backoff)
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                attempt += 1
            except aiohttp.ClientError as e:
                logger.warning(f"[{wallet_name}] Client error for page {page}: {e}. Retrying in {backoff} seconds.")
                await asyncio.sleep(backoff)
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                attempt += 1
            except Exception as e:
                logger.error(f"[{wallet_name}] Unexpected error for page {page}: {e}")
                return None

    logger.error(f"[{wallet_name}] Exceeded maximum retries for page {page}. Skipping.")
    return None

def construct_url(program_address, page):
    """Construct the API URL with the required parameters."""
    return (
        f"https://pro-api.solscan.io/v2.0/account/defi/activities"
        f"?address={program_address}"
        f"&activity_type[]=ACTIVITY_TOKEN_SWAP"
        f"&page={page}"
        f"&page_size={PAGE_SIZE}"
        f"&sort_by=block_time"
        f"&sort_order=desc"
    )

async def write_addresses(file_handle, addresses):
    """Asynchronously write a batch of addresses to the CSV file."""
    writer = csv.writer(file_handle)
    for address in addresses:
        await file_handle.write(f"{address}\n")  # Write each address on a new line

async def process_wallet(session, wallet_name, program_address, cutoff_time, file_handle, global_set):
    """
    Process a single wallet by fetching and processing its activities until the cutoff_time is reached.

    Steps:
    1. Fetch pages concurrently with rate limiting and retries.
    2. Collect unique 'from_address' entries from activities.
    3. Write collected addresses incrementally to the CSV file.
    4. Stop fetching more pages once a transaction older than the cutoff_time is found.
    """
    logger.info(f"[{wallet_name}] Starting processing for wallet address: {program_address}")
    pages_processed = 0

    async def process_page(page):
        nonlocal pages_processed
        url = construct_url(program_address, page)
        activities = await fetch_page(session, url, {"token": API_KEY}, wallet_name, page)

        if activities is None:
            logger.warning(f"[{wallet_name}] Skipping page {page} due to previous errors.")
            return

        if not activities:
            logger.info(f"[{wallet_name}] No activities found on page {page}. Ending pagination.")
            # Returning a special signal to stop further processing
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
                hours_remaining = (activity_time - cutoff_time).total_seconds() / 3600
                logger.info(f"[{wallet_name}] Reached cutoff time on page {page}. Activity time: {activity_time_str} ({hours_remaining:.2f} hours from cutoff). Stopping further requests.")
                # Returning a special signal to stop further processing
                raise StopAsyncIteration

            from_address = activity.get("from_address")
            if from_address and from_address not in global_set:
                batch_addresses.append(from_address)
                global_set.add(from_address)

        if batch_addresses:
            await write_addresses(file_handle, batch_addresses)
            logger.info(f"[{wallet_name}] Page {page}: Collected {len(batch_addresses)} new 'from_address' entries.")

        # Trigger garbage collection periodically
        pages_processed += 1
        if pages_processed % 1000 == 0:
            gc.collect()
            logger.info(f"[{wallet_name}] Garbage collection triggered after {pages_processed} pages.")

    try:
        # Create a list of page numbers
        page_numbers = list(range(1, MAX_PAGES + 1))
        # Use asyncio.gather with concurrency control
        tasks = []
        for page in page_numbers:
            await semaphore.acquire()
            task = asyncio.create_task(process_page(page))
            tasks.append(task)

            # Callback to release semaphore once the task is done
            def _callback(fut):
                semaphore.release()
                try:
                    fut.result()
                except StopAsyncIteration:
                    # Cancel all pending tasks
                    for t in tasks:
                        if not t.done():
                            t.cancel()

            task.add_done_callback(_callback)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
    except StopAsyncIteration:
        logger.info(f"[{wallet_name}] Pagination stopped as cutoff time was reached.")
    except Exception as e:
        logger.error(f"[{wallet_name}] Error during processing: {e}")

    logger.info(f"[{wallet_name}] Finished processing. Total unique 'from_address' collected: {len(global_set)}")

# ----------------------- Main Function -----------------------

async def main():
    start_time = time.time()
    connector = aiohttp.TCPConnector(limit=1000)  # Increased limit for higher concurrency

    # Define the combined CSV filename
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"defi_from_addresses_{timestamp}.csv"

    # Open the CSV file asynchronously
    async with aiofiles.open(combined_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        # Write header
        await csv_file.write("wallet\n")

        # Use a global set for deduplication across wallets
        global_set = set()

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            current_time = datetime.utcnow()
            cutoff_time = current_time - TIME_THRESHOLD

            for wallet_name, program_address in WALLETS.items():
                task = asyncio.create_task(
                    process_wallet(session, wallet_name, program_address, cutoff_time, csv_file, global_set)
                )
                tasks.append(task)

            # Gather all results
            await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(f"Addresses have been written to {combined_filename}")

    end_time = time.time()
    elapsed = end_time - start_time
    logger.info(f"Script completed in {elapsed:.2f} seconds.")

# ----------------------- Entry Point -----------------------

if __name__ == "__main__":
    # Prompt user to select wallets
    WALLETS = select_wallets()

    # Prompt user to enter max pages
    MAX_PAGES = get_max_pages()

    # Ensure the API key is provided
    API_KEY = "YOUR_SOLSCAN_API_KEY"
    if not API_KEY or API_KEY == "YOUR_SOLSCAN_API_KEY":
        logger.error("Please replace 'YOUR_SOLSCAN_API_KEY' with your actual Solscan API key in the script.")
        sys.exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
