import pandas as pd
import aiohttp
import asyncio
import time
import orjson
import logging
import aiofiles
from datetime import datetime
import sys
from asyncio import Queue
from aiolimiter import AsyncLimiter
import random

# ---------------------------- Configuration ---------------------------- #

API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # <-- Replace with your real API key
BASE_URL = "https://data.solanatracker.io"
INPUT_CSV = "wallets.csv"  # CSV must have a column named 'wallet'

# Dynamically set OUTPUT_MD and OUTPUT_CSV to include today's date in 'YYYY-MM-DD' format
today_str = datetime.today().strftime('%Y-%m-%d')  # Get today's date as a string
OUTPUT_MD = f"Alpha_Wallets_{today_str}.md"  # Renamed output file with today's date
OUTPUT_CSV = f"Qualified_Wallets_{today_str}.csv"  # CSV file for qualified wallets

# Filtering Constants
SOL_THRESHOLD = 3
WSOL_THRESHOLD = 3
FARMING_TIME_THRESHOLD = 60      # seconds
FARMING_RATIO_THRESHOLD = 0.1    # 10%
WINRATE_LOWER_THRESHOLD = 45.0   # 45%
WINRATE_UPPER_THRESHOLD = 85.0   # 85%
REALIZED_GAINS_THRESHOLD = 1000  # USD
TOTAL_TOKENS_MIN = 12             # Minimum total tokens required
ROI_MIN_THRESHOLD = -0.001         # Minimum ROI for 1d, 7d, 30d
ROI_7D_NONZERO = True             # Flag to disqualify wallets with 0% 7d ROI
UNREALIZED_MIN_PERCENT = 1.5    # Minimum unrealized gains percentage
UNREALIZED_MAX_PERCENT = 85.0     # Maximum allowable unrealized gains as a percentage of the portfolio value.
UNREALIZED_TO_REALIZED_RATIO = 0.75  # 75%

# Logging
LOG_FILE = "solana_wallet_analyzer.log"

WSOL_ADDRESS = "So11111111111111111111111111111111111111112"
HEADERS = {
    "x-api-key": API_KEY,
    "User-Agent": "SolanaWalletAnalyzer/1.0"  # Custom User-Agent to potentially reduce Cloudflare blocks
}

# Retry Configuration
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 5  # Initial backoff in seconds

# Rate Limiting Configuration
REQUESTS_PER_SECOND = 100  # Reduced from 800 to 100 to mitigate Cloudflare rate limiting

# Concurrency Configuration
CONCURRENCY_LIMIT = 100  # Reduced from 500 to 100 based on Cloudflare considerations

# ---------------------------- Logging Setup ---------------------------- #
logging.basicConfig(
    level=logging.WARNING,  # Changed from INFO to WARNING to reduce I/O overhead
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# Initialize the rate limiter
limiter = AsyncLimiter(max_rate=REQUESTS_PER_SECOND, time_period=1)

# ---------------------------- Helper Functions ---------------------------- #
def read_wallets(csv_file):
    """
    Reads wallet addresses from a CSV with a 'wallet' column.
    """
    try:
        df = pd.read_csv(csv_file)
        if "wallet" not in df.columns:
            raise ValueError("CSV must contain a 'wallet' column.")
        wallets = df["wallet"].dropna().unique().tolist()
        logging.warning(f"Read {len(wallets)} wallet addresses from '{csv_file}'.")
        return wallets
    except Exception as e:
        logging.error(f"Error reading '{csv_file}': {e}")
        return []

def safe_milli_to_sec(value):
    """
    Convert a millisecond timestamp to seconds safely.
    Returns 0 if None or invalid.
    """
    if not value:
        return 0
    try:
        return float(value) / 1000
    except (ValueError, TypeError) as e:
        logging.warning(f"Invalid timestamp value '{value}': {e}. Defaulting to 0.")
        return 0

async def fetch(session, url, wallet, retry_count=0):
    """
    Asynchronously fetch JSON data from the given URL using the provided session.
    Implements retry logic with exponential backoff and jitter.
    """
    async with limiter:
        try:
            async with session.get(url, timeout=60) as response:
                if response.status == 429:
                    # Handle rate limiting
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        delay = float(retry_after)
                    else:
                        # Exponential backoff with jitter
                        delay = RETRY_BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0, 1)
                    if retry_count < MAX_RETRIES:
                        logging.warning(f"Received 429 for wallet {wallet}. Retrying in {delay:.2f} seconds... (Retry {retry_count + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(delay)
                        return await fetch(session, url, wallet, retry_count + 1)
                    else:
                        logging.error(f"Exceeded max retries for wallet {wallet} due to 429 errors.")
                        return None
                response.raise_for_status()
                text = await response.text()
                return orjson.loads(text)
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as e:
            if retry_count < MAX_RETRIES:
                # Exponential backoff with jitter
                delay = RETRY_BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0, 1)
                logging.warning(f"Error fetching {url} for wallet {wallet}: {e}. Retrying in {delay:.2f} seconds... (Retry {retry_count + 1}/{MAX_RETRIES})")
                await asyncio.sleep(delay)
                return await fetch(session, url, wallet, retry_count + 1)
            else:
                logging.error(f"Failed to fetch {url} for wallet {wallet} after {MAX_RETRIES} retries.")
                return None
        except orjson.JSONDecodeError:
            logging.error(f"JSON decode error while fetching {url} for wallet {wallet}.")
            return None

async def get_wallet_basic(session, wallet):
    """
    Asynchronously fetch basic wallet data.
    """
    url = f"{BASE_URL}/wallet/{wallet}/basic"
    return await fetch(session, url, wallet)

async def get_wallet_pnl(session, wallet):
    """
    Asynchronously fetch wallet PnL data with showHistoricPnL=true.
    """
    url = f"{BASE_URL}/pnl/{wallet}?showHistoricPnL=true"
    return await fetch(session, url, wallet)

# ---------------------------- Main Filtering Logic ---------------------------- #
async def process_wallet(session, wallet, sol_price):
    """
    Asynchronously fetches data and applies filtering criteria.
    Returns a tuple (success: bool, data or reason: str/dict).
    """
    try:
        logging.debug(f"Processing wallet: {wallet}")

        basic_data, pnl_data = await asyncio.gather(
            get_wallet_basic(session, wallet),
            get_wallet_pnl(session, wallet)
        )

        if not basic_data or not pnl_data:
            # If either basic_data or pnl_data is None, consider it an API error
            logging.warning(f"Wallet {wallet}: API data retrieval failed.")
            return (False, 'API_ERROR')

        # 1) Calculate Portfolio Value in USD
        total_sol = basic_data.get("totalSol", 0)
        portfolio_value = total_sol * sol_price  # New calculation
        logging.debug(f"Wallet {wallet}: Portfolio Value = {portfolio_value} USD")

        # 2) Check Wsol balance
        tokens = basic_data.get("tokens", [])
        wsol_balance = 0  # Renamed for clarity
        for t in tokens:
            if t.get("address") == WSOL_ADDRESS:
                wsol_balance = t.get("balance", 0) or 0
                break

        # Must have >= SOL_THRESHOLD SOL OR >= WSOL_THRESHOLD WSOL
        if (total_sol < SOL_THRESHOLD) and (wsol_balance < WSOL_THRESHOLD):
            logging.warning(f"Wallet {wallet} does not meet SOL/WSOL balance criteria (SOL: {total_sol}, WSOL Balance: {wsol_balance}).")
            return (False, 'DISQUALIFIED')

        # 3) Check PnL summary
        summary = pnl_data.get("summary")
        if not summary:
            logging.warning(f"Wallet {wallet}: 'summary' section missing in PnL data.")
            return (False, 'DISQUALIFIED')

        # Extract and validate PnL data
        total_pnl = summary.get("total")
        if total_pnl is None:
            logging.warning(f"Wallet {wallet}: 'total' PnL missing. Defaulting to 0.")
            total_pnl = 0
        else:
            try:
                total_pnl = float(total_pnl)
            except (ValueError, TypeError) as e:
                logging.error(f"Wallet {wallet}: 'total' PnL is invalid ({total_pnl}). Error: {e}. Defaulting to 0.")
                total_pnl = 0

        # It's possible for total_pnl to be negative, representing a loss
        # The original disqualification based on total_pnl < 0 is maintained
        if total_pnl < 0:
            logging.warning(f"Wallet {wallet} disqualified due to negative total PnL: {total_pnl}")
            return (False, 'DISQUALIFIED')

        total_invested = summary.get("totalInvested")
        if total_invested is None or total_invested == 0:
            logging.warning(f"Wallet {wallet}: 'totalInvested' is {total_invested}. Disqualifying wallet due to invalid investment data.")
            return (False, 'DISQUALIFIED')
        else:
            try:
                total_invested = float(total_invested)
            except (ValueError, TypeError) as e:
                logging.error(f"Wallet {wallet}: 'totalInvested' is invalid ({total_invested}). Error: {e}. Disqualifying wallet.")
                return (False, 'DISQUALIFIED')

        # 4) NEW FILTER: Check Unrealized Gains
        unrealized_gains = summary.get("unrealized", 0)
        try:
            unrealized_gains = float(unrealized_gains)
        except (ValueError, TypeError) as e:
            logging.error(f"Wallet {wallet}: 'unrealized' gains invalid ({unrealized_gains}). Error: {e}. Defaulting to 0.")
            unrealized_gains = 0

        # Corrected: Allow unrealized_gains to be as low as -1.5% of portfolio_value
        min_unrealized = -(UNREALIZED_MIN_PERCENT / 100) * portfolio_value
        if unrealized_gains < min_unrealized:
            logging.warning(f"Wallet {wallet} disqualified due to unrealized gains {unrealized_gains} < {min_unrealized} (-1.5% of portfolio value).")
            return (False, 'DISQUALIFIED')

        # Unrealized Gains should be <= 85% of portfolio value
        max_unrealized = (UNREALIZED_MAX_PERCENT / 100) * portfolio_value
        if unrealized_gains > max_unrealized:
            logging.warning(f"Wallet {wallet} disqualified due to unrealized gains {unrealized_gains} > {max_unrealized} (85% of portfolio value).")
            return (False, 'DISQUALIFIED')

        # 5) Calculate ROI
        try:
            roi = ((total_pnl) / total_invested) * 100
        except (TypeError, ZeroDivisionError) as e:
            logging.exception(f"Wallet {wallet} - Error calculating ROI: {e}")
            return (False, 'DISQUALIFIED')

        # 6) Calculate Farming Ratio
        tokens_pnl = pnl_data.get("tokens", {})
        total_tokens = len(tokens_pnl)  # Track total tokens
        farming_attempts = 0
        holding_times = []

        for token_address, tinfo in tokens_pnl.items():
            last_buy = safe_milli_to_sec(tinfo.get("last_buy_time"))
            last_sell = safe_milli_to_sec(tinfo.get("last_sell_time"))
            first_buy = safe_milli_to_sec(tinfo.get("first_buy_time"))

            # If last_buy_time is 0, skip farming attempt calculation
            if last_buy == 0:
                continue

            # If sold soon after last buy => "farming attempt"
            time_diff = last_sell - last_buy
            if 0 < time_diff < FARMING_TIME_THRESHOLD:
                farming_attempts += 1

            holding_time = last_sell - first_buy
            if holding_time > 0:
                holding_times.append(holding_time)

        # Validate farming_attempts and total_tokens
        if not isinstance(farming_attempts, (int, float)):
            logging.error(f"Wallet {wallet} - Invalid type for farming_attempts: {type(farming_attempts)}. Expected int or float.")
            return (False, 'DISQUALIFIED')
        if not isinstance(total_tokens, int):
            logging.error(f"Wallet {wallet} - Invalid type for total_tokens: {type(total_tokens)}. Expected int.")
            return (False, 'DISQUALIFIED')

        # Calculate farming_ratio with error handling
        try:
            if total_tokens > 0:
                farming_ratio = farming_attempts / total_tokens
            else:
                farming_ratio = 0
        except (TypeError, ZeroDivisionError) as e:
            logging.exception(f"Wallet {wallet} - Error calculating farming ratio: {e}")
            return (False, 'DISQUALIFIED')

        if farming_ratio > FARMING_RATIO_THRESHOLD:
            logging.warning(f"Wallet {wallet} disqualified due to farming ratio {farming_ratio*100:.2f}% > {FARMING_RATIO_THRESHOLD*100}%.")
            return (False, 'DISQUALIFIED')

        # 7) Check Winrate Disqualification
        winrate = summary.get("winPercentage", "N/A")
        if isinstance(winrate, (int, float)):
            try:
                winrate = float(winrate)
                winrate = max(0, min(100, winrate))
                if winrate < WINRATE_LOWER_THRESHOLD or winrate > WINRATE_UPPER_THRESHOLD:
                    logging.warning(f"Wallet {wallet} disqualified due to winrate {winrate:.2f}% not within [{WINRATE_LOWER_THRESHOLD}%, {WINRATE_UPPER_THRESHOLD}%].")
                    return (False, 'DISQUALIFIED')
            except (ValueError, TypeError) as e:
                logging.error(f"Wallet {wallet}: 'winPercentage' invalid ({winrate}). Error: {e}. Disqualifying wallet.")
                return (False, 'DISQUALIFIED')
        else:
            logging.warning(f"Wallet {wallet} disqualified due to invalid winrate value: {winrate}.")
            return (False, 'DISQUALIFIED')

        # 8) NEW FILTER: Disqualify wallets with realized gains less than REALIZED_GAINS_THRESHOLD
        realized_gains = summary.get("realized", 0)
        try:
            realized_gains = float(realized_gains)
        except (ValueError, TypeError) as e:
            logging.error(f"Wallet {wallet}: 'realized' gains invalid ({realized_gains}). Error: {e}. Defaulting to 0.")
            realized_gains = 0

        if realized_gains < REALIZED_GAINS_THRESHOLD:
            logging.warning(f"Wallet {wallet} disqualified due to realized gains ${realized_gains:.2f} < threshold ${REALIZED_GAINS_THRESHOLD}.")
            return (False, 'DISQUALIFIED')

        # 9) NEW FILTER: Disqualify wallets where unrealized gains >= 60% of realized gains
        if realized_gains > 0:  # Avoid division by zero
            if unrealized_gains >= (UNREALIZED_TO_REALIZED_RATIO * realized_gains):
                logging.warning(f"Wallet {wallet} disqualified because unrealized gains (${unrealized_gains:.2f}) >= 60% of realized gains (${realized_gains:.2f}).")
                return (False, 'DISQUALIFIED')
        else:
            # If realized_gains is 0, any unrealized_gains >=0 would be >=60% of 0, but since we already checked realized_gains >= 1000, this case might not occur
            pass

        avg_holding = 0
        if holding_times:
            try:
                avg_holding = sum(holding_times) / len(holding_times)
                avg_holding /= 60  # Convert from seconds to minutes
            except TypeError as e:
                logging.exception(f"Wallet {wallet} - TypeError calculating average holding time: {e}")
                avg_holding = 0

        # 10) Extract Historic ROI and Win Rates from percentageChange
        historic_summary = pnl_data.get("historic", {}).get("summary", {})

        # Initialize ROIs with default 0
        roi_1d = 0
        roi_7d = 0
        roi_30d = 0

        # Initialize Win Rates with default 0
        winrate_1d = 0
        winrate_7d = 0
        winrate_30d = 0

        # Safely extract percentageChange and winPercentage for each interval and assign to ROI and Win Rate variables
        for interval in ["1d", "7d", "30d"]:
            interval_data = historic_summary.get(interval, {})
            percentage_change = interval_data.get("percentageChange", 0)
            try:
                percentage_change = float(percentage_change)
            except (TypeError, ValueError):
                logging.warning(f"Wallet {wallet}: Invalid percentageChange for {interval}. Defaulting to 0.")
                percentage_change = 0

            win_percentage = interval_data.get("winPercentage", 0)
            try:
                win_percentage = float(win_percentage)
                win_percentage = max(0, min(100, win_percentage))
            except (TypeError, ValueError):
                logging.warning(f"Wallet {wallet}: Invalid winPercentage for {interval}. Defaulting to 0.")
                win_percentage = 0

            if interval == "1d":
                roi_1d = percentage_change
                winrate_1d = win_percentage
            elif interval == "7d":
                roi_7d = percentage_change
                winrate_7d = win_percentage
            elif interval == "30d":
                roi_30d = percentage_change
                winrate_30d = win_percentage

        # 11) NEW FILTER: Disqualify wallets with less than TOTAL_TOKENS_MIN
        if total_tokens < TOTAL_TOKENS_MIN:
            logging.warning(f"Wallet {wallet} disqualified due to total_tokens {total_tokens} < {TOTAL_TOKENS_MIN}.")
            return (False, 'DISQUALIFIED')

        # 12) NEW FILTER: Disqualify wallets with exactly 0% 7d ROI
        if ROI_7D_NONZERO and roi_7d == 0:
            logging.warning(f"Wallet {wallet} disqualified due to ROI_7d being exactly 0%.")
            return (False, 'DISQUALIFIED')

        # 13) NEW FILTER: Disqualify wallets with any ROI period less than ROI_MIN_THRESHOLD
        if (roi_1d < ROI_MIN_THRESHOLD) or (roi_7d < ROI_MIN_THRESHOLD) or (roi_30d < ROI_MIN_THRESHOLD):
            logging.warning(
                f"Wallet {wallet} disqualified due to ROI thresholds: "
                f"ROI_1d={roi_1d:.2f}%, ROI_7d={roi_7d:.2f}%, ROI_30d={roi_30d:.2f}% "
                f"which are below the minimum threshold of {ROI_MIN_THRESHOLD}%."
            )
            return (False, 'DISQUALIFIED')

        # 14) New column: "Avg Buy Size" from "summary":"averageBuyAmount"
        avg_buy_size = summary.get("averageBuyAmount", 0)
        try:
            avg_buy_size = float(avg_buy_size)
        except (ValueError, TypeError):
            logging.warning(f"Wallet {wallet}: 'averageBuyAmount' invalid ({avg_buy_size}). Defaulting to 0.")
            avg_buy_size = 0

        # 15) Calculate Largest Single 30 Day Trade Profit and Loss
        largest_profit_percent = None
        largest_loss_percent = None
        historic_tokens = pnl_data.get("historic", {}).get("tokens", {})  # Moved here to ensure availability
        profit_percentages = []
        loss_percentages = []

        for token_address, token_data in historic_tokens.items():
            thirty_d_metrics = token_data.get("30d", {}).get("metrics", {})
            total_pnl_token = thirty_d_metrics.get("total")
            total_invested_token = thirty_d_metrics.get("total_invested")
            if total_pnl_token is not None and total_invested_token:
                try:
                    total_pnl_token = float(total_pnl_token)
                    total_invested_token = float(total_invested_token)
                    if total_invested_token == 0:
                        logging.warning(f"Wallet {wallet}, Token {token_address}: total_invested is 0. Skipping PnL calculation.")
                        continue
                    pnl_percent = (total_pnl_token / total_invested_token) * 100
                    if pnl_percent > 0:
                        profit_percentages.append(pnl_percent)
                    elif pnl_percent < 0:
                        loss_percentages.append(pnl_percent)
                except (ValueError, TypeError):
                    logging.warning(f"Wallet {wallet}, Token {token_address}: Invalid total '{total_pnl_token}' or total_invested '{total_invested_token}'. Skipping.")
                    continue

        # Calculate average profit and loss percentages
        avg_profit_percent = sum(profit_percentages) / len(profit_percentages) if profit_percentages else None
        avg_loss_percent = sum(loss_percentages) / len(loss_percentages) if loss_percentages else None

        # 16) NEW FILTER: Handle wallets with 0% Avg Profit or Loss Per Trade
        # Instead of disqualifying, set to None to indicate no data
        if avg_profit_percent is None:
            logging.info(f"Wallet {wallet}: Avg Profit % Per Trade is 0 or no profitable trades.")
        if avg_loss_percent is None:
            logging.info(f"Wallet {wallet}: Avg Loss % Per Trade is 0 or no losing trades.")

        # Log qualified wallet details (set to DEBUG to reduce logging overhead)
        logging.debug(
            f"Qualified Wallet: {wallet} | Portfolio Value: {portfolio_value:.2f} USD | ROI: {roi:.2f}% | "
            f"ROI 1d: {roi_1d:.2f}% | Win Rate 1d: {winrate_1d:.2f}% | "
            f"ROI 7d: {roi_7d:.2f}% | Win Rate 7d: {winrate_7d:.2f}% | "
            f"ROI 30d: {roi_30d:.2f}% | Win Rate 30d: {winrate_30d:.2f}% | "
            f"Realized Gains: ${realized_gains:.2f} | Unrealized Gains: ${unrealized_gains:.2f} | Farming Attempts: {farming_attempts} | "
            f"Total Tokens: {total_tokens} | Avg Buy Size: ${avg_buy_size:.2f} | "
            f"Avg Profit %: {avg_profit_percent if avg_profit_percent is not None else '-'}% | Avg Loss %: {avg_loss_percent if avg_loss_percent is not None else '-'}% | "
            f"Average Holding Time: {avg_holding:.2f} minutes"
        )

        # Return final record with all required fields
        return (True, {
            "wallet": wallet,
            "portfolio_value_usd": portfolio_value,
            "SOL_balance": wsol_balance,  # Renamed field
            "Farming_Attempts": farming_attempts,
            "Total_Tokens": total_tokens,  # Included for additional filtering
            "Farming_Ratio_Percentage": farming_ratio * 100,
            "Winrate": winrate,
            "ROI": roi,
            "ROI_1d": roi_1d,
            "Win_Rate_1d": winrate_1d,  # New field
            "ROI_7d": roi_7d,
            "Win_Rate_7d": winrate_7d,  # New field
            "ROI_30d": roi_30d,
            "Win_Rate_30d": winrate_30d,  # New field
            "Realized_Gains": realized_gains,
            "Unrealized_Gains": unrealized_gains,
            "Average_Holding_Time_min": avg_holding,  # Converted to minutes
            "Avg_Buy_Size": avg_buy_size,  # New field
            "Avg_Profit_Per_Trade_%": avg_profit_percent,  # New field
            "Avg_Loss_Per_Trade_%": avg_loss_percent     # New field
        })
    except Exception as e:
        logging.exception(f"Error processing wallet {wallet}: {e}")
        return (False, 'DISQUALIFIED')

# ---------------------------- Executor & Markdown Output ---------------------------- #
async def filter_wallets(wallets, sol_price, concurrency=CONCURRENCY_LIMIT):
    """
    Processes wallets using a producer-consumer pattern with a bounded queue.
    Writes qualified wallets to output files incrementally.
    Logs progress as {processed}/{total} wallets. Qualified {qualified} wallets. Skipped {skipped} wallets.
    """
    qualified_count = 0
    skipped_due_api_errors = 0
    failed_wallets = []
    queue = Queue(maxsize=concurrency * 2)  # Buffer size

    # Initialize progress tracking variables
    total_wallets = len(wallets)
    processed_count = 0
    lock = asyncio.Lock()

    # Configure aiohttp connector with increased number of connections
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
        keepalive_timeout=30
    )

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        # Open output files asynchronously
        async with aiofiles.open(OUTPUT_MD, mode='w') as md_file, aiofiles.open(OUTPUT_CSV, mode='w') as csv_file:
            # Write headers to Markdown and CSV
            md_header = f"# Alpha Wallets ({today_str})\n\n"
            md_header += "| Wallet | Portfolio Value (USD) | WSOL Balance | Farming Attempts / Total Tokens | Farming Ratio (%) | Winrate (%) | ROI (%) | ROI (1D) (%) | Win Rate 1D (%) | ROI (7D) (%) | Win Rate 7D (%) | ROI (30D) (%) | Win Rate 30D (%) | Realized Gains (USD) | Unrealized Gains (USD) | Average Holding Time (min) | Avg Buy Size | Avg Profit % Per Trade | Avg Loss % Per Trade |\n"
            md_header += "|" + "|".join(["-" * len(col) for col in [
                "Wallet", "Portfolio Value (USD)", "WSOL Balance", "Farming Attempts / Total Tokens",
                "Farming Ratio (%)", "Winrate (%)", "ROI (%)", "ROI (1D) (%)", "Win Rate 1D (%)",
                "ROI (7D) (%)", "Win Rate 7D (%)", "ROI (30D) (%)", "Win Rate 30D (%)",
                "Realized Gains (USD)", "Unrealized Gains (USD)", "Average Holding Time (min)",
                "Avg Buy Size", "Avg Profit % Per Trade", "Avg Loss % Per Trade"
            ]]) + "|\n"
            await md_file.write(md_header)

            # Update CSV header to include new fields and renamed columns
            csv_header = "wallet,portfolio_value_usd,WSOL_balance,Farming_Attempts,Total_Tokens,Farming_Ratio_Percentage,Winrate,ROI,ROI_1d,Win_Rate_1d,ROI_7d,Win_Rate_7d,ROI_30d,Win_Rate_30d,Realized_Gains,Unrealized_Gains,Average_Holding_Time_min,Avg_Buy_Size,Avg_Profit_Per_Trade_%,Avg_Loss_Per_Trade_%\n"
            await csv_file.write(csv_header)

            # Define progress logging coroutine
            async def log_progress():
                while processed_count < total_wallets:
                    await asyncio.sleep(10)  # Log every 10 seconds
                    async with lock:
                        logging.warning(f"Processed {processed_count}/{total_wallets} wallets. Qualified {qualified_count} wallets. Skipped {skipped_due_api_errors} wallets.")
                # Final log after processing is complete
                async with lock:
                    logging.warning(f"Processed {processed_count}/{total_wallets} wallets. Qualified {qualified_count} wallets. Skipped {skipped_due_api_errors} wallets.")

            # Start the progress logger
            progress_task = asyncio.create_task(log_progress())

            # Define worker coroutine
            async def worker():
                nonlocal qualified_count, processed_count, skipped_due_api_errors
                while True:
                    wallet = await queue.get()
                    if wallet is None:
                        queue.task_done()
                        break
                    success, result = await process_wallet(session, wallet, sol_price)
                    if success:
                        qualified_count += 1
                        # Handle None values for Avg Profit/Loss Percent
                        avg_profit = f"{result['Avg_Profit_Per_Trade_%']:.2f}%" if result['Avg_Profit_Per_Trade_%'] is not None else "-"
                        avg_loss = f"{result['Avg_Loss_Per_Trade_%']:.2f}%" if result['Avg_Loss_Per_Trade_%'] is not None else "-"
                        # Write to Markdown
                        md_row = (
                            f"| {result['wallet']} | "
                            f"${result['portfolio_value_usd']:.2f} | "
                            f"{result['SOL_balance']:.4f} | "
                            f"{result['Farming_Attempts']} / {result['Total_Tokens']} | "
                            f"{result['Farming_Ratio_Percentage']:.2f}% | "
                            f"{result['Winrate']:.2f}% | "
                            f"{result['ROI']:.2f}% | "
                            f"{result['ROI_1d']:.2f}% | "
                            f"{result['Win_Rate_1d']:.2f}% | "  # New column
                            f"{result['ROI_7d']:.2f}% | "
                            f"{result['Win_Rate_7d']:.2f}% | "  # New column
                            f"{result['ROI_30d']:.2f}% | "
                            f"{result['Win_Rate_30d']:.2f}% | "  # New column
                            f"${result['Realized_Gains']:.2f} | "
                            f"${result['Unrealized_Gains']:.2f} | "
                            f"{result['Average_Holding_Time_min']:.2f} | "
                            f"${result['Avg_Buy_Size']:.2f} | "  # Added $ sign
                            f"{avg_profit} | "
                            f"{avg_loss} |\n"
                        )
                        await md_file.write(md_row)
                        # Write to CSV
                        csv_row = (
                            f"{result['wallet']},"
                            f"{result['portfolio_value_usd']:.2f},"
                            f"{result['SOL_balance']:.4f},"
                            f"{result['Farming_Attempts']},"
                            f"{result['Total_Tokens']},"
                            f"{result['Farming_Ratio_Percentage']:.2f},"
                            f"{result['Winrate']:.2f},"
                            f"{result['ROI']:.2f},"
                            f"{result['ROI_1d']:.2f},"
                            f"{result['Win_Rate_1d']:.2f},"  # New column
                            f"{result['ROI_7d']:.2f},"
                            f"{result['Win_Rate_7d']:.2f},"  # New column
                            f"{result['ROI_30d']:.2f},"
                            f"{result['Win_Rate_30d']:.2f},"  # New column
                            f"{result['Realized_Gains']:.2f},"
                            f"{result['Unrealized_Gains']:.2f},"
                            f"{result['Average_Holding_Time_min']:.2f},"
                            f"{result['Avg_Buy_Size']:.2f},"
                            f"{avg_profit},"
                            f"{avg_loss}\n"
                        )
                        await csv_file.write(csv_row)
                    else:
                        # Determine if the wallet was skipped due to API errors
                        if result == 'API_ERROR':
                            skipped_due_api_errors += 1
                        # Wallets that failed to qualify are not counted as skipped due to API errors
                    # Increment the processed count
                    async with lock:
                        processed_count += 1
                    queue.task_done()

            # Start worker coroutines
            tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]

            # Enqueue wallets
            for wallet in wallets:
                await queue.put(wallet)

            # Add sentinel values to stop workers
            for _ in range(concurrency):
                await queue.put(None)

            # Wait for all tasks to complete
            await queue.join()

            # Cancel the progress logger coroutine
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

            # Cancel worker tasks
            for task in tasks:
                task.cancel()

    # Optionally, handle retries here if desired

    logging.warning(f"Total Qualified Wallets: {qualified_count}")
    logging.warning(f"Total Skipped Wallets Due to API Errors: {skipped_due_api_errors}")
    return qualified_count, skipped_due_api_errors

def create_markdown_and_csv(data, output_md, output_csv):
    """
    Placeholder function. In this optimized version, writing is done incrementally.
    """
    pass  # Implemented in the `filter_wallets` function

# ---------------------------- Main Function ---------------------------- #
def get_sol_price():
    """
    Synchronously get SOL price from user input.
    """
    while True:
        try:
            sol_price_input = input("Please enter the current SOL price in USD: ").strip()
            sol_price = float(sol_price_input)
            if sol_price <= 0:
                print("SOL price must be a positive number. Please try again.")
                continue
            return sol_price
        except ValueError:
            print("Invalid input. Please enter a numeric value for SOL price.")

async def main(wallets_subset=None):
    start_time = time.time()

    sol_price = get_sol_price()

    wallets = wallets_subset if wallets_subset else read_wallets(INPUT_CSV)
    if not wallets:
        logging.error("No wallets found. Exiting.")
        return

    # Process wallets with controlled concurrency and incremental writing
    qualified_count, skipped_due_api_errors = await filter_wallets(wallets, sol_price, concurrency=CONCURRENCY_LIMIT)

    # Optionally, implement retry logic for failed_wallets here

    elapsed = time.time() - start_time
    logging.warning(f"Script completed in {elapsed:.2f} seconds.")
    logging.warning(f"Total Wallets Processed: {len(wallets)}")
    logging.warning(f"Total Wallets Qualified: {qualified_count}")
    logging.warning(f"Total Wallets Skipped Due to API Errors: {skipped_due_api_errors}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
