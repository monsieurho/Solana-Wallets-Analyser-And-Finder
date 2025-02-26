import aiohttp
import asyncio
import time
import orjson
import logging
import aiofiles
from datetime import datetime
import sys
import csv
from asyncio import Queue
from aiolimiter import AsyncLimiter
import random


# ---------------------------- Configuration ---------------------------- #

API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # <-- Replace with your real API key
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
LOG_LEVEL = logging.WARNING  # Changed from INFO to WARNING to reduce I/O overhead

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
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Initialize the rate limiter
limiter = AsyncLimiter(max_rate=REQUESTS_PER_SECOND, time_period=1)


# ---------------------------- Helper Functions ---------------------------- #
def format_market_cap(value):
    """
    Formats a numeric market cap value to a human-readable string using K, M, or B.
    """
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "$0.00"
    if value >= 1e9:
        return f"${value/1e9:.2f}B"
    elif value >= 1e6:
        return f"${value/1e6:.2f}M"
    elif value >= 1e3:
        return f"${value/1e3:.2f}K"
    else:
        return f"${value:.2f}"



def read_wallets_generator(csv_file):
    """
    Reads wallet addresses from a CSV with a 'wallet' column using a generator.
    """
    try:
        with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if 'wallet' not in reader.fieldnames:
                raise ValueError("CSV must contain a 'wallet' column.")
            for row in reader:
                wallet = row.get('wallet')
                if wallet:
                    yield wallet.strip()
    except Exception as e:
        logging.error(f"Error reading '{csv_file}': {e}")
        return

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

async def fetch(session, url, identifier, retry_count=0):
    """
    Asynchronously fetch JSON data from the given URL using the provided session.
    Implements retry logic with exponential backoff and jitter.
    The 'identifier' is used for logging purposes.
    """
    async with limiter:
        try:
            async with session.get(url, timeout=60) as response:
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        delay = float(retry_after)
                    else:
                        delay = RETRY_BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0, 1)
                    if retry_count < MAX_RETRIES:
                        logging.warning(f"Received 429 for {identifier}. Retrying in {delay:.2f} seconds... (Retry {retry_count + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(delay)
                        return await fetch(session, url, identifier, retry_count + 1)
                    else:
                        logging.error(f"Exceeded max retries for {identifier} due to 429 errors.")
                        return None
                response.raise_for_status()
                text = await response.text()
                return orjson.loads(text)
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as e:
            if retry_count < MAX_RETRIES:
                delay = RETRY_BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0, 1)
                logging.warning(f"Error fetching {url} for {identifier}: {e}. Retrying in {delay:.2f} seconds... (Retry {retry_count + 1}/{MAX_RETRIES})")
                await asyncio.sleep(delay)
                return await fetch(session, url, identifier, retry_count + 1)
            else:
                logging.error(f"Failed to fetch {url} for {identifier} after {MAX_RETRIES} retries.")
                return None
        except orjson.JSONDecodeError:
            logging.error(f"JSON decode error while fetching {url} for {identifier}.")
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

async def fetch_token_info(session, token_address):
    """
    Asynchronously fetch token data for a given token address.
    """
    url = f"{BASE_URL}/tokens/{token_address}"
    return await fetch(session, url, token_address)

# ---------------------------- Main Filtering Logic ---------------------------- #
async def process_wallet(session, wallet, sol_price):
    """
    Asynchronously processes a wallet in two steps:
    1. Fetch basic wallet data and verify SOL/WSOL balances.
    2. If balance qualifies, fetch PnL data and apply additional filters.
    Also extracts the list of last traded token addresses (up to 10) based on last_sell_time.
    Returns a tuple (success: bool, data or reason: str/dict).
    """
    try:
        logging.debug(f"Processing wallet: {wallet}")

        # Step 1: Fetch basic wallet data
        basic_data = await get_wallet_basic(session, wallet)
        if not basic_data:
            logging.warning(f"Wallet {wallet}: API basic data retrieval failed.")
            return (False, 'API_ERROR')

        # Calculate Portfolio Value in USD from basic data
        total_sol = basic_data.get("totalSol", 0)
        portfolio_value = total_sol * sol_price
        logging.debug(f"Wallet {wallet}: Portfolio Value = {portfolio_value:.2f} USD")

        # Check WSOL balance
        tokens = basic_data.get("tokens", [])
        wsol_balance = 0
        for t in tokens:
            if t.get("address") == WSOL_ADDRESS:
                wsol_balance = t.get("balance", 0) or 0
                break

        # Must have >= SOL_THRESHOLD SOL OR >= WSOL_THRESHOLD WSOL
        if (total_sol < SOL_THRESHOLD) and (wsol_balance < WSOL_THRESHOLD):
            logging.warning(f"Wallet {wallet} does not meet SOL/WSOL balance criteria (SOL: {total_sol}, WSOL Balance: {wsol_balance}).")
            return (False, 'DISQUALIFIED')

        # Step 2: Since the wallet qualified basic checks, fetch PnL data
        pnl_data = await get_wallet_pnl(session, wallet)
        if not pnl_data:
            logging.warning(f"Wallet {wallet}: API PnL data retrieval failed.")
            return (False, 'API_ERROR')

        # 3) Check PnL summary
        summary = pnl_data.get("summary")
        if not summary:
            logging.warning(f"Wallet {wallet}: 'summary' section missing in PnL data.")
            return (False, 'DISQUALIFIED')

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

        min_unrealized = -(UNREALIZED_MIN_PERCENT / 100) * portfolio_value
        if unrealized_gains < min_unrealized:
            logging.warning(f"Wallet {wallet} disqualified due to unrealized gains {unrealized_gains:.2f} < {min_unrealized:.2f} (-1.5% of portfolio value).")
            return (False, 'DISQUALIFIED')

        max_unrealized = (UNREALIZED_MAX_PERCENT / 100) * portfolio_value
        if unrealized_gains > max_unrealized:
            logging.warning(f"Wallet {wallet} disqualified due to unrealized gains {unrealized_gains:.2f} > {max_unrealized:.2f} (85% of portfolio value).")
            return (False, 'DISQUALIFIED')

        # 5) Calculate ROI
        try:
            roi = ((total_pnl) / total_invested) * 100
        except (TypeError, ZeroDivisionError) as e:
            logging.exception(f"Wallet {wallet} - Error calculating ROI: {e}")
            return (False, 'DISQUALIFIED')

        # 6) Calculate Farming Ratio and extract last traded tokens info
        tokens_pnl = pnl_data.get("tokens", {})
        total_tokens = len(tokens_pnl)
        farming_attempts = 0
        holding_times = []
        # For capturing token addresses and last sell times
        token_trade_list = []

        for token_address, tinfo in tokens_pnl.items():
            last_buy = safe_milli_to_sec(tinfo.get("last_buy_time"))
            last_sell = safe_milli_to_sec(tinfo.get("last_sell_time"))
            first_buy = safe_milli_to_sec(tinfo.get("first_buy_time"))

            # Capture tokens that have a valid last_sell_time (used for "last traded" list)
            if last_sell > 0:
                token_trade_list.append((token_address, last_sell))

            if last_buy == 0:
                continue

            time_diff = last_sell - last_buy
            if 0 < time_diff < FARMING_TIME_THRESHOLD:
                farming_attempts += 1

            holding_time = last_sell - first_buy
            if holding_time > 0:
                holding_times.append(holding_time)

        try:
            farming_ratio = (farming_attempts / total_tokens) if total_tokens > 0 else 0
        except (TypeError, ZeroDivisionError) as e:
            logging.exception(f"Wallet {wallet} - Error calculating farming ratio: {e}")
            return (False, 'DISQUALIFIED')

        if farming_ratio > FARMING_RATIO_THRESHOLD:
            logging.warning(f"Wallet {wallet} disqualified due to farming ratio {farming_ratio*100:.2f}% > {FARMING_RATIO_THRESHOLD*100}%.")
            return (False, 'DISQUALIFIED')

        # Sort tokens by last_sell_time descending and take top 10 addresses
        token_trade_list.sort(key=lambda x: x[1], reverse=True)
        last_traded_tokens = [token for token, _ in token_trade_list[:10]]

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

        # 9) NEW FILTER: Disqualify wallets where unrealized gains >= 75% of realized gains
        if realized_gains > 0:
            if unrealized_gains >= (UNREALIZED_TO_REALIZED_RATIO * realized_gains):
                logging.warning(f"Wallet {wallet} disqualified because unrealized gains (${unrealized_gains:.2f}) >= 75% of realized gains (${realized_gains:.2f}).")
                return (False, 'DISQUALIFIED')

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
        roi_1d = 0
        roi_7d = 0
        roi_30d = 0
        winrate_1d = 0
        winrate_7d = 0
        winrate_30d = 0

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
        historic_tokens = pnl_data.get("historic", {}).get("tokens", {})
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
                    logging.warning(f"Wallet {wallet}, Token {token_address}: Invalid total or total_invested. Skipping.")
                    continue

        avg_profit_percent = sum(profit_percentages) / len(profit_percentages) if profit_percentages else None
        avg_loss_percent = sum(loss_percentages) / len(loss_percentages) if loss_percentages else None

        if avg_profit_percent is None:
            logging.info(f"Wallet {wallet}: Avg Profit % Per Trade is 0 or no profitable trades.")
        if avg_loss_percent is None:
            logging.info(f"Wallet {wallet}: Avg Loss % Per Trade is 0 or no losing trades.")

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

        # Return result dictionary with all values plus the list of last traded tokens for later token API calls.
        return (True, {
            "wallet": wallet,
            "portfolio_value_usd": portfolio_value,
            "SOL_balance": wsol_balance,
            "Farming_Attempts": farming_attempts,
            "Total_Tokens": total_tokens,
            "Farming_Ratio_Percentage": farming_ratio * 100,
            # New fields to be added later after token API calls:
            "Avg_Risk_Last_10_Tokens": None,
            "Avg_MC_Last_10_Tokens": None,
            "Winrate": winrate,
            "ROI": roi,
            "ROI_1d": roi_1d,
            "Win_Rate_1d": winrate_1d,
            "ROI_7d": roi_7d,
            "Win_Rate_7d": winrate_7d,
            "ROI_30d": roi_30d,
            "Win_Rate_30d": winrate_30d,
            "Realized_Gains": realized_gains,
            "Unrealized_Gains": unrealized_gains,
            "Average_Holding_Time_min": avg_holding,
            "Avg_Buy_Size": avg_buy_size,
            "Avg_Profit_Per_Trade_%": avg_profit_percent,
            "Avg_Loss_Per_Trade_%": avg_loss_percent,
            # Save the list of last traded tokens (up to 10) for later processing:
            "last_traded_tokens": last_traded_tokens
        })
    except Exception as e:
        logging.exception(f"Error processing wallet {wallet}: {e}")
        return (False, 'DISQUALIFIED')

# ---------------------------- Executor & Output Writing ---------------------------- #
async def filter_wallets(wallets_generator, sol_price, concurrency=CONCURRENCY_LIMIT):
    """
    Processes wallets using a producer-consumer pattern with a bounded queue.
    Instead of writing out results immediately, this version collects all qualified wallet results
    in a list. Then, for each qualified wallet, it sends token API requests for the wallet’s last traded tokens
    (up to 10) to compute the average risk score and average market cap.
    Finally, it writes the complete output (with new columns) to Markdown and CSV files.
    """
    qualified_count = 0
    skipped_due_api_errors = 0
    processed_count = 0
    lock = asyncio.Lock()
    qualified_wallets = []

    queue = Queue(maxsize=concurrency * 2)

    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
        keepalive_timeout=30
    )

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        async def log_progress():
            while True:
                await asyncio.sleep(60)
                async with lock:
                    logging.warning(f"Processed {processed_count} wallets. Qualified {qualified_count} wallets. Skipped {skipped_due_api_errors} wallets.")

        progress_task = asyncio.create_task(log_progress())

        async def worker():
            nonlocal qualified_count, processed_count, skipped_due_api_errors
            while True:
                wallet = await queue.get()
                if wallet is None:
                    queue.task_done()
                    break
                success, result = await process_wallet(session, wallet, sol_price)
                if success:
                    qualified_wallets.append(result)
                    qualified_count += 1
                else:
                    if result == 'API_ERROR':
                        skipped_due_api_errors += 1
                async with lock:
                    processed_count += 1
                queue.task_done()

        tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]

        async def enqueue_wallets():
            for wallet in wallets_generator:
                await queue.put(wallet)
            for _ in range(concurrency):
                await queue.put(None)

        enqueue_task = asyncio.create_task(enqueue_wallets())
        await enqueue_task
        await queue.join()
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass
        for task in tasks:
            task.cancel()

# ---------------------------- Post-Processing: Token API Calls ---------------------------- #
    print("Starting token API requests for qualified wallets...")  # Output to terminal

    async with aiohttp.ClientSession(headers=HEADERS) as token_session:
        for wallet in qualified_wallets:
            tokens_list = wallet.get("last_traded_tokens", [])
            if not tokens_list:
                wallet["Avg_Risk_Last_10_Tokens"] = 0
                wallet["Avg_MC_Last_10_Tokens"] = 0
                continue

            # Create tasks for each token
            token_tasks = [asyncio.create_task(fetch_token_info(token_session, token_addr)) for token_addr in tokens_list]
            token_results = await asyncio.gather(*token_tasks)
            risk_scores = []
            market_caps = []
            # Print API call details for each token
            for token_addr, token_data in zip(tokens_list, token_results):
                if token_data:
                    risk = token_data.get("risk", {}).get("score")
                    pools = token_data.get("pools", [])
                    mc = None
                    if pools and "marketCap" in pools[0] and "usd" in pools[0]["marketCap"]:
                        mc = pools[0]["marketCap"]["usd"]
                    # Output token details to the terminal
                    print(f"Token Address: {token_addr} | Market Cap: {mc} | Token Risk: {risk}")
                    if risk is not None:
                        try:
                            risk_scores.append(float(risk))
                        except (ValueError, TypeError):
                            pass
                    if mc is not None:
                        try:
                            market_caps.append(float(mc))
                        except (ValueError, TypeError):
                            pass
            wallet["Avg_Risk_Last_10_Tokens"] = sum(risk_scores)/len(risk_scores) if risk_scores else 0
            wallet["Avg_MC_Last_10_Tokens"] = sum(market_caps)/len(market_caps) if market_caps else 0

    # ---------------------------- Risk Score Calculation & Sorting ---------------------------- #
    # For each qualified wallet, compute the risk score (out of 100) based on 8 criteria
    for wallet in qualified_wallets:
        # 1. Portfolio Value: each $1,000 = 1 point, max 10 points
        score1 = min(wallet["portfolio_value_usd"] / 1000, 10)
        
        # 2. Win Rate 7D:
        #    - below 35%: 0 points;
        #    - between 35%-85%: 0.2 points per % above 35 (max 10 points);
        #    - between 85%-95%: 4 points;
        #    - above 95%: 0 points.
        win_rate = wallet["Win_Rate_7d"]
        if win_rate < 35:
            score2 = 0
        elif win_rate <= 85:
            score2 = (win_rate - 35) * 0.2
        elif win_rate <= 95:
            score2 = 4
        else:
            score2 = 0

        # 3. Farming Ratio:
        #    0% = 10 points; each 1% increase subtracts 1 point (minimum 0).
        farming_ratio = wallet["Farming_Ratio_Percentage"]
        score3 = max(10 - farming_ratio, 0)

        # 4. 7D ROI:
        #    Negative: 0; 0-10%: 3; 10-25%: 6; 25-50%: 9; 50-75%: 12; 75%+: 15.
        roi_7d = wallet["ROI_7d"]
        if roi_7d < 0:
            score4 = 0
        elif roi_7d < 10:
            score4 = 3
        elif roi_7d < 25:
            score4 = 6
        elif roi_7d < 50:
            score4 = 9
        elif roi_7d < 75:
            score4 = 12
        else:
            score4 = 15

        # 5. Average Holding Time (minutes):
        #    <= 60: 0; 60-120: 2.5; 120-720: 5; 720-1440: 2.5; >1440: 0.
        hold_time = wallet["Average_Holding_Time_min"]
        if hold_time <= 60:
            score5 = 0
        elif hold_time <= 120:
            score5 = 2.5
        elif hold_time <= 720:
            score5 = 5
        elif hold_time <= 1440:
            score5 = 2.5
        else:
            score5 = 0

        # 6. Total Tokens Traded:
        #    < 12: 0; every 10 tokens after 12 add 1 point; >=120 tokens = 15 points.
        total_tokens = wallet["Total_Tokens"]
        if total_tokens < 12:
            score6 = 0
        elif total_tokens >= 120:
            score6 = 15
        else:
            score6 = (total_tokens - 12) // 10

        # 7. Avg MC of Last 10 Tokens:
        #    <50k: 0; each 100k over 50k adds 1 point; >=2.05M: 20 points.
        avg_mc = wallet["Avg_MC_Last_10_Tokens"]
        if avg_mc < 50000:
            score7 = 0
        elif avg_mc >= 2050000:
            score7 = 20
        else:
            score7 = int((avg_mc - 50000) // 100000)
            score7 = min(score7, 20)

        # 8. Avg Risk of Last 10 Tokens:
        #    0 risk = 15 points; each 1 risk subtracts 1.5 points.
        avg_risk = wallet["Avg_Risk_Last_10_Tokens"]
        score8 = max(15 - (avg_risk * 1.5), 0)

        total_safe_score = score1 + score2 + score3 + score4 + score5 + score6 + score7 + score8

        wallet["Risk_Score"] = 100 - total_safe_score

    # Sort wallets by Risk Score descending (lowest score = safest)
    qualified_wallets.sort(key=lambda x: x["Risk_Score"])

    # ---------------------------- Write Output Files ---------------------------- #
    md_header = f"# Alpha Wallets ({today_str})\n\n"
    md_header += (
        "| Wallet | Risk Score | Portfolio Value (USD) | SOL Balance | Farming Attempts / Total Tokens | "
        "Farming Ratio (%) | Avg Risk of Last 10 Tokens | Avg MC of Last 10 Tokens | Winrate (%) | "
        "ROI (%) | ROI (1D) (%) | Win Rate 1D (%) | ROI (7D) (%) | Win Rate 7D (%) | "
        "ROI (30D) (%) | Win Rate 30D (%) | Realized Gains (USD) | Unrealized Gains (USD) | "
        "Average Holding Time (min) | Avg Buy Size | Avg Profit % Per Trade | Avg Loss % Per Trade |\n"
    )
    md_header += "|" + "|".join(["-" * 10 for _ in range(22)]) + "|\n"

    csv_header = (
        "wallet,Risk_Score,portfolio_value_usd,SOL_balance,Farming_Attempts,Total_Tokens,Farming_Ratio_Percentage,"
        "Avg_Risk_Last_10_Tokens,Avg_MC_Last_10_Tokens,Winrate,ROI,ROI_1d,Win_Rate_1d,ROI_7d,Win_Rate_7d,"
        "ROI_30d,Win_Rate_30d,Realized_Gains,Unrealized_Gains,Average_Holding_Time_min,Avg_Buy_Size,"
        "Avg_Profit_Per_Trade_%,Avg_Loss_Per_Trade_%\n"
    )

    async with aiofiles.open(OUTPUT_MD, mode='w') as md_file, aiofiles.open(OUTPUT_CSV, mode='w') as csv_file:
        await md_file.write(md_header)
        await csv_file.write(csv_header)
        for result in qualified_wallets:
            avg_profit = f"{result['Avg_Profit_Per_Trade_%']:.2f}%" if result['Avg_Profit_Per_Trade_%'] is not None else "-"
            avg_loss = f"{result['Avg_Loss_Per_Trade_%']:.2f}%" if result['Avg_Loss_Per_Trade_%'] is not None else "-"
            md_row = (
                f"| {result['wallet']} | {result['Risk_Score']:.2f} | "
                f"${result['portfolio_value_usd']:.2f} | "
                f"{result['SOL_balance']:.4f} | "
                f"{result['Farming_Attempts']} / {result['Total_Tokens']} | "
                f"{result['Farming_Ratio_Percentage']:.2f}% | "
                f"{result['Avg_Risk_Last_10_Tokens']:.2f} | "
                f"{format_market_cap(result['Avg_MC_Last_10_Tokens'])} | "
                f"{result['Winrate']:.2f}% | "
                f"{result['ROI']:.2f}% | "
                f"{result['ROI_1d']:.2f}% | "
                f"{result['Win_Rate_1d']:.2f}% | "
                f"{result['ROI_7d']:.2f}% | "
                f"{result['Win_Rate_7d']:.2f}% | "
                f"{result['ROI_30d']:.2f}% | "
                f"{result['Win_Rate_30d']:.2f}% | "
                f"${result['Realized_Gains']:.2f} | "
                f"${result['Unrealized_Gains']:.2f} | "
                f"{result['Average_Holding_Time_min']:.2f} | "
                f"${result['Avg_Buy_Size']:.2f} | "
                f"{avg_profit} | "
                f"{avg_loss} |\n"
            )
            await md_file.write(md_row)
            csv_row = (
                f"{result['wallet']},"
                f"{result['Risk_Score']:.2f},"
                f"{result['portfolio_value_usd']:.2f},"
                f"{result['SOL_balance']:.4f},"
                f"{result['Farming_Attempts']},"
                f"{result['Total_Tokens']},"
                f"{result['Farming_Ratio_Percentage']:.2f},"
                f"{result['Avg_Risk_Last_10_Tokens']:.2f},"
                f"{result['Avg_MC_Last_10_Tokens']:.2f},"
                f"{result['Winrate']:.2f},"
                f"{result['ROI']:.2f},"
                f"{result['ROI_1d']:.2f},"
                f"{result['Win_Rate_1d']:.2f},"
                f"{result['ROI_7d']:.2f},"
                f"{result['Win_Rate_7d']:.2f},"
                f"{result['ROI_30d']:.2f},"
                f"{result['Win_Rate_30d']:.2f},"
                f"{result['Realized_Gains']:.2f},"
                f"{result['Unrealized_Gains']:.2f},"
                f"{result['Average_Holding_Time_min']:.2f},"
                f"{result['Avg_Buy_Size']:.2f},"
                f"{avg_profit},"
                f"{avg_loss}\n"
            )
            await csv_file.write(csv_row)

    return qualified_count, skipped_due_api_errors

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
    wallets_generator = read_wallets_generator(INPUT_CSV)
    if wallets_generator is None:
        logging.error("No wallets found. Exiting.")
        return
    qualified_count, skipped_due_api_errors = await filter_wallets(wallets_generator, sol_price, concurrency=CONCURRENCY_LIMIT)
    elapsed = time.time() - start_time
    logging.warning(f"Script completed in {elapsed:.2f} seconds.")
    logging.warning(f"Total Wallets Qualified: {qualified_count}")
    logging.warning(f"Total Wallets Skipped Due to API Errors: {skipped_due_api_errors}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
