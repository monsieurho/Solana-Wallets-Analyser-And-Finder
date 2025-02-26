# Solana Wallet Analyzer & Scraper

Welcome to the **Solana Wallet Analyzer & Scraper** repository! This project provides two powerful Python scripts to help you analyze and scrape Solana wallet data efficiently. Whether you're a developer, analyst, or enthusiast, these tools will assist you in gaining valuable insights into Solana wallets and their DeFi activities.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [1. Wallet Analyzer (`wallet-analyzer.py`)](#1-wallet-analyzerywallet-analyzerpy)
  - [2. Wallet Scraper (`wallet-scraper.py`)](#2-wallet-scraperwallet-scraperpy)
- [Input & Output](#input--output)


## Features

### Wallet Analyzer (`wallet-analyzer.py`)
- **Asynchronous Processing:** Utilizes `asyncio` and `aiohttp` for efficient concurrent API requests.
- **Comprehensive Filtering:** Filters wallets based on multiple criteria such as SOL/WSOL balance, ROI, realized/unrealized gains, and more.
- **Rate Limiting & Retries:** Implements rate limiting and exponential backoff to handle API rate limits and transient errors gracefully.
- **Incremental Output:** Writes qualified wallets incrementally to Markdown and CSV files, including detailed wallet metrics.
- **Logging:** Provides detailed logging to monitor the script's progress and handle errors.

### Wallet Scraper (`wallet-scraper.py`)
- **User-Friendly Selection:** Allows users to select specific exchanges (e.g., Pumpfun, Orca, Raydium) to scrape.
- **Configurable Pagination:** Users can specify the number of pages to scrape for DeFi activities.
- **Asynchronous Scraping:** Leverages `asyncio` and `aiohttp` for high-performance data scraping.
- **Rate Limiting & Retries:** Manages API rate limits with `aiolimiter` and includes robust retry logic with exponential backoff.
- **Deduplication:** Ensures unique collection of 'from_address' entries across wallets.
- **Logging:** Provides informative logs to track scraping progress and handle issues.

## Prerequisites

- **Python 3.8+**
- **API Keys:**
  - **SolanaTracker API Key** for `wallet-analyzer.py`
  - **Solscan API Key** for `wallet-scraper.py`

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/wallets-alpha/Solana-Wallets.git
   cd Solana-Wallets
   ```

2. **Create a Virtual Environment (Optional but Recommended):**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

   **`requirements.txt` Content:**

   ```plaintext
   aiohttp
   aiofiles
   aiolimiter
   pandas
   orjson
   ```

## Configuration

### 1. Wallet Analyzer (`wallet-analyzer.py`)

- **API Key:**
  
  Replace the placeholder API key with your actual [SolanaTracker API Key](https://www.solanatracker.io/data-api?via=data):

  ```python
  API_KEY = "your_actual_solana_tracker_api_key"  # Replace with your real API key
  ```

- **Input CSV:**
  
  Ensure you have a `wallets.csv` file in the repository directory with a column named `wallet` containing the Solana wallet addresses you want to analyze.

  **Example `wallets.csv`:**

  ```csv
  wallet
  F7gk3Hj8...
  9mK2L8s...
  ...
  ```

### 2. Wallet Scraper (`wallet-scraper.py`)

- **API Key:**
  
  Replace the placeholder API key with your actual [Solscan API Key](https://solscan.io/):

  ```python
  API_KEY = "your_actual_solscan_api_key"
  ```

## Usage

### 1. Wallet Analyzer (`wallet-analyzer.py`)

Analyze Solana wallets based on various financial and transactional metrics.

**Run the Script:**

```bash
python wallet-analyzer.py
```

**Steps:**

1. **Enter SOL Price:**
   
   The script will prompt you to enter the current SOL price in USD. This is used to calculate the portfolio value of each wallet.

   ```plaintext
   Please enter the current SOL price in USD: 25.50
   ```

2. **Processing:**
   
   The script will read wallet addresses from `wallets.csv`, fetch data from the SolanaTracker API, apply filtering criteria, and write the results to:
   
   - **Markdown File:** `Alpha_Wallets_YYYY-MM-DD.md`
   - **CSV File:** `Qualified_Wallets_YYYY-MM-DD.csv`
   
   Logs will be written to `solana_wallet_analyzer.log`.

**Example Output:**

- **Markdown File:**

  | Wallet | Portfolio Value (USD) | WSOL Balance | Farming Attempts / Total Tokens | Farming Ratio (%) | Winrate (%) | ROI (%) | ROI (1D) (%) | Win Rate 1D (%) | ROI (7D) (%) | Win Rate 7D (%) | ROI (30D) (%) | Win Rate 30D (%) | Realized Gains (USD) | Unrealized Gains (USD) | Average Holding Time (min) | Avg Buy Size | Avg Profit % Per Trade | Avg Loss % Per Trade |
  |--------|-----------------------|--------------|---------------------------------|-------------------|-------------|---------|---------------|------------------|---------------|------------------|----------------|-------------------|-----------------------|------------------------|----------------------------|--------------|------------------------|-----------------------|
  | ABC123 | $2550.00              | 100.0000     | 5 / 20                          | 25.00%            | 70.00%      | 10.00%  | 1.00%         | 65.00%           | 7.00%         | 68.00%           | 30.00%         | 72.00%            | $1500.00              | $200.00                | 45.00                      | $50.00       | 5.00%                  | -3.00%                |

- **CSV File:**

  ```csv
  wallet,portfolio_value_usd,WSOL_balance,Farming_Attempts,Total_Tokens,Farming_Ratio_Percentage,Winrate,ROI,ROI_1d,Win_Rate_1d,ROI_7d,Win_Rate_7d,ROI_30d,Win_Rate_30d,Realized_Gains,Unrealized_Gains,Average_Holding_Time_min,Avg_Buy_Size,Avg_Profit_Per_Trade_%,Avg_Loss_Per_Trade_%
  ABC123,2550.00,100.0000,5,20,25.00,70.00,10.00,1.00,65.00,7.00,68.00,30.00,72.00,1500.00,200.00,45.00,50.00,5.00,-3.00
  ```

### 2. Wallet Scraper (`wallet-scraper.py`)

Scrape DeFi activities from selected Solana exchange wallets and extract unique 'from_address' entries.

**Run the Script:**

```bash
python wallet-scraper.py
```

**Steps:**

1. **Select Exchanges:**
   
   You'll be prompted to select which exchanges' wallets to scrape. Enter the corresponding numbers separated by commas.

   ```plaintext
   Select Exchange(s) To Scrape:
   1. Pumpfun
   2. Orca
   3. Raydium
   Enter the numbers separated by commas (e.g., 1,3): 1,2
   ```

2. **Enter Maximum Pages:**
   
   Specify the number of pages to scrape for each selected wallet.

   ```plaintext
   Enter the number of max pages to scrape: 100
   ```

3. **Processing:**
   
   The script will fetch DeFi activities from the Solscan API, collect unique 'from_address' entries, and write them to a CSV file named `defi_from_addresses_YYYYMMDD_HHMMSS.csv`.

**Example Output:**

- **CSV File:**

  ```csv
  wallet
  from_address_1
  from_address_2
  from_address_3
  ...
  ```

## Input & Output

### Wallet Analyzer

- **Input:**
  - `wallets.csv`: CSV file containing a column named `wallet` with Solana wallet addresses.

- **Output:**
  - `Alpha_Wallets_YYYY-MM-DD.md`: Markdown file listing qualified wallets with detailed metrics.
  - `Qualified_Wallets_YYYY-MM-DD.csv`: CSV file containing the same data as the Markdown file.
  - `solana_wallet_analyzer.log`: Log file recording the script's operations and any issues encountered.

### Wallet Scraper

- **Input:**
  - User selections for exchanges and the number of pages to scrape.

- **Output:**
  - `defi_from_addresses_YYYYMMDD_HHMMSS.csv`: CSV file containing unique 'from_address' entries scraped from DeFi activities.



---

*Disclaimer: Use these scripts responsibly and ensure compliance with Solana's API usage policies. The authors are not liable for any misuse or damages resulting from the use of these tools.*
