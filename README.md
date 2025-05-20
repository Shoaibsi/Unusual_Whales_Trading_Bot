# Unusual Whales Trading Bot

A modular, automated trading assistant that scans the market using Unusual Whales Discord signals, analyzes tickers with multiple confirmation signals, and generates actionable trade alerts. Includes a paper trading mode for safe testing.

## Features
- **Daily Setup:** Identifies market trends and top sectors at 9:00 AM ET
- **Real-Time Scanning:** Scans for high-probability tickers every 5 minutes
- **Deep-Dive Analysis:** Confirms signals with detailed Unusual Whales commands
- **Alert Generation:** Recommends specific options with entry, target, stop, and confidence levels
- **Paper Trading Mode:** Test strategies without risking capital
- **Error Handling:** Robust logging and error skipping to keep the bot running
- **Advanced Filters:** Gamma squeeze detection and IV filters for smarter trades

## Architecture
```
trading_bot/
├── main.py              # Orchestrates the bot’s workflow
├── discord_client.py    # Handles Discord interactions
├── ocr_parser.py        # Parses OCR text from screenshots
├── data_analyzer.py     # Scores tickers and selects top candidates
├── alert_generator.py   # Generates and sends trade alerts
├── config.py            # Stores settings (token, channel ID, thresholds)
├── utils.py             # Helper functions (logging, retries)
├── logs/                # Directory for log files
│   └── bot.log
└── data/                # Directory for data output
    └── alerts.json      # Logs trade alerts
    └── paper_trades.json # Logs paper trading results
```

## Setup
1. **Clone the repo:**
   ```bash
   git clone https://github.com/Shoaibsi/Unusual_Whales_Trading_Bot.git
   cd Unusual_Whales_Trading_Bot
   ```
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
   - Also install Tesseract OCR (see docs for your OS)
3. **Configure `config.py`:**
   - Add your Discord bot token, Unusual Whales channel ID, and scoring thresholds
4. **Run the bot:**
   ```bash
   python trading_bot/main.py
   ```

## Usage
- The bot runs during market hours (9:00 AM - 4:00 PM ET)
- Trade alerts are logged to `data/alerts.json`
- Paper trades are logged to `data/paper_trades.json`
- Logs are stored in `logs/bot.log`

## Contributing
Pull requests and suggestions are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License
MIT

---

*Created by Shoaib Rahimi*
