import asyncio
import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta, time as datetime_time
from typing import Dict, List, Optional, Set
from dotenv import load_dotenv
import traceback

# Set global logging level to DEBUG
logging.basicConfig(level=logging.DEBUG)

# Import the configured logger first
from src.logger_config import logger, output_manager, verify_logging_setup, log_exceptions, log_api_call, log_trading_activity, format_for_logging

# Import other modules
from src.ws_signal_detector import WSSignalDetector, SignalConfig, TradingSignal
from src.telegram_notifier import TelegramNotifier
from src.tasty_trader import TastyTrader
from src.unusual_whales_client import WSUnusualWhalesClient
from src.api_circuit_breaker import CircuitBreaker
from src.endpoint_monitor import EndpointMonitor
# from src.chart_generator import ChartGenerator  # Commented out chart generator
from src.grok_analyzer import GrokAnalyzer  # Import GrokAnalyzer
from src.performance_tracker import PerformanceTracker

class UnusualWhalesBot:
    def __init__(self):
        """Initialize the trading bot with live trading configuration."""
        # Verify logging is properly configured
        verify_logging_setup()
        
        # Load environment variables
        load_dotenv()
        self._validate_env_vars()
        
        # Initialize API clients and components
        self.uw_client = WSUnusualWhalesClient(os.getenv("UNUSUAL_WHALES_API_KEY"))
        self.tasty_trader = TastyTrader(
            username=os.getenv("TASTY_USERNAME"),
            password=os.getenv("TASTY_PASSWORD"),
            account_id=os.getenv("TASTY_ACCOUNT_ID")
        )
        
        # Initialize Grok Analyzer
        self.grok_analyzer = GrokAnalyzer(api_key=os.getenv("XAI_API_KEY"))
        
        # Initialize Signal Detector with dependencies
        self.signal_detector = WSSignalDetector(
            api_client=self.uw_client,
            config=SignalConfig(),
            grok_analyzer=self.grok_analyzer
        )
        
        # Initialize Telegram notifier
        self.telegram = TelegramNotifier(
            token=os.getenv("TELEGRAM_BOT_TOKEN"),
            chat_id=os.getenv("TELEGRAM_CHAT_ID")
        )
        
        # Initialize Performance Tracker
        self.performance_tracker = PerformanceTracker()
        
        # Load configuration
        self.config = self._load_config()
        
        # Initialize state
        self.active_signals = []
        self.market_context = {}
        
        # Data refresh intervals (in seconds)
        self.refresh_intervals = {
            "flow_alerts": 120,  # 2 minutes
            "dark_pool": 900,    # 15 minutes
            "market_data": 3600  # 1 hour
        }
        
        # Last update timestamps
        self.last_updates = {
            "flow_alerts": datetime.min,
            "dark_pool": datetime.min,
            "market_data": datetime.min
        }
        
        # Cache for API data
        self.data_cache = {
            "flow_alerts": [],
            "dark_pool": {},
            "market_data": {}
        }
        
        # Set of tickers currently being processed to avoid duplicate processing
        self.processing_tickers = set()

    def _validate_env_vars(self):
        """Validate all required environment variables are present."""
        required_vars = [
            "UNUSUAL_WHALES_API_KEY",
            "TASTY_USERNAME",
            "TASTY_PASSWORD",
            "TASTY_ACCOUNT_ID",
            "TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID",
            "XAI_API_KEY"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def _load_config(self) -> Dict:
        """Load and validate configuration for live trading."""
        try:
            config_path = "config.json"
            logger.info(f"Loading configuration from {config_path}")
            
            with open(config_path, "r") as f:
                config = json.load(f)
            
            # Validate required configuration fields
            required_fields = [
                "signal_thresholds",
                "trading_hours",
                "risk_limits",
                "notification_settings"
            ]
            
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                error_msg = f"Missing required configuration fields: {', '.join(missing_fields)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Add scan intervals if not present
            if "scan_intervals" not in config:
                config["scan_intervals"] = {
                    "main_loop": 60,  # 1 minute between main cycles
                    "market_update": 3600,  # 1 hour for market data
                    "signal_update": 120   # 2 minutes for signal checks
                }
                
            logger.info("Configuration loaded successfully")
            return config
            
        except Exception as e:
            error_msg = f"Error loading configuration: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    async def update_market_context(self, force: bool = False):
        """
        Update market context data for signal analysis.
        
        Args:
            force: If True, force update regardless of refresh interval
        """
        try:
            current_time = datetime.now()
            
            # Check if update is needed based on refresh interval
            if not force and (current_time - self.last_updates["market_data"]).total_seconds() < self.refresh_intervals["market_data"]:
                logger.debug("Using cached market data, next update in "
                             f"{self.refresh_intervals['market_data'] - (current_time - self.last_updates['market_data']).total_seconds():.0f}s")
                return self.market_context
            
            # Try with market_tide first as it's the replacement for market_breadth
            market_data_tasks = [
                self.uw_client.get_market_tide(),  # API now uses /api/market/market-tide
                self.uw_client.get_darkpool_recent()  # /api/darkpool/recent
            ]
            
            # Gather results with a timeout
            market_data = await asyncio.gather(*market_data_tasks, return_exceptions=True)
            
            # Process market data
            market_breadth = None
            if not isinstance(market_data[0], Exception) and market_data[0]:
                if isinstance(market_data[0], dict) and not market_data[0].get('error'):
                    if 'data' in market_data[0]:
                        market_breadth = market_data[0]['data']
                    else:
                        market_breadth = market_data[0]
                    self.last_updates["market_data"] = current_time
                else:
                    logger.warning(f"Market tide data error: {market_data[0].get('message') if isinstance(market_data[0], dict) else 'Unknown error'}")
            
            # Process darkpool data
            dark_pool_data = None
            if not isinstance(market_data[1], Exception) and market_data[1]:
                if isinstance(market_data[1], dict) and not market_data[1].get('error'):
                    if 'data' in market_data[1]:
                        dark_pool_data = market_data[1]['data']
                    else:
                        dark_pool_data = market_data[1]
                    self.last_updates["dark_pool"] = current_time
                    self.data_cache["dark_pool"] = dark_pool_data
                else:
                    logger.warning(f"Dark pool data error: {market_data[1].get('message') if isinstance(market_data[1], dict) else 'Unknown error'}")
            
            self.market_context = {
                "market_data": market_breadth,
                "dark_pool_data": dark_pool_data,
                "timestamp": current_time.isoformat()
            }
            
            # Log market context contents for debugging
            logger.debug(f"Market context contents: {self.market_context}")
            
            # Log success with update times
            logger.info("Market context updated successfully")
            return self.market_context
            
        except Exception as e:
            logger.error(f"Error updating market context: {str(e)}")
            return self.market_context

    async def get_flow_alerts(self) -> List[Dict]:
        """
        Get flow alerts from API with caching based on refresh interval.
        """
        current_time = datetime.now()
        
        # Use cached data if available and fresh
        if "flow_alerts" in self.data_cache and "flow_alerts" in self.last_updates and \
           (current_time - self.last_updates["flow_alerts"]).total_seconds() < self.refresh_intervals["flow_alerts"]:
            logger.debug(f"Using cached flow alerts data ({len(self.data_cache['flow_alerts'])} alerts)")
            return self.data_cache["flow_alerts"]
        
        # Otherwise make a new API call
        try:
            logger.info("Fetching fresh flow alerts data")
            flow_alerts_response = await self.uw_client.get_flow_alerts()
            
            # Debug the raw response structure
            logger.debug(f"Flow alerts response structure: {type(flow_alerts_response)}")
            
            # Handle different response formats
            alerts_data = []
            
            if isinstance(flow_alerts_response, dict):
                # Try to extract data from standard API response format
                if 'data' in flow_alerts_response:
                    alerts_data = flow_alerts_response.get('data', [])
                # If no 'data' key but has other keys that look like flow data
                elif 'ticker' in flow_alerts_response or 'alerts' in flow_alerts_response:
                    if 'alerts' in flow_alerts_response:
                        alerts_data = flow_alerts_response.get('alerts', [])
                    else:
                        # It might be a single alert
                        alerts_data = [flow_alerts_response]
            elif isinstance(flow_alerts_response, list):
                # The API might be returning a direct list of alerts
                alerts_data = flow_alerts_response
                
            # Validate alerts data
            if not isinstance(alerts_data, list):
                logger.error(f"Invalid alerts data type: {format_for_logging(alerts_data)}")
                return []
                
            # Update cache if we got valid data
            if alerts_data:
                logger.info(f"Received {len(alerts_data)} flow alerts")
                
                # Log a summary of the data (not the full data)
                if len(alerts_data) > 0:
                    # Count alerts by ticker
                    ticker_counts = {}
                    for alert in alerts_data:
                        if isinstance(alert, dict) and 'ticker' in alert:
                            ticker = alert['ticker']
                            ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
                    
                    # Log top tickers
                    top_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                    logger.info(f"Top tickers in flow alerts: {format_for_logging(top_tickers)}")
                
                # Update cache
                self.data_cache["flow_alerts"] = alerts_data
                self.last_updates["flow_alerts"] = current_time
                
                return alerts_data
            else:
                logger.warning("No flow alerts data found in API response")
                return []
            
        except Exception as e:
            logger.error(f"Error getting flow alerts: {str(e)}")
            return []

    def _format_flow_alerts_summary(self, flow_alerts_data: List[Dict]) -> str:
        """
        Format flow alerts data into a readable summary.
        """
        if not flow_alerts_data:
            return "No flow alerts data available"
            
        # Count alerts by ticker
        ticker_counts = {}
        call_count = 0
        put_count = 0
        total_premium = 0
        
        for alert in flow_alerts_data:
            if isinstance(alert, dict):
                # Count by ticker
                if 'ticker' in alert:
                    ticker = alert['ticker']
                    ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
                
                # Count calls vs puts
                if 'call_or_put' in alert:
                    if alert['call_or_put'] == 'call':
                        call_count += 1
                    elif alert['call_or_put'] == 'put':
                        put_count += 1
                
                # Sum premium
                if 'premium' in alert:
                    try:
                        premium = float(str(alert['premium']).replace(',', ''))
                        total_premium += premium
                    except (ValueError, TypeError):
                        pass
        
        # Format the summary
        top_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        ticker_summary = ", ".join([f"{ticker}: {count}" for ticker, count in top_tickers])
        
        summary = [
            f"Flow Alerts Summary:",
            f"Total Alerts: {len(flow_alerts_data)}",
            f"Calls: {call_count} | Puts: {put_count} | Ratio: {call_count/(put_count or 1):.2f}",
            f"Total Premium: ${total_premium/1000000:.2f}M",
            f"Top Tickers: {ticker_summary}"
        ]
        
        return "\n".join(summary)

    def _format_darkpool_summary(self, darkpool_data: Dict) -> str:
        """
        Format darkpool data into a readable summary.
        """
        if not darkpool_data or not isinstance(darkpool_data, dict):
            return "No darkpool data available"
            
        # Extract relevant data
        ticker = darkpool_data.get('ticker', 'Unknown')
        trades = darkpool_data.get('trades', [])
        
        if not trades or not isinstance(trades, list):
            return f"No darkpool trades for {ticker}"
            
        # Analyze trades
        total_volume = sum(trade.get('size', 0) for trade in trades if isinstance(trade, dict))
        total_premium = sum(float(str(trade.get('premium', '0')).replace(',', '')) 
                           for trade in trades if isinstance(trade, dict))
        
        # Get recent trades
        recent_trades = trades[:5] if len(trades) > 5 else trades
        
        # Format the summary
        trade_details = []
        for trade in recent_trades:
            if isinstance(trade, dict):
                size = trade.get('size', 0)
                price = trade.get('price', 0)
                premium = trade.get('premium', 0)
                executed_at = trade.get('executed_at', '')
                
                # Format the time
                time_str = executed_at.split('T')[1].split('Z')[0] if 'T' in str(executed_at) else ''
                
                trade_details.append(f"  {time_str} | Size: {size} | Price: ${price} | Premium: ${premium}")
        
        summary = [
            f"Darkpool Summary for {ticker}:",
            f"Total Trades: {len(trades)}",
            f"Total Volume: {total_volume}",
            f"Total Premium: ${total_premium/1000000:.2f}M",
            f"Recent Trades:",
            *trade_details
        ]
        
        return "\n".join(summary)

    async def get_darkpool_data(self, ticker: str) -> Dict:
        """
        Get darkpool data for a specific ticker with caching.
        """
        # First check if we have fresh overall darkpool data
        current_time = datetime.now()
        
        # Use cached data for this ticker if available and fresh
        ticker_cache_key = f"darkpool_{ticker}"
        if ticker_cache_key in self.data_cache and (current_time - self.last_updates.get(ticker_cache_key, datetime.min)).total_seconds() < self.refresh_intervals["dark_pool"]:
            return self.data_cache[ticker_cache_key]
        
        # Otherwise make a new API call
        try:
            darkpool_data = await self.uw_client.get_darkpool_for_ticker(ticker)
            
            if darkpool_data and not (isinstance(darkpool_data, dict) and darkpool_data.get('error')):
                # Handle different response formats
                if isinstance(darkpool_data, dict) and 'data' in darkpool_data:
                    darkpool_data = darkpool_data['data']
                
                # Update cache
                self.data_cache[ticker_cache_key] = darkpool_data
                self.last_updates[ticker_cache_key] = current_time
                return darkpool_data
            
            logger.warning(f"Failed to get darkpool data for {ticker}")
            return {}
            
        except Exception as e:
            logger.error(f"Error getting darkpool data for {ticker}: {str(e)}")
            return {}

    async def process_signals(self, signals: List[TradingSignal]) -> List[TradingSignal]:
        """Process and enhance trading signals with strategy evaluation using signal confluence."""
        try:
            if not signals:
                return []
                
            # Enhance detector signals with AI insights
            enhanced_detector_signals = []
            for signal in signals:
                # Generate appropriate chart based on signal type
                chart_path = None
                if hasattr(signal, 'signal_type'):
                    if signal.signal_type == "DARK_POOL_ACTIVITY" and hasattr(self, 'chart_generator'):
                        dark_pool_data = await self.uw_client.get_darkpool_data(signal.ticker)
                        if dark_pool_data:
                            chart_path = self.chart_generator.generate_dark_pool_chart(signal.ticker, dark_pool_data)
                            # Add dark pool context to market context
                            if signal.ticker not in self.market_context.get('dark_pool_data', {}):
                                if 'dark_pool_data' not in self.market_context:
                                    self.market_context['dark_pool_data'] = {}
                                self.market_context['dark_pool_data'][signal.ticker] = dark_pool_data
                    
                    elif signal.signal_type == "OPTIONS_FLOW" and hasattr(self, 'chart_generator'):
                        flow_data = await self.uw_client.get_flow_alerts(ticker=signal.ticker)
                        if flow_data:
                            chart_path = self.chart_generator.generate_flow_analysis_chart(signal.ticker, flow_data)
                
                # Store chart path in signal for later use
                if chart_path:
                    signal.chart_path = chart_path
                
                enhanced_detector_signals.append(signal)
            
            return enhanced_detector_signals
            
        except Exception as e:
            logger.error(f"Error processing signals: {str(e)}")
            return signals  # Return original signals if enhancement fails

    async def process_ticker_batch(self, tickers: List[str]) -> List[TradingSignal]:
        """
        Process a batch of tickers to generate signals.
        This is more efficient than processing one ticker at a time.
        """
        if not tickers:
            return []
            
        signals = []
        
        try:
            # Get flow alerts first (shared across all tickers)
            flow_alerts = await self.get_flow_alerts()
            
            # Filter alerts by the tickers in this batch
            batch_alerts = [alert for alert in flow_alerts if alert.get('ticker') in tickers]
            
            # Process each ticker in the batch
            for ticker in tickers:
                # Skip if this ticker is already being processed
                if ticker in self.processing_tickers:
                    continue
                    
                self.processing_tickers.add(ticker)
                try:
                    # Get ticker-specific alerts
                    ticker_alerts = [alert for alert in batch_alerts if alert.get('ticker') == ticker]
                    
                    if not ticker_alerts:
                        logger.debug(f"No flow alerts for {ticker}")
                        continue
                        
                    # Get darkpool data for this ticker
                    darkpool_data = await self.get_darkpool_data(ticker)
                    
                    # Process through signal detector
                    ticker_signals = []
                    for alert in ticker_alerts:
                        # Add the darkpool data to the alert for context
                        alert['darkpool_data'] = darkpool_data
                        
                        # Detect signals
                        detected_signals = await self.signal_detector.detect_signals(alert)
                        if detected_signals:
                            ticker_signals.extend(detected_signals)
                    
                    if ticker_signals:
                        signals.extend(ticker_signals)
                        logger.info(f"Generated {len(ticker_signals)} signals for {ticker}")
                        
                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {str(e)}")
                finally:
                    # Always remove from processing set
                    self.processing_tickers.remove(ticker)
                    
                    # Add a small delay between tickers to smooth API calls
                    await asyncio.sleep(0.2)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            return []

    async def start(self):
        """Start the trading bot."""
        try:
            logger.info("Starting Unusual Whales Trading Bot...")
            
            # Initialize all components
            init_success = await self.initialize()
            if not init_success:
                logger.critical("Failed to initialize bot components")
                return False
            
            # Initialize Telegram notifier
            telegram_connected = False
            if self.telegram is not None:
                telegram_connected = await self.telegram.start()
                if not telegram_connected:
                    logger.warning("Failed to connect to Telegram. Notifications will be queued.")
            else:
                logger.warning("Telegram notifier is not initialized. No notifications will be sent.")
            
            # Send startup notification only once here, not in telegram.start()
            if self.telegram is not None:
                await self.telegram.send_message("🚀 *Unusual Whales Trading Bot Started*\nInitializing and connecting to APIs...")
            
            # No longer update market context here - only in run_trading_loop method
            # Initialize market_context as empty dict
            self.market_context = {}
            
            # Set running flag
            self.running = True
            
            logger.info("Bot started successfully")
            return True
            
        except Exception as e:
            logger.critical(f"Error during startup: {str(e)}")
            if self.telegram is not None:
                await self.telegram.send_message(f"❌ *Startup Error*\n{str(e)}")
            # Clean up resources
            await self.stop()
            raise

    async def initialize(self):
        """Initialize all bot components."""
        try:
            logger.info(f"Initializing Unusual Whales Trading Bot components")
            
            # Initialize API clients
            if self.uw_client is not None:
                try:
                    await self.uw_client.initialize()
                    logger.info("Unusual Whales API client initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize Unusual Whales API client: {str(e)}")
                    raise
            else:
                logger.warning("Unusual Whales API client is None")
            
            # Initialize TastyTrade client
            if self.tasty_trader is not None:
                try:
                    await self.tasty_trader.initialize()
                    logger.info("TastyTrade client initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize TastyTrade client: {str(e)}")
                    raise
            else:
                logger.warning("TastyTrade client is None")
            
            # Initialize Grok Analyzer
            if self.grok_analyzer is not None:
                try:
                    success = await self.grok_analyzer.initialize()
                    if success:
                        logger.info("Grok Analyzer initialized")
                    else:
                        logger.error("Failed to initialize Grok Analyzer")
                        self.grok_analyzer = None
                except Exception as e:
                    logger.error(f"Failed to initialize Grok Analyzer: {str(e)}")
                    self.grok_analyzer = None
            else:
                logger.warning("Grok Analyzer is None")
            
            # Initialize signal detector
            if self.signal_detector is not None:
                try:
                    await self.signal_detector.initialize()
                    logger.info("Signal detector initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize Signal detector: {str(e)}")
                    raise
            else:
                logger.warning("Signal detector is None")
            
            # Initialize Performance Tracker
            if self.performance_tracker is not None:
                try:
                    await self.performance_tracker.initialize()
                    logger.info("Performance Tracker initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize Performance Tracker: {str(e)}")
                    raise
            
            # Initialize Telegram notifier
            if self.telegram is not None:
                try:
                    await self.telegram.start()
                    logger.info("Telegram notifier initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize Telegram notifier: {str(e)}")
                    raise
            else:
                logger.warning("Telegram notifier is None")
            
            logger.info("All bot components initialized successfully")
            return True
            
        except Exception as e:
            logger.critical(f"Error during component initialization: {str(e)}")
            return False

    async def run_trading_loop(self):
        """Run the main trading loop."""
        logger.info("Starting main trading loop")
        
        # Set initial last update time
        self._last_market_context_update = datetime.now()
        logger.debug(f"Initialized last market update time: {self._last_market_context_update}")
        
        try:
            while True:
                try:
                    logger.info("Starting trading cycle")
                    
                    # 1. Get flow alerts
                    logger.info("Requesting flow alerts from Unusual Whales API...")
                    flow_alerts = await self.uw_client.get_flow_alerts(min_premium=10000, limit=50)
                    logger.info(f"Received {len(flow_alerts)} flow alerts")
                    
                    # Group alerts by ticker for more efficient processing
                    ticker_groups = {}
                    for alert in flow_alerts:
                        ticker = alert.get('ticker', '')
                        if ticker:
                            if ticker not in ticker_groups:
                                ticker_groups[ticker] = []
                            ticker_groups[ticker].append(alert)
                    
                    logger.info(f"Grouped flow alerts into {len(ticker_groups)} ticker groups")
                    
                    # Calculate time since last market context update
                    time_since_update = (datetime.now() - self._last_market_context_update).total_seconds()
                    logger.info(f"Time since last market context update: {time_since_update:.2f}s")
                    
                    # 2. Update market context (once per hour)
                    if time_since_update > 3600 or not self.market_context:
                        logger.info(f"Market context update triggered (last update was {time_since_update:.2f}s ago or context is empty)")
                        
                        # Log the state of market_context before update
                        if self.market_context:
                            logger.info(f"Market context before update: {len(self.market_context.keys()) if isinstance(self.market_context, dict) else 0} keys")
                        else:
                            logger.info("Market context before update: None or empty")
                        
                        # Get market tide data (replacement for market breadth)
                        try:
                            market_tide = await self.uw_client.get_market_tide()
                            if market_tide:
                                self.market_context = market_tide
                                logger.info(f"Updated market tide data successfully")
                            else:
                                logger.error("Market tide data empty")
                        except Exception as e:
                            logger.error(f"Failed to get market tide data: {str(e)}")
                        
                        # Log detailed market context information
                        if isinstance(self.market_context, dict):
                            context_keys = list(self.market_context.keys())
                            logger.info(f"MARKET CONTEXT KEYS: {context_keys}")
                            # Log a sample of the market context content (truncated to avoid excessive logging)
                            logger.info(f"MARKET CONTEXT CONTENTS: {str(self.market_context)[:1000]}...")
                            logger.info(f"Updated market context with {len(context_keys)} data sources")
                        else:
                            logger.warning("Market context is not a dictionary")
                            self.market_context = {}
                        
                        self._last_market_context_update = datetime.now()
                        logger.debug(f"Reset last market update time: {self._last_market_context_update}")
                    else:
                        logger.info(f"Skipping market context update (last update was {time_since_update:.2f}s ago)")
                    
                    # Rest of the trading loop remains the same
                    
                    # 3. Get congressional trading data (third priority)
                    congress_trades = await self.uw_client.get_congress_recent_trades()
                    if congress_trades and 'trades' in congress_trades:
                        logger.info(f"Received {len(congress_trades['trades'])} recent congressional trades")
                        
                        # Group congressional trades by ticker
                        congress_by_ticker = {}
                        for trade in congress_trades['trades']:
                            ticker = trade.get('ticker', 'UNKNOWN')
                            if ticker not in congress_by_ticker:
                                congress_by_ticker[ticker] = []
                            congress_by_ticker[ticker].append(trade)
                        
                        logger.info(f"Grouped congressional trades into {len(congress_by_ticker)} ticker groups")
                        
                        # Add tickers from congressional trades to our processing list if they're not already there
                        for ticker in congress_by_ticker:
                            if ticker not in ticker_groups and ticker != "UNKNOWN":
                                ticker_groups[ticker] = []
                    else:
                        logger.info("No congressional trade data received or empty response")
                        congress_by_ticker = {}
                    
                    # 4. Get insider trading data (fourth priority)
                    insider_trades = await self.uw_client.get_insider_transactions()
                    if insider_trades and 'transactions' in insider_trades:
                        logger.info(f"Received {len(insider_trades['transactions'])} recent insider transactions")
                        
                        # Group insider trades by ticker
                        insider_by_ticker = {}
                        for trade in insider_trades['transactions']:
                            ticker = trade.get('ticker', 'UNKNOWN')
                            if ticker not in insider_by_ticker:
                                insider_by_ticker[ticker] = []
                            insider_by_ticker[ticker].append(trade)
                        
                        logger.info(f"Grouped insider transactions into {len(insider_by_ticker)} ticker groups")
                        
                        # Add tickers from insider trades to our processing list if they're not already there
                        for ticker in insider_by_ticker:
                            if ticker not in ticker_groups and ticker != "UNKNOWN":
                                ticker_groups[ticker] = []
                    else:
                        logger.info("No insider transaction data received or empty response")
                        insider_by_ticker = {}
                    
                    # 5. Process each ticker group and generate signals
                    all_signals = []
                    for ticker, alerts in ticker_groups.items():
                        logger.info(f"Processing ticker {ticker} with {len(alerts)} flow alerts")
                        
                        # Combine all data sources for this ticker
                        ticker_data = {
                            'flow_alerts': alerts,
                            'dark_pool': congress_by_ticker.get(ticker, []),
                            'congress_trades': congress_by_ticker.get(ticker, []),
                            'insider_trades': insider_by_ticker.get(ticker, [])
                        }
                        
                        # Get ticker-specific dark pool data if we have alerts but no recent dark pool data
                        if alerts and not ticker_data['dark_pool']:
                            try:
                                ticker_dark_pool = await self.uw_client.get_darkpool_for_ticker(ticker)
                                if ticker_dark_pool and 'trades' in ticker_dark_pool:
                                    ticker_data['dark_pool'] = ticker_dark_pool['trades']
                                    logger.info(f"Added {len(ticker_data['dark_pool'])} ticker-specific dark pool trades for {ticker}")
                            except Exception as e:
                                logger.error(f"Error fetching dark pool data for {ticker}: {str(e)}")
                        
                        # Generate signals from flow alerts
                        signals = []
                        if alerts:  # If we have flow alerts
                            try:
                                flow_signals = await self.signal_detector.detect_signals_from_flow(
                                    ticker=ticker,
                                    flow_alerts=alerts
                                )
                                if flow_signals:
                                    logger.info(f"Generated {len(flow_signals)} signals from flow alerts for {ticker}")
                                    signals.extend(flow_signals)
                            except Exception as e:
                                logger.error(f"Error generating signals from flow for {ticker}: {str(e)}")
                        
                        # Enhance signals with additional data sources
                        for signal in signals:
                            # Add dark pool data
                            if ticker_data['dark_pool']:
                                signal.metadata['dark_pool_trades'] = ticker_data['dark_pool']
                                signal.metadata['dark_pool_count'] = len(ticker_data['dark_pool'])
                            
                            # Add congressional trade data
                            if ticker_data['congress_trades']:
                                signal.metadata['congress_trades'] = ticker_data['congress_trades']
                                signal.metadata['congress_count'] = len(ticker_data['congress_trades'])
                            
                            # Add insider trade data
                            if ticker_data['insider_trades']:
                                signal.metadata['insider_trades'] = ticker_data['insider_trades']
                                signal.metadata['insider_count'] = len(ticker_data['insider_trades'])
                        
                        if signals:
                            all_signals.extend(signals)
                    
                    # Process all signals through the signal processor
                    if all_signals:
                        logger.info(f"Processing {len(all_signals)} signals through signal processor")
                        # Log signal details before processing
                        for i, signal in enumerate(all_signals[:5]):  # Log first 5 signals
                            logger.info(f"Signal {i+1} before processing: Ticker={signal.ticker}, Type={signal.signal_type if hasattr(signal, 'signal_type') else 'Unknown'}, Confidence={getattr(signal, 'confidence', 'Unknown')}")
                        
                        processed_signals = await self.process_signals(all_signals)
                        
                        # Send trading alerts for processed signals
                        if processed_signals:
                            logger.info(f"Sending trading alerts for {len(processed_signals)} processed signals")
                            # Log processed signal details
                            for i, signal in enumerate(processed_signals[:5]):  # Log first 5 signals
                                logger.info(f"Processed signal {i+1}: Ticker={signal.ticker}, Type={signal.signal_type if hasattr(signal, 'signal_type') else 'Unknown'}, Confidence={getattr(signal, 'confidence', 'Unknown')}")
                            
                            # 6. Select optimal trading strategies for each signal
                            try:
                                # Get available strategies
                                available_strategies = [
                                    "momentum_options",
                                    "mean_reversion",
                                    "gamma_scalping",
                                    "volatility_arbitrage",
                                    "dark_pool_reversal",
                                    "options_flow_following",
                                    "congressional_following",
                                    "insider_sentiment",
                                    "earnings_volatility",
                                    "sector_rotation"
                                ]
                                
                                # Convert signals to dict format for strategy selection
                                signals_dict = []
                                for signal in processed_signals:
                                    signal_data = {
                                        "ticker": signal.ticker,
                                        "type": signal.signal_type.value if hasattr(signal.signal_type, 'value') else str(signal.signal_type),
                                        "direction": signal.suggested_direction,
                                        "confidence": signal.confidence,
                                        "metadata": signal.metadata
                                    }
                                    signals_dict.append(signal_data)
                                
                                # Select strategies based on signals and market context
                                # Get recommended strategies from strategy manager instead of Grok
                                recommended_strategies = []
                                logger.info(f"Recommended strategies: {recommended_strategies}")
                                
                                # Assign strategies to signals
                                for signal in processed_signals:
                                    # Determine the best strategy for this signal
                                    best_strategy = "options_flow_following"  # Default fallback
                                    signal.strategy_name = best_strategy
                                    logger.info(f"Assigned strategy '{best_strategy}' to signal for {signal.ticker}")
                            
                            except Exception as e:
                                logger.error(f"Error selecting strategies: {str(e)}")
                            
                            # 7. Get Grok analysis for each signal
                            for signal in processed_signals:
                                try:
                                    # Get signal analysis from strategy manager
                                    logger.info(f"Getting signal analysis for signal: {signal.ticker}")
                                    
                                    # Use the Grok analysis that was already attached to the signal
                                    signal_analysis = getattr(signal, 'grok_analysis', {})
                                    if not signal_analysis:
                                        signal_analysis = {
                                            'direction': signal.suggested_direction,
                                            'confidence': signal.confidence,
                                            'summary': f"Signal based on {signal.signal_type} with {signal.confidence:.0%} confidence",
                                            'risk_factors': ["No Grok analysis available"]
                                        }
                                    
                                    logger.info(f"Using signal analysis for {signal.ticker}: {list(signal_analysis.keys()) if isinstance(signal_analysis, dict) else 'Invalid analysis'}")
                                    
                                    # Attach signal analysis to signal for Telegram notification
                                    signal.ai_analysis = signal_analysis
                                    
                                    # Get chart path if available
                                    chart_path = getattr(signal, 'chart_path', None)
                                    logger.info(f"Chart path for {signal.ticker}: {chart_path}")
                                    
                                    # Send notification via Telegram
                                    await self.telegram.send_signal_alert(signal, signal_analysis, chart_path)
                                except Exception as e:
                                    logger.error(f"Error processing signal for {signal.ticker}: {str(e)}")
                            else:
                                logger.info("No signals passed confidence threshold for trading alerts")
                        else:
                            logger.info("No signals generated for processing")
                    else:
                        logger.info("No signals generated for processing")
                
                except Exception as e:
                    logger.error(f"Error in trading cycle: {str(e)}", exc_info=True)
                    # Sleep before retrying
                    await asyncio.sleep(self.config.get("error_retry_interval", 30))
        
        except Exception as e:
            logger.error(f"Fatal error in trading loop: {str(e)}", exc_info=True)
            raise
            
    async def execute_trades(self, signals: List[TradingSignal], is_live: bool = True):
        """Execute or simulate trades based on signals."""
        try:
            for signal in signals:
                if signal.confidence >= self.config["signal_thresholds"]["high_confidence"]:
                    # Prepare trade parameters
                    if signal.options_data:
                        opt = signal.options_data[0]
                        order_data = {
                            'symbol': signal.ticker,
                            'quantity': 1,  # Start with 1 contract
                            'order_type': 'limit',
                            'side': 'buy' if signal.suggested_direction == "bullish" else 'sell',
                            'option_type': opt['type'],
                            'strike': opt['strike'],
                            'expiration': opt['expiry']
                        }
                        
                        if is_live:
                            # Place real order during market hours
                            order_response = await self.tasty_trader.place_order(order_data)
                            
                            if 'error' not in order_response:
                                order_id = order_response['data']['order-id']
                                logger.info(f"🚨 LIVE TRADE EXECUTED 🚨 for {signal.ticker} - Order ID: {order_id}")
                                
                                # Get chart path if available
                                chart_path = getattr(signal, 'chart_path', None)
                                
                                # Send notification with chart if available
                                if hasattr(self.telegram, 'send_signal_alert') and chart_path:
                                    await self.telegram.send_signal_alert(
                                        signal=signal,
                                        grok_analysis=getattr(signal, 'ai_analysis', None),
                                        chart_path=chart_path
                                    )
                                else:
                                    await self.telegram.send_message(
                                        f"🚨 *LIVE TRADE EXECUTED* 🚨\n"
                                        f"Ticker: {signal.ticker}\n"
                                        f"Direction: {signal.suggested_direction.upper()}\n"
                                        f"Strike: ${opt['strike']}\n"
                                        f"Expiry: {opt['expiry']}\n"
                                        f"Order ID: {order_id}"
                                    )
                            else:
                                logger.error(f"Trade execution failed for {signal.ticker}: {order_response['error']}")
                        else:
                            # Send alert for potential trade outside market hours
                            logger.info(f"🔍 TRADE SIGNAL DETECTED (Outside Market Hours) for {signal.ticker}")
                            
                            # Get chart path if available
                            chart_path = getattr(signal, 'chart_path', None)
                            
                            # Send notification with chart if available
                            if hasattr(self.telegram, 'send_signal_alert') and chart_path:
                                # Add outside market hours note to signal
                                signal.note = "This trade would have been executed if within market hours."
                                await self.telegram.send_signal_alert(
                                    signal=signal,
                                    grok_analysis=getattr(signal, 'ai_analysis', None),
                                    chart_path=chart_path
                                )
                            else:
                                await self.telegram.send_message(
                                    f"🔍 *TRADE SIGNAL DETECTED* (Outside Market Hours)\n"
                                    f"Ticker: {signal.ticker}\n"
                                    f"Direction: {signal.suggested_direction.upper()}\n"
                                    f"Strike: ${opt['strike']}\n"
                                    f"Expiry: {opt['expiry']}\n"
                                    f"Confidence: {signal.confidence:.1%}\n"
                                    f"*Note: This trade would have been executed if within market hours.*"
                                )
                    
        except Exception as e:
            logger.error(f"Error in trade execution: {str(e)}")

    def _is_market_hours(self) -> bool:
        """
        Check if current time is within market hours.
        Always returns True to allow operation at all times, but logs market hours status.
        """
        now = datetime.now().time()
        market_open = datetime_time(9, 30)  # 9:30 AM EST
        market_close = datetime_time(16, 0)  # 4:00 PM EST
        
        is_market_hours = market_open <= now <= market_close
        
        if not is_market_hours:
            logger.info("Outside regular market hours, but continuing to operate for after-hours activity (congressional trading, etc.)")
        
        # Always return True to allow operation at all times
        return True

    async def stop(self):
        """Stop the bot and clean up resources."""
        try:
            # Stop the trading loop
            self.running = False
            
            # Don't call cleanup here - we'll do it in the main cleanup method
            logger.info("Bot stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping bot: {str(e)}")

    async def cleanup(self):
        """Clean up resources before shutting down."""
        try:
            # Close API clients
            if hasattr(self, 'uw_client') and self.uw_client is not None:
                await self.uw_client.cleanup()
            
            # Close Telegram client
            if hasattr(self, 'telegram') and self.telegram is not None:
                await self.telegram.close()
            
            # Close TastyTrade client
            if hasattr(self, 'tasty_trader') and self.tasty_trader is not None:
                await self.tasty_trader.close()
            
            # Close Grok Analyzer
            if hasattr(self, 'grok_analyzer') and self.grok_analyzer is not None:
                await self.grok_analyzer.cleanup()
            
            # Close Performance Tracker
            if hasattr(self, 'performance_tracker') and self.performance_tracker is not None:
                await self.performance_tracker.cleanup()
            
            logger.info("All resources cleaned up successfully")
            return True
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            return False

    async def _process_signal(self, signal: TradingSignal) -> None:
        """Process a trading signal."""
        try:
            # Get signal analysis
            signal_analysis = await self._analyze_signal(signal)
            
            # Send notification
            if hasattr(self.telegram, 'send_signal_alert'):
                await self.telegram.send_signal_alert(signal, signal_analysis)
                logger.info(f"Sent signal alert for {signal.ticker}")
            
            # Track performance
            if self.performance_tracker:
                await self.performance_tracker.track_signal(signal)
                
        except Exception as e:
            logger.error(f"Error processing signal for {signal.ticker}: {str(e)}")
            
    async def _analyze_signal(self, signal: TradingSignal) -> Dict:
        """Analyze a single trading signal using Grok."""
        try:
            # Prepare analysis data
            analysis_data = {
                "market_context": self.market_context if hasattr(self, 'market_context') else {},
                "signal_data": signal.to_dict() if hasattr(signal, 'to_dict') else signal.__dict__
            }
            
            # Get Grok analysis
            analyses = await self.grok_analyzer.analyze_signals([signal], analysis_data)
            if analyses and len(analyses) > 0:
                return analyses[0]  # Return the first analysis since we only sent one signal
            else:
                logger.warning(f"No analysis returned for {signal.ticker}")
                return {}
                
        except Exception as e:
            logger.error(f"Error analyzing signal: {str(e)}")
            return {}

    async def _process_flow_alerts(self, flow_alerts: List[Dict]) -> None:
        """Process flow alerts and generate signals."""
        try:
            if not flow_alerts:
                logger.warning("No flow alerts to process")
                return

            # Validate flow alerts
            valid_alerts = []
            for alert in flow_alerts:
                try:
                    # Handle string alerts
                    if isinstance(alert, str):
                        try:
                            alert = json.loads(alert)
                        except json.JSONDecodeError:
                            continue
                    
                    # Validate dictionary format
                    if not isinstance(alert, dict):
                        continue
                    
                    # Ensure required fields
                    if 'ticker' not in alert:
                        continue
                    
                    valid_alerts.append(alert)
                except Exception as e:
                    logger.error(f"Error validating alert: {str(e)}")
                    continue

            if not valid_alerts:
                logger.warning("No valid alerts to process")
                return

            logger.info(f"=== Starting to process {len(valid_alerts)} flow alerts ===")
            
            # Group alerts by ticker for better logging
            ticker_alerts = {}
            for alert in valid_alerts:
                ticker = alert.get('ticker')
                if ticker:
                    if ticker not in ticker_alerts:
                        ticker_alerts[ticker] = []
                    ticker_alerts[ticker].append(alert)
            
            logger.info(f"Grouped alerts by ticker: {list(ticker_alerts.keys())}")
            
            for ticker, alerts in ticker_alerts.items():
                try:
                    logger.info(f"\n=== Processing alerts for {ticker} ===")
                    logger.info(f"Number of alerts for {ticker}: {len(alerts)}")
                    
                    # Get options data
                    logger.info(f"Fetching options chain for {ticker}")
                    options_chain = await self.uw_client.get_options_chain(ticker)
                    if options_chain and isinstance(options_chain, dict) and not options_chain.get("error"):
                        logger.info(f"Successfully retrieved options chain for {ticker}")
                    else:
                        logger.warning(f"Failed to retrieve options chain for {ticker}")
                    
                    # Detect signals
                    logger.info(f"Detecting signals for {ticker}")
                    signals = await self.signal_detector.detect_signals(alerts)
                    
                    if signals:
                        logger.info(f"Generated {len(signals)} signals for {ticker}")
                        for signal in signals:
                            logger.info(f"\nSignal details for {ticker}:")
                            logger.info(f"  Type: {signal.signal_type}")
                            logger.info(f"  Direction: {signal.suggested_direction}")
                            logger.info(f"  Confidence: {signal.confidence}")
                            logger.info(f"  Risk Score: {signal.risk_score}")
                            
                            if hasattr(signal, 'options_data') and signal.options_data:
                                logger.info(f"  Options contracts:")
                                for contract in signal.options_data:
                                    logger.info(f"    {contract['type']} ${contract['strike']} exp {contract['expiration']}")
                                    logger.info(f"      Volume: {contract['volume']} | OI: {contract['open_interest']} | IV: {contract['implied_volatility']}")
                            
                            if hasattr(signal, 'grok_analysis') and signal.grok_analysis:
                                logger.info(f"  Grok Analysis: {json.dumps(signal.grok_analysis, indent=2)}")
                            
                            # Send notification
                            logger.info(f"Sending notification for {ticker}")
                            await self.telegram.send_signal_alert(signal)
                    else:
                        logger.info(f"No signals generated for {ticker}")
                    
                except Exception as e:
                    logger.error(f"Error processing alerts for {ticker}: {str(e)}")
                    continue
            
            logger.info("=== Completed processing flow alerts ===")
            
        except Exception as e:
            logger.error(f"Error in _process_flow_alerts: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    async def main():
        """Main entry point for the trading bot."""
        bot = None
        try:
            # Create the trading bot
            bot = UnusualWhalesBot()
            
            # Initialize the bot
            await bot.initialize()
            
            # Start the bot
            await bot.start()
            
            # Run the trading loop
            await bot.run_trading_loop()
            
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.error(f"Unhandled exception: {str(e)}")
        finally:
            # Ensure we clean up resources
            try:
                if bot is not None:
                    await bot.cleanup()
            except Exception as e:
                logger.error(f"Error in final cleanup: {str(e)}")
            
            # Final message
            logger.info("Bot shutdown complete")
            
            # Check logger_config.py every time the bot is done running
            logger.info("Remember to check logger_config.py for any issues")
    
    # Run the main function
    asyncio.run(main())