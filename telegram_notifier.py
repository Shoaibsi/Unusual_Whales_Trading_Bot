import asyncio
import aiohttp
import os
from typing import Optional, Dict, Any, List, Union
import json
from src.logger_config import logger, log_exceptions, log_api_call, log_notification
from src.ws_signal_detector import TradingSignal
from datetime import datetime
from math import ceil

class TelegramNotifier:
    """Send notifications via Telegram."""
    
    def __init__(self, token: str = None, chat_id: str = None):
        """Initialize Telegram notifier with token and chat ID."""
        self.token = token or os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else None
        self.session = None
        self.connected = False
        self.message_queue = asyncio.Queue()
        self.worker_task = None
        
        # Initialize session in the background
        if self.token and self.chat_id:
            self.init_task = asyncio.create_task(self._initialize())
            logger.info("Telegram notifier initialization started in background")
        else:
            logger.warning("Telegram token or chat ID not provided, notifications will be disabled")
    
    async def _initialize(self):
        """Initialize the session and test connection in the background."""
        try:
            await self._ensure_session()
            self.connected = await self._test_connection()
            if self.connected:
                logger.info("Telegram notifier connected successfully")
                if self.worker_task is None or self.worker_task.done():
                    self.worker_task = asyncio.create_task(self._message_worker())
                    logger.info("Started Telegram message worker task")
                if not self.message_queue.empty():
                    logger.info(f"Processing {self.message_queue.qsize()} queued messages")
            else:
                logger.error("Failed to connect to Telegram API during initialization")
        except Exception as e:
            logger.error(f"Error during Telegram notifier initialization: {str(e)}")
            self.connected = False
    
    @log_exceptions
    @log_api_call("Telegram Start")
    async def start(self):
        """Explicitly start the notifier, ensuring initialization is complete."""
        if self.token and self.chat_id:
            if hasattr(self, 'init_task') and not self.init_task.done():
                try:
                    await self.init_task
                except Exception as e:
                    logger.error(f"Error during Telegram notifier initialization: {str(e)}")
                    await self._initialize()
            if not hasattr(self, 'init_task') or self.init_task.done() and not self.connected:
                await self._initialize()
            if self.connected:
                logger.info("Initialized TelegramNotifier successfully")
                return True
            else:
                logger.warning("Failed to initialize TelegramNotifier")
                return False
        else:
            logger.warning("Telegram token or chat ID not provided, notifications will be disabled")
            return False
    
    async def _ensure_session(self):
        """Ensure an aiohttp session exists and is active."""
        try:
            if self.session is None or self.session.closed:
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self.session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    headers={"Content-Type": "application/json"}
                )
                logger.debug("Created new aiohttp session for Telegram")
            return True
        except Exception as e:
            logger.error(f"Error creating aiohttp session: {str(e)}")
            return False
    
    @log_exceptions
    @log_api_call("Telegram Test Connection")
    async def _test_connection(self) -> bool:
        """Test connection to Telegram API."""
        try:
            await self._ensure_session()
            async with self.session.get(f"{self.base_url}/getMe") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        bot_name = data.get("result", {}).get("username")
                        logger.info(f"Connected to Telegram bot: @{bot_name}")
                        return True
            return False
        except Exception as e:
            logger.error(f"Error testing Telegram connection: {str(e)}")
            return False
    
    @log_exceptions
    @log_notification("Telegram Message")
    async def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        """Send a message to the Telegram chat."""
        if not self.token or not self.chat_id:
            logger.warning("Telegram token or chat ID not provided, cannot send message")
            return False
        if not self.connected:
            if hasattr(self, 'init_task') and not self.init_task.done():
                try:
                    await self.init_task
                except Exception as e:
                    logger.error(f"Error during Telegram notifier initialization: {str(e)}")
            if not self.connected:
                await self._initialize()
                if not self.connected:
                    logger.error("Failed to connect to Telegram API, message will be queued")
                    await self.message_queue.put({"text": text, "parse_mode": parse_mode})
                    return False
        await self._ensure_session()
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode
            }
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    response_json = await response.json()
                    if response_json.get("ok"):
                        logger.debug(f"Successfully sent Telegram message: {text[:50]}...")
                    else:
                        logger.error(f"Telegram API error: {response_json}")
                else:
                    logger.error(f"Telegram API HTTP error: {response.status}, {await response.text()}")
            return False
        except Exception as e:
            logger.error(f"Error sending Telegram message: {str(e)}")
            await self.message_queue.put({"text": text, "parse_mode": parse_mode})
            return False
    
    async def _message_worker(self):
        """Background worker to process message queue."""
        logger.info("Starting Telegram message worker")
        
        while True:
            try:
                # Wait for a message in the queue
                message_data = await self.message_queue.get()
                
                # Check if we're connected
                if not self.connected:
                    # Try to reconnect
                    await self._initialize()
                    if not self.connected:
                        logger.warning("Failed to reconnect to Telegram API, requeueing message")
                        # Put the message back in the queue and wait
                        await self.message_queue.put(message_data)
                        await asyncio.sleep(30)  # Wait 30 seconds before retrying
                        continue
                
                # Extract message details
                text = message_data.get("text", "")
                parse_mode = message_data.get("parse_mode", "Markdown")
                
                # Send the message
                try:
                    url = f"{self.base_url}/sendMessage"
                    data = {
                        "chat_id": self.chat_id,
                        "text": text,
                        "parse_mode": parse_mode
                    }
                    
                    async with self.session.post(url, json=data) as response:
                        if response.status == 200:
                            response_json = await response.json()
                            if response_json.get("ok"):
                                logger.debug(f"Successfully sent queued Telegram message: {text[:50]}...")
                            else:
                                logger.error(f"Telegram API error for queued message: {response_json}")
                        else:
                            logger.error(f"Telegram API HTTP error for queued message: {response.status}")
                
                except Exception as e:
                    logger.error(f"Error sending queued Telegram message: {str(e)}")
                    # Requeue the message for later retry
                    await self.message_queue.put(message_data)
                
                # Mark the task as done
                self.message_queue.task_done()
                
                # Avoid rate limiting
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("Telegram message worker cancelled")
                break
            except Exception as e:
                logger.error(f"Error in Telegram message worker: {str(e)}")
                await asyncio.sleep(5)  # Wait before continuing
    
    # @log_exceptions
    # @log_api_call("Telegram Send Photo")
    # async def send_photo(self, photo_path: str, caption: str = None) -> bool:
    #     """Send a photo to the Telegram chat."""
    #     if not self.connected:
    #         logger.warning("Telegram notifier not connected, cannot send photo")
    #         return False
    #     try:
    #         await self._ensure_session()
    #         if not os.path.exists(photo_path):
    #             logger.error(f"Photo file not found: {photo_path}")
    #             return False
    #         form_data = aiohttp.FormData()
    #         form_data.add_field("chat_id", self.chat_id)
    #         if caption:
    #             form_data.add_field("caption", caption)
    #         with open(photo_path, "rb") as photo:
    #             form_data.add_field("photo", photo, filename=os.path.basename(photo_path))
    #         async with self.session.post(f"{self.base_url}/sendPhoto", data=form_data) as response:
    #             if response.status == 200:
    #                 data = await response.json()
    #                 if data.get("ok"):
    #                     logger.debug(f"Telegram photo sent successfully")
    #                     return True
    #             error_text = await response.text()
    #             logger.error(f"Error sending Telegram photo: {response.status} - {error_text}")
    #             return False
    #     except Exception as e:
    #         logger.error(f"Exception sending Telegram photo: {str(e)}")
    #         return False
    
    @log_exceptions
    async def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up Telegram notifier")
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
        if self.session and not self.session.closed:
            await self.session.close()
            logger.debug("Telegram session closed")
    
    @log_exceptions
    async def send_trade_execution(self, signal: TradingSignal, order_id: str) -> None:
        """Send a detailed trade execution alert."""
        try:
            direction_emoji = "🟢" if signal.suggested_direction == "bullish" else "🔴"
            header = (
                f"{direction_emoji} <b>LIVE TRADE EXECUTED</b> {direction_emoji}\n\n"
                f"<b>Ticker:</b> {signal.ticker}\n"
                f"<b>Order ID:</b> {order_id}\n"
                f"<b>Direction:</b> {signal.suggested_direction.upper()}\n"
                f"<b>Confidence:</b> {signal.confidence:.1%}\n"
                f"<b>Risk Score:</b> {signal.risk_score:.1%}\n\n"
            )
            details = ""
            if signal.signal_type == "DARK_POOL_ACTIVITY":
                data = signal.primary_data
                details = (
                    "<b>Dark Pool Activity Details:</b>\n"
                    f"• Volume: {data.get('total_volume', 0):,.0f} ({data.get('volume_percentage', 0):.1f}% of daily)\n"
                    f"• Notional Value: ${data.get('notional_value', 0):,.0f}\n"
                    f"• Buy/Sell Ratio: {data.get('buy_ratio', 0):.2f}\n"
                    f"• Price Range: ${data.get('min_price', 0):.2f} - ${data.get('max_price', 0):.2f}\n"
                    f"• Price Volatility: {data.get('price_volatility', 0):.2f}\n"
                )
            elif signal.signal_type == "OPTIONS_FLOW":
                for option in signal.options_data:
                    details += (
                        "<b>Options Flow Details:</b>\n"
                        f"• Contract: {option.get('type', '').upper()} {option.get('strike')} {option.get('expiration')}\n"
                        f"• Volume: {option.get('volume', 0)} contracts\n"
                        f"• Premium: ${option.get('premium', 0):,.0f}\n"
                        f"• IV: {option.get('implied_volatility', 0):.1f}%\n"
                        f"• Greeks: Δ={option.get('delta', 0):.2f} Γ={option.get('gamma', 0):.4f} "
                        f"Θ={option.get('theta', 0):.2f} V={option.get('vega', 0):.2f}\n"
                        f"• Sweep: {'Yes 🔄' if option.get('is_sweep') else 'No'}\n"
                    )
            analysis = ""
            # Commented out Grok analysis section
            # if signal.grok_analysis:
            #     grok = signal.grok_analysis
            #     logger.debug(f"Grok analysis for {signal.ticker}: {grok}")
            #     analysis = "\n<b>Grok Analysis:</b>\n"
            #     sentiment = grok.get('direction', grok.get('sentiment', 'NEUTRAL'))
            #     analysis += f"*Sentiment:* {sentiment.upper()}\n\n"
            #     key_factors = grok.get('key_factors', [])
            #     if key_factors:
            #         analysis += "*Key Factors:*\n"
            #         for factor in key_factors[:3]:
            #             analysis += f"• {factor}\n"
            #         analysis += "\n"
            #     if 'risk_factors' in grok or 'risks' in grok:
            #         risks = grok.get('risk_factors', grok.get('risks', []))
            #         if risks:
            #             analysis += "*Risks:*\n"
            #             for risk in risks[:2]:
            #                 analysis += f"• {risk}\n"
            #             analysis += "\n"
            #     trade_setup = grok.get('trade_setup', {})
            #     if trade_setup:
            #         entry_points = trade_setup.get('entry_points', [])
            #         if entry_points:
            #             analysis += f"*Entry Points:*\n"
            #             for point in entry_points[:2]:
            #                 analysis += f"• {point}\n"
            #             analysis += "\n"
            #     option_recommendations = grok.get('option_recommendations', [])
            #     if option_recommendations:
            #         analysis += "*Option Recommendations:*\n"
            #         for rec in option_recommendations[:2]:
            #             if isinstance(rec, dict):
            #                 strike = rec.get('strike', 'N/A')
            #                 option_type = rec.get('type', 'N/A')
            #                 expiry = rec.get('expiration', 'N/A')
            #                 rationale = rec.get('rationale', '')
            #                 analysis += f"• {option_type.upper()} ${strike} exp {expiry}\n"
            #                 if rationale:
            #                     analysis += f"  {rationale}\n"
            #             else:
            #                 analysis += f"• {rec}\n"
            #         analysis += "\n"
            #     if 'historical_context' in grok:
            #         analysis += f"*Historical Context:*\n{grok.get('historical_context')}\n\n"
            #     logger.debug(f"Added Grok analysis section for {signal.ticker}")
            message = header + details + analysis
            message += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            message += f"Generated: {datetime.now().isoformat()}"
            logger.debug(f"Final notification message for {signal.ticker}: {message[:100]}...")
            await self.send_message(message)
            logger.info(f"Sent trade execution alert for {signal.ticker}")
        except Exception as e:
            logger.error(f"Error sending trade execution alert: {str(e)}")

    @log_exceptions
    @log_api_call("Telegram Send Signal Notification")
    @log_notification("Signal Notification")
    async def send_signal_notification(self, signal: TradingSignal) -> None:
        """Send a detailed signal notification with optimized formatting and technical analysis."""
        try:
            logger.info(f"Preparing signal notification for {signal.ticker}")
            signal_type = getattr(signal, 'signal_type', 'UNKNOWN')
            direction = getattr(signal, 'suggested_direction', 'neutral').upper()
            # Add detailed logging for NEUTRAL signals
            if direction == "NEUTRAL":
                logger.info(f"Processing NEUTRAL signal for {signal.ticker}")
                options_data = getattr(signal, 'options_data', [])
                logger.info(f"Options data for NEUTRAL signal: {len(options_data)} items")
                if options_data:
                    logger.info(f"First option item: {options_data[0]}")
            confidence = getattr(signal, 'confidence', 0.5)
            risk_score = getattr(signal, 'risk_score', 0.5)
            timestamp = datetime.now().isoformat()
            analysis_data = getattr(signal, 'analysis_data', {})  # Renamed from grok_analysis
            direction_emoji = "ud83dudfe2" if direction in ["BUY", "BULLISH", "LONG", "CALL"] else "ud83dudd34" if direction in ["SELL", "BEARISH", "SHORT", "PUT"] else "u26aa"
            
            # Use HTML formatting instead of Markdown to avoid parsing issues
            header = (
                f"ud83dudea8 <b>NEW TRADING SIGNAL ALERT</b> ud83dudea8\n\n"
                f"<b>ud83dudccc TICKER:</b> {signal.ticker} {direction_emoji}\n"
                f"  - Type: {signal_type}\n"
                f"  - Direction: {direction}\n"
                f"  - Confidence: {confidence:.2f}\n"
                f"  - Risk Score: {risk_score:.2f}\n"
                f"  - Generated: {timestamp}\n\n"
            )
            options_data = getattr(signal, 'options_data', [])
            key_metrics = "<b>ud83dudcca KEY METRICS:</b>\n"

            # Special handling for NEUTRAL signals
            if direction == "NEUTRAL":
                logger.info(f"Processing NEUTRAL signal for {signal.ticker} with {len(options_data)} options items")
                
                if options_data and len(options_data) >= 2:
                    # Find call and put options
                    call_options = [opt for opt in options_data if opt.get('type', '').lower() == 'call']
                    put_options = [opt for opt in options_data if opt.get('type', '').lower() == 'put']
                    
                    if call_options and put_options:
                        logger.info(f"Found both call and put options for NEUTRAL signal on {signal.ticker}")
                        call_option = call_options[0]
                        put_option = put_options[0]
                        # Display metrics for both options
                        call_vol_oi_ratio = call_option.get('volume', 0) / max(1, call_option.get('open_interest', 1))
                        put_vol_oi_ratio = put_option.get('volume', 0) / max(1, put_option.get('open_interest', 1))
                        
                        key_metrics += (
                            f"  - CALL Option:\n"
                            f"    - Strike Price: ${float(call_option.get('strike', 0)):.2f}\n"
                            f"    - Expiry Date: {call_option.get('expiration', 'N/A')}\n"
                            f"    - Volume: {float(call_option.get('volume', 0)):,}\n"
                            f"    - Vol/OI Ratio: {float(call_vol_oi_ratio):.2f}\n"
                            f"  - PUT Option:\n"
                            f"    - Strike Price: ${float(put_option.get('strike', 0)):.2f}\n"
                            f"    - Expiry Date: {put_option.get('expiration', 'N/A')}\n"
                            f"    - Volume: {float(put_option.get('volume', 0)):,}\n"
                            f"    - Vol/OI Ratio: {float(put_vol_oi_ratio):.2f}\n\n"
                        )
                    else:
                        key_metrics += f"  - Limited options data available for balanced analysis\n\n"
                        logger.warning(f"NEUTRAL signal for {signal.ticker} has options data but missing balanced call/put options")
                else:
                    key_metrics += f"  - No options data available for NEUTRAL analysis\n\n"
                    logger.warning(f"NEUTRAL signal for {signal.ticker} has no options data")
                    key_metrics += f"  - Using technical indicators instead:\n"
                    key_metrics += f"    • Consider monitoring volume/OI changes\n"
                    key_metrics += f"    • Watch for price action near support/resistance levels\n"
                    key_metrics += f"    • Use technical indicators\n"
                    key_metrics += "\n"
            elif options_data and len(options_data) > 0:
                # Standard handling for directional signals (existing code)
                option = options_data[0]
                vol_oi_ratio = option.get('volume', 0) / max(1, option.get('open_interest', 1))
                key_metrics += (
                    f"  - Option Type: {option.get('type', '').upper()}\n"
                    f"  - Strike Price: ${float(option.get('strike', 0)):.2f}\n"
                    f"  - Expiry Date: {option.get('expiration', 'N/A')}\n"
                    f"  - Volume: {float(option.get('volume', 0)):,}\n"
                    f"  - Open Interest: {float(option.get('open_interest', 0)):,}\n"
                    f"  - Vol/OI Ratio: {float(vol_oi_ratio):.2f}\n"
                    f"  - Implied Volatility: {float(option.get('implied_volatility', 0)):.1f}%\n\n"
                )
            else:
                key_metrics += "  - No options data available\n\n"
                logger.warning(f"Missing options data for {signal.ticker}")
            
            # Technical insights section (previously Grok AI insights)
            technical_section = "<b>ud83dudca1 TECHNICAL INSIGHTS:</b>\n"
            technical_section += f"  - Sentiment: {signal.suggested_direction.upper()}\n"
            technical_section += f"  - Key Factors: Volume/OI ratio, Premium threshold, Technical indicators\n"
            technical_section += f"  - Risks: Market volatility, Liquidity constraints\n"
            technical_section += f"  - Market Context: Based on current market conditions\n\n"
            
            # Entry points calculation
            if hasattr(signal, 'options_data') and signal.options_data:
                option = signal.options_data[0]
                current_price = option.get('underlying_price', 0)
                if current_price > 0:
                    # Calculate entry points based on current price and a percentage buffer
                    buffer_pct = 0.02  # 2% buffer
                    entry_low = current_price * (1 - buffer_pct)
                    entry_high = current_price * (1 + buffer_pct)
                    position_size = min(ceil(confidence * 10), 5000 // max(1, float(entry_high)))
                else:
                    entry_low, entry_high = 0.0, 0.0
                    position_size = 0
            else:
                entry_low, entry_high = 0.0, 0.0
                position_size = 0
                
            entry_section = (
                f"<b>ud83cudfaf ENTRY POINTS:</b>\n"
                f"  - Range: ${float(entry_low):.2f} - ${float(entry_high):.2f}\n"
                f"  - Suggested Size: {position_size} contracts\n\n"
            )
            
            options_recommendations = "<b>ud83dudcc8 OPTION RECOMMENDATIONS:</b>\n"

            # Special handling for NEUTRAL signals in recommendations section
            if direction == "NEUTRAL":
                logger.info(f"Processing options recommendations for NEUTRAL signal on {signal.ticker}")
                
                # Check if we have options data
                if options_data:
                    # Find call and put options
                    call_options = [opt for opt in options_data if opt.get('type', '').lower() == 'call']
                    put_options = [opt for opt in options_data if opt.get('type', '').lower() == 'put']
                    
                    if call_options and put_options:
                        logger.info(f"Found balanced call/put options for NEUTRAL recommendations on {signal.ticker}")
                        options_recommendations += "  <b>BULLISH SCENARIO:</b>\n"
                        for i, option in enumerate(call_options[:2]):
                            options_recommendations += (
                                f"  • CALL ${float(option.get('strike', 0)):.2f} exp {option.get('expiration', 'N/A')}\n"
                                f"    Vol: {float(option.get('volume', 0)):,} | OI: {float(option.get('open_interest', 0)):,} | IV: {float(option.get('implied_volatility', 0)):.1f}%\n"
                            )
                        
                        options_recommendations += "\n  <b>BEARISH SCENARIO:</b>\n"
                        for i, option in enumerate(put_options[:2]):
                            options_recommendations += (
                                f"  • PUT ${float(option.get('strike', 0)):.2f} exp {option.get('expiration', 'N/A')}\n"
                                f"    Vol: {float(option.get('volume', 0)):,} | OI: {float(option.get('open_interest', 0)):,} | IV: {float(option.get('implied_volatility', 0)):.1f}%\n"
                            )
                    else:
                        # Fall back to original logic if we have options but not balanced call/put
                        logger.warning(f"NEUTRAL signal for {signal.ticker} has options data but not balanced call/put options")
                        for i, option in enumerate(options_data[:3]):
                            options_recommendations += (
                                f"  • {option.get('type', '').upper()} ${float(option.get('strike', 0)):.2f} exp {option.get('expiration', 'N/A')}\n"
                                f"    Vol: {float(option.get('volume', 0)):,} | OI: {float(option.get('open_interest', 0)):,} | IV: {float(option.get('implied_volatility', 0)):.1f}%\n"
                            )
                            if i < len(options_data[:3]) - 1:
                                options_recommendations += "    ---------------\n"
                else:
                    logger.warning(f"No options data available for NEUTRAL signal on {signal.ticker}")
                    options_recommendations += "  <b>TECHNICAL RECOMMENDATIONS:</b>\n"
                    options_recommendations += "  • Consider straddle/strangle strategies\n    Position for volatility rather than direction\n"
                    options_recommendations += "  • Monitor for breakout signals\n    Watch key technical levels for directional clues\n"
                    options_recommendations += "  • Use technical indicators\n    RSI, MACD, and Bollinger Bands for confirmation\n"
            else:
                # Original logic for directional signals
                for i, option in enumerate(options_data[:3]):
                    options_recommendations += (
                        f"  • {option.get('type', '').upper()} ${float(option.get('strike', 0)):.2f} exp {option.get('expiration', 'N/A')}\n"
                        f"    Vol: {float(option.get('volume', 0)):,} | OI: {float(option.get('open_interest', 0)):,} | IV: {float(option.get('implied_volatility', 0)):.1f}%\n"
                    )
                    if i < len(options_data[:3]) - 1:
                        options_recommendations += "    ---------------\n"
                if not options_data:
                    options_recommendations += "  No data available\n\n"
                    logger.error(f"Options data empty for {signal.ticker}")

            # Calculate risk/reward
            risk_reward_ratio = 0.0 if entry_low == entry_high else (entry_high - entry_low) / max(0.01, (entry_low - min(entry_low, entry_high - (entry_high - entry_low) * 0.5)))
            stop_loss = entry_low - (entry_high - entry_low) * 0.5 if entry_low > 0 else 0.0
            risk_section = (
                f"<b>⚖️ RISK/REWARD:</b>\n"
                f"  - Risk/Reward Ratio: {float(risk_reward_ratio):.1f}:1\n"
                f"  - Stop Loss: ${float(stop_loss):.2f}\n\n"
            )
            
            # Historical section
            historical_section = f"<b>ud83dudcc5 HISTORICAL NOTE:</b>\n  - Historical performance should be analyzed using traditional metrics and technical indicators\n\n"
            
            # Update section
            update_interval = 30
            update_section = f"<b>ud83dudd04 UPDATE:</b> Check back in {update_interval} mins for signal refresh"
            
            # Combine all sections
            message = header + key_metrics + technical_section + entry_section + options_recommendations + risk_section + historical_section + update_section
            message += "\n───────────────────"
            
            logger.debug(f"Final notification message for {signal.ticker}: {message[:100]}...")
            
            # Use HTML parse mode instead of Markdown to avoid formatting issues
            await self.send_message(message, parse_mode="HTML")
            logger.info(f"Sent signal notification for {signal.ticker}")
        except Exception as e:
            logger.error(f"Error sending signal notification for {signal.ticker}: {str(e)}")
            # Try a simplified message as fallback
            try:
                simplified_message = f"🚨 <b>TRADING SIGNAL: {signal.ticker}</b> 🚨\n\nDirection: {direction}\nType: {signal_type}\nConfidence: {confidence:.2f}\n\nPlease check logs for details - notification formatting error occurred."
                await self.send_message(simplified_message, parse_mode="HTML")
                logger.info(f"Sent simplified fallback notification for {signal.ticker}")
            except Exception as fallback_error:
                logger.error(f"Even simplified notification failed for {signal.ticker}: {str(fallback_error)}")

    @log_notification("Signal Alert")
    async def send_signal_alert(self, signal: TradingSignal, grok_analysis: Dict = None, chart_path: str = None) -> None:
        """Send a signal alert with analysis via Telegram."""
        try:
            # Format the message with proper HTML escaping
            message = self._format_signal_message(signal, grok_analysis)
            
            # Send text message (charts disabled)
            await self.send_message(message, parse_mode="HTML")
            logger.info(f"Sent signal alert for {signal.ticker}")
                
        except Exception as e:
            logger.error(f"Error sending signal alert for {signal.ticker}: {str(e)}")
            # Try sending a simplified fallback message
            try:
                fallback_message = (
                    f"🚨 <b>Trading Alert</b>\n"
                    f"Ticker: {self._escape_html(signal.ticker)}\n"
                    f"Type: {self._escape_html(signal.signal_type)}\n"
                    f"Direction: {self._escape_html(signal.suggested_direction.upper())}"
                )
                await self.send_message(fallback_message, parse_mode="HTML")
                logger.info(f"Sent fallback signal alert for {signal.ticker}")
            except Exception as fallback_error:
                logger.error(f"Even fallback message failed for {signal.ticker}: {str(fallback_error)}")
    
    def _format_signal_message(self, signal: TradingSignal, grok_analysis: Dict = None) -> str:
        """Format the signal message with Grok analysis using HTML formatting."""
        try:
            # Determine direction emoji
            direction = signal.suggested_direction.upper()
            direction_emoji = "🟢" if direction in ["BUY", "BULLISH", "LONG", "CALL"] else "🔴" if direction in ["SELL", "BEARISH", "SHORT", "PUT"] else "⚪"
            
            # Header
            message = f"🚨 <b>New Trading Signal Detected!</b>\n\n"
            message += f"<b>Ticker: {self._escape_html(signal.ticker)}</b> {direction_emoji}\n"
            message += f"Type: {self._escape_html(signal.signal_type)}\n"
            message += f"Direction: {self._escape_html(direction)}\n"
            
            # Convert confidence to percentage
            try:
                confidence = float(signal.confidence)
                message += f"Confidence: {confidence:.2%}\n"
            except (ValueError, TypeError):
                message += f"Confidence: {self._escape_html(str(signal.confidence))}\n"
            
            # Add risk score if available
            try:
                risk_score = float(signal.risk_score)
                message += f"Risk Score: {risk_score:.2%}\n\n"
            except (ValueError, TypeError):
                if hasattr(signal, 'risk_score'):
                    message += f"Risk Score: {self._escape_html(str(signal.risk_score))}\n\n"
            
            # Technical Analysis section
            if hasattr(signal, 'technical_indicators') and signal.technical_indicators:
                message += "📊 <b>Technical Analysis:</b>\n"
                tech_data = signal.technical_indicators
                
                # RSI
                if 'rsi' in tech_data:
                    rsi = tech_data['rsi']
                    rsi_emoji = "🔥" if rsi > 70 else "❄️" if rsi < 30 else "➖"
                    message += f"RSI: {rsi:.2f} {rsi_emoji}\n"
                
                # MACD
                if 'macd' in tech_data and isinstance(tech_data['macd'], dict):
                    macd = tech_data['macd']
                    message += f"MACD Histogram: {macd.get('histogram', 0):.3f}\n"
                
                # VWAP
                if 'vwap' in tech_data:
                    message += f"VWAP: ${tech_data['vwap']:.2f}\n"
                
                # Volume Analysis
                if 'volume' in tech_data and 'volume_ma' in tech_data:
                    vol_ratio = tech_data['volume'] / tech_data['volume_ma'] if tech_data['volume_ma'] else 0
                    vol_emoji = "📈" if vol_ratio > 1.5 else "📉" if vol_ratio < 0.5 else "➖"
                    message += f"Volume vs 20MA: {vol_ratio:.1f}x {vol_emoji}\n\n"
            
            # Market Sentiment section
            if hasattr(signal, 'market_sentiment') and signal.market_sentiment:
                message += "🎯 <b>Market Sentiment:</b>\n"
                sentiment = signal.market_sentiment
                
                if 'sector_performance' in sentiment:
                    perf = sentiment['sector_performance']
                    perf_emoji = "📈" if perf > 0 else "📉" if perf < 0 else "➖"
                    message += f"Sector Performance: {perf:.2%} {perf_emoji}\n"
                
                if 'market_trend' in sentiment:
                    message += f"Market Trend: {self._escape_html(sentiment['market_trend'])}\n"
                
                if 'volatility_index' in sentiment:
                    message += f"VIX: {sentiment['volatility_index']:.2f}\n"
                
                if 'news_sentiment' in sentiment:
                    sent = sentiment['news_sentiment']
                    sent_emoji = "🟢" if sent > 0.2 else "🔴" if sent < -0.2 else "⚪"
                    message += f"News Sentiment: {sent:.2f} {sent_emoji}\n\n"
            
            # Greeks section
            if hasattr(signal, 'greeks') and signal.greeks:
                message += "🔢 <b>Options Greeks:</b>\n"
                greeks = signal.greeks
                
                if 'delta' in greeks:
                    message += f"Delta: {greeks['delta']:.3f}\n"
                if 'gamma' in greeks:
                    message += f"Gamma: {greeks['gamma']:.3f}\n"
                if 'theta' in greeks:
                    message += f"Theta: {greeks['theta']:.3f}\n"
                if 'vega' in greeks:
                    message += f"Vega: {greeks['vega']:.3f}\n"
                if 'implied_volatility' in greeks:
                    message += f"IV: {greeks['implied_volatility']:.1%}\n\n"
            
            # Short Interest section
            if hasattr(signal, 'short_interest') and signal.short_interest:
                message += "🎯 <b>Short Interest:</b>\n"
                si = signal.short_interest
                
                if 'short_percentage' in si:
                    si_emoji = "⚠️" if si['short_percentage'] > 20 else "➖"
                    message += f"Short Interest: {si['short_percentage']:.1f}% {si_emoji}\n"
                
                if 'days_to_cover' in si:
                    message += f"Days to Cover: {si['days_to_cover']:.1f}\n"
                
                if 'short_interest_change' in si:
                    change = si['short_interest_change']
                    change_emoji = "📈" if change > 0 else "📉" if change < 0 else "➖"
                    message += f"SI Change: {change:.1f}% {change_emoji}\n\n"
            
            # Institutional Data section
            if hasattr(signal, 'institutional_data') and signal.institutional_data:
                message += "🏢 <b>Institutional Activity:</b>\n"
                inst = signal.institutional_data
                
                if 'institutional_ownership' in inst:
                    message += f"Institutional Ownership: {inst['institutional_ownership']:.1f}%\n"
                
                if 'ownership_change' in inst:
                    change = inst['ownership_change']
                    change_emoji = "📈" if change > 0 else "📉" if change < 0 else "➖"
                    message += f"Ownership Change: {change:.1f}% {change_emoji}\n"
                
                if 'recent_transactions' in inst and inst['recent_transactions']:
                    message += "<b>Recent Transactions:</b>\n"
                    for tx in inst['recent_transactions'][:2]:  # Show only top 2
                        message += f"• {self._escape_html(tx)}\n"
                message += "\n"
            
            # Key Metrics section if options data is available
            if hasattr(signal, 'options_data') and signal.options_data:
                first_contract = signal.options_data[0]
                message += "🎯 <b>Key Metrics:</b>\n"
                message += f"Option Type: {self._escape_html(first_contract.get('type', 'UNKNOWN').upper())}\n"
                message += f"Strike Price: ${self._escape_html(str(first_contract.get('strike', '0.0')))}\n"
                message += f"Expiry Date: {self._escape_html(first_contract.get('expiration', 'Unknown'))}\n"
                message += f"Volume: {self._format_number(first_contract.get('volume', 0))}\n"
                message += f"Open Interest: {self._format_number(first_contract.get('open_interest', 0))}\n"
                message += f"Vol/OI Ratio: {first_contract.get('vol_oi_ratio', 0):.2f}\n"
                message += f"IV: {first_contract.get('implied_volatility', 0):.1%}\n\n"
            
            # Recommended Options section
            if hasattr(signal, 'options_data') and signal.options_data:
                message += "🎯 <b>Recommended Options:</b>\n"
                for contract in signal.options_data[:3]:  # Show top 3 contracts
                    message += f"• {contract.get('type', '').upper()} ${contract.get('strike', '0.0')} "
                    message += f"exp {contract.get('expiration', 'Unknown')}\n"
                    message += f"  Vol: {self._format_number(contract.get('volume', 0))} | "
                    message += f"OI: {self._format_number(contract.get('open_interest', 0))} | "
                    message += f"IV: {contract.get('implied_volatility', 0):.1%}\n"
                    message += "  ---------------\n"
                message += "\n"
            
            # Grok AI Analysis section
            if grok_analysis:
                message += "🤖 <b>Grok AI Analysis:</b>\n"
                message += f"Sentiment: {self._escape_html(grok_analysis.get('direction', 'UNKNOWN'))}\n\n"
                
                if 'key_factors' in grok_analysis:
                    message += "<b>Key Factors:</b>\n"
                    for factor in grok_analysis.get('key_factors', []):
                        message += f"• {self._escape_html(factor)}\n"
                    message += "\n"
                
                if 'risks' in grok_analysis:
                    message += "<b>Risks:</b>\n"
                    for risk in grok_analysis.get('risks', []):
                        message += f"• {self._escape_html(risk)}\n"
                    message += "\n"
                
                if 'entry_points' in grok_analysis:
                    message += "<b>Entry Points:</b>\n"
                    message += f"• {self._escape_html(grok_analysis.get('entry_points', 'Not available'))}\n\n"
                
                if 'option_recommendations' in grok_analysis:
                    message += "<b>Grok Option Recommendations:</b>\n"
                    message += f"• {self._escape_html(grok_analysis.get('option_recommendations', 'No specific recommendations'))}\n\n"
                
                if 'historical_context' in grok_analysis:
                    message += "<b>Historical Context:</b>\n"
                    message += f"{self._escape_html(grok_analysis.get('historical_context', 'No historical context available'))}\n\n"
            
            # Timestamp
            message += f"\nGenerated at: {datetime.now().isoformat()}"
            
            return message
            
        except Exception as e:
            logger.error(f"Error formatting signal message: {str(e)}")
            return self._format_fallback_message(signal)
            
    def _format_number(self, number: Union[int, float, str]) -> str:
        """Format numbers with commas for readability."""
        try:
            return f"{int(number):,}"
        except (ValueError, TypeError):
            return str(number)
            
    def _format_fallback_message(self, signal: TradingSignal) -> str:
        """Create a simple fallback message when full formatting fails."""
        return (
            f"🚨 <b>Trading Alert</b>\n\n"
            f"Ticker: {self._escape_html(signal.ticker)}\n"
            f"Type: {self._escape_html(signal.signal_type)}\n"
            f"Direction: {self._escape_html(signal.suggested_direction.upper())}"
        )

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters and encode emojis in text."""
        if not isinstance(text, str):
            text = str(text)
            
        # HTML special characters escaping
        html_escape_table = {
            "&": "&amp;",
            '"': "&quot;",
            "'": "&#39;",
            ">": "&gt;",
            "<": "&lt;",
            "\n": "<br/>",
            "—": "&mdash;",
            "–": "&ndash;",
            "•": "&bull;"
        }
        
        # First escape HTML special chars
        for char, entity in html_escape_table.items():
            text = text.replace(char, entity)
            
        # Convert unicode emoji codes to actual emojis
        emoji_map = {
            "ud83dudea8": "🚨",  # alert
            "ud83dudfe2": "🟢",  # green circle
            "ud83dudd34": "🔴",  # red circle
            "u26aa": "⚪",      # white circle
            "ud83dudccc": "📌",  # pushpin
            "ud83dudcca": "📊",  # bar chart
            "ud83dudca1": "💡",  # light bulb
            "ud83cudfaf": "🎯",  # dart
            "ud83dudcc8": "📈",  # chart increasing
            "ud83dudcc5": "📅",  # calendar
            "ud83dudd04": "🔄"   # arrows counterclockwise
        }
        
        for code, emoji in emoji_map.items():
            text = text.replace(code, emoji)
            
        return text

    @log_exceptions
    @log_api_call("Telegram Send Error Alert")
    @log_notification("Error Alert")
    async def send_error_alert(self, title: str, message: str, context: Dict = None) -> bool:
        """Send an error alert to the Telegram chat."""
        try:
            formatted_message = f"❌ *ERROR: {title}*\n\n"
            formatted_message += f"{message}\n\n"
            if context:
                formatted_message += "*Context:*\n"
                for key, value in context.items():
                    if isinstance(value, str) and len(value) > 100:
                        value = value[:97] + "..."
                    formatted_message += f"- {key}: `{value}`\n"
            formatted_message += f"\n⏰ Time: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
            return await self.send_message(formatted_message)
        except Exception as e:
            logger.error(f"Error sending Telegram error alert: {str(e)}")
            return False

    @log_exceptions
    async def send_status_update(self, status: Dict) -> None:
        """Send a status update."""
        try:
            message = (
                "📊 <b>BOT STATUS UPDATE</b> 📊\n\n"
                f"<b>Active Since:</b> {status.get('active_since', 'Unknown')}\n"
                f"<b>Signals Processed:</b> {status.get('signals_processed', 0)}\n"
                f"<b>Trades Executed:</b> {status.get('trades_executed', 0)}\n"
                f"<b>Success Rate:</b> {status.get('success_rate', 0):.1%}\n\n"
            )
            if 'errors' in status:
                message += "<b>Recent Errors:</b>\n"
                for error in status['errors'][-5:]:
                    message += f"• {error}\n"
            await self.send_message(message, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error sending status update: {str(e)}")

    @log_exceptions
    async def close(self):
        """Close the Telegram notifier and clean up resources."""
        try:
            if self.worker_task and not self.worker_task.done():
                self.worker_task.cancel()
                try:
                    await self.worker_task
                except asyncio.CancelledError:
                    pass
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            logger.info("Telegram notifier closed successfully")
            return True
        except Exception as e:
            logger.error(f"Error closing Telegram notifier: {str(e)}")
            return False