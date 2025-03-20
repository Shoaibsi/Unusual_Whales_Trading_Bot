import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from loguru import logger
from src.trading_models import TradingSignal, SignalType, SignalStrength
import traceback
import json
import websockets
from src.logger_config import logger
from src.technical_indicators import TechnicalIndicatorCalculator

@dataclass
class SignalConfig:
    # General settings
    scan_interval: int = 60
    confidence_threshold: float = 0.6
    risk_threshold: float = 0.8
    
    # Dark pool settings
    min_dark_pool_size: int = 10000
    min_volume: int = 10000
    volume_threshold: int = 20000
    
    # Options flow settings
    min_premium: float = 3000  # Lowered from 5000 to catch more alerts
    min_options_volume: int = 30  # Lowered from 50 to catch more alerts
    min_open_interest: int = 75  # Lowered from 100 to catch more alerts
    
    # Gamma squeeze settings
    min_gamma_exposure: float = 100000
    gamma_threshold: float = 0.3
    volume_spike_threshold: float = 5000
    imbalance_threshold: float = 0.2
    
    # Analysis window
    confirmation_window: int = 5

class BaseSignalDetector:
    """Base class for signal detectors."""
    def __init__(self, *, api_client=None, config: SignalConfig = None):
        self.api_client = api_client
        self.config = config or SignalConfig()
        self.logger = logger
        
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        """
        Detect trading signals from market data.
        
        Args:
            ticker: The ticker symbol to analyze
            alert: Optional flow alert data
            
        Returns:
            TradingSignal if a signal is detected, None otherwise
        """
        raise NotImplementedError("Subclasses must implement detect()")
        
    async def _get_recommended_contracts(self, ticker: str, direction: str) -> List[Dict]:
        """Get recommended options contracts based on detected signal."""
        try:
            contracts = []
            self.logger.debug(f"Fetching options chain for {ticker}")
            options_chain = await self.api_client.get_options_chain(ticker)
            
            # Validate response format
            if not self._validate_response(options_chain, expected_type=dict):
                self.logger.warning(f"Invalid options chain response for {ticker}")
                return []
                
            # Extract options data safely
            options_data = self._safe_get(options_chain, 'data', default=[])
            if not options_data or not isinstance(options_data, list):
                self.logger.warning(f"No valid options data for {ticker}")
                return []
            
            self.logger.debug(f"Retrieved {len(options_data)} options contracts for {ticker}")
            
            # For NEUTRAL signals, include both calls and puts
            if direction.upper() == 'NEUTRAL':
                self.logger.debug(f"Processing NEUTRAL signal for {ticker}, including both calls and puts")
                
                # Log a sample of the options data to debug format issues
                if options_data and len(options_data) > 0:
                    sample_opt = options_data[0]
                    self.logger.debug(f"Sample option data format: {sample_opt}")
                    self.logger.debug(f"Option type field: {self._safe_get(sample_opt, 'type', 'NOT_FOUND')}")
                    
                # Filter and sort calls - more lenient filtering
                call_options = []
                for opt in options_data:
                    if not isinstance(opt, dict):
                        continue
                        
                    opt_type = self._safe_get(opt, 'type', '')
                    if not opt_type:
                        # Try alternative field names
                        opt_type = self._safe_get(opt, 'option_type', '')
                        
                    if opt_type and ('call' in opt_type.lower() or 'c' == opt_type.lower()):
                        call_options.append(opt)
                
                self.logger.debug(f"Found {len(call_options)} CALL options for {ticker}")
                
                # Sort calls by volume and open interest - RELAXED CRITERIA
                # First filter by basic liquidity (very lenient)
                liquid_calls = [opt for opt in call_options if self._safe_get(opt, 'volume', 0) > 5]
                self.logger.debug(f"Filtered to {len(liquid_calls)} liquid CALL options for {ticker}")
                
                # Sort by combined volume and open interest score
                sorted_calls = sorted(
                    liquid_calls if liquid_calls else call_options, # Fallback to all calls if filtering removed everything
                    key=lambda x: (self._safe_get(x, 'volume', 0) * 0.7 + self._safe_get(x, 'open_interest', 0) * 0.3), 
                    reverse=True
                )
                
                # Filter and sort puts - more lenient filtering
                put_options = []
                for opt in options_data:
                    if not isinstance(opt, dict):
                        continue
                        
                    opt_type = self._safe_get(opt, 'type', '')
                    if not opt_type:
                        # Try alternative field names
                        opt_type = self._safe_get(opt, 'option_type', '')
                        
                    if opt_type and ('put' in opt_type.lower() or 'p' == opt_type.lower()):
                        put_options.append(opt)
                
                self.logger.debug(f"Found {len(put_options)} PUT options for {ticker}")
                
                # Filter puts by basic liquidity (very lenient)
                liquid_puts = [opt for opt in put_options if self._safe_get(opt, 'volume', 0) > 5]
                self.logger.debug(f"Filtered to {len(liquid_puts)} liquid PUT options for {ticker}")
                
                # Sort puts by volume and open interest
                sorted_puts = sorted(
                    liquid_puts if liquid_puts else put_options, # Fallback to all puts if filtering removed everything
                    key=lambda x: (self._safe_get(x, 'volume', 0) * 0.7 + self._safe_get(x, 'open_interest', 0) * 0.3), 
                    reverse=True
                )
                
                # If we still don't have options, try to get any with non-zero volume
                if not sorted_calls and not sorted_puts and options_data:
                    self.logger.debug(f"No valid options found for {ticker} with standard filtering, trying fallback")
                    # Fallback: get any options with volume > 0
                    all_options = [
                        opt for opt in options_data 
                        if isinstance(opt, dict) and self._safe_get(opt, 'volume', 0) > 0
                    ]
                    
                    # Take up to 3 options with highest volume
                    result = sorted(
                        all_options,
                        key=lambda x: self._safe_get(x, 'volume', 0),
                        reverse=True
                    )[:3]
                    
                    self.logger.debug(f"Fallback found {len(result)} options for {ticker}")
                    return result
                
                # Take top options from each type (more for NEUTRAL signals)
                top_calls = sorted_calls[:3] if len(sorted_calls) >= 3 else sorted_calls
                top_puts = sorted_puts[:3] if len(sorted_puts) >= 3 else sorted_puts
                
                # Combine and sort again for final ranking
                combined = top_calls + top_puts
                result = sorted(
                    combined,
                    key=lambda x: (self._safe_get(x, 'volume', 0) * 0.7 + self._safe_get(x, 'open_interest', 0) * 0.3),
                    reverse=True
                )[:4]  # Return more options for NEUTRAL signals
                
                self.logger.debug(f"Selected top {len(result)} contracts (mixed calls/puts) for NEUTRAL signal on {ticker}")
            else:
                # Original logic for directional signals but with RELAXED CRITERIA
                contract_type = 'call' if direction.upper() in ['LONG', 'BUY', 'BULLISH'] else 'put'
                self.logger.debug(f"Processing {direction} signal for {ticker}, looking for {contract_type.upper()} options")
                
                # Step 1: Filter by contract type
                type_filtered = [
                    opt for opt in options_data 
                    if isinstance(opt, dict) and self._safe_get(opt, 'type', '').lower() == contract_type.lower()
                ]
                self.logger.debug(f"Filtering {len(options_data)} contracts before applying criteria")
                self.logger.debug(f"Step 1: Filtered to {len(type_filtered)} {contract_type.upper()} contracts for {ticker}")
                
                # Step 2: Filter by basic liquidity (RELAXED from 30 to 10)
                volume_filtered = [
                    opt for opt in type_filtered
                    if self._safe_get(opt, 'volume', 0) >= 10
                ]
                self.logger.debug(f"Step 2: Filtered contracts after volume check: {len(volume_filtered)}")
                
                # Step 3: Filter by open interest (RELAXED from 75 to 30)
                oi_filtered = [
                    opt for opt in volume_filtered
                    if self._safe_get(opt, 'open_interest', 0) >= 30
                ]
                self.logger.debug(f"Step 3: Filtered contracts after open interest check: {len(oi_filtered)}")
                
                # If we've filtered out all options, fall back to just volume filtering
                if not oi_filtered and type_filtered:
                    self.logger.warning(f"Strict filtering removed all {contract_type.upper()} contracts for {ticker}, using fallback")
                    # Fallback to just basic volume filtering
                    filtered_options = [opt for opt in type_filtered if self._safe_get(opt, 'volume', 0) > 5]
                    self.logger.debug(f"Fallback filtering found {len(filtered_options)} contracts with volume > 5")
                else:
                    filtered_options = oi_filtered
                
                # If we still have no options, use any with the right type
                if not filtered_options and type_filtered:
                    self.logger.warning(f"All filtering failed for {ticker}, returning any {contract_type.upper()} contracts")
                    filtered_options = type_filtered[:5]  # Just take the first 5 of the right type
                
                # Sort by volume and open interest
                sorted_options = sorted(
                    filtered_options, 
                    key=lambda x: (self._safe_get(x, 'volume', 0) * 0.7 + self._safe_get(x, 'open_interest', 0) * 0.3), 
                    reverse=True
                )
                
                # Return top 3 contracts
                result = sorted_options[:3]
                self.logger.debug(f"Final selected {contract_type.upper()} contracts: {len(result)}")
            
            # Log sample contract data
            if result and len(result) > 0:
                sample = result[0]
                self.logger.debug(f"Sample contract for {ticker}: Strike={sample.get('strike')}, Expiry={sample.get('expiry')}, Premium=${sample.get('premium', 0)}")
            else:
                self.logger.warning(f"No contracts selected for {ticker} with direction {direction} after all filtering")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting recommended contracts for {ticker}: {str(e)}")
            return []
            
    def _validate_response(self, response, expected_type=None):
        """Validate API response."""
        if response is None:
            return False
            
        # If no specific type is expected, consider any non-None response valid
        if expected_type is None:
            return True
            
        # For dict responses with data field, check the data field
        if isinstance(response, dict) and 'data' in response:
            if expected_type is list and isinstance(response['data'], list):
                return True
            elif expected_type is dict and isinstance(response['data'], dict):
                return True
                
        # Direct type check
        if isinstance(response, expected_type):
            return True
            
        # Log warning for unexpected types
        self.logger.warning(f"Expected {expected_type.__name__} but got {type(response).__name__}")
        return False
        
    def _safe_get(self, data: Dict, key: str, default: Any = None) -> Any:
        """Safely get a value from a dictionary."""
        if not isinstance(data, dict):
            return default
        return data.get(key, default)

    def _extract_numeric(self, data: Dict, keys: List[str], default: float = 0) -> float:
        """Extract a numeric value from a dictionary with flexible key handling."""
        for key in keys:
            if key in data:
                try:
                    return float(data[key])
                except (ValueError, TypeError):
                    pass
        return default

class GammaSqueezeDetector(BaseSignalDetector):
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        try:
            # Get options chain for gamma analysis
            options_chain = await self.api_client.get_options_chain(ticker)
            
            # Validate response
            if not self._validate_response(options_chain):
                return None
                
            # Check if data is in expected format
            if not isinstance(options_chain, dict) or not options_chain.get('data'):
                self.logger.warning(f"Invalid options chain format for {ticker}")
                return None
                
            options_data = options_chain.get('data', [])
            if not isinstance(options_data, list):
                self.logger.warning(f"Expected list of options but got {type(options_data).__name__}")
                return None
                
            # Calculate total gamma exposure
            total_gamma = 0
            total_volume = 0
            total_oi = 0
            
            for option in options_data:
                if not isinstance(option, dict):
                    continue
                    
                try:
                    gamma = float(self._safe_get(option, 'gamma', 0))
                    volume = int(self._safe_get(option, 'volume', 0))
                    oi = int(self._safe_get(option, 'open_interest', 0))
                    
                    total_gamma += gamma
                    total_volume += volume
                    total_oi += oi
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Error parsing option data: {str(e)}")
                    continue
            
            if total_gamma > self.config.min_gamma_exposure and total_volume > 500:
                # Get market data for directional bias
                market_data = await self.api_client.get_market_data(ticker)
                
                # Validate market data
                if not self._validate_response(market_data):
                    self.logger.warning(f"Invalid market data for {ticker}")
                    return None
                    
                direction = "LONG" if self._safe_get(market_data, 'percent_change', 0) > 0 else "SHORT"
                
                confidence = min(1.0, (total_gamma / self.config.min_gamma_exposure) * 0.4 +
                               (total_volume / 10000) * 0.3 + (total_oi / 50000) * 0.3)
                               
                risk_score = min(1.0, abs(total_gamma) / (self.config.min_gamma_exposure * 2))
                
                options_data = await self._get_recommended_contracts(ticker, direction)
                
                return TradingSignal(
                    ticker=ticker,
                    signal_type="GAMMA_SQUEEZE",
                    confidence=confidence,
                    suggested_direction=direction,
                    risk_score=risk_score,
                    primary_data={'total_gamma': total_gamma, 'total_volume': total_volume, 'total_oi': total_oi},
                    confirmation_data=market_data,
                    options_data=options_data,
                )
        except Exception as e:
            self.logger.error(f"Gamma squeeze error for {ticker}: {str(e)}")
        return None

class OptionsFlowAnomalyDetector(BaseSignalDetector):
    def __init__(self, api_client, config: SignalConfig = None):
        super().__init__(api_client, config)
        self.grok_analyzer = None
        
    async def initialize_grok(self, api_key: Optional[str] = None):
        """Initialize the Grok analyzer."""
        from src.grok_analyzer import GrokAnalyzer
        self.grok_analyzer = GrokAnalyzer(api_key)
        success = await self.grok_analyzer.initialize()
        if not success:
            self.logger.error("Failed to initialize GrokAnalyzer")
            self.grok_analyzer = None
        
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        try:
            # Use the alert passed from the main loop instead of making another API call
            if not alert:
                self.logger.debug(f"No alert data provided for {ticker}")
                return None
                
            # Validate alert data
            if not isinstance(alert, dict):
                self.logger.warning(f"Invalid alert format for {ticker}: {type(alert).__name__}")
                return None
                
            # Extract key fields with validation
            try:
                total_premium = float(self._safe_get(alert, 'premium_usd', 0))
                option_type = self._safe_get(alert, 'contract_type', '').upper()
                volume = int(self._safe_get(alert, 'volume', 0))
                open_interest = int(self._safe_get(alert, 'open_interest', 0))
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error parsing alert data for {ticker}: {str(e)}")
                return None
                
            # Check for minimum thresholds
            if total_premium < self.config.min_premium:
                self.logger.debug(f"Premium {total_premium} below threshold {self.config.min_premium} for {ticker}")
                return None
                
            if volume < self.config.min_options_volume:
                self.logger.debug(f"Volume {volume} below threshold {self.config.min_options_volume} for {ticker}")
                return None
                
            # Check volume/OI ratio
            vol_oi_ratio = volume / max(1, open_interest)
            if open_interest > 0 and vol_oi_ratio < 0.1:
                self.logger.debug(f"Volume/OI ratio {vol_oi_ratio:.2f} too low for {ticker}")
                return None
                
            # Determine direction from option type
            if option_type == 'CALL':
                direction = "LONG"
            elif option_type == 'PUT':
                direction = "SHORT"
            else:
                self.logger.warning(f"Unknown option type: {option_type}")
                return None
                
            # Calculate days to expiration
            expiry_str = self._safe_get(alert, 'expiry', None)
            dte = 0
            if expiry_str:
                try:
                    expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                    dte = (expiry_date - datetime.now()).days
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid expiry date format: {expiry_str}")
                    dte = 30  # Default value
            
            # Calculate confidence based on volume and premium
            volume_score = min(1.0, volume / self.config.volume_threshold)
            premium_score = min(1.0, total_premium / (self.config.min_premium * 2))
            confidence = (volume_score + premium_score) / 2
            
            # Calculate risk score based on DTE and premium
            dte_risk = min(1.0, dte / 90)  # Higher DTE = higher risk
            premium_risk = min(1.0, total_premium / (self.config.min_premium * 5))
            risk_score = (dte_risk + premium_risk) / 2
            
            # Get recommended options contracts
            options_data = await self._get_recommended_contracts(ticker, direction)
            
            # Create alert_data list with the current alert
            alert_data = [alert] if alert else []
            
            # Log the options and alert data being attached to the signal
            self.logger.debug(f"Attaching {len(options_data)} options contracts to signal for {ticker}")
            self.logger.debug(f"Attaching {len(alert_data)} alert items to signal for {ticker}")
            
            # Create the signal first
            signal = TradingSignal(
                ticker=ticker,
                signal_type=SignalType.OPTIONS_FLOW,
                strength=SignalStrength.MODERATE,  # Default to MODERATE
                confidence=confidence,
                suggested_direction=direction,
                risk_score=risk_score,
                primary_data={**alert, "raw_alert": alert},  
                options_data=options_data,
                alert_data=alert_data,
                timestamp=datetime.now()
            )
            
            # Format the signal first (this ensures all data is properly structured)
            formatted_signal = self._format_signal_template(signal)
            
            # Then send to Grok for analysis
            if self.grok_analyzer:
                try:
                    grok_analysis = await self.grok_analyzer.analyze_signals([formatted_signal])
                    if grok_analysis and len(grok_analysis) > 0:
                        signal.grok_analysis = grok_analysis[0]
                        # Update confidence if Grok provides a better estimate
                        if 'confidence' in grok_analysis[0]:
                            signal.confidence = max(confidence, float(grok_analysis[0]['confidence']))
                except Exception as e:
                    logger.error(f'Error in Grok analysis: {str(e)}')
                    signal.grok_analysis = None
            
            return signal
        except Exception as e:
            self.logger.error(f"Options flow error for {ticker}: {str(e)}")
        return None

    def _format_signal_template(self, signal: TradingSignal) -> TradingSignal:
        """Format the signal according to our template before Grok analysis."""
        try:
            # Ensure options data is properly formatted
            if signal.options_data:
                formatted_options = []
                for contract in signal.options_data:
                    formatted_contract = {
                        'type': str(contract.get('type', '')).upper(),
                        'strike': float(contract.get('strike', 0)),
                        'expiration': contract.get('expiration', ''),
                        'volume': int(contract.get('volume', 0)),
                        'open_interest': int(contract.get('open_interest', 0)),
                        'implied_volatility': float(contract.get('implied_volatility', 0))
                    }
                    if all(formatted_contract.values()):  # Only include if all fields are present
                        formatted_options.append(formatted_contract)
                signal.options_data = formatted_options

            # Ensure other fields are properly formatted
            signal.confidence = float(signal.confidence) if signal.confidence else 0.0
            signal.risk_score = float(signal.risk_score) if signal.risk_score else 0.0
            signal.suggested_direction = str(signal.suggested_direction).upper() if signal.suggested_direction else "UNKNOWN"
            
            return signal
        except Exception as e:
            logger.error(f"Error formatting signal template: {str(e)}")
            return signal

class DarkPoolSignalDetector(BaseSignalDetector):
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        try:
            # Check if darkpool data is already in the alert
            dark_pool_data = None
            if alert and 'darkpool_data' in alert:
                dark_pool_data = alert['darkpool_data']
            else:
                # Get dark pool data from API
                dark_pool_data = await self.api_client.get_darkpool_for_ticker(ticker)
            
            # Handle different response formats
            if isinstance(dark_pool_data, dict):
                if 'data' in dark_pool_data:
                    dark_pool_data = dark_pool_data['data']
                elif 'trades' in dark_pool_data:
                    dark_pool_data = dark_pool_data['trades']
            
            # Ensure we have a list of trades
            if not isinstance(dark_pool_data, list):
                self.logger.warning(f"Dark pool data for {ticker} is not a list: {type(dark_pool_data)}")
                return None
                
            if not dark_pool_data:
                self.logger.info(f"No dark pool trades found for {ticker}")
                return None

            # Filter for significant dark pool trades
            significant_trades = []
            for trade in dark_pool_data:
                if not isinstance(trade, dict):
                    continue
                    
                try:
                    # Extract trade data with flexible key handling
                    volume = self._extract_numeric(trade, ['volume', 'size', 'quantity'], 0)
                    price = self._extract_numeric(trade, ['price', 'trade_price'], 0)
                    premium = self._extract_numeric(trade, ['premium', 'notional', 'value'], 0)
                    
                    # Determine direction if available
                    direction = ''
                    for key in ['direction', 'side', 'trade_side']:
                        if key in trade:
                            direction = str(trade[key]).lower()
                            break
                    
                    if volume >= self.config.min_dark_pool_size and price > 0:
                        significant_trades.append({
                            **trade,
                            'volume': volume,
                            'price': price,
                            'premium': premium,
                            'direction': direction,
                            'normalized_size': volume / self.config.min_dark_pool_size
                        })
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Error processing trade: {e}")
                    continue

            if not significant_trades:
                return None

            # Sort by volume and get the largest trade
            largest_trade = sorted(significant_trades, key=lambda x: x['volume'], reverse=True)[0]
            
            # Calculate confidence based on volume
            confidence = min(1.0, largest_trade['normalized_size'])
            
            # Calculate risk score based on price volatility
            try:
                price_change = float(self._safe_get(largest_trade, 'price_change', 0))
                risk_score = min(1.0, abs(price_change) / 5)
            except (ValueError, TypeError):
                risk_score = 0.5
            
            # Determine signal type based on direction
            if largest_trade['direction'].lower() == 'bullish':
                signal_type = SignalType.BUY
            elif largest_trade['direction'].lower() == 'bearish':
                signal_type = SignalType.SELL
            else:
                signal_type = SignalType.NEUTRAL
                
            # Determine signal strength based on volume
            if confidence > 0.8:
                strength = SignalStrength.VERY_STRONG
            elif confidence > 0.6:
                strength = SignalStrength.STRONG
            elif confidence > 0.4:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK
                
            # Create analysis for analysis field
            analysis = f"Dark pool activity: {largest_trade['volume']:,.0f} shares at ${largest_trade['price']:.2f} ({largest_trade['direction']})"
                
            # Get market data for confirmation
            market_data = await self.api_client.get_market_data(ticker)
            
            # Validate market data
            if not self._validate_response(market_data):
                market_data = {}  # Use empty dict if invalid
                
            # Get recommended options contracts based on signal type
            direction = "NEUTRAL" if signal_type == SignalType.NEUTRAL else "LONG" if signal_type == SignalType.BUY else "SHORT"
            options_data = await self._get_recommended_contracts(ticker, direction)
            self.logger.info(f"Retrieved {len(options_data)} options contracts for {ticker} with direction {direction}")
                
            return TradingSignal(
                ticker=ticker,
                signal_type=signal_type,
                strength=strength,
                confidence=confidence,
                price=price,
                timestamp=datetime.now(),
                analysis=analysis,
                source=SignalType.DARK_POOL,
                metadata={
                    "risk_score": risk_score,
                    "trade_data": largest_trade,
                    "market_data": market_data,
                    "alert_data": alert
                },
                options_data=options_data
            )
        except Exception as e:
            self.logger.error(f"Dark pool error for {ticker}: {str(e)}")
        return None

class WSSignalDetector(BaseSignalDetector):
    def __init__(self, api_client=None, config=None, grok_analyzer=None):
        super().__init__(api_client=api_client, config=config)
        self.grok_analyzer = grok_analyzer
        self.price_cache = {}
        self.volume_cache = {}
        self.signal_cache = {}
        self.technical_indicators = TechnicalIndicatorCalculator()

    async def initialize(self):
        """Initialize the signal detector."""
        if not self.api_client:
            self.logger.error("No API client provided")
            return False
        
        # Initialize caches
        self.price_cache = {}
        self.volume_cache = {}
        self.signal_cache = {}
        
        # Setup real-time monitoring
        await self.technical_indicators.initialize()
        
        self.logger.info("Signal detector initialized successfully")
        return True

    async def detect_signals_from_flow(self, flow_alerts: List[Dict]) -> List[TradingSignal]:
        """
        Process flow alerts and detect trading signals.
        This is the main entry point called by the bot.
        """
        if not flow_alerts:
            self.logger.debug("No flow alerts to process")
                return []
            
        signals = []
        for alert in flow_alerts:
            try:
                detected_signals = await self.detect_signals(alert)
                if detected_signals:
                    if isinstance(detected_signals, list):
                        signals.extend(detected_signals)
                    else:
                        signals.append(detected_signals)
            except Exception as e:
                self.logger.error(f"Error processing alert: {str(e)}")
                        continue
                
        return signals

    async def detect_signals(self, alert: Dict) -> Union[TradingSignal, List[TradingSignal]]:
        """
        Process a single flow alert and detect trading signals.
        """
        try:
                if not isinstance(alert, dict):
                self.logger.warning(f"Invalid alert format: {type(alert)}")
                return []
                    
            ticker = alert.get('ticker')
                if not ticker:
                self.logger.warning("Alert missing ticker")
                return []
        
            self.logger.info(f"Processing alert for {ticker}")

            # Get technical indicators
            technical_data = await self.technical_indicators.get_indicators(ticker)
            
            # Get market sentiment
            market_sentiment = await self.api_client.get_market_sentiment(ticker)
            
            # Get Greeks data
            greeks_data = await self.api_client.get_greeks(ticker)
            
            # Get short interest data
            short_interest = await self.api_client.get_short_interest(ticker)
            
            # Get institutional data
            institutional_data = await self.api_client.get_institutional_holdings(ticker)
            
            # Get options data
            options_data = await self._get_recommended_contracts(ticker, alert.get('direction', 'UNKNOWN'))

            # Create signal with enhanced validation
                signal = TradingSignal(
                    ticker=ticker,
                signal_type=SignalType.OPTIONS_FLOW,
                confidence=self._calculate_confidence(alert, technical_data, market_sentiment),
                suggested_direction=self._determine_direction(alert, technical_data, market_sentiment),
                risk_score=self._calculate_risk_score(alert, technical_data, greeks_data),
                primary_data=alert,
                technical_indicators=technical_data,
                market_sentiment=market_sentiment,
                greeks=greeks_data,
                short_interest=short_interest,
                institutional_data=institutional_data,
                options_data=options_data,
                alert_data=[alert],
                timestamp=datetime.now(timezone.utc)
            )

            # Enhance with Grok analysis if available
            if self.grok_analyzer:
                try:
                    grok_analysis = await self.grok_analyzer.analyze_signal(signal)
                    if grok_analysis:
                        signal.grok_analysis = grok_analysis
                    except Exception as e:
                    self.logger.error(f"Error in Grok analysis: {str(e)}")
                
            return signal

            except Exception as e:
            self.logger.error(f"Error detecting signals: {str(e)}\n{traceback.format_exc()}")
            return []

    def _calculate_confidence(self, alert: Dict, technical_data: Dict, market_sentiment: Dict) -> float:
        """Calculate signal confidence based on multiple factors."""
        try:
            # Base confidence from alert data
            volume = float(alert.get('volume', 0))
            premium = float(alert.get('premium', 0))
            
            # Volume-based confidence (0.3 weight)
            volume_confidence = min(1.0, volume / self.config.volume_threshold)
            
            # Premium-based confidence (0.3 weight)
            premium_confidence = min(1.0, premium / (self.config.min_premium * 2))
            
            # Technical indicator confidence (0.2 weight)
            technical_confidence = 0.5  # Default neutral
            if technical_data:
                rsi = technical_data.get('rsi', 50)
                macd = technical_data.get('macd', {}).get('histogram', 0)
                
                # RSI extremes increase confidence
                if rsi > 70 or rsi < 30:
                    technical_confidence = 0.8
                
                # Strong MACD signals increase confidence
                if abs(macd) > 0.5:
                    technical_confidence = max(technical_confidence, 0.7)
            
            # Market sentiment confidence (0.2 weight)
            sentiment_confidence = 0.5  # Default neutral
            if market_sentiment:
                sentiment_score = market_sentiment.get('sentiment_score', 0.5)
                sentiment_confidence = abs(sentiment_score - 0.5) * 2  # Convert to 0-1 range
            
            # Weighted average
            final_confidence = (
                volume_confidence * 0.3 +
                premium_confidence * 0.3 +
                technical_confidence * 0.2 +
                sentiment_confidence * 0.2
            )
            
            return min(1.0, max(0.0, final_confidence))
            
        except Exception as e:
            self.logger.error(f"Error calculating confidence: {str(e)}")
            return 0.5  # Default to neutral confidence

    def _determine_direction(self, alert: Dict, technical_data: Dict, market_sentiment: Dict) -> str:
        """Determine signal direction based on multiple factors."""
        try:
            # Get base direction from alert
            base_direction = alert.get('direction', 'UNKNOWN')
            if base_direction != 'UNKNOWN':
                return base_direction
            
            # Use technical indicators
            if technical_data:
                rsi = technical_data.get('rsi', 50)
                macd = technical_data.get('macd', {}).get('histogram', 0)
                
                if rsi > 70 and macd < 0:
                    return 'BEARISH'
                elif rsi < 30 and macd > 0:
                    return 'BULLISH'
            
            # Use market sentiment
            if market_sentiment:
                sentiment_score = market_sentiment.get('sentiment_score', 0.5)
                if sentiment_score > 0.6:
                    return 'BULLISH'
                elif sentiment_score < 0.4:
                    return 'BEARISH'
            
            return 'NEUTRAL'
                        
                except Exception as e:
            self.logger.error(f"Error determining direction: {str(e)}")
            return 'UNKNOWN'

    def _calculate_risk_score(self, alert: Dict, technical_data: Dict, greeks_data: Dict) -> float:
        """Calculate risk score based on multiple factors."""
        try:
            # Base risk from alert data
            premium = float(alert.get('premium', 0))
            days_to_expiry = float(alert.get('days_to_expiry', 30))
            
            # Premium risk (higher premium = higher risk)
            premium_risk = min(1.0, premium / (self.config.min_premium * 5))
            
            # Time risk (shorter time = higher risk)
            time_risk = 1.0 - min(1.0, days_to_expiry / 90)
            
            # Technical risk
            technical_risk = 0.5  # Default neutral
            if technical_data:
                rsi = technical_data.get('rsi', 50)
                # Extreme RSI values indicate higher risk
                if rsi > 75 or rsi < 25:
                    technical_risk = 0.8
            
            # Greeks risk
            greeks_risk = 0.5  # Default neutral
            if greeks_data:
                gamma = abs(float(greeks_data.get('gamma', 0)))
                theta = abs(float(greeks_data.get('theta', 0)))
                
                # High gamma or theta indicate higher risk
                if gamma > 0.1 or theta > 0.1:
                    greeks_risk = 0.8
            
            # Weighted average
            final_risk = (
                premium_risk * 0.3 +
                time_risk * 0.3 +
                technical_risk * 0.2 +
                greeks_risk * 0.2
            )
            
            return min(1.0, max(0.0, final_risk))
            
        except Exception as e:
            self.logger.error(f"Error calculating risk score: {str(e)}")
            return 0.5  # Default to moderate risk