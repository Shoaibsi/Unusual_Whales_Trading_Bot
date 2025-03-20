import os
import re
import json
import asyncio
import aiohttp
from typing import Dict, List, Optional, Union
import traceback
from datetime import datetime
from src.logger_config import logger, log_exceptions, log_api_call, log_grok_analysis
from src.ws_signal_detector import TradingSignal


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class GrokAnalyzer:
    """Analyze trading signals using Grok AI."""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """Initialize the Grok analyzer with API key and model."""
        self.api_key = api_key or os.getenv('XAI_API_KEY')
        self.model = model or "grok-beta"  # Updated to use grok-beta model
        self.timeout = 30  # Default timeout in seconds
        self.base_url = "https://api.x.ai/v1"  # Updated to correct base URL
        self.session = None
        
        # Initialize logger
        logger.info(f"Initializing GrokAnalyzer with model: {self.model}")
    
    async def _ensure_session(self):
        """Ensure that an aiohttp session exists."""
        try:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession(
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                )
                logger.info("Created new aiohttp session for GrokAnalyzer")
        except Exception as e:
            logger.error(f"Error creating aiohttp session: {str(e)}")
            raise
    
    async def initialize(self):
        """Initialize the Grok analyzer."""
        try:
            # Validate API key
            if not self.api_key:
                logger.error("No API key provided for GrokAnalyzer")
                return False
            
            # Create session
            await self._ensure_session()
            
            # Test API connection with a simple request
            logger.info("Testing Grok API connection...")
            
            test_messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Test connection"}
            ]
            
            response = await self._make_grok_api_call(test_messages, self.model)
            
            if isinstance(response, dict) and "error" in response:
                logger.error(f"Failed to initialize GrokAnalyzer: {response['error']}")
                return False
                
            logger.info("GrokAnalyzer initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing GrokAnalyzer: {str(e)}")
            return False
            
    async def _make_grok_api_call(self, messages: List[Dict], model: str, max_retries: int = 3) -> Dict:
        """Make a call to the Grok API with retries and proper error handling."""
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                await self._ensure_session()
                
                url = f"{self.base_url}/chat/completions"
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
                
                data = {
                    "model": model,
                    "messages": messages,
                    "temperature": 0.7,
                    "max_tokens": 1000,
                    "stream": False
                }
                
                async with self.session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("choices") and len(result["choices"]) > 0:
                            return result["choices"][0]["message"]["content"]
                        else:
                            raise ValueError("Empty response from Grok API")
                    elif response.status == 401:
                        raise ValueError("Invalid API key")
                    elif response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning(f"Rate limited by Grok API, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        retry_count += 1
                        continue
                    else:
                        error_text = await response.text()
                        raise ValueError(f"Grok API error (status {response.status}): {error_text}")
                        
            except aiohttp.ClientError as e:
                last_error = f"Network error: {str(e)}"
                logger.error(f"Network error on attempt {retry_count + 1}: {str(e)}")
            except asyncio.TimeoutError:
                last_error = "Request timed out"
                logger.error(f"Timeout on attempt {retry_count + 1}")
            except Exception as e:
                last_error = str(e)
                logger.error(f"Error on attempt {retry_count + 1}: {str(e)}")
            
            # Exponential backoff
            wait_time = min(2 ** retry_count, 30)  # Max 30 seconds
            logger.info(f"Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            retry_count += 1
        
        logger.error(f"Failed to call Grok API after {max_retries} attempts. Last error: {last_error}")
        return {"error": f"Failed after {max_retries} attempts: {last_error}"}
    
    def _prepare_system_message(self, market_context: Optional[Dict] = None) -> str:
        """Prepare the system message for Grok analysis."""
        system_msg = (
            "You are an expert financial analyst. Analyze the trading signal and provide analysis in the following format:\n\n"
            "Key Factors:\n"
            "• List 2-3 key factors that influenced this signal\n"
            "• Include volume/OI ratios if available\n"
            "• Note any sector-specific trends\n\n"
            "Risks:\n"
            "• List 1-2 key risks to this trade\n"
            "• Consider market volatility and timing\n\n"
            "Entry Points:\n"
            "• Specify price ranges for entry\n"
            "• Base on technical indicators if available\n\n"
            "Option Recommendations:\n"
            "• Analyze the provided options contracts\n"
            "• Recommend specific strikes and expirations\n"
            "• Consider IV, volume, and open interest\n\n"
            "Historical Context:\n"
            "• Brief relevant history for this ticker\n"
            "• Note any similar past setups\n\n"
            "Format your response with bullet points and ensure all sections are present.\n"
            "Be concise but thorough. Include specific numbers and data points when available.\n"
            "If options data is provided, focus on analyzing those specific contracts."
        )
        
        if market_context:
            system_msg += "\n\nMarket Context:\n"
            if 'sector_performance' in market_context:
                system_msg += f"• Sector Performance: {market_context['sector_performance']}\n"
            if 'market_conditions' in market_context:
                system_msg += f"• Market Conditions: {market_context['market_conditions']}\n"
            if 'volatility_index' in market_context:
                system_msg += f"• VIX Level: {market_context['volatility_index']}\n"
        
        return system_msg
    
    def _prepare_system_message_fallback(self, market_context: Optional[Dict] = None) -> str:
        """Fallback method for preparing system message if the primary method fails."""
        return (
            "You are an expert financial analyst. Analyze the given trading signals and provide "
            "a detailed analysis with direction (BULLISH, BEARISH, or NEUTRAL) and confidence score (0-1)."
        )
    
    def _prepare_signals_for_analysis(self, signals: List[Union[TradingSignal, Dict]]) -> List[Dict]:
        """Prepare signals for analysis by converting them to dictionaries."""
        prepared_signals = []
        
        for signal in signals:
            if isinstance(signal, TradingSignal):
                # Convert TradingSignal to dictionary
                signal_dict = {
                    "ticker": signal.ticker,
                    "signal_type": signal.signal_type.value if hasattr(signal.signal_type, 'value') else str(signal.signal_type),
                    "timestamp": signal.timestamp,
                    "metadata": getattr(signal, 'metadata', {})
                }
                
                # Add contract details if available
                if hasattr(signal, 'contract') and signal.contract:
                    signal_dict["contract"] = signal.contract
                
                prepared_signals.append(signal_dict)
            elif isinstance(signal, dict):
                # Use the dictionary directly
                prepared_signals.append(signal)
            else:
                logger.warning(f"Unsupported signal type: {type(signal)}")
        
        return prepared_signals
    
    def _serialize_for_json(self, obj):
        """Recursively serialize objects for JSON encoding."""
        if isinstance(obj, dict):
            return {k: self._serialize_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_json(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return self._serialize_for_json(obj.__dict__)
        else:
            return obj
    
    def _get_default_analysis(self) -> Dict:
        """Get default analysis values when Grok API fails."""
        return {
            "direction": "NEUTRAL",
            "confidence": 0.5,
            "key_factors": ["Analysis not available"],
            "risks": ["Unknown due to analysis failure"],
            "entry_points": "Not available",
            "option_recommendations": "No recommendations available",
            "historical_context": "No historical context available"
        }
    
    def parse_grok_response(self, response: str) -> Dict:
        """Parse the response from Grok API into a structured format."""
        try:
            # First try to parse as JSON
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                pass
            
            # If not JSON, parse the text response
            analysis = {
                "direction": "UNKNOWN",
                "confidence": 0.5,
                "key_factors": [],
                "risks": [],
                "entry_points": "",
                "option_recommendations": "",
                "historical_context": ""
            }
            
            # Extract direction and confidence
            direction_match = re.search(r"Sentiment:\s*(BULLISH|BEARISH|NEUTRAL)", response, re.IGNORECASE)
            if direction_match:
                analysis["direction"] = direction_match.group(1).upper()
            
            confidence_match = re.search(r"Confidence:\s*(\d+(?:\.\d+)?)[%]?", response)
            if confidence_match:
                confidence = float(confidence_match.group(1))
                analysis["confidence"] = confidence / 100 if confidence > 1 else confidence
            
            # Extract key factors
            factors_match = re.search(r"Key Factors:(.*?)(?:Risks:|$)", response, re.DOTALL)
            if factors_match:
                factors = re.findall(r"[•\-\*]\s*([^\n]+)", factors_match.group(1))
                analysis["key_factors"] = [f.strip() for f in factors if f.strip()]
            
            # Extract risks
            risks_match = re.search(r"Risks:(.*?)(?:Entry Points:|$)", response, re.DOTALL)
            if risks_match:
                risks = re.findall(r"[•\-\*]\s*([^\n]+)", risks_match.group(1))
                analysis["risks"] = [r.strip() for r in risks if r.strip()]
            
            # Extract entry points
            entry_match = re.search(r"Entry Points:(.*?)(?:Option Recommendations:|$)", response, re.DOTALL)
            if entry_match:
                analysis["entry_points"] = entry_match.group(1).strip()
            
            # Extract option recommendations
            options_match = re.search(r"Option Recommendations:(.*?)(?:Historical Context:|$)", response, re.DOTALL)
            if options_match:
                analysis["option_recommendations"] = options_match.group(1).strip()
            
            # Extract historical context
            history_match = re.search(r"Historical Context:(.*?)$", response, re.DOTALL)
            if history_match:
                analysis["historical_context"] = history_match.group(1).strip()
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error parsing Grok response: {str(e)}")
            return self._get_default_analysis()
    
    @log_exceptions
    @log_grok_analysis('batch_signal_analysis')
    @log_api_call('X.AI Batch Signal Analysis')
    async def analyze_signals(self, signals: List[Union[TradingSignal, Dict]], market_context: Optional[Dict] = None) -> List[Dict]:
        """Analyze trading signals using Grok AI in live trading mode."""
        if not signals:
            logger.warning("No signals to analyze")
            return []
        
        try:
            logger.info(f"Analyzing {len(signals)} signals with X.AI")
            
            # Log each signal being analyzed
            for signal in signals:
                ticker = signal.ticker if isinstance(signal, TradingSignal) else signal.get('ticker', 'UNKNOWN')
                signal_type = signal.signal_type if isinstance(signal, TradingSignal) else signal.get('signal_type', 'UNKNOWN')
                direction = signal.suggested_direction if isinstance(signal, TradingSignal) else signal.get('direction', 'UNKNOWN')
                logger.info(f"Processing signal: {ticker} - {signal_type} - {direction}")
                
                # Log options data if available
                if isinstance(signal, TradingSignal) and hasattr(signal, 'options_data'):
                    for contract in signal.options_data:
                        logger.info(f"  Contract: {contract['type']} ${contract['strike']} exp {contract['expiration']}")
                        logger.info(f"    Volume: {contract['volume']} | OI: {contract['open_interest']} | IV: {contract['implied_volatility']}")
            
            # Prepare system message with market context
            try:
                system_msg = self._prepare_system_message(market_context)
                logger.debug(f"Prepared system message: {system_msg[:200]}...")
            except AttributeError:
                logger.warning("_prepare_system_message method not found, using fallback system message")
                system_msg = self._prepare_system_message_fallback(market_context)
            
            # Prepare signal data
            try:
                prepared_signals = self._serialize_for_json(self._prepare_signals_for_analysis(signals))
                signals_json = json.dumps(prepared_signals, indent=2, cls=DateTimeEncoder)
                context_json = json.dumps(self._serialize_for_json(market_context or {}), indent=2, cls=DateTimeEncoder)
                logger.debug(f"Prepared signals JSON: {signals_json[:200]}...")
            except Exception as json_error:
                logger.error(f"Error preparing signals for analysis: {str(json_error)}")
                return [self._get_default_analysis() for _ in signals]
            
            # Make API call
            try:
                messages = [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": f"Analyze the following trading signals and provide a detailed analysis for each:\n\n{signals_json}\n\nMarket Context: {context_json}"}
                ]
                
                logger.info("Making Grok API call...")
                response = await self._make_grok_api_call(messages, self.model)
                logger.debug(f"Received Grok API response: {str(response)[:200]}...")
                
                if response and not (isinstance(response, dict) and 'error' in response):
                    # Parse the response
                    analysis = self.parse_grok_response(response)
                    logger.info("Successfully parsed Grok response")
                    
                    # Process results
                    results = []
                    if isinstance(analysis, dict):
                        if len(signals) == 1:
                            results = [analysis]
                            logger.info(f"Single signal analysis: {str(analysis)[:200]}...")
                        else:
                            signal_keys = [k for k in analysis.keys() if k.startswith('signal_')]
                            if signal_keys:
                                for i in range(len(signals)):
                                    key = f"signal_{i}"
                                    if key in analysis:
                                        results.append(analysis[key])
                                        logger.info(f"Analysis for signal {i}: {str(analysis[key])[:200]}...")
                                    else:
                                        results.append(self._get_default_analysis())
                                        logger.warning(f"No analysis found for signal {i}, using default")
                            else:
                                results = [analysis] + [self._get_default_analysis() for _ in range(len(signals)-1)]
                                logger.warning("Single analysis for multiple signals, using defaults for extras")
                    elif isinstance(analysis, list):
                        results = analysis
                        if len(results) < len(signals):
                            results.extend([self._get_default_analysis() for _ in range(len(signals) - len(results))])
                            logger.warning(f"Padded {len(signals) - len(analysis)} missing analyses with defaults")
                    else:
                        logger.warning(f"Unexpected analysis format: {type(analysis)}")
                        results = [self._get_default_analysis() for _ in signals]
                    
                    # Validate results
                    for i, result in enumerate(results):
                        if not isinstance(result, dict):
                            logger.warning(f"Invalid analysis format for signal {i}: {result}")
                            results[i] = self._get_default_analysis()
                        elif 'confidence' not in result or not isinstance(result['confidence'], (int, float)):
                            logger.warning(f"Missing or invalid confidence for signal {i}")
                            result['confidence'] = 0.5
                        elif result['confidence'] <= 0:
                            logger.warning(f"Signal {i} has low confidence {result['confidence']}. Setting to 0.3.")
                            result['confidence'] = 0.3
                    
                    return results
                else:
                    if isinstance(response, dict) and 'error' in response:
                        logger.warning(f"Error in Grok API response: {response['error']}")
                    else:
                        logger.warning("Empty or invalid response from Grok API")
                    
                    logger.info("Using default analysis values due to Grok API issues")
                    return [self._get_default_analysis() for _ in signals]
            except Exception as api_error:
                logger.error(f"Error in Grok API call: {str(api_error)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                logger.info("Using default analysis values due to Grok API exception")
                return [self._get_default_analysis() for _ in signals]
        except Exception as e:
            logger.error(f"Error in batch signal analysis: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return [self._get_default_analysis() for _ in signals]
    
    async def enhance_signals(self, signals: List[TradingSignal], analyses: List[Dict]) -> List[TradingSignal]:
        """Enhance trading signals with Grok analysis.
        
        Args:
            signals: List of trading signals to enhance
            analyses: List of analyses from Grok
            
        Returns:
            List of enhanced trading signals with Grok analysis
        """
        enhanced_signals = []
        for signal, analysis in zip(signals, analyses):
            try:
                # Store the full analysis in the signal
                signal.grok_analysis = analysis
                
                # Extract confidence and set it on the signal
                confidence = analysis.get('confidence', 0.5)
                # Ensure confidence is a float between 0 and 1
                if isinstance(confidence, (int, float)):
                    if confidence > 1:
                        confidence /= 100  # Convert percentage to decimal
                    signal.confidence = max(0.1, min(1.0, confidence))  # Clamp between 0.1 and 1.0
                else:
                    signal.confidence = 0.5  # Default confidence
                
                # Set score to match confidence for compatibility
                signal.score = signal.confidence
                
                logger.info(f"Enhanced signal for {signal.ticker} with confidence {signal.confidence:.2f}")
                enhanced_signals.append(signal)
            except Exception as e:
                logger.error(f"Error enhancing signal for {signal.ticker}: {str(e)}")
                enhanced_signals.append(signal)  # Keep original signal on error
        
        return enhanced_signals
    
    async def recommend_strategy_for_signal(self, signal_data: Dict, available_strategies: List[str]) -> Dict:
        """Recommend the best trading strategy for a given signal.
        
        Args:
            signal_data: Trading signal data including ticker, type, and metadata
            available_strategies: List of available strategy names
            
        Returns:
            Dictionary with recommended strategy name and confidence score
        """
        try:
            logger.info(f"Recommending strategy for {signal_data.get('ticker', 'unknown')} from {len(available_strategies)} strategies")
            
            # Prepare system message
            system_msg = (
                "You are an expert trading strategy advisor. Your task is to recommend the best trading "
                "strategy for a given market signal based on the signal characteristics and market context. "
                "Analyze the signal data carefully and select the most appropriate strategy from the available options."
            )
            
            # Format available strategies with descriptions
            strategy_descriptions = {
                "BasicStrategy": "Simple strategy that follows the direction of the signal",
                "UnusualOptionsActivityStrategy": "Identifies stocks with unusually high options volume or premium",
                "DarkPoolReversalStrategy": "Identifies potential reversals based on dark pool activity",
                "GammaSqueezeStrategy": "Identifies potential gamma squeeze setups",
                "GreekFlowMomentumStrategy": "Uses options greek flows to identify momentum",
                "OpenInterestDivergenceStrategy": "Identifies divergences between price and open interest",
                "ShortInterestHedgingStrategy": "Identifies stocks with high short interest and hedging activity",
                "SectorRotationStrategy": "Identifies sector rotation opportunities",
                "CongressTradingStrategy": "Follows trading patterns of congressional members",
                "InsiderTradingStrategy": "Follows legal insider trading patterns",
                "InstitutionalActivityStrategy": "Follows institutional buying and selling patterns"
            }
            
            strategies_info = ""
            for strategy in available_strategies:
                description = strategy_descriptions.get(strategy, f"{strategy} strategy")
                strategies_info += f"- {strategy}: {description}\n"
            
            # Format signal data as JSON
            signal_json = json.dumps(signal_data, indent=2, cls=DateTimeEncoder)
            
            # Construct the messages for the API call
            messages = [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": (
                    f"Recommend the best trading strategy for the following signal:\n\n"
                    f"{signal_json}\n\n"
                    f"Available strategies:\n{strategies_info}\n\n"
                    f"Return your recommendation as a JSON object with 'strategy' (the exact name from the available list) "
                    f"and 'confidence' (a number between 0 and 1 indicating your confidence in this recommendation)."
                )}
            ]
            
            # Make the API call with a shorter timeout
            self.timeout = 10  # Reduce timeout to 10 seconds
            response = await self._make_grok_api_call(messages, self.model)
            
            # Parse the response
            if response and not (isinstance(response, dict) and 'error' in response):
                # Extract the recommendation from the response
                recommendation = self._extract_strategy_recommendation(response, available_strategies)
                
                if recommendation and 'strategy' in recommendation:
                    logger.info(f"Recommended strategy: {recommendation['strategy']} with confidence {recommendation.get('confidence', 0.7)}")
                    return recommendation
                else:
                    logger.warning("Could not extract valid strategy recommendation from Grok response")
            else:
                if isinstance(response, dict) and 'error' in response:
                    logger.warning(f"Error in Grok API response: {response['error']}")
                else:
                    logger.warning("Empty or invalid response from Grok API")
            
            # Fallback: return a default recommendation
            default_strategy = self._get_default_strategy_recommendation(signal_data, available_strategies)
            logger.info(f"Using default strategy recommendation: {default_strategy}")
            return default_strategy
            
        except Exception as e:
            logger.error(f"Error recommending strategy: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Return a default recommendation on error
            return self._get_default_strategy_recommendation(signal_data, available_strategies)
    
    def _extract_strategy_recommendation(self, response: Dict, available_strategies: List[str]) -> Dict:
        """Extract strategy recommendation from Grok response."""
        try:
            if 'choices' in response and len(response['choices']) > 0:
                content = response['choices'][0]['message']['content']
                
                # Try to parse JSON from the content
                try:
                    # Find JSON-like content within the response
                    json_match = re.search(r'\{[\s\S]*\}', content)
                    if json_match:
                        json_content = json_match.group(0)
                        recommendation = json.loads(json_content)
                        
                        # Validate the recommendation
                        if 'strategy' in recommendation:
                            strategy = recommendation['strategy']
                            
                            # Check if the strategy is in the available list
                            if strategy in available_strategies:
                                # Ensure confidence is a float between 0 and 1
                                confidence = recommendation.get('confidence', 0.7)
                                if isinstance(confidence, (int, float)):
                                    if confidence > 1:
                                        confidence /= 100  # Convert percentage to decimal
                                    confidence = max(0.1, min(1.0, confidence))
                                else:
                                    confidence = 0.7  # Default confidence
                                
                                return {
                                    'strategy': strategy,
                                    'confidence': confidence
                                }
                            else:
                                logger.warning(f"Recommended strategy '{strategy}' not in available strategies")
                        else:
                            logger.warning("No strategy field in recommendation")
                    else:
                        logger.warning("No JSON found in Grok response")
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON from Grok response: {str(e)}")
                
                # If JSON parsing fails, try to extract strategy name from text
                for strategy in available_strategies:
                    if strategy in content:
                        logger.info(f"Extracted strategy {strategy} from text response")
                        return {
                            'strategy': strategy,
                            'confidence': 0.6  # Lower confidence for text extraction
                        }
        except Exception as e:
            logger.error(f"Error extracting strategy recommendation: {str(e)}")
        
        return None
    
    def _get_default_strategy_recommendation(self, signal_data: Dict, available_strategies: List[str]) -> Dict:
        """Get default strategy recommendation when Grok API fails."""
        # Default strategy selection logic
        
        # Check for options data
        if 'options_data' in signal_data and signal_data['options_data']:
            if 'UnusualOptionsActivityStrategy' in available_strategies:
                return {
                    'strategy': 'UnusualOptionsActivityStrategy',
                    'confidence': 0.7
                }
        
        # Check for dark pool data
        if 'metadata' in signal_data and signal_data['metadata'] and 'darkpool' in signal_data['metadata']:
            if 'DarkPoolReversalStrategy' in available_strategies:
                return {
                    'strategy': 'DarkPoolReversalStrategy',
                    'confidence': 0.65
                }
        
        # Default to BasicStrategy if available
        if 'BasicStrategy' in available_strategies:
            return {
                'strategy': 'BasicStrategy',
                'confidence': 0.5
            }
        
        # Last resort: use the first available strategy
        if available_strategies:
            return {
                'strategy': available_strategies[0],
                'confidence': 0.5
            }
        
        # No strategies available
        return {
            'strategy': None,
            'confidence': 0.0
        }

    async def cleanup(self):
        """Clean up resources used by the Grok analyzer."""
        try:
            logger.info("Cleaning up GrokAnalyzer resources")
            if hasattr(self, 'session') and self.session and not self.session.closed:
                await self.session.close()
                logger.info("Closed Grok API session")
            return True
        except Exception as e:
            logger.error(f"Error cleaning up GrokAnalyzer: {str(e)}")
            return False