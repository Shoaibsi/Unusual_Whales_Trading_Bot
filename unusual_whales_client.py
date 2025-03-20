import json
import asyncio
import aiohttp
import os
import time
from typing import Dict, Optional, List, Any, Union
from datetime import datetime, timedelta

# Import the configured logger and decorators
from src.logger_config import logger, log_exceptions, log_api_call, format_for_logging, output_manager

from src.api_circuit_breaker import CircuitBreaker
from src.rate_limiter import RateLimiter
from src.endpoint_monitor import EndpointMonitor  # Import EndpointMonitor

class WSUnusualWhalesClient:
    """Client for Unusual Whales API."""
    
    def __init__(self, api_key: str = None):
        """Initialize API client with API key."""
        self.api_key = api_key or os.environ.get("UNUSUAL_WHALES_API_KEY")
        self.base_url = "https://api.unusualwhales.com"
        self.session = None
        
        # Initialize circuit breaker with conservative defaults
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            reset_timeout=60,
            rate_limit_window=60,
            rate_limit_max_calls=100  # Keep below actual limit of 120
        )
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            global_rate_limit=90,       # Total requests per minute
            endpoint_rate_limit=25,     # Requests per endpoint per minute  
            time_window=60,             # 1 minute window
            min_request_spacing=0.1     # 100ms minimum between requests
        )
        
        # Cache for responses
        self.cache = {}
        self.cache_ttl = {
            # Core endpoints - cached with different TTLs
            "/api/option-trades/flow-alerts": 120,        # 2 minutes for flow alerts
            "/api/darkpool/recent": 900,                  # 15 minutes for recent dark pool
            "/api/market/market-tide": 3600,              # 1 hour for market tide
            
            # Defaults for other endpoint types
            "darkpool": 900,                              # 15 minutes for ticker-specific dark pool
            "option-contracts": 1800,                     # 30 minutes for option chains
            "default": 300                                # 5 minutes default TTL
        }
        
        self.monitor = EndpointMonitor()  # Initialize monitoring
        
        if not self.api_key:
            raise ValueError("API key is required")
    
    @log_exceptions
    async def initialize(self):
        """Initialize the session."""
        await self._ensure_session()
        
    @log_exceptions
    async def _ensure_session(self):
        """Ensure that an aiohttp session exists."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.api_key}"}
            )
    
    @log_exceptions
    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("API client session closed")
            
    @log_exceptions
    async def cleanup(self):
        """Cleanup resources."""
        await self.close()
        logger.info("API client resources cleaned up")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    @log_exceptions
    @log_api_call("API Request")
    async def _make_request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None, 
                           retry_count: int = 0, max_retries: int = 2) -> Dict:
        """
        Make a request to the Unusual Whales API with improved error handling.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body
            retry_count: Current retry attempt (internal use)
            max_retries: Maximum number of retries for failed requests
            
        Returns:
            Response data
        """
        # Create a cache key if this is a GET request
        cache_key = None
        if method.upper() == "GET":
            cache_key = f"{endpoint}:{json.dumps(params or {})}"
            
            # Check cache first
            cached_data = self._get_from_cache(cache_key, endpoint)
            if cached_data:
                return cached_data
        
        # Ensure session is created
        if self.session is None:
            await self._ensure_session()
            
        # Check if circuit is open for this endpoint
        if self.circuit_breaker.is_open(endpoint):
            logger.warning(f"Circuit breaker open for {endpoint}, skipping request")
            return {"error": True, "message": "Circuit breaker open", "endpoint": endpoint}
            
        # Apply rate limiting
        await self.rate_limiter.acquire(endpoint)
        
        # Construct full URL
        url = f"{self.base_url}{endpoint}"
        
        try:
            # Log the request details for debugging
            if params:
                logger.info(f"Making {method} request to {endpoint} with params: {format_for_logging(params, 200)}")
            else:
                logger.info(f"Making {method} request to {endpoint}")
            
            # Make the request with appropriate timeout
            async with self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=self._get_headers(),
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                # Record success for circuit breaker
                if response.status == 200:
                    self.circuit_breaker.record_success(endpoint)
                
                # Handle successful response
                if response.status == 200:
                    try:
                        # Always try to parse as JSON first
                        data = await response.json()
                        
                        # Use OutputManager for large responses to prevent garbled output
                        await output_manager.add_output({
                            "event": "api_response",
                            "endpoint": endpoint,
                            "status": response.status,
                            "timestamp": datetime.now().isoformat(),
                            "success": True
                        })
                        
                        # For large responses, don't log the full data
                        if isinstance(data, dict) and len(str(data)) > 1000:
                            # Only log a summary of the response
                            if "data" in data and isinstance(data["data"], list):
                                logger.debug(f"Received response from {endpoint} with {len(data['data'])} items")
                            else:
                                keys = list(data.keys())
                                logger.debug(f"Received response from {endpoint} with keys: {', '.join(keys[:5])}{', ...' if len(keys) > 5 else ''}")
                        
                        # Cache the successful response
                        self._add_to_cache(cache_key, data, endpoint)
                        return data
                    except json.JSONDecodeError as e:
                        # If JSON parsing fails, log the error and return text
                        logger.error(f"JSON decode error for {endpoint}: {str(e)}")
                        text_response = await response.text()
                        
                        # Use OutputManager for error details
                        await output_manager.add_output({
                            "event": "api_error",
                            "endpoint": endpoint,
                            "error_type": "json_decode",
                            "message": str(e),
                            "timestamp": datetime.now().isoformat()
                        })
                        
                        logger.error(f"Raw response text: {format_for_logging(text_response)}")
                        
                        return {
                            "error": True,
                            "message": f"JSON decode error: {str(e)}",
                            "endpoint": endpoint
                        }
                # Handle error responses
                else:
                    # Record failure for circuit breaker
                    self.circuit_breaker.record_failure(endpoint)
                    
                    # Log the error details
                    error_text = await response.text()
                    
                    # Use OutputManager for error details
                    await output_manager.add_output({
                        "event": "api_error",
                        "endpoint": endpoint,
                        "status": response.status,
                        "message": error_text[:100] + "..." if len(error_text) > 100 else error_text,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    logger.error(f"API error for {endpoint}: Status {response.status}, Response: {format_for_logging(error_text)}")
                    
                    # Retry logic for 404 errors or error responses
                    if (response.status == 404 or (isinstance(error_text, dict) and error_text.get("error"))) and retry_count < max_retries:
                        retry_delay = 5 * (retry_count + 1)  # Exponential backoff
                        logger.warning(f"API call failed for {endpoint}, retrying in {retry_delay}s (attempt {retry_count+1}/{max_retries})")
                        await asyncio.sleep(retry_delay)
                        return await self._make_request(method, endpoint, params, data, retry_count + 1, max_retries)
                    
                    # For market data endpoints that return 404, try alternative endpoints
                    if response.status == 404 and endpoint.startswith("/api/market/data/"):
                        ticker = endpoint.split("/")[-1]
                        logger.warning(f"Market data endpoint 404 for {ticker}, trying alternative endpoint")
                        # Try alternative endpoint
                        alt_endpoint = f"/api/stock/{ticker}/data"
                        return await self._make_request(method, alt_endpoint, params, data, 0, max_retries)
                    
                    return {
                        "error": True,
                        "status": response.status,
                        "message": error_text[:100] + "..." if len(error_text) > 100 else error_text,
                        "endpoint": endpoint
                    }
                
        except asyncio.TimeoutError:
            # Record timeout failure
            self.circuit_breaker.record_failure(endpoint, status_code=408)
            
            # Use OutputManager for timeout details
            await output_manager.add_output({
                "event": "api_timeout",
                "endpoint": endpoint,
                "timestamp": datetime.now().isoformat()
            })
            
            logger.error(f"Request timeout for {endpoint}")
            return {
                "error": True,
                "message": "Request timed out",
                "endpoint": endpoint
            }
        except Exception as e:
            # Record general failure
            self.circuit_breaker.record_failure(endpoint)
            
            # Use OutputManager for error details
            await output_manager.add_output({
                "event": "api_exception",
                "endpoint": endpoint,
                "error_type": type(e).__name__,
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            })
            
            logger.error(f"Request error for {endpoint}: {str(e)}")
            return {
                "error": True,
                "message": str(e),
                "endpoint": endpoint
            }
    
    def _get_from_cache(self, cache_key: str, endpoint: str) -> Optional[Union[Dict, List]]:
        """Get data from cache if it exists and is still valid."""
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            if time.time() < entry["expires"]:
                return entry["data"]
        return None
    
    def is_cached(self, endpoint: str, params: Dict = None, ttl: int = None) -> bool:
        """
        Check if data for the given endpoint and params is cached and valid.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            ttl: Optional custom TTL to check against (in seconds)
            
        Returns:
            bool: True if valid cached data exists, False otherwise
        """
        cache_key = f"{endpoint}:{json.dumps(params or {})}"
        
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            
            # If custom TTL is provided, check against that instead of the cache entry's expiration
            if ttl is not None:
                return (time.time() - (entry["expires"] - self._get_ttl_for_endpoint(endpoint))) < ttl
            else:
                return time.time() < entry["expires"]
                
        return False
    
    def get_cached(self, endpoint: str, params: Dict = None) -> Optional[Union[Dict, List]]:
        """
        Get cached data for the given endpoint and params if it exists and is valid.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            Optional[Union[Dict, List]]: Cached data if valid, None otherwise
        """
        cache_key = f"{endpoint}:{json.dumps(params or {})}"
        return self._get_from_cache(cache_key, endpoint)
    
    def _get_ttl_for_endpoint(self, endpoint: str) -> int:
        """Get the TTL for a given endpoint based on cache_ttl configuration."""
        # Check for exact endpoint match
        if endpoint in self.cache_ttl:
            return self.cache_ttl[endpoint]
        
        # Check for partial matches (e.g., all darkpool endpoints)
        for key, val in self.cache_ttl.items():
            if key in endpoint:
                return val
                
        # Default TTL
        return self.cache_ttl.get("default", 300)  # Default 5 minutes
    
    def _add_to_cache(self, cache_key: str, data: Union[Dict, List], endpoint: str) -> None:
        """Add data to cache with appropriate TTL."""
        # Determine TTL based on endpoint
        ttl = self.cache_ttl.get("default", 300)  # Default 5 minutes
        
        # Check for exact endpoint match
        if endpoint in self.cache_ttl:
            ttl = self.cache_ttl[endpoint]
        else:
            # Check for partial matches (e.g., all darkpool endpoints)
            for key, val in self.cache_ttl.items():
                if key in endpoint:
                    ttl = val
                    break
        
        # Store with expiration
        self.cache[cache_key] = {
            "data": data,
            "expires": time.time() + ttl
        }
        
        logger.debug(f"Cached response for {endpoint} with TTL {ttl}s")
    
    def _parse_option_symbol(self, option_symbol: str) -> Dict[str, Any]:
        """
        Parse an OCC option symbol to extract strike price, expiration date, and option type.
        
        Args:
            option_symbol: OCC-formatted option symbol (e.g., "AAPL230616C00150000")
            
        Returns:
            Dictionary with extracted information
        """
        try:
            # Handle different symbol formats
            if len(option_symbol) < 6:
                logger.warning(f"Option symbol too short to parse: {option_symbol}")
                return {}
                
            # Find where the date part starts (after the ticker symbol)
            # This is tricky because ticker symbols can be variable length
            # Look for the pattern of 6 digits (date) followed by C or P and more digits
            import re
            match = re.search(r'([A-Z]+)(\d{6})([CP])(\d+)', option_symbol)
            
            if match:
                ticker, date_str, option_type, strike_str = match.groups()
                
                # Parse expiration date (YYMMDD format)
                year = int("20" + date_str[0:2])  # Assume 20xx for years
                month = int(date_str[2:4])
                day = int(date_str[4:6])
                expiration = f"{year}-{month:02d}-{day:02d}"
                
                # Parse strike price (divide by 1000 to get actual price)
                # The number of decimal places can vary by exchange
                strike_price = float(strike_str) / 1000.0
                
                # Determine option type
                option_type = "call" if option_type == "C" else "put"
                
                return {
                    "ticker": ticker,
                    "expiration": expiration,
                    "strike": strike_price,
                    "option_type": option_type
                }
            else:
                logger.warning(f"Could not parse option symbol with regex: {option_symbol}")
                return {}
                
        except Exception as e:
            logger.error(f"Error parsing option symbol {option_symbol}: {str(e)}")
            return {}
    
    async def get_options_chain(self, ticker: str) -> Dict:
        """Get options chain data for a ticker."""
        try:
            # Check cache first
            cache_key = f"options_chain_{ticker}"
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                logger.debug(f"Using cached options chain for {ticker}")
                return cached_data

            # Prepare request
            endpoint = f"/api/stock/{ticker}/option-contracts"
            params = {
                "ticker": ticker
            }

            # Make request
            logger.info(f"Making GET request to /api/stock/{ticker}/option-contracts with params: {json.dumps(params, default=str)}")
            response = await self._make_request(
                method="GET",
                endpoint=f"/api/stock/{ticker}/option-contracts",
                params=params
            )

            if response and isinstance(response, dict) and not response.get("error"):
                if "data" in response:
                    # Validate and normalize options data
                    response["data"] = self._validate_options_data(response["data"])
                    
                    # Only cache if we have valid data
                    if response["data"]:
                        self._add_to_cache(cache_key, response, ttl=self.cache_ttl["option-contracts"])
                        logger.info(f"Cached options chain for {ticker} with {len(response['data'])} contracts")
                    else:
                        logger.warning(f"No valid options contracts found for {ticker}")
                else:
                    logger.warning(f"No data field in options chain response for {ticker}")
            else:
                logger.error(f"Error in options chain response for {ticker}: {response.get('error') if response else 'No response'}")

            return response or {}

        except Exception as e:
            logger.error(f"Error fetching options chain for {ticker}: {str(e)}")
            return {}
    
    @log_exceptions
    @log_api_call("Flow Alerts")
    async def get_flow_alerts(self, ticker: str = None, min_premium: int = 10000, limit: int = 50) -> List[Dict]:
        """
        Get flow alerts from Unusual Whales API.
        
        Args:
            ticker (str, optional): Filter alerts by ticker.
            min_premium (int, optional): Minimum premium threshold.
            limit (int, optional): Maximum number of alerts to return.
            
        Returns:
            List[Dict]: List of flow alerts.
        """
        try:
            # Prepare parameters
            params = {}
            if ticker:
                params["ticker"] = ticker
                
            if min_premium:
                params["min_premium"] = min_premium
                
            if limit:
                params["limit"] = limit
                
            response = await self._make_request(
                method="GET",
                endpoint="/api/option-trades/flow-alerts",
                params=params
            )
            
            # Handle string responses (parse JSON)
            if isinstance(response, str):
                logger.warning(f"Flow alerts response is a string, attempting to parse as JSON")
                try:
                    response = json.loads(response)
                    logger.info(f"Successfully parsed flow alerts string response to JSON")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse flow alerts response as JSON: {str(e)}")
                    # Return empty list to prevent further errors
                    return []
            
            # Log response structure
            if isinstance(response, dict):
                logger.debug(f"Flow alerts response keys: {list(response.keys())}")
                # Check if there's a data field containing the actual alerts
                if "data" in response and isinstance(response["data"], list):
                    # Map new field names to expected field names for compatibility
                    normalized_alerts = []
                    for alert in response["data"]:
                        # Create a normalized alert with both old and new field names
                        normalized_alert = alert.copy()
                        
                        # Map total_premium to premium_usd if needed
                        if "total_premium" in alert and "premium_usd" not in alert:
                            normalized_alert["premium_usd"] = alert["total_premium"]
                        
                        # Map type to contract_type if needed
                        if "type" in alert and "contract_type" not in alert:
                            normalized_alert["contract_type"] = alert["type"]
                        
                        normalized_alerts.append(normalized_alert)
                    
                    logger.info(f"Extracted {len(normalized_alerts)} flow alerts from data field")
                    return normalized_alerts
                # Check for error responses
                elif "error" in response and response["error"]:
                    logger.error(f"Flow alerts API error: {response.get('message', 'Unknown error')}")
                    return []
            elif isinstance(response, list):
                logger.debug(f"Flow alerts response is a list with {len(response)} items")
                # Map new field names to expected field names for compatibility
                normalized_alerts = []
                for alert in response:
                    # Create a normalized alert with both old and new field names
                    normalized_alert = alert.copy()
                    
                    # Map total_premium to premium_usd if needed
                    if "total_premium" in alert and "premium_usd" not in alert:
                        normalized_alert["premium_usd"] = alert["total_premium"]
                    
                    # Map type to contract_type if needed
                    if "type" in alert and "contract_type" not in alert:
                        normalized_alert["contract_type"] = alert["type"]
                    
                    normalized_alerts.append(normalized_alert)
                
                return normalized_alerts
            else:
                logger.error(f"Unexpected flow alerts response type: {type(response)}")
                return []
            
            return response.get("data", []) if isinstance(response, dict) else response
        except Exception as e:
            logger.error(f"Error in get_flow_alerts: {str(e)}")
            return []
    
    @log_exceptions
    @log_api_call("Market Breadth")
    async def get_market_breadth(self) -> Dict:
        """
        Get market breadth data.
        
        Note: This method now uses the market-tide endpoint as the breadth endpoint
        returns a 404 error according to API documentation.
        """
        try:
            # First try with the new recommended endpoint
            endpoint = "/api/market/market-tide"
            logger.info(f"Making GET request to {endpoint} (replacement for market breadth)")
            
            response = await self._make_request(
                method="GET",
                endpoint=endpoint
            )
            
            if response and isinstance(response, dict) and not response.get("error"):
                logger.info("Successfully retrieved market tide data as replacement for market breadth")
                return response
            
            # If that fails, try the old endpoint as a fallback
            logger.warning("Market tide endpoint failed, trying legacy market breadth endpoint as fallback")
            fallback_response = await self._make_request(
                method="GET",
                endpoint="/api/market/breadth"
            )
            
            if fallback_response and isinstance(fallback_response, dict) and not fallback_response.get("error"):
                logger.info("Successfully retrieved market breadth data using legacy endpoint")
                return fallback_response
            
            logger.warning("All attempts to retrieve market breadth/tide data failed")
            return {"error": True, "message": "Could not retrieve market breadth/tide data"}
            
        except Exception as e:
            logger.error(f"Error fetching market breadth/tide data: {str(e)}")
            return {"error": True, "message": str(e)}
    
    @log_exceptions
    @log_api_call("Market Data")
    async def get_market_data(self, ticker: str = None) -> Dict:
        """
        Get market data for a specific ticker or overall market.
        
        Args:
            ticker: Optional ticker symbol to get data for
            
        Returns:
            Market data as a dictionary
        """
        # Try the primary endpoint first
        try:
            if ticker:
                # For specific tickers, try the stock data endpoint
                endpoint = f"/api/stock/{ticker}/option-contracts"
                params = {
                    "ticker": ticker,
                    "limit": 1  # Just get minimal data to check if ticker exists
                }
                
                response = await self._make_request("GET", endpoint, params)
                
                # If successful, extract basic ticker data
                if response and isinstance(response, dict) and not response.get("error"):
                    # Create a simplified market data response
                    return {
                        "success": True,
                        "ticker": ticker,
                        "has_options": True if response.get("data") else False,
                        "data_source": "option_contracts"
                    }
            
            # For market-wide data or fallback
            endpoint = "/api/market/market-tide"
            response = await self._make_request("GET", endpoint)
            
            if response and isinstance(response, dict) and not response.get("error"):
                # For market-wide data, return the full response
                # For ticker-specific, extract relevant info if available
                if ticker and "tickers" in response and ticker in response["tickers"]:
                    return {
                        "success": True,
                        "ticker": ticker,
                        "data": response["tickers"][ticker],
                        "data_source": "market_tide"
                    }
                elif not ticker:
                    return response
            
            # If we reach here, try one more alternative
            if ticker:
                # Try the flow data endpoint which might have ticker info
                flow_endpoint = "/api/option-trades/flow"
                flow_params = {
                    "ticker": ticker,
                    "limit": 5  # Just get a few records to check if ticker exists
                }
                
                flow_response = await self._make_request("GET", flow_endpoint, flow_params)
                
                if flow_response and isinstance(flow_response, dict) and not flow_response.get("error"):
                    if "data" in flow_response and flow_response["data"]:
                        # Extract basic ticker data from flow
                        return {
                            "success": True,
                            "ticker": ticker,
                            "has_flow_data": True,
                            "flow_count": len(flow_response["data"]),
                            "data_source": "option_flow"
                        }
            
            # If all attempts failed
            return {
                "error": True,
                "message": f"Could not retrieve market data for {ticker if ticker else 'market'}",
                "data_source": "none"
            }
            
        except Exception as e:
            logger.error(f"Error in get_market_data for {ticker if ticker else 'market'}: {str(e)}")
            return {
                "error": True,
                "message": f"Exception retrieving market data: {str(e)}",
                "data_source": "error"
            }
    
    @log_exceptions
    @log_api_call("Market Tide")
    async def get_market_tide(self) -> Dict:
        """Get market tide data (replacement for market breadth)."""
        return await self._make_request(
            method="GET",
            endpoint="/api/market/market-tide"
        )
    
    @log_exceptions
    @log_api_call("Dark Pool Recent")
    async def get_darkpool_recent(self) -> Dict:
        """
        Get recent darkpool data.
        
        Returns:
            Dict: Recent darkpool data.
        """
        try:
            endpoint = "/api/darkpool/recent"
            response = await self._make_request(
                method="GET",
                endpoint=endpoint
            )
            
            # Log response summary for debugging
            if isinstance(response, dict) and not response.get("error", False):
                # Handle dictionary response
                if "data" in response and "trades" in response["data"]:
                    trades_count = len(response["data"]["trades"])
                    logger.info(f"Received {trades_count} recent darkpool trades")
                    
                    # Limit the number of trades to prevent overwhelming output
                    if trades_count > 20:
                        logger.debug(f"Truncating darkpool trades from {trades_count} to 20 for display")
                        response["data"]["trades"] = response["data"]["trades"][:20]
            elif isinstance(response, list):
                # Handle list response
                trades_count = len(response)
                logger.info(f"Received {trades_count} recent darkpool trades (list format)")
                
                # Convert list to dictionary format for consistency
                response = {
                    "data": {
                        "trades": response[:20] if trades_count > 20 else response
                    }
                }
                
                # Log the conversion
                if trades_count > 20:
                    logger.debug(f"Truncating darkpool trades from {trades_count} to 20 for display")
            
            # Add API response event for monitoring
            logger.info({
                'event': 'api_response',
                'endpoint': endpoint,
                'status': 200,
                'timestamp': datetime.now().isoformat(),
                'success': True,
            })
            
            return response
        except Exception as e:
            logger.error(f"Error in get_darkpool_recent: {str(e)}")
            return {"error": True, "message": str(e)}
    
    @log_exceptions
    @log_api_call("Dark Pool Ticker")
    async def get_darkpool_for_ticker(self, ticker: str) -> Dict:
        """
        Get darkpool data for a specific ticker.
        
        Args:
            ticker (str): Ticker symbol.
            
        Returns:
            Dict: Darkpool data for the ticker.
        """
        try:
            endpoint = f"/api/darkpool/{ticker}"
            response = await self._make_request(
                method="GET",
                endpoint=endpoint
            )
            
            # Log response summary for debugging
            if isinstance(response, dict) and not response.get("error", False):
                trades_count = len(response.get("trades", []))
                logger.info(f"Received {trades_count} darkpool trades for {ticker}")
                
                # Process the response to make it more manageable
                if "trades" in response:
                    # Limit the number of trades to prevent overwhelming output
                    if len(response["trades"]) > 20:
                        logger.debug(f"Truncating darkpool trades for {ticker} from {len(response['trades'])} to 20 for display")
                        response["trades"] = response["trades"][:20]
            
            return response
        except Exception as e:
            logger.error(f"Error in get_darkpool_ticker for {ticker}: {str(e)}")
            return {"error": True, "message": str(e)}
    
    @log_exceptions
    @log_api_call("Congress Recent Trades")
    async def get_congress_recent_trades(self) -> Dict:
        """Get recent trades by members of Congress."""
        return await self._make_request(
            method="GET",
            endpoint="/api/congress/recent-trades"
        )
    
    @log_exceptions
    @log_api_call("Congress Trader")
    async def get_congress_trader(self, member_name: Optional[str] = None) -> Dict:
        """Get trading activity for a specific Congress member or all members."""
        if member_name:
            # Use member-specific endpoint if name provided
            return await self._make_request(
                method="GET",
                endpoint=f"/api/congress/congress-trader/{member_name}"
            )
        else:
            # Use general Congress trader endpoint
            return await self._make_request(
                method="GET",
                endpoint="/api/congress/congress-trader"
            )
    
    @log_exceptions
    @log_api_call("Insider Transactions")
    async def get_insider_transactions(self) -> Dict:
        """Get recent insider transactions across all companies."""
        return await self._make_request(
            method="GET",
            endpoint="/api/insider/transactions"
        )
    
    @log_exceptions
    @log_api_call("Insider Flow Ticker")
    async def get_insider_flow_for_ticker(self, ticker: str) -> Dict:
        """Get insider trading flow for a specific ticker."""
        try:
            # First try with the documented endpoint from the API memory
            endpoint = f"/api/insider/ticker-flow"
            params = {"ticker": ticker}
            
            logger.info(f"Making GET request to {endpoint} with params: {json.dumps(params, default=str)}")
            
            response = await self._make_request(
                method="GET",
                endpoint=endpoint,
                params=params
            )
            
            if response and isinstance(response, dict) and not response.get("error"):
                logger.info(f"Successfully retrieved insider flow for {ticker} using primary endpoint")
                return response
            
            # If that fails, try the alternative endpoint format
            alt_endpoint = f"/api/insider/transactions"
            alt_params = {"ticker": ticker}
            
            logger.info(f"Trying alternative endpoint: Making GET request to {alt_endpoint} with params: {json.dumps(alt_params, default=str)}")
            
            alt_response = await self._make_request(
                method="GET",
                endpoint=alt_endpoint,
                params=alt_params
            )
            
            if alt_response and isinstance(alt_response, dict) and not alt_response.get("error"):
                logger.info(f"Successfully retrieved insider flow for {ticker} using alternative endpoint")
                return alt_response
            
            logger.warning(f"All attempts to retrieve insider flow for {ticker} failed")
            return {"error": True, "message": f"Could not retrieve insider flow for {ticker}"}
            
        except Exception as e:
            logger.error(f"Error fetching insider flow for {ticker}: {str(e)}")
            return {"error": True, "message": str(e)}
    
    @log_exceptions
    @log_api_call("Options Chain")
    async def get_options_chains(self, ticker: str) -> Dict:
        """
        Get options chains for a specific ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dict: Options chains data
        """
        try:
            # Try the primary endpoint first with proper parameters
            # API expects string values for boolean parameters, not actual booleans
            params = {
                "exclude_zero_vol_chains": "true",  # API expects string "true" not boolean True
                "limit": 500,  # Maximum results
                "ticker": ticker  # Explicitly include ticker in params
            }
            
            logger.info(f"Making GET request to /api/stock/{ticker}/option-contracts with params: {json.dumps(params, default=str)}")
            
            response = await self._make_request(
                method="GET",
                endpoint=f"/api/stock/{ticker}/option-contracts",
                params=params
            )
            
            # Check if we got a valid response
            if response and not response.get("error", False):
                return response
            
            # If primary endpoint fails, try alternative endpoint
            logger.warning(f"Primary options chain endpoint failed for {ticker}, trying alternative")
            alt_response = await self._make_request(
                method="GET",
                endpoint=f"/api/options/chains/{ticker}"
            )
            
            if alt_response and not alt_response.get("error", False):
                return alt_response
            
            # If both fail, try minimal endpoint
            logger.warning(f"Alternative options chain endpoint failed for {ticker}, trying minimal endpoint")
            minimal_response = await self._make_request(
                method="GET",
                endpoint=f"/api/stock/{ticker}/options"
            )
            
            return minimal_response
        except Exception as e:
            logger.error(f"Error in get_options_chains for {ticker}: {str(e)}")
            return {"error": True, "message": str(e)}

    @log_exceptions
    @log_api_call("Flow Per Strike")
    async def get_flow_per_strike(self, ticker: str) -> Dict:
        """Get options flow aggregated by strike price."""
        return await self._make_request(
            method="GET",
            endpoint=f"/api/stock/{ticker}/flow-per-strike"
        )
    
    @log_exceptions
    @log_api_call("Greek Flow")
    async def get_greek_flow(self) -> Dict:
        """Get Greek flow analysis."""
        return await self._make_request(
            method="GET",
            endpoint="/api/greek-flow"
        )
    
    @log_exceptions
    @log_api_call("Greek Flow Expiry")
    async def get_greek_flow_expiry(self) -> Dict:
        """Get Greek flow analysis by expiry."""
        return await self._make_request(
            method="GET",
            endpoint="/api/greek-flow-expiry"
        )
    
    @log_exceptions
    @log_api_call("Total Options Volume")
    async def get_total_options_volume(self) -> Dict:
        """Get total options volume data."""
        return await self._make_request(
            method="GET",
            endpoint="/api/market/total-options-volume"
        )
    
    @log_exceptions
    @log_api_call("Sector ETFs")
    async def get_sector_etfs(self) -> Dict:
        """Get sector ETF data."""
        return await self._make_request(
            method="GET",
            endpoint="/api/market/sector-etfs"
        )

    @log_exceptions
    @log_api_call("Institutional Activity")
    async def get_institutional_activity(self, institution_name: str) -> Dict:
        """Get activity for a specific institution."""
        return await self._make_request(
            method="GET",
            endpoint=f"/api/institution/{institution_name}/activity"
        )
    
    @log_exceptions
    @log_api_call("Institutional Ownership")
    async def get_institutional_ownership(self, ticker: str) -> Dict:
        """Get institutional ownership for a specific ticker."""
        return await self._make_request(
            method="GET",
            endpoint=f"/api/institution/{ticker}/ownership"
        )

    @log_exceptions
    @log_api_call("Congress Trading")
    async def get_congress_trading(self, ticker: str = None, lookback_days: int = 30) -> List[Dict]:
        """
        Get trading activity by members of Congress for a specific ticker.
        
        Args:
            ticker: Optional ticker symbol to filter by
            lookback_days: Number of days to look back for trades
            
        Returns:
            List[Dict]: Congress trading data
        """
        try:
            # First try to get data from the congress-trader endpoint
            endpoint = "/api/congress/congress-trader"
            params = {}
            
            if ticker:
                params["ticker"] = ticker
                
            if lookback_days:
                # Calculate the date range
                end_date = datetime.now()
                start_date = end_date - timedelta(days=lookback_days)
                params["start_date"] = start_date.strftime("%Y-%m-%d")
                params["end_date"] = end_date.strftime("%Y-%m-%d")
            
            logger.info(f"Making GET request to {endpoint} with params: {json.dumps(params, default=str)}")
            
            response = await self._make_request(
                method="GET",
                endpoint=endpoint,
                params=params
            )
            
            # Check if we got a valid response
            if response and isinstance(response, dict) and not response.get("error", False):
                if "data" in response:
                    trades = response["data"]
                    logger.info(f"Retrieved {len(trades)} congress trades for {ticker if ticker else 'all tickers'}")
                    return trades
                else:
                    logger.warning(f"Congress trading data missing 'data' field for {ticker if ticker else 'all tickers'}")
            
            # If that fails, try the recent-trades endpoint
            alt_endpoint = "/api/congress/recent-trades"
            logger.info(f"Trying alternative endpoint: Making GET request to {alt_endpoint}")
            
            alt_response = await self._make_request(
                method="GET",
                endpoint=alt_endpoint
            )
            
            if alt_response and isinstance(alt_response, dict) and not alt_response.get("error", False):
                # Filter by ticker if specified
                if ticker and "data" in alt_response:
                    filtered_trades = [
                        trade for trade in alt_response["data"]
                        if trade.get("ticker") == ticker
                    ]
                    logger.info(f"Filtered {len(filtered_trades)} congress trades for {ticker}")
                    return filtered_trades
                elif "data" in alt_response:
                    logger.info(f"Retrieved {len(alt_response['data'])} congress trades from alternative endpoint")
                    return alt_response["data"]
            
            logger.warning(f"All attempts to retrieve congress trading data failed")
            return []
            
        except Exception as e:
            logger.error(f"Error in get_congress_trading: {str(e)}")
            return []

    @log_exceptions
    @log_api_call("Insider Trading")
    async def get_insider_trading(self, ticker: str = None, lookback_days: int = 30) -> List[Dict]:
        """
        Get insider trading data for a specific ticker.
        
        Args:
            ticker: Optional ticker symbol to filter by
            lookback_days: Number of days to look back for trades
            
        Returns:
            List[Dict]: Insider trading data
        """
        try:
            # First try the ticker-specific endpoint if ticker is provided
            if ticker:
                endpoint = f"/api/ticker/{ticker}/insider-flow"
                logger.info(f"Making GET request to {endpoint}")
                
                response = await self._make_request(
                    method="GET",
                    endpoint=endpoint
                )
                
                if response and isinstance(response, dict) and not response.get("error"):
                    if "data" in response:
                        trades = response["data"]
                        logger.info(f"Retrieved {len(trades)} insider trades for {ticker}")
                        return trades
                    else:
                        logger.warning(f"Insider trading data missing 'data' field for {ticker}")
                
                # Try alternative ticker-specific endpoint
                alt_endpoint = f"/api/insider/{ticker}/ticker-flow"
                logger.info(f"Trying alternative endpoint: Making GET request to {alt_endpoint}")
                
                alt_response = await self._make_request(
                    method="GET",
                    endpoint=alt_endpoint
                )
                
                if alt_response and isinstance(alt_response, dict) and not alt_response.get("error"):
                    if "data" in alt_response:
                        trades = alt_response["data"]
                        logger.info(f"Retrieved {len(trades)} insider trades for {ticker} from alternative endpoint")
                        return trades
            
            # Try the transactions endpoint with optional ticker filter
            endpoint = "/api/insider/transactions"
            params = {}
            
            if ticker:
                params["ticker"] = ticker
                
            if lookback_days:
                # Calculate the date range
                end_date = datetime.now()
                start_date = end_date - timedelta(days=lookback_days)
                params["start_date"] = start_date.strftime("%Y-%m-%d")
                params["end_date"] = end_date.strftime("%Y-%m-%d")
            
            logger.info(f"Making GET request to {endpoint} with params: {json.dumps(params, default=str)}")
            
            response = await self._make_request(
                method="GET",
                endpoint=endpoint,
                params=params
            )
            
            # Check if we got a valid response
            if response and isinstance(response, dict) and not response.get("error", False):
                if "data" in response:
                    trades = response["data"]
                    logger.info(f"Retrieved {len(trades)} insider trades for {ticker if ticker else 'all tickers'}")
                    return trades
                else:
                    logger.warning(f"Insider trading data missing 'data' field")
            
            logger.warning(f"All attempts to retrieve insider trading data failed")
            return []
            
        except Exception as e:
            logger.error(f"Error in get_insider_trading: {str(e)}")
            return []

    def _validate_options_data(self, options_data: List[Dict]) -> List[Dict]:
        """Validate and normalize options data format."""
        if not isinstance(options_data, list):
            logger.warning("Options data is not a list, returning empty list")
            return []
        
        validated = []
        for opt in options_data:
            try:
                if not isinstance(opt, dict):
                    logger.warning(f"Skipping invalid options data format: {type(opt)}")
                    continue
                    
                # Ensure required fields with proper types
                required_fields = {
                    'type': str(opt.get('type', '')).upper(),
                    'strike': float(opt.get('strike', 0)),
                    'expiration': opt.get('expiration', ''),
                    'volume': int(opt.get('volume', 0)),
                    'open_interest': int(opt.get('open_interest', 0)),
                    'implied_volatility': float(opt.get('implied_volatility', 0))
                }
                
                # Additional validation
                if not required_fields['type'] in ['CALL', 'PUT']:
                    logger.warning(f"Invalid option type: {required_fields['type']}")
                    continue
                    
                if required_fields['strike'] <= 0:
                    logger.warning(f"Invalid strike price: {required_fields['strike']}")
                    continue
                    
                try:
                    # Validate expiration date format
                    datetime.strptime(required_fields['expiration'], '%Y-%m-%d')
                except ValueError:
                    logger.warning(f"Invalid expiration date format: {required_fields['expiration']}")
                    continue
                    
                if required_fields['volume'] < 0:
                    logger.warning(f"Invalid volume: {required_fields['volume']}")
                    continue
                    
                if required_fields['open_interest'] < 0:
                    logger.warning(f"Invalid open interest: {required_fields['open_interest']}")
                    continue
                    
                if not (0 <= required_fields['implied_volatility'] <= 500):  # 500% as a reasonable max
                    logger.warning(f"Invalid implied volatility: {required_fields['implied_volatility']}")
                    continue
                
                # Add computed fields
                required_fields['vol_oi_ratio'] = (
                    round(required_fields['volume'] / required_fields['open_interest'], 2)
                    if required_fields['open_interest'] > 0 else 0
                )
                
                # Merge original data with validated fields
                validated_contract = {**opt, **required_fields}
                validated.append(validated_contract)
                
            except Exception as e:
                logger.error(f"Error validating options contract: {str(e)}")
                continue
        
        if not validated:
            logger.warning("No valid options contracts found after validation")
        else:
            logger.info(f"Validated {len(validated)} options contracts")
            
        return validated

    @log_exceptions
    @log_api_call("Greeks Data")
    async def get_stock_greeks(self, ticker: str) -> Dict:
        """Get Greeks data for a stock."""
        endpoint = f"/api/stock/{ticker}/greeks"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("Spot Exposures")
    async def get_spot_exposures(self, ticker: str) -> Dict:
        """Get spot exposures by expiry and strike for a stock."""
        endpoint = f"/api/stock/{ticker}/spot-exposures/expiry-strike"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("NOPE Data")
    async def get_nope_data(self, ticker: str) -> Dict:
        """Get NOPE (Net Options Pricing Effect) data for a stock."""
        endpoint = f"/api/stock/{ticker}/nope"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("Short Interest")
    async def get_short_interest(self, ticker: str) -> Dict:
        """Get short interest data for a stock."""
        endpoint = f"/api/shorts/{ticker}/data"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("Institutional Holdings")
    async def get_institutional_holdings(self, ticker: str) -> Dict:
        """Get institutional holdings data for a stock."""
        endpoint = f"/api/institution/{ticker}/holdings"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("Insider Trading Activity")
    async def get_insider_activity(self, ticker: str) -> Dict:
        """Get insider buy/sell activity for a stock."""
        endpoint = f"/api/stock/{ticker}/insider-buy-sells"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("News Headlines")
    async def get_news_headlines(self) -> Dict:
        """Get latest news headlines."""
        endpoint = "/api/news/headlines"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("Realized Volatility")
    async def get_realized_volatility(self, ticker: str) -> Dict:
        """Get realized volatility data for a stock."""
        endpoint = f"/api/stock/{ticker}/volatility/realized"
        return await self._make_request("GET", endpoint)

    @log_exceptions
    @log_api_call("OHLC Data")
    async def get_ohlc_data(self, ticker: str, candle_size: str = "1m") -> Dict:
        """
        Get OHLC (Open, High, Low, Close) data for a stock.
        
        Args:
            ticker: Stock symbol
            candle_size: Candle size (e.g., "1m", "5m", "1h", "1d")
        """
        endpoint = f"/api/stock/{ticker}/ohlc/{candle_size}"
        return await self._make_request("GET", endpoint)
