import os
import json
import aiohttp
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger

# Import the configured logger and decorators
from src.logger_config import logger, log_exceptions, log_api_call, log_trading_activity

class TastyTrader:
    def __init__(self, username: str, password: str, account_id: str):
        """Initialize TastyTrade client for live trading."""
        self.username = username
        self.password = password
        self.account_id = account_id
        self.base_url = "https://api.tastytrade.com"  # Updated to live trading endpoint
        self.session_token = None
        self.session = None
        self.logger = logger
        
        if not all([self.username, self.password, self.account_id]):
            raise ValueError("TastyTrade credentials not found")
    
    @log_exceptions
    async def initialize(self):
        """Initialize the TastyTrade session and authenticate."""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            # Authenticate with TastyTrade
            success = await self.authenticate()
            if not success:
                self.logger.error("Failed to authenticate with TastyTrade")
                return False
                
            self.logger.info("TastyTrade client initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing TastyTrade client: {str(e)}")
            return False
    
    @log_exceptions
    async def authenticate(self):
        """Authenticate with TastyTrade and obtain session token."""
        try:
            login_data = {
                "login": self.username,
                "password": self.password
            }
            
            async with self.session.post(f"{self.base_url}/sessions", json=login_data) as response:
                if response.status != 201:
                    error_text = await response.text()
                    self.logger.error(f"TastyTrade authentication failed with status {response.status}: {error_text}")
                    return False
                
                data = await response.json()
                self.session_token = data.get("data", {}).get("session-token")
                
                if not self.session_token:
                    self.logger.error("No session token in TastyTrade response")
                    return False
                
                # Update session headers with token
                self.session.headers.update({
                    "Authorization": self.session_token
                })
                
                self.logger.info("Successfully authenticated with TastyTrade")
                return True
                
        except Exception as e:
            self.logger.error(f"Error during TastyTrade authentication: {str(e)}")
            return False
    
    @log_exceptions
    async def close_session(self):
        """Close the TastyTrade session."""
        try:
            if self.session:
                await self.session.close()
                self.session = None
                self.logger.info("TastyTrade session closed")
        except Exception as e:
            self.logger.error(f"Error closing TastyTrade session: {str(e)}")

    @log_exceptions
    async def close(self):
        """Close the TastyTrader client and clean up resources."""
        try:
            # Close the session
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            
            # Clear session token
            self.session_token = None
            
            logger.info("TastyTrader client closed successfully")
            return True
        except Exception as e:
            logger.error(f"Error closing TastyTrader client: {str(e)}")
            return False

    @log_exceptions
    @log_api_call("TastyTrade Account Balance")
    async def get_account_balance(self) -> Dict:
        """Get current account balance and positions."""
        try:
            async with self.session.get(f"{self.base_url}/accounts/{self.account_id}/balances") as response:
                if response.status != 200:
                    raise Exception(f"Failed to get account balance: {response.status}")
                return await response.json()
        except Exception as e:
            self.logger.error(f"Error getting account balance: {str(e)}")
            return {}

    @log_exceptions
    @log_api_call("TastyTrade Positions")
    async def get_positions(self) -> List[Dict]:
        """Get current positions."""
        try:
            async with self.session.get(f"{self.base_url}/accounts/{self.account_id}/positions") as response:
                if response.status != 200:
                    raise Exception(f"Failed to get positions: {response.status}")
                return await response.json()
        except Exception as e:
            self.logger.error(f"Error getting positions: {str(e)}")
            return []

    @log_exceptions
    @log_trading_activity("Place Order")
    async def place_order(self, order_data: Dict) -> Dict:
        """Place a live trading order."""
        try:
            # Validate order data
            required_fields = ['symbol', 'quantity', 'order_type', 'side']
            if not all(field in order_data for field in required_fields):
                raise ValueError("Missing required order fields")

            # Add account ID to order
            order_data['account_id'] = self.account_id
            
            # Place the order
            async with self.session.post(f"{self.base_url}/accounts/{self.account_id}/orders", json=order_data) as response:
                if response.status not in [200, 201]:
                    raise Exception(f"Order placement failed: {response.status}")
                
                order_response = await response.json()
                self.logger.info(f"Order placed successfully: {order_response['data']['order-id']}")
                return order_response
                
        except Exception as e:
            self.logger.error(f"Error placing order: {str(e)}")
            return {'error': str(e)}

    @log_exceptions
    @log_api_call("TastyTrade Option Chain")
    async def get_option_chain(self, symbol: str) -> Dict:
        """Get option chain data for a symbol."""
        try:
            async with self.session.get(f"{self.base_url}/option-chains/{symbol}") as response:
                if response.status != 200:
                    raise Exception(f"Failed to get option chain: {response.status}")
                return await response.json()
        except Exception as e:
            self.logger.error(f"Error getting option chain: {str(e)}")
            return {}

    @log_exceptions
    @log_api_call("TastyTrade Order Status")
    async def get_order_status(self, order_id: str) -> Dict:
        """Get the status of a specific order."""
        try:
            async with self.session.get(f"{self.base_url}/accounts/{self.account_id}/orders/{order_id}") as response:
                if response.status != 200:
                    raise Exception(f"Failed to get order status: {response.status}")
                return await response.json()
        except Exception as e:
            self.logger.error(f"Error getting order status: {str(e)}")
            return {}

    @log_exceptions
    @log_trading_activity("Cancel Order")
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order."""
        try:
            async with self.session.delete(f"{self.base_url}/accounts/{self.account_id}/orders/{order_id}") as response:
                success = response.status in [200, 202]
                if success:
                    self.logger.info(f"Order {order_id} cancelled successfully")
                else:
                    self.logger.error(f"Failed to cancel order {order_id}: {response.status}")
                return success
        except Exception as e:
            self.logger.error(f"Error cancelling order: {str(e)}")
            return False
