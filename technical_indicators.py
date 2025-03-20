import numpy as np
from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
from src.logger_config import logger, log_exceptions

class TechnicalIndicatorCalculator:
    """Calculate technical indicators for trading signals."""
    
    def __init__(self, cache_duration: int = 300):  # 5 minutes default cache duration
        """
        Initialize the Technical Indicator Calculator.
        
        Args:
            cache_duration: Duration in seconds to cache calculations
        """
        self.cache = {}
        self.cache_duration = cache_duration
        self.cache_timestamps = {}
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached value is still valid."""
        if key not in self.cache_timestamps:
            return False
        return (datetime.now() - self.cache_timestamps[key]).total_seconds() < self.cache_duration
    
    def _get_cached(self, key: str) -> Optional[Union[float, Dict]]:
        """Get cached value if valid."""
        if self._is_cache_valid(key):
            return self.cache.get(key)
        return None
    
    def _set_cache(self, key: str, value: Union[float, Dict]):
        """Set cache value with timestamp."""
        self.cache[key] = value
        self.cache_timestamps[key] = datetime.now()
    
    @log_exceptions
    def calculate_vwap(self, ohlcv_data: List[Dict]) -> float:
        """
        Calculate Volume Weighted Average Price (VWAP).
        
        Args:
            ohlcv_data: List of dictionaries containing OHLCV data
                Each dict should have: 'high', 'low', 'close', 'volume'
        
        Returns:
            VWAP value
        """
        cache_key = f"vwap_{hash(str(ohlcv_data))}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        try:
            typical_prices = [(d['high'] + d['low'] + d['close']) / 3 for d in ohlcv_data]
            volumes = [d['volume'] for d in ohlcv_data]
            
            vwap = sum(tp * vol for tp, vol in zip(typical_prices, volumes)) / sum(volumes)
            
            self._set_cache(cache_key, vwap)
            return vwap
            
        except Exception as e:
            logger.error(f"Error calculating VWAP: {str(e)}")
            return 0.0
    
    @log_exceptions
    def calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """
        Calculate Relative Strength Index (RSI).
        
        Args:
            prices: List of closing prices
            period: RSI period (default 14)
            
        Returns:
            RSI value
        """
        cache_key = f"rsi_{hash(str(prices))}_{period}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        try:
            if len(prices) < period + 1:
                return 50.0  # Default neutral value if not enough data
                
            deltas = np.diff(prices)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            avg_gain = np.mean(gains[:period])
            avg_loss = np.mean(losses[:period])
            
            for i in range(period, len(deltas)):
                avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            
            self._set_cache(cache_key, rsi)
            return rsi
            
        except Exception as e:
            logger.error(f"Error calculating RSI: {str(e)}")
            return 50.0
    
    @log_exceptions
    def calculate_macd(self, prices: List[float], 
                      fast_period: int = 12, 
                      slow_period: int = 26, 
                      signal_period: int = 9) -> Dict[str, float]:
        """
        Calculate Moving Average Convergence Divergence (MACD).
        
        Args:
            prices: List of closing prices
            fast_period: Fast EMA period (default 12)
            slow_period: Slow EMA period (default 26)
            signal_period: Signal line period (default 9)
            
        Returns:
            Dictionary containing MACD line, signal line, and histogram values
        """
        cache_key = f"macd_{hash(str(prices))}_{fast_period}_{slow_period}_{signal_period}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        try:
            # Convert to pandas Series for easier calculation
            close_prices = pd.Series(prices)
            
            # Calculate EMAs
            fast_ema = close_prices.ewm(span=fast_period, adjust=False).mean()
            slow_ema = close_prices.ewm(span=slow_period, adjust=False).mean()
            
            # Calculate MACD line
            macd_line = fast_ema - slow_ema
            
            # Calculate signal line
            signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
            
            # Calculate histogram
            histogram = macd_line - signal_line
            
            result = {
                'macd_line': float(macd_line.iloc[-1]),
                'signal_line': float(signal_line.iloc[-1]),
                'histogram': float(histogram.iloc[-1])
            }
            
            self._set_cache(cache_key, result)
            return result
            
        except Exception as e:
            logger.error(f"Error calculating MACD: {str(e)}")
            return {'macd_line': 0.0, 'signal_line': 0.0, 'histogram': 0.0}
    
    def clear_cache(self):
        """Clear all cached calculations."""
        self.cache.clear()
        self.cache_timestamps.clear()
        logger.info("Technical indicator cache cleared") 