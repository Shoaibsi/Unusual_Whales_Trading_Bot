"""
Shared data models for the Unusual Whales Trading Bot.
This module contains data classes and models used across multiple components
to prevent circular imports.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any

class SignalType(str, Enum):
    """Types of trading signals."""
    BUY = "BUY"
    SELL = "SELL"
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    STRONG_BUY = "STRONG_BUY"
    STRONG_SELL = "STRONG_SELL"
    NEUTRAL = "NEUTRAL"
    OPTIONS_FLOW = "OPTIONS_FLOW"
    DARK_POOL = "DARK_POOL"
    UNKNOWN = "UNKNOWN"  # For cases where signal type cannot be determined

class SignalStrength(str, Enum):
    """Strength categories for trading signals."""
    WEAK = "WEAK"
    MODERATE = "MODERATE"
    STRONG = "STRONG"
    VERY_STRONG = "VERY_STRONG"

@dataclass
class TradingSignal:
    """
    Trading signal generated from market data analysis.
    Contains essential information for trade execution.
    """
    # Basic signal information
    ticker: str
    signal_type: SignalType
    strength: SignalStrength = SignalStrength.MODERATE
    confidence: float = 0.0
    timestamp: Optional[datetime] = None
    expiry: Optional[datetime] = None
    
    # Direction information
    suggested_direction: str = "neutral"  # "long", "short", or "neutral"
    
    # Price information
    price: Optional[float] = None
    strike_price: Optional[float] = None
    underlying_price: Optional[float] = None
    
    # Risk metrics
    risk_score: float = 0.5  # 0.0 to 1.0, higher means more risky
    expected_return: Optional[float] = None
    max_loss: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    
    # Options specific data
    is_options: bool = False
    options_type: Optional[str] = None
    options_data: Optional[List[Dict]] = field(default_factory=list)
    
    # Alert data from Unusual Whales
    alert_data: Optional[List[Dict]] = field(default_factory=list)
    
    # Dark pool specific data
    primary_data: Dict[str, Any] = field(default_factory=dict)
    
    # Source and analysis information
    source: str = "UNKNOWN"
    strategy_name: Optional[str] = None
    analysis: Optional[str] = None
    reasoning: Optional[str] = None
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        """String representation of the trading signal."""
        signal_str = f"{self.ticker} - {self.signal_type.value} ({self.strength.value})"
        if self.suggested_direction:
            signal_str += f" - Direction: {self.suggested_direction.upper()}"
        if self.is_options and self.options_type and self.strike_price:
            signal_str += f" - {self.options_type} ${self.strike_price}"
        if self.expiry:
            signal_str += f" - Expiry: {self.expiry.strftime('%Y-%m-%d')}"
        if self.confidence:
            signal_str += f" - Confidence: {self.confidence:.2f}"
        return signal_str
