"""
Rate limiter implementation for API clients.

This module provides a configurable rate limiter to prevent
exceeding API rate limits by tracking request timestamps.
"""

import time
from typing import Dict, List, Optional
from loguru import logger
import asyncio

class RateLimiter:
    """
    Rate limiter for API endpoints.
    
    Tracks request timestamps and limits requests based on
    configured maximums per time window.
    """
    
    def __init__(
        self,
        global_rate_limit: int = 100,  # Total requests per window
        endpoint_rate_limit: int = 30,  # Requests per endpoint per window
        time_window: int = 60,         # Window in seconds
        min_request_spacing: float = 0.05  # Minimum time between requests in seconds
    ):
        """
        Initialize rate limiter.
        
        Args:
            global_rate_limit: Maximum requests across all endpoints in window
            endpoint_rate_limit: Maximum requests per endpoint in window
            time_window: Time window in seconds
            min_request_spacing: Minimum time between any two requests
        """
        self.global_rate_limit = global_rate_limit
        self.endpoint_rate_limit = endpoint_rate_limit
        self.time_window = time_window
        self.min_request_spacing = min_request_spacing
        
        # Timestamp tracking
        self.global_timestamps: List[float] = []
        self.endpoint_timestamps: Dict[str, List[float]] = {}
        self.last_request_time: float = 0
    
    def allow_request(self, endpoint: str) -> bool:
        """
        Check if a request is allowed based on rate limits.
        
        Args:
            endpoint: API endpoint to check
            
        Returns:
            True if request is allowed, False otherwise
        """
        current_time = time.time()
        window_start = current_time - self.time_window
        
        # Clean up old timestamps
        self._clean_old_timestamps(window_start)
        
        # Check global rate limit
        if len(self.global_timestamps) >= self.global_rate_limit:
            logger.warning(f"Global rate limit reached: {len(self.global_timestamps)}/{self.global_rate_limit}")
            return False
        
        # Check endpoint rate limit
        if endpoint in self.endpoint_timestamps:
            if len(self.endpoint_timestamps[endpoint]) >= self.endpoint_rate_limit:
                logger.warning(f"Endpoint rate limit reached for {endpoint}: "
                              f"{len(self.endpoint_timestamps[endpoint])}/{self.endpoint_rate_limit}")
                return False
        
        # Check request spacing
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_request_spacing:
            time.sleep(self.min_request_spacing - elapsed)
        
        return True
    
    def record_request(self, endpoint: str) -> None:
        """
        Record a request for rate limiting purposes.
        
        Args:
            endpoint: API endpoint that was requested
        """
        current_time = time.time()
        
        # Record global timestamp
        self.global_timestamps.append(current_time)
        
        # Record endpoint timestamp
        if endpoint not in self.endpoint_timestamps:
            self.endpoint_timestamps[endpoint] = []
        self.endpoint_timestamps[endpoint].append(current_time)
        
        # Update last request time
        self.last_request_time = current_time
    
    def get_wait_time(self, endpoint: str) -> float:
        """
        Calculate wait time needed before a request can be made.
        
        Args:
            endpoint: API endpoint to check
            
        Returns:
            Time in seconds to wait (0 if no wait needed)
        """
        current_time = time.time()
        window_start = current_time - self.time_window
        
        # Clean up old timestamps
        self._clean_old_timestamps(window_start)
        
        # Check global rate limit
        if len(self.global_timestamps) >= self.global_rate_limit:
            # Find the oldest timestamp that will expire
            oldest = sorted(self.global_timestamps)[0]
            return oldest + self.time_window - current_time
        
        # Check endpoint rate limit
        if endpoint in self.endpoint_timestamps:
            if len(self.endpoint_timestamps[endpoint]) >= self.endpoint_rate_limit:
                # Find the oldest timestamp that will expire
                oldest = sorted(self.endpoint_timestamps[endpoint])[0]
                return oldest + self.time_window - current_time
        
        # Check request spacing
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_request_spacing:
            return self.min_request_spacing - elapsed
        
        return 0
    
    async def acquire(self, endpoint: str) -> None:
        """
        Asynchronously acquire permission to make a request, waiting if necessary.
        
        Args:
            endpoint: API endpoint to acquire permission for
        """
        import asyncio
        
        # Check if we can make the request immediately
        if self.allow_request(endpoint):
            self.record_request(endpoint)
            return
            
        # Calculate wait time
        wait_time = self.get_wait_time(endpoint)
        
        if wait_time > 0:
            logger.info(f"Rate limiting active, waiting {wait_time:.2f}s before request to {endpoint}")
            await asyncio.sleep(wait_time + 0.1)  # Add a small buffer
            
            # Record the request after waiting
            self.record_request(endpoint)
        else:
            # If no wait time but allow_request returned False, there might be an issue
            # Record the request anyway but log a warning
            logger.warning(f"Rate limiter inconsistency for {endpoint}, proceeding with caution")
            self.record_request(endpoint)
    
    def _clean_old_timestamps(self, window_start: float) -> None:
        """
        Clean up timestamps older than the window.
        
        Args:
            window_start: Start of the current time window
        """
        # Clean global timestamps
        self.global_timestamps = [ts for ts in self.global_timestamps if ts >= window_start]
        
        # Clean endpoint timestamps
        for endpoint in list(self.endpoint_timestamps.keys()):
            self.endpoint_timestamps[endpoint] = [
                ts for ts in self.endpoint_timestamps[endpoint] if ts >= window_start
            ]
