"""
Circuit breaker implementation for API clients.

This module implements the circuit breaker pattern to prevent
repeated calls to failing endpoints and to handle rate limiting gracefully.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List
from loguru import logger

class CircuitBreaker:
    """
    Circuit breaker for API endpoints.
    
    Tracks failures and successes for API endpoints and
    prevents calls to failing endpoints for a period.
    """
    
    def __init__(
        self,
        failure_threshold: int = 3,
        reset_timeout: int = 60,
        rate_limit_window: int = 60,
        rate_limit_max_calls: int = 110  # Slightly below Unusual Whales limit of 120
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            reset_timeout: Seconds before attempting to close circuit
            rate_limit_window: Time window in seconds for rate limiting
            rate_limit_max_calls: Maximum calls allowed in the window
        """
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.rate_limit_window = rate_limit_window
        self.rate_limit_max_calls = rate_limit_max_calls
        
        # Track failures and status for each endpoint
        self.failures: Dict[str, int] = {}
        self.circuit_open_until: Dict[str, float] = {}
        
        # Rate limiting tracking - global and per endpoint
        self.global_request_timestamps: List[float] = []
        self.request_timestamps: Dict[str, List[float]] = {}
        
        # Last request time to ensure minimum spacing between requests
        self.last_request_time = 0
        
    def is_open(self, endpoint: str) -> bool:
        """
        Check if circuit is open for endpoint.
        
        Args:
            endpoint: API endpoint to check
            
        Returns:
            True if circuit is open (calls should be prevented)
        """
        # Check if we have a timeout set and it's still active
        if endpoint in self.circuit_open_until:
            if time.time() < self.circuit_open_until[endpoint]:
                logger.warning(f"Circuit open for {endpoint} until {datetime.fromtimestamp(self.circuit_open_until[endpoint])}")
                return True
            else:
                # Reset if the timeout has passed
                logger.info(f"Circuit reset attempt for {endpoint}")
                self.circuit_open_until.pop(endpoint)
                self.failures[endpoint] = 0
                
        return False
        
    def record_success(self, endpoint: str) -> None:
        """
        Record successful call to endpoint.
        
        Args:
            endpoint: API endpoint that succeeded
        """
        # Reset failure count on success
        self.failures[endpoint] = 0
        
    def record_failure(self, endpoint: str, status_code: Optional[int] = None) -> bool:
        """
        Record failed call to endpoint.
        
        Args:
            endpoint: API endpoint that failed
            status_code: HTTP status code if available
            
        Returns:
            True if circuit was opened
        """
        # Special handling for rate limit failures
        if status_code == 429:
            logger.warning(f"Rate limit hit for {endpoint} - opening circuit immediately")
            self._open_circuit(endpoint, timeout_multiplier=5.0)  # Longer timeout for rate limits
            return True
            
        # Increment failure count
        self.failures[endpoint] = self.failures.get(endpoint, 0) + 1
        
        # Check if we've hit the threshold
        if self.failures[endpoint] >= self.failure_threshold:
            logger.warning(f"Failure threshold reached for {endpoint} - opening circuit")
            self._open_circuit(endpoint)
            return True
            
        return False
        
    def _open_circuit(self, endpoint: str, timeout_multiplier: float = 1.0) -> None:
        """
        Open circuit for endpoint.
        
        Args:
            endpoint: API endpoint to open circuit for
            timeout_multiplier: Multiplier for reset timeout
        """
        self.circuit_open_until[endpoint] = time.time() + (self.reset_timeout * timeout_multiplier)
        
    def check_rate_limit(self, endpoint: str = None) -> bool:
        """
        Check if request should be allowed based on rate limiting.
        
        Args:
            endpoint: API endpoint to check, or None for global check
            
        Returns:
            True if request is allowed, False if it should be rate limited
        """
        # First check global rate limit
        current_time = time.time()
        window_start = current_time - self.rate_limit_window
        
        # Clean up old timestamps
        self.global_request_timestamps = [
            ts for ts in self.global_request_timestamps if ts > window_start
        ]
        
        # Check if we're over the global limit
        if len(self.global_request_timestamps) >= self.rate_limit_max_calls:
            logger.warning(f"Global rate limit would be exceeded - delaying request")
            return False
        
        # Check endpoint-specific rate limit if provided
        if endpoint:
            # Initialize timestamps list if needed
            if endpoint not in self.request_timestamps:
                self.request_timestamps[endpoint] = []
                
            # Remove timestamps outside the window
            self.request_timestamps[endpoint] = [
                ts for ts in self.request_timestamps[endpoint] if ts > window_start
            ]
            
            # Check if this specific endpoint has too many recent calls
            endpoint_limit = min(20, self.rate_limit_max_calls // 5)  # Limit per endpoint
            if len(self.request_timestamps[endpoint]) >= endpoint_limit:
                logger.warning(f"Endpoint rate limit would be exceeded for {endpoint}")
                return False
                
            # Add current timestamp to endpoint tracking
            self.request_timestamps[endpoint].append(current_time)
            
        # Add to global tracking and allow request
        self.global_request_timestamps.append(current_time)
        self.last_request_time = current_time
        return True
        
    def get_wait_time(self, endpoint: str = None) -> float:
        """
        Get time to wait before making the next request to stay within rate limits.
        
        Args:
            endpoint: API endpoint to check, or None for global check
            
        Returns:
            Time to wait in seconds (0 if no wait needed)
        """
        current_time = time.time()
        wait_time = 0.0
        
        # First check if we need to wait for global rate limit
        if len(self.global_request_timestamps) >= self.rate_limit_max_calls:
            oldest_timestamp = min(self.global_request_timestamps)
            global_wait = max(0, oldest_timestamp + self.rate_limit_window - current_time)
            wait_time = max(wait_time, global_wait)
        
        # Then check endpoint-specific rate limit if provided
        if endpoint and endpoint in self.request_timestamps:
            endpoint_limit = min(20, self.rate_limit_max_calls // 5)
            if len(self.request_timestamps[endpoint]) >= endpoint_limit:
                oldest_endpoint_timestamp = min(self.request_timestamps[endpoint])
                endpoint_wait = max(0, oldest_endpoint_timestamp + self.rate_limit_window - current_time)
                wait_time = max(wait_time, endpoint_wait)
        
        # Add a small delay between requests for smoothing (at least 0.5 seconds between requests)
        min_spacing = 0.5
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < min_spacing:
            spacing_wait = min_spacing - time_since_last_request
            wait_time = max(wait_time, spacing_wait)
            
        return wait_time

    def allow_request(self, endpoint: str) -> bool:
        """
        Check if a request to this endpoint should be allowed based on
        circuit status AND rate limiting.
        
        Args:
            endpoint: API endpoint to check
            
        Returns:
            True if request should be allowed
        """
        current_time = time.time()
        
        # First check if circuit breaker is open
        if self.is_open(endpoint):
            return False
            
        # Then check rate limits
        if not self.check_rate_limit(endpoint):
            # If we're close to the rate limit, don't immediately open circuit
            # but still deny the request
            call_count = self._count_recent_calls(endpoint)
            if call_count >= self.rate_limit_max_calls - 5:  # Within 5 of limit
                logger.warning(f"Rate limit almost exceeded for {endpoint}: {call_count}/{self.rate_limit_max_calls}")
                # Record the time we started throttling to limit logs
                if endpoint not in self.circuit_open_until:
                    self.circuit_open_until[endpoint] = current_time + 5  # Just to mark as throttling
            return False
            
        # Ensure minimum spacing between requests (50ms)
        elapsed = current_time - self.last_request_time
        if elapsed < 0.05:  # 50ms
            time.sleep(0.05 - elapsed)
            
        # Record this request
        self.last_request_time = time.time()
        
        # Track the request for rate limiting
        self._record_request(endpoint)
        
        return True

    def _count_recent_calls(self, endpoint: str) -> int:
        """
        Count recent calls to the given endpoint.
        
        Args:
            endpoint: API endpoint to check
            
        Returns:
            Number of recent calls
        """
        current_time = time.time()
        window_start = current_time - self.rate_limit_window
        
        if endpoint in self.request_timestamps:
            return len([ts for ts in self.request_timestamps[endpoint] if ts > window_start])
        else:
            return 0

    def _record_request(self, endpoint: str) -> None:
        """
        Record a request to the given endpoint.
        
        Args:
            endpoint: API endpoint to record
        """
        current_time = time.time()
        
        if endpoint not in self.request_timestamps:
            self.request_timestamps[endpoint] = []
        
        self.request_timestamps[endpoint].append(current_time)
