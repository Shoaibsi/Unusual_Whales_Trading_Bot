from collections import defaultdict
import asyncio
import time

# Import the configured logger
from src.logger_config import logger

class EndpointMonitor:
    def __init__(self, api_client=None):
        self.api_client = api_client
        self.stats = defaultdict(lambda: {
            'success': 0,
            'errors': 0,
            'avg_duration': 0.0,
            'last_update': time.time()
        })
        self.running = False
        
    async def start_monitoring(self):
        """Start the endpoint monitoring process."""
        self.running = True
        logger.info("Endpoint monitoring started")
        
        try:
            while self.running:
                # Sleep first to allow other initialization to complete
                await asyncio.sleep(60)  # Check every minute
                
                if not self.running:
                    break
                    
                # Log current stats
                logger.debug(f"Current endpoint stats: {dict(self.stats)}")
        except Exception as e:
            logger.error(f"Error in endpoint monitoring: {str(e)}")
        finally:
            logger.info("Endpoint monitoring stopped")
            
    def stop(self):
        """Stop the monitoring process."""
        self.running = False
        logger.info("Endpoint monitoring stopping")

    def track(self, endpoint):
        start_time = time.monotonic()
        return EndpointTracker(self.stats[endpoint], start_time)

class EndpointTracker:
    def __init__(self, stats, start_time):
        self.stats = stats
        self.start_time = start_time

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *args):
        duration = time.monotonic() - self.start_time
        if exc_type is None:
            self.stats['success'] += 1
        else:
            self.stats['errors'] += 1
        total_calls = self.stats['success'] + self.stats['errors']
        self.stats['avg_duration'] = ((self.stats['avg_duration'] * (total_calls - 1) + duration) / total_calls)
        self.stats['last_update'] = time.time()
