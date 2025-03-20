import os
import sys
from datetime import datetime
from loguru import logger
import functools
import inspect
import asyncio
import json
from typing import Callable, Any, Optional, Union, Dict, List

# Remove default logger
logger.remove()

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Add console handler with color and improved formatting
logger.add(
    sink=sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <white>{message}</white>",
    level="DEBUG",
    enqueue=True,  # Prevent overlapping output from async tasks
    colorize=True,
    serialize=False,  # Changed to False to fix JSON formatting issue
    backtrace=False,  # Disable backtrace for cleaner console output
    diagnose=False   # Disable diagnose for cleaner console output
)

# Add file handler with rotation for all logs
logger.add(
    "logs/trading_bot_{time:YYYY-MM-DD}.log",
    rotation="500 MB",
    retention="30 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    backtrace=True,
    diagnose=True
)

# Add error file handler for critical errors only
logger.add(
    "logs/errors_{time:YYYY-MM-DD}.log",
    rotation="100 MB",
    retention="60 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="ERROR",
    backtrace=True,
    diagnose=True
)

# Add API call logging for debugging and monitoring
logger.add(
    "logs/api_calls_{time:YYYY-MM-DD}.log",
    rotation="200 MB",
    retention="14 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    filter=lambda record: "api_call" in record["extra"]
)

# Add trading activity logging
logger.add(
    "logs/trading_activity_{time:YYYY-MM-DD}.log",
    rotation="200 MB",
    retention="90 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
    filter=lambda record: "trading" in record["extra"]
)

# Add signal detection logging
logger.add(
    "logs/signals_{time:YYYY-MM-DD}.log",
    rotation="200 MB",
    retention="60 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    filter=lambda record: "signal" in record["extra"]
)

# Add Grok analysis logging
logger.add(
    "logs/grok_analysis_{time:YYYY-MM-DD}.log",
    rotation="200 MB",
    retention="60 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    filter=lambda record: "grok_analysis" in record["extra"]
)

# Add notification logging
logger.add(
    "logs/notifications_{time:YYYY-MM-DD}.log",
    rotation="100 MB",
    retention="30 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
    filter=lambda record: "notification" in record["extra"]
)

# Add performance metrics logging
logger.add(
    "logs/performance_{time:YYYY-MM-DD}.log",
    rotation="100 MB",
    retention="90 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
    filter=lambda record: "performance" in record["extra"]
)

# Add data validation logging
logger.add(
    "logs/data_validation_{time:YYYY-MM-DD}.log",
    rotation="100 MB",
    retention="14 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    filter=lambda record: "validation" in record["extra"]
)

# Decorator for exception logging in async functions
def log_exceptions(func):
    if asyncio.iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Get caller information
                frame = inspect.currentframe().f_back
                filename = frame.f_code.co_filename
                lineno = frame.f_lineno
                
                # Log the exception with context
                logger.opt(exception=True).error(
                    f"Exception in {func.__name__} (called from {os.path.basename(filename)}:{lineno}): {str(e)}"
                )
                
                # Re-raise the exception
                raise
        return async_wrapper
    else:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get caller information
                frame = inspect.currentframe().f_back
                filename = frame.f_code.co_filename
                lineno = frame.f_lineno
                
                # Log the exception with context
                logger.opt(exception=True).error(
                    f"Exception in {func.__name__} (called from {os.path.basename(filename)}:{lineno}): {str(e)}"
                )
                
                # Re-raise the exception
                raise
        return sync_wrapper

# Decorator for API call logging
def log_api_call(endpoint_name: str):
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = datetime.now()
                try:
                    result = await func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    
                    # Log successful API call
                    logger.bind(api_call=True).debug(
                        f"API Call to {endpoint_name} completed in {elapsed:.3f}s"
                    )
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    
                    # Log failed API call
                    logger.bind(api_call=True).error(
                        f"API Call to {endpoint_name} failed after {elapsed:.3f}s: {str(e)}"
                    )
                    raise
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = datetime.now()
                try:
                    result = func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    
                    # Log successful API call
                    logger.bind(api_call=True).debug(
                        f"API Call to {endpoint_name} completed in {elapsed:.3f}s"
                    )
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    
                    # Log failed API call
                    logger.bind(api_call=True).error(
                        f"API Call to {endpoint_name} failed after {elapsed:.3f}s: {str(e)}"
                    )
                    raise
            return sync_wrapper
    return decorator

# Decorator for trading activity logging
def log_trading_activity(activity_type: str):
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = datetime.now()
                logger.bind(trading=True).info(f"Starting {activity_type}: {func.__name__}")
                try:
                    result = await func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(trading=True).info(
                        f"Completed {activity_type}: {func.__name__} in {elapsed:.2f}s"
                    )
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(trading=True).error(
                        f"Failed {activity_type}: {func.__name__} after {elapsed:.2f}s - {str(e)}"
                    )
                    raise
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = datetime.now()
                logger.bind(trading=True).info(f"Starting {activity_type}: {func.__name__}")
                try:
                    result = func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(trading=True).info(
                        f"Completed {activity_type}: {func.__name__} in {elapsed:.2f}s"
                    )
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(trading=True).error(
                        f"Failed {activity_type}: {func.__name__} after {elapsed:.2f}s - {str(e)}"
                    )
                    raise
            return sync_wrapper
    return decorator

# Decorator for signal detection logging
def log_signal_detection(signal_type: str):
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = datetime.now()
                logger.bind(signal=True).info(f"Starting {signal_type} detection: {func.__name__}")
                try:
                    result = await func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    
                    # Log the number of signals detected
                    signal_count = len(result) if isinstance(result, list) else (1 if result else 0)
                    logger.bind(signal=True).info(
                        f"Detected {signal_count} {signal_type} signals in {elapsed:.2f}s"
                    )
                    
                    # Log details of each signal
                    if signal_count > 0:
                        signals = result if isinstance(result, list) else [result]
                        for i, signal in enumerate(signals):
                            if signal:
                                logger.bind(signal=True).debug(
                                    f"Signal {i+1}/{signal_count}: {str(signal)}"
                                )
                    
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(signal=True).error(
                        f"Failed {signal_type} detection: {func.__name__} after {elapsed:.2f}s - {str(e)}"
                    )
                    raise
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = datetime.now()
                logger.bind(signal=True).info(f"Starting {signal_type} detection: {func.__name__}")
                try:
                    result = func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    
                    # Log the number of signals detected
                    signal_count = len(result) if isinstance(result, list) else (1 if result else 0)
                    logger.bind(signal=True).info(
                        f"Detected {signal_count} {signal_type} signals in {elapsed:.2f}s"
                    )
                    
                    # Log details of each signal
                    if signal_count > 0:
                        signals = result if isinstance(result, list) else [result]
                        for i, signal in enumerate(signals):
                            if signal:
                                logger.bind(signal=True).debug(
                                    f"Signal {i+1}/{signal_count}: {str(signal)}"
                                )
                    
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(signal=True).error(
                        f"Failed {signal_type} detection: {func.__name__} after {elapsed:.2f}s - {str(e)}"
                    )
                    raise
            return sync_wrapper
    return decorator

# Decorator for notification logging
def log_notification(notification_type: str):
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = datetime.now()
                logger.bind(notification=True).info(f"Sending {notification_type} notification: {func.__name__}")
                try:
                    result = await func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(notification=True).info(
                        f"Sent {notification_type} notification in {elapsed:.2f}s"
                    )
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(notification=True).error(
                        f"Failed to send {notification_type} notification after {elapsed:.2f}s - {str(e)}"
                    )
                    raise
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = datetime.now()
                logger.bind(notification=True).info(f"Sending {notification_type} notification: {func.__name__}")
                try:
                    result = func(*args, **kwargs)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(notification=True).info(
                        f"Sent {notification_type} notification in {elapsed:.2f}s"
                    )
                    return result
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.bind(notification=True).error(
                        f"Failed to send {notification_type} notification after {elapsed:.2f}s - {str(e)}"
                    )
                    raise
            return sync_wrapper
    return decorator

# Decorator for Grok analysis logging
def log_grok_analysis(analysis_type: str):
    """
    Decorator for logging Grok analysis operations.
    
    Args:
        analysis_type: Type of analysis being performed
    
    Returns:
        Callable: Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get function details
            func_name = func.__name__
            module_name = func.__module__
            
            # Get calling class name if method
            class_name = ""
            if args and hasattr(args[0], "__class__"):
                class_name = args[0].__class__.__name__
                
            # Log start of analysis
            start_time = datetime.now()
            logger.bind(grok_analysis=True).info(
                f"GROK ANALYSIS START: {analysis_type} | {class_name}.{func_name} | "
                f"Started at {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
            )
            
            # Extract and log input data
            input_data = None
            if len(args) > 1 and args[1]:  # Assuming signals are the second argument
                input_data = args[1]
                # Log the input data with proper formatting
                logger.bind(grok_analysis=True).debug(
                    f"GROK ANALYSIS INPUT: {analysis_type} | {class_name}.{func_name} | "
                    f"Input data: {format_for_logging(input_data)}"
                )
            
            # Extract and log market context if provided
            market_context = kwargs.get('market_context', None)
            if market_context:
                # Log the market context keys
                logger.bind(grok_analysis=True).debug(
                    f"GROK ANALYSIS CONTEXT: {analysis_type} | {class_name}.{func_name} | "
                    f"Market context keys: {list(market_context.keys())}"
                )
                # Log a sample of the market context data
                for key, value in market_context.items():
                    logger.bind(grok_analysis=True).debug(
                        f"GROK ANALYSIS CONTEXT: {analysis_type} | {class_name}.{func_name} | "
                        f"Market context '{key}' sample: {format_for_logging(value, max_length=300)}"
                    )
            
            try:
                # Execute the function
                result = await func(*args, **kwargs)
                
                # Calculate execution time
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                # Log successful completion with result
                logger.bind(grok_analysis=True).info(
                    f"GROK ANALYSIS COMPLETE: {analysis_type} | {class_name}.{func_name} | "
                    f"Execution time: {execution_time:.3f}s"
                )
                
                # Log the result with proper formatting
                if result:
                    logger.bind(grok_analysis=True).debug(
                        f"GROK ANALYSIS RESULT: {analysis_type} | {class_name}.{func_name} | "
                        f"Result: {format_for_logging(result)}"
                    )
                
                return result
                
            except Exception as e:
                # Calculate execution time
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                # Log error
                logger.bind(grok_analysis=True).error(
                    f"GROK ANALYSIS ERROR: {analysis_type} | {class_name}.{func_name} | "
                    f"Error after {execution_time:.3f}s: {str(e)}"
                )
                
                # Re-raise the exception
                raise
                
        return wrapper
    return decorator

# Configure exception handler for uncaught exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        # Don't log keyboard interrupt
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    # Format the exception message to truncate large data structures
    error_msg = str(exc_value)
    if len(error_msg) > 500:  # Truncate very long error messages
        error_msg = error_msg[:500] + "... [truncated]"
    
    logger.opt(exception=(exc_type, exc_value, exc_traceback)).critical(f"Uncaught exception: {error_msg}")

# Set the exception handler
sys.excepthook = handle_exception

# Function to check if all components are logging correctly
def verify_logging_setup():
    logger.debug("Debug message - verify logging setup")
    logger.info("Info message - verify logging setup")
    logger.warning("Warning message - verify logging setup")
    logger.error("Error message - verify logging setup")
    logger.critical("Critical message - verify logging setup")
    
    # Test API call logging
    logger.bind(api_call=True).debug("Test API call logging")
    
    # Test trading activity logging
    logger.bind(trading=True).info("Test trading activity logging")
    
    # Test exception handling - COMMENTED OUT to prevent initialization errors
    # try:
    #     raise ValueError("Test exception for logging")
    # except Exception as e:
    #     # Format exception message to prevent large data dumps
    #     error_msg = str(e)
    #     if len(error_msg) > 200:
    #         error_msg = error_msg[:200] + "... [truncated]"
    #     logger.exception(f"Test exception handling: {error_msg}")
    
    logger.success("Logging verification complete")

# Add a helper function to format large data structures for logging
def format_for_logging(data, max_items=10, indent=2, max_length=1000):
    """
    Format complex data structures for logging in a readable way.
    
    Args:
        data: The data to format
        max_items: Maximum number of items to show for dictionaries and lists
        indent: Number of spaces to indent nested structures
        max_length: Maximum length of the formatted string
        
    Returns:
        Formatted string representation of the data
    """
    try:
        if data is None:
            return "None"
        
        if isinstance(data, (str, int, float, bool)):
            return str(data)
        
        if isinstance(data, dict):
            # Special handling for option contracts to prioritize important fields
            if "option_symbol" in data and ("strike" in data or "option_type" in data or "expiration" in data):
                # This is likely an option contract - prioritize key fields
                priority_fields = ["ticker", "strike", "expiration", "option_type", "volume", "open_interest", 
                                  "implied_volatility", "last_price", "option_symbol"]
                
                # Start with priority fields that exist in the data
                formatted_items = []
                for field in priority_fields:
                    if field in data:
                        formatted_items.append(f"{field}: {format_for_logging(data[field], max_items, indent)}")
                
                # Add remaining fields up to max_items
                remaining_fields = [k for k in data.keys() if k not in priority_fields]
                remaining_count = max_items - len(formatted_items)
                
                if remaining_count > 0 and remaining_fields:
                    for field in remaining_fields[:remaining_count]:
                        formatted_items.append(f"{field}: {format_for_logging(data[field], max_items, indent)}")
                    
                    if len(remaining_fields) > remaining_count:
                        formatted_items.append(f"... and {len(remaining_fields) - remaining_count} more items")
                
                return "{{\n" + " " * indent + (",\n" + " " * indent).join(formatted_items) + "\n}}"
            
            # Regular dictionary handling
            if len(data) > max_items:
                items = list(data.items())[:max_items]
                formatted_items = [f"{k}: {format_for_logging(v, max_items, indent + 2)}" for k, v in items]
                formatted_string = "{{\n" + " " * indent + (",\n" + " " * indent).join(formatted_items) 
                formatted_string += f",\n{' ' * indent}... and {len(data) - max_items} more items\n}}"
                
                if len(formatted_string) > max_length:
                    return "{{\n" + " " * indent + (",\n" + " " * indent).join(formatted_items) + f",\n{' ' * indent}... [truncated]\n}}"
                else:
                    return formatted_string
            elif len(data) > 0:
                formatted_items = [f"{k}: {format_for_logging(v, max_items, indent + 2)}" for k, v in data.items()]
                return "{{\n" + " " * indent + (",\n" + " " * indent).join(formatted_items) + "\n}}"
            else:
                return "{}"
        
        if isinstance(data, (list, tuple, set)):
            if len(data) > max_items:
                items = list(data)[:max_items]
                formatted_items = [format_for_logging(item, max_items, indent + 2) for item in items]
                formatted_string = "[\n" + " " * indent + (",\n" + " " * indent).join(formatted_items)
                formatted_string += f",\n{' ' * indent}... and {len(data) - max_items} more items\n]"
                
                if len(formatted_string) > max_length:
                    return "[\n" + " " * indent + (",\n" + " " * indent).join(formatted_items) + f",\n{' ' * indent}... [truncated]\n]"
                else:
                    return formatted_string
            elif len(data) > 0:
                return "[\n" + " " * indent + (",\n" + " " * indent).join([format_for_logging(item, max_items, indent + 2) for item in data]) + "\n]"
            else:
                return "[]"
        
        # For other types, use their string representation
        return str(data)
    except Exception as e:
        return f"[Error formatting data: {str(e)}]"

# Create a custom output manager for handling async output
class OutputManager:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.running = False
        self.thread = None
        
    async def add_output(self, message):
        await self.queue.put(message)
        
    async def print_output(self):
        self.running = True
        while self.running:
            try:
                message = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                print(message)
                self.queue.task_done()
            except asyncio.TimeoutError:
                # No message in queue, just continue
                pass
            except Exception as e:
                logger.error(f"Error in output manager: {{str(e)}}")
                
    def stop(self):
        self.running = False
        if self.thread and self.thread.is_alive():
            try:
                self.thread.join(timeout=1.0)
            except Exception as e:
                logger.error(f"Error stopping output manager: {{str(e)}}")

# Initialize the output manager
output_manager = OutputManager()

# Export the configured logger and utilities
__all__ = ['logger', 'log_exceptions', 'log_api_call', 'log_trading_activity', 
           'log_signal_detection', 'log_notification', 'log_grok_analysis', 
           'verify_logging_setup', 'format_for_logging', 'OutputManager', 'output_manager']
