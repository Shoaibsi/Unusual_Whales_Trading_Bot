"""
Chart Generator Module

This module provides functionality to generate charts and visualizations
for trading signals and market data.
"""

import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from loguru import logger
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

class ChartGenerator:
    """Generates charts and visualizations for trading signals."""
    
    def __init__(self, output_dir: str = None):
        """Initialize the chart generator."""
        self.output_dir = output_dir or os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'charts')
        os.makedirs(self.output_dir, exist_ok=True)
        
    def generate_option_chain_chart(self, ticker: str, option_data: Dict, 
                                   expiry_date: str = None) -> str:
        """
        Generate a visualization of the option chain for a specific expiry date.
        
        Args:
            ticker: The ticker symbol
            option_data: Option chain data
            expiry_date: Specific expiry date to visualize (optional)
            
        Returns:
            Path to the generated chart image
        """
        try:
            # Extract option chain data
            if not option_data or 'data' not in option_data:
                logger.error(f"Invalid option data for {ticker}")
                return None
                
            options_list = option_data['data']
            
            if not options_list or not isinstance(options_list, list):
                logger.error(f"No options data found for {ticker}")
                return None
                
            # Group options by expiration date
            options_by_expiry = {}
            for option in options_list:
                # Extract expiry date from option symbol if not present directly
                exp_date = None
                if 'expiration' in option:
                    exp_date = option['expiration']
                elif 'option_symbol' in option:
                    # Try to extract from option symbol (format like AAPL250228C00240000)
                    symbol = option['option_symbol']
                    if len(symbol) >= 15:
                        # Extract date part (format YY MM DD)
                        year = f"20{symbol[4:6]}"
                        month = symbol[6:8]
                        day = symbol[8:10]
                        exp_date = f"{year}-{month}-{day}"
                
                if exp_date:
                    if exp_date not in options_by_expiry:
                        options_by_expiry[exp_date] = []
                    options_by_expiry[exp_date].append(option)
            
            # If no expiry date specified, use the closest one
            if not expiry_date:
                if not options_by_expiry:
                    logger.error(f"No valid expiry dates found for {ticker}")
                    return None
                    
                # Sort and get the closest expiry date
                today = datetime.now().date()
                future_expirations = [exp for exp in options_by_expiry.keys() 
                                    if datetime.strptime(exp, '%Y-%m-%d').date() >= today]
                if future_expirations:
                    expiry_date = min(future_expirations)
                else:
                    # If no future expirations, use the latest one
                    expiry_date = max(options_by_expiry.keys())
            
            # Get options for the selected expiry date
            if expiry_date not in options_by_expiry:
                logger.warning(f"No options found for {ticker} with expiry {expiry_date}")
                return None
                
            filtered_options = options_by_expiry[expiry_date]
            
            # Separate calls and puts
            calls = [opt for opt in filtered_options if 'option_symbol' in opt and 'C' in opt['option_symbol']]
            puts = [opt for opt in filtered_options if 'option_symbol' in opt and 'P' in opt['option_symbol']]
            
            if not calls and not puts:
                logger.warning(f"No valid call/put options found for {ticker} with expiry {expiry_date}")
                return None
                
            # Extract strike prices and implied volatilities
            call_strikes = []
            call_ivs = []
            for opt in calls:
                if 'strike' in opt and 'implied_volatility' in opt:
                    try:
                        strike = float(opt['strike'])
                        iv = float(opt['implied_volatility'])
                        call_strikes.append(strike)
                        call_ivs.append(iv)
                    except (ValueError, TypeError):
                        continue
            
            put_strikes = []
            put_ivs = []
            for opt in puts:
                if 'strike' in opt and 'implied_volatility' in opt:
                    try:
                        strike = float(opt['strike'])
                        iv = float(opt['implied_volatility'])
                        put_strikes.append(strike)
                        put_ivs.append(iv)
                    except (ValueError, TypeError):
                        continue
            
            # Create the plot
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Plot calls and puts
            has_legend_items = False
            if call_strikes and call_ivs:
                ax.plot(call_strikes, call_ivs, 'g-', label='Calls IV', marker='o')
                has_legend_items = True
            if put_strikes and put_ivs:
                ax.plot(put_strikes, put_ivs, 'r-', label='Puts IV', marker='o')
                has_legend_items = True
            
            # Add current stock price line if available
            # Try to get underlying price from first option
            current_price = None
            if filtered_options:
                if 'underlying_price' in filtered_options[0]:
                    try:
                        current_price = float(filtered_options[0]['underlying_price'])
                    except (ValueError, TypeError):
                        pass
            
            if current_price:
                ax.axvline(x=current_price, color='blue', linestyle='--', 
                          label=f'Current Price: ${current_price:.2f}')
                has_legend_items = True
            
            # Set labels and title
            ax.set_xlabel('Strike Price')
            ax.set_ylabel('Implied Volatility')
            ax.set_title(f'{ticker} Option Chain - Expiry: {expiry_date}')
            
            # Only add legend if we have items to show
            if has_legend_items:
                ax.legend()
            
            ax.grid(True)
            
            # If we don't have any data to plot, add a text note
            if not has_legend_items:
                ax.text(0.5, 0.5, 'No valid option data available', 
                       horizontalalignment='center',
                       verticalalignment='center',
                       transform=ax.transAxes,
                       fontsize=14)
            
            # Save the chart
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.output_dir, f'{ticker}_option_chain_{timestamp}.png')
            plt.savefig(chart_path, dpi=100, bbox_inches='tight')
            plt.close(fig)
            
            logger.info(f"Generated option chain chart for {ticker}: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"Error generating option chain chart for {ticker}: {str(e)}")
            return None
            
    def generate_flow_analysis_chart(self, ticker: str, flow_data: Dict) -> str:
        """
        Generate a visualization of the options flow data.
        
        Args:
            ticker: The ticker symbol
            flow_data: Options flow data
            
        Returns:
            Path to the generated chart image
        """
        try:
            # Extract flow data
            if not flow_data or 'data' not in flow_data:
                logger.error(f"Invalid flow data for {ticker}")
                return None
                
            flow_list = flow_data['data']
            
            if not flow_list or not isinstance(flow_list, list):
                logger.error(f"No flow data found in response for {ticker}")
                return None
                
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(flow_list)
            
            # Ensure we have the necessary columns
            if 'timestamp' not in df.columns:
                logger.error(f"Flow data missing timestamp column for {ticker}")
                return None
                
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            # Create the plot
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
            
            # Plot delta flow over time
            if 'dir_delta_flow' in df.columns and 'total_delta_flow' in df.columns:
                ax1.plot(df['timestamp'], df['dir_delta_flow'].astype(float), 'g-', label='Directional Delta Flow')
                ax1.plot(df['timestamp'], df['total_delta_flow'].astype(float), 'b-', label='Total Delta Flow')
            
            # Format x-axis as dates
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
            
            # Set labels and title
            ax1.set_ylabel('Delta Flow')
            ax1.set_title(f'{ticker} Options Flow Analysis')
            ax1.legend()
            ax1.grid(True)
            
            # Plot vega flow
            if 'dir_vega_flow' in df.columns and 'total_vega_flow' in df.columns:
                ax2.plot(df['timestamp'], df['dir_vega_flow'].astype(float), 'g-', label='Directional Vega Flow')
                ax2.plot(df['timestamp'], df['total_vega_flow'].astype(float), 'b-', label='Total Vega Flow')
            
            # Set labels
            ax2.set_xlabel('Time')
            ax2.set_ylabel('Vega Flow')
            ax2.legend()
            ax2.grid(True)
            
            # Rotate date labels for better readability
            fig.autofmt_xdate()
            
            # Save the chart
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.output_dir, f'{ticker}_flow_analysis_{timestamp}.png')
            plt.savefig(chart_path, dpi=100, bbox_inches='tight')
            plt.close(fig)
            
            logger.info(f"Generated flow analysis chart for {ticker}: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"Error generating flow analysis chart for {ticker}: {str(e)}")
            return None
            
    def generate_signal_chart(self, ticker: str, signal_data: Dict) -> str:
        """
        Generate a visualization of a trading signal.
        
        Args:
            ticker: The ticker symbol
            signal_data: Signal data including price history and indicators
            
        Returns:
            Path to the generated chart image
        """
        try:
            # Extract signal data
            if not signal_data or 'data' not in signal_data or not signal_data['data']:
                logger.error(f"Invalid signal data for {ticker}")
                return None
                
            data = signal_data['data']
            
            # Check if price history exists in the data
            if 'price_history' not in data or not data['price_history']:
                logger.error(f"No price history found for {ticker}")
                return None
                
            price_history = data['price_history']
            
            if not price_history:
                logger.warning(f"No price history found for {ticker}")
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(price_history)
            
            # Ensure we have the necessary columns
            required_cols = ['timestamp', 'close']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"Price history missing required columns for {ticker}")
                return None
                
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            # Create the plot
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Plot price history
            ax.plot(df['timestamp'], df['close'], 'b-', label='Price')
            
            # Add signal markers if available
            if 'signals' in data:
                signals = data['signals']
                for signal in signals:
                    if 'timestamp' in signal and 'price' in signal:
                        signal_time = pd.to_datetime(signal['timestamp'])
                        signal_price = signal['price']
                        signal_type = signal.get('type', 'unknown')
                        
                        if signal_type.lower() == 'buy':
                            ax.scatter(signal_time, signal_price, color='green', s=100, marker='^', 
                                      label=f'Buy Signal ({signal_time.strftime("%m-%d %H:%M")})')
                        elif signal_type.lower() == 'sell':
                            ax.scatter(signal_time, signal_price, color='red', s=100, marker='v', 
                                      label=f'Sell Signal ({signal_time.strftime("%m-%d %H:%M")})')
            
            # Format x-axis as dates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            
            # Set labels and title
            ax.set_xlabel('Time')
            ax.set_ylabel('Price ($)')
            ax.set_title(f'{ticker} Trading Signal Analysis')
            
            # Add legend with unique entries only
            handles, labels = ax.get_legend_handles_labels()
            by_label = dict(zip(labels, handles))
            ax.legend(by_label.values(), by_label.keys())
            
            ax.grid(True)
            
            # Rotate date labels for better readability
            fig.autofmt_xdate()
            
            # Save the chart
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.output_dir, f'{ticker}_signal_{timestamp}.png')
            plt.savefig(chart_path, dpi=100, bbox_inches='tight')
            plt.close(fig)
            
            logger.info(f"Generated signal chart for {ticker}: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"Error generating signal chart for {ticker}: {str(e)}")
            return None

    def generate_dark_pool_chart(self, ticker: str, dark_pool_data: Dict) -> str:
        """
        Generate a visualization of dark pool activity.
        
        Args:
            ticker: The ticker symbol
            dark_pool_data: Dark pool activity data
            
        Returns:
            Path to the generated chart image
        """
        try:
            if not dark_pool_data or 'data' not in dark_pool_data:
                logger.error(f"Invalid dark pool data for {ticker}")
                return None
                
            trades = dark_pool_data['data']
            if not trades or not isinstance(trades, list):
                logger.error(f"No dark pool trades found for {ticker}")
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(trades)
            
            # Ensure we have the necessary columns
            required_cols = ['tape_time', 'size', 'price', 'buy_sell_ratio']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"Dark pool data missing required columns for {ticker}")
                return None
                
            # Convert timestamp to datetime
            df['tape_time'] = pd.to_datetime(df['tape_time'])
            df['size'] = df['size'].astype(float)
            df['price'] = df['price'].astype(float)
            df['buy_sell_ratio'] = df['buy_sell_ratio'].astype(float)
            
            # Sort by timestamp
            df = df.sort_values('tape_time')
            
            # Create the plot with 3 subplots
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15), height_ratios=[2, 1, 1])
            
            # Plot 1: Price and Volume
            ax1.scatter(df['tape_time'], df['price'], 
                      s=df['size']/df['size'].mean()*100,  # Size proportional to trade size
                      c=df['buy_sell_ratio'].apply(lambda x: 'g' if x > 1 else 'r'),  # Green for buys, red for sells
                      alpha=0.6)
            
            # Add price line
            ax1.plot(df['tape_time'], df['price'], 'b-', alpha=0.3, label='Price')
            
            # Format x-axis as dates
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
            
            # Set labels
            ax1.set_ylabel('Price ($)')
            ax1.set_title(f'{ticker} Dark Pool Activity')
            ax1.grid(True)
            
            # Plot 2: Cumulative Volume
            df['cumulative_volume'] = df['size'].cumsum()
            ax2.plot(df['tape_time'], df['cumulative_volume'], 'b-', label='Cumulative Volume')
            ax2.fill_between(df['tape_time'], df['cumulative_volume'], alpha=0.2)
            
            # Set labels
            ax2.set_ylabel('Cumulative Volume')
            ax2.grid(True)
            
            # Plot 3: Buy/Sell Ratio
            window = 5  # Moving average window
            df['ma_ratio'] = df['buy_sell_ratio'].rolling(window=window).mean()
            ax3.plot(df['tape_time'], df['ma_ratio'], 'b-', label=f'{window}-Period MA')
            ax3.axhline(y=1.0, color='gray', linestyle='--', alpha=0.5)
            
            # Color regions above/below 1.0
            ax3.fill_between(df['tape_time'], df['ma_ratio'], 1, 
                           where=(df['ma_ratio'] >= 1), 
                           color='green', alpha=0.2)
            ax3.fill_between(df['tape_time'], df['ma_ratio'], 1, 
                           where=(df['ma_ratio'] < 1), 
                           color='red', alpha=0.2)
            
            # Set labels
            ax3.set_xlabel('Time')
            ax3.set_ylabel('Buy/Sell Ratio')
            ax3.grid(True)
            
            # Add legend
            ax3.legend()
            
            # Rotate date labels for better readability
            fig.autofmt_xdate()
            
            # Adjust layout
            plt.tight_layout()
            
            # Save the chart
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.output_dir, f'{ticker}_dark_pool_{timestamp}.png')
            plt.savefig(chart_path, dpi=100, bbox_inches='tight')
            plt.close(fig)
            
            logger.info(f"Generated dark pool chart for {ticker}: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"Error generating dark pool chart for {ticker}: {str(e)}")
            return None
