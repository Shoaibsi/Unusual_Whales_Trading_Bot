import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger
import matplotlib.pyplot as plt
from dataclasses import asdict
from src.logger_config import logger, log_exceptions, log_trading_activity

class PerformanceTracker:
    """Tracks and analyzes trading signals and executed trades performance."""
    
    def __init__(self, data_dir: str = None):
        """Initialize the performance tracker."""
        self.data_dir = data_dir or os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.signals_file = os.path.join(self.data_dir, 'signals_history.json')
        self.trades_file = os.path.join(self.data_dir, 'trade_history.json')
        self.performance_file = os.path.join(self.data_dir, 'performance_metrics.json')
        
        # Load existing data
        self.signals_history = self._load_json(self.signals_file, [])
        self.trade_history = self._load_json(self.trades_file, [])
        self.performance_metrics = self._load_json(self.performance_file, {})
        
    @log_exceptions
    def _load_json(self, file_path: str, default_value):
        """Load data from a JSON file with a default fallback."""
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
        return default_value
        
    @log_exceptions
    def _save_json(self, data, file_path: str) -> bool:
        """Save data to a JSON file."""
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            return True
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {str(e)}")
            return False
            
    @log_exceptions
    @log_trading_activity("Record Signal")
    def record_signal(self, signal, grok_analysis: Optional[Dict] = None) -> None:
        """Record a detected trading signal."""
        # Convert signal to dict if it's not already
        if hasattr(signal, '__dict__'):
            signal_dict = asdict(signal)
        else:
            signal_dict = signal
            
        # Add timestamp if not present
        if 'timestamp' not in signal_dict:
            signal_dict['timestamp'] = datetime.now().isoformat()
            
        # Add grok analysis if available
        if grok_analysis:
            signal_dict['grok_analysis'] = grok_analysis
            
        # Add to history
        self.signals_history.append(signal_dict)
        
        # Save to file
        self._save_json(self.signals_history, self.signals_file)
        
    @log_exceptions
    @log_trading_activity("Record Trade")
    def record_trade(self, trade_data: Dict) -> None:
        """Record an executed trade."""
        # Add timestamp if not present
        if 'timestamp' not in trade_data:
            trade_data['timestamp'] = datetime.now().isoformat()
            
        # Add to history
        self.trade_history.append(trade_data)
        
        # Save to file
        self._save_json(self.trade_history, self.trades_file)
        
    @log_exceptions
    @log_trading_activity("Update Trade")
    def update_trade_result(self, trade_id: str, result_data: Dict) -> None:
        """Update a trade with result information (profit/loss, exit price, etc.)."""
        for trade in self.trade_history:
            if trade.get('id') == trade_id:
                trade.update(result_data)
                trade['updated_at'] = datetime.now().isoformat()
                break
                
        # Save updated history
        self._save_json(self.trade_history, self.trades_file)
        
    @log_exceptions
    def calculate_metrics(self) -> Dict:
        """Calculate performance metrics from signals and trades."""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'signal_count': len(self.signals_history),
            'trade_count': len(self.trade_history),
            'signal_types': {},
            'win_rate': 0,
            'average_profit': 0,
            'profit_factor': 0,
            'sharpe_ratio': 0,
            'max_drawdown': 0,
            'confidence_correlation': 0
        }
        
        # Count signal types
        for signal in self.signals_history:
            signal_type = signal.get('signal_type', 'UNKNOWN')
            if signal_type not in metrics['signal_types']:
                metrics['signal_types'][signal_type] = 0
            metrics['signal_types'][signal_type] += 1
            
        # Calculate trade metrics if we have completed trades
        completed_trades = [t for t in self.trade_history if 'exit_price' in t]
        if completed_trades:
            # Win rate
            winning_trades = [t for t in completed_trades if t.get('profit', 0) > 0]
            metrics['win_rate'] = len(winning_trades) / len(completed_trades) * 100
            
            # Average profit
            profits = [t.get('profit', 0) for t in completed_trades]
            metrics['average_profit'] = sum(profits) / len(completed_trades)
            
            # Profit factor
            gains = sum([p for p in profits if p > 0])
            losses = sum([abs(p) for p in profits if p < 0])
            metrics['profit_factor'] = gains / losses if losses > 0 else float('inf')
            
            # Confidence correlation
            if len(completed_trades) >= 5:
                confidences = [t.get('confidence', 0) for t in completed_trades]
                returns = [t.get('profit', 0) / t.get('cost', 1) for t in completed_trades]
                if len(confidences) == len(returns):
                    try:
                        metrics['confidence_correlation'] = np.corrcoef(confidences, returns)[0, 1]
                    except:
                        metrics['confidence_correlation'] = 0
        
        # Save metrics
        self.performance_metrics = metrics
        self._save_json(metrics, self.performance_file)
        
        return metrics
        
    @log_exceptions
    def generate_report(self, days: int = 30) -> Dict:
        """Generate a comprehensive performance report."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Handle different timestamp formats
        def parse_timestamp(ts):
            if isinstance(ts, str):
                try:
                    return datetime.fromisoformat(ts)
                except ValueError:
                    return datetime.now() - timedelta(days=365)  # Default to old date if invalid
            elif isinstance(ts, datetime):
                return ts
            else:
                return datetime.now() - timedelta(days=365)  # Default to old date if invalid
        
        # Filter recent data
        recent_signals = [s for s in self.signals_history if parse_timestamp(s.get('timestamp', '2000-01-01')) > cutoff_date]
        recent_trades = [t for t in self.trade_history if parse_timestamp(t.get('timestamp', '2000-01-01')) > cutoff_date]
        
        # Calculate metrics
        metrics = self.calculate_metrics()
        
        # Generate report
        report = {
            'period': f"Last {days} days",
            'generated_at': datetime.now().isoformat(),
            'metrics': metrics,
            'signal_count': len(recent_signals),
            'trade_count': len(recent_trades),
            'signal_quality': self._analyze_signal_quality(recent_signals),
            'trade_performance': self._analyze_trade_performance(recent_trades),
            'ticker_performance': self._analyze_ticker_performance(recent_trades)
        }
        
        # Save report
        report_file = os.path.join(self.data_dir, f'performance_report_{datetime.now().strftime("%Y%m%d")}.json')
        self._save_json(report, report_file)
        
        return report
        
    @log_exceptions
    def _analyze_signal_quality(self, signals: List[Dict]) -> Dict:
        """Analyze the quality of signals."""
        if not signals:
            return {'signal_count': 0}
            
        result = {
            'signal_count': len(signals),
            'average_confidence': sum(s.get('confidence', 0) for s in signals) / len(signals),
            'signal_types': {},
            'confidence_distribution': {
                'high': len([s for s in signals if s.get('confidence', 0) > 80]),
                'medium': len([s for s in signals if 50 <= s.get('confidence', 0) <= 80]),
                'low': len([s for s in signals if s.get('confidence', 0) < 50])
            }
        }
        
        # Count signal types
        for signal in signals:
            signal_type = signal.get('signal_type', 'UNKNOWN')
            if signal_type not in result['signal_types']:
                result['signal_types'][signal_type] = 0
            result['signal_types'][signal_type] += 1
            
        return result
        
    @log_exceptions
    def _analyze_trade_performance(self, trades: List[Dict]) -> Dict:
        """Analyze the performance of trades."""
        if not trades:
            return {'trade_count': 0}
            
        completed_trades = [t for t in trades if 'exit_price' in t]
        
        result = {
            'trade_count': len(trades),
            'completed_count': len(completed_trades),
            'open_positions': len(trades) - len(completed_trades)
        }
        
        if completed_trades:
            profits = [t.get('profit', 0) for t in completed_trades]
            result.update({
                'win_rate': len([p for p in profits if p > 0]) / len(profits) * 100,
                'average_profit': sum(profits) / len(profits),
                'max_profit': max(profits),
                'max_loss': min(profits),
                'total_profit': sum(profits)
            })
            
        return result
        
    @log_exceptions
    def _analyze_ticker_performance(self, trades: List[Dict]) -> Dict:
        """Analyze performance by ticker."""
        if not trades:
            return {}
            
        ticker_performance = {}
        
        for trade in trades:
            ticker = trade.get('ticker', 'UNKNOWN')
            if ticker not in ticker_performance:
                ticker_performance[ticker] = {
                    'count': 0,
                    'wins': 0,
                    'losses': 0,
                    'total_profit': 0
                }
                
            ticker_performance[ticker]['count'] += 1
            
            if 'profit' in trade:
                profit = trade['profit']
                ticker_performance[ticker]['total_profit'] += profit
                
                if profit > 0:
                    ticker_performance[ticker]['wins'] += 1
                else:
                    ticker_performance[ticker]['losses'] += 1
                    
        # Calculate win rates
        for ticker, data in ticker_performance.items():
            completed = data['wins'] + data['losses']
            if completed > 0:
                data['win_rate'] = data['wins'] / completed * 100
                
        return ticker_performance
        
    @log_exceptions
    @log_trading_activity("Track Signal")
    def track_signal(self, signal: Dict, grok_analysis: Optional[Dict] = None) -> None:
        """Track a signal and its analysis for performance monitoring."""
        # Record the signal
        self.record_signal(signal, grok_analysis)
        
        # Update metrics
        self.calculate_metrics()
        
        # Log tracking
        logger.debug(f"Tracked signal for {signal.get('ticker', 'Unknown')}: {signal.get('signal_type', 'Unknown')}")
        
    @log_exceptions
    def plot_performance(self, output_dir: str = None) -> None:
        """Generate performance charts and save them to the output directory."""
        if not output_dir:
            output_dir = os.path.join(self.data_dir, 'charts')
        os.makedirs(output_dir, exist_ok=True)
        
        # Only proceed if we have completed trades
        completed_trades = [t for t in self.trade_history if 'exit_price' in t and 'profit' in t]
        if not completed_trades:
            logger.warning("Not enough completed trades to generate performance charts")
            return
            
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(completed_trades)
        
        # Convert timestamp strings to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Plot cumulative profit
        plt.figure(figsize=(12, 6))
        df['cumulative_profit'] = df['profit'].cumsum()
        plt.plot(df['timestamp'], df['cumulative_profit'])
        plt.title('Cumulative Profit Over Time')
        plt.xlabel('Date')
        plt.ylabel('Profit ($)')
        plt.grid(True)
        plt.savefig(os.path.join(output_dir, 'cumulative_profit.png'))
        plt.close()
        
        # Plot win rate by signal type
        if 'signal_type' in df.columns:
            win_rates = df.groupby('signal_type').apply(
                lambda x: (x['profit'] > 0).mean() * 100
            ).reset_index()
            win_rates.columns = ['signal_type', 'win_rate']
            
            plt.figure(figsize=(10, 6))
            plt.bar(win_rates['signal_type'], win_rates['win_rate'])
            plt.title('Win Rate by Signal Type')
            plt.xlabel('Signal Type')
            plt.ylabel('Win Rate (%)')
            plt.grid(True, axis='y')
            plt.savefig(os.path.join(output_dir, 'win_rate_by_signal.png'))
            plt.close()
            
        # Plot confidence vs. return scatter plot
        if 'confidence' in df.columns and 'profit' in df.columns and 'cost' in df.columns:
            plt.figure(figsize=(10, 6))
            df['return'] = df['profit'] / df['cost']
            plt.scatter(df['confidence'], df['return'], alpha=0.6)
            plt.title('Signal Confidence vs. Return')
            plt.xlabel('Confidence (%)')
            plt.ylabel('Return (%)')
            plt.grid(True)
            plt.savefig(os.path.join(output_dir, 'confidence_vs_return.png'))
            plt.close()
            
        logger.info(f"Performance charts saved to {output_dir}")

    def __init__(self):
        self.signals = []
        self.trades = []
        
    async def initialize(self):
        """Initialize the performance tracker."""
        try:
            logger.info("Initializing Performance Tracker")
            return True
        except Exception as e:
            logger.error(f"Error initializing Performance Tracker: {str(e)}")
            return False
            
    async def cleanup(self):
        """Clean up resources."""
        try:
            logger.info("Cleaning up Performance Tracker")
            return True
        except Exception as e:
            logger.error(f"Error cleaning up Performance Tracker: {str(e)}")
            return False
