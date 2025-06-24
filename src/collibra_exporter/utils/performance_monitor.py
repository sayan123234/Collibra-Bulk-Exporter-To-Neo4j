"""
Performance Monitor Module

This module provides performance monitoring and metrics collection.
"""

import time
import logging
import threading
from collections import defaultdict, deque
from typing import Dict, List

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """Thread-safe performance monitoring for connection pools and operations."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.metrics = defaultdict(list)
            self.counters = defaultdict(int)
            self.timers = {}
            self.metrics_lock = threading.Lock()
            self.initialized = True
    
    def start_timer(self, operation: str) -> str:
        """
        Start a timer for an operation.
        
        Args:
            operation: Name of the operation
            
        Returns:
            Timer ID for stopping the timer
        """
        timer_id = f"{operation}_{int(time.time() * 1000000)}"
        self.timers[timer_id] = {
            'operation': operation,
            'start_time': time.time()
        }
        return timer_id
    
    def stop_timer(self, timer_id: str):
        """
        Stop a timer and record the duration.
        
        Args:
            timer_id: Timer ID returned by start_timer
        """
        if timer_id in self.timers:
            timer_info = self.timers.pop(timer_id)
            duration = time.time() - timer_info['start_time']
            
            with self.metrics_lock:
                self.metrics[f"{timer_info['operation']}_duration"].append(duration)
                # Keep only last 1000 measurements
                if len(self.metrics[f"{timer_info['operation']}_duration"]) > 1000:
                    self.metrics[f"{timer_info['operation']}_duration"].pop(0)
    
    def increment_counter(self, counter_name: str, value: int = 1):
        """
        Increment a counter.
        
        Args:
            counter_name: Name of the counter
            value: Value to increment by (default: 1)
        """
        with self.metrics_lock:
            self.counters[counter_name] += value
    
    def record_metric(self, metric_name: str, value: float):
        """
        Record a metric value.
        
        Args:
            metric_name: Name of the metric
            value: Value to record
        """
        with self.metrics_lock:
            self.metrics[metric_name].append(value)
            # Keep only last 1000 measurements
            if len(self.metrics[metric_name]) > 1000:
                self.metrics[metric_name].pop(0)
    
    def get_stats(self) -> Dict:
        """
        Get performance statistics.
        
        Returns:
            Dictionary containing performance statistics
        """
        with self.metrics_lock:
            stats = {
                'counters': dict(self.counters),
                'metrics': {}
            }
            
            for metric_name, values in self.metrics.items():
                if values:
                    stats['metrics'][metric_name] = {
                        'count': len(values),
                        'avg': sum(values) / len(values),
                        'min': min(values),
                        'max': max(values),
                        'recent_avg': sum(values[-10:]) / min(len(values), 10)
                    }
            
            return stats
    
    def log_performance_summary(self):
        """Log a performance summary."""
        stats = self.get_stats()
        
        logger.info("="*60)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("="*60)
        
        # Log counters
        if stats['counters']:
            logger.info("Counters:")
            for counter, value in stats['counters'].items():
                logger.info(f"  {counter}: {value}")
        
        # Log metrics
        if stats['metrics']:
            logger.info("Metrics:")
            for metric, data in stats['metrics'].items():
                logger.info(f"  {metric}:")
                logger.info(f"    Count: {data['count']}")
                logger.info(f"    Average: {data['avg']:.3f}s")
                logger.info(f"    Min: {data['min']:.3f}s")
                logger.info(f"    Max: {data['max']:.3f}s")
                logger.info(f"    Recent Avg: {data['recent_avg']:.3f}s")
        
        logger.info("="*60)

# Global performance monitor instance
performance_monitor = PerformanceMonitor()

def start_timer(operation: str) -> str:
    """Start a performance timer."""
    return performance_monitor.start_timer(operation)

def stop_timer(timer_id: str):
    """Stop a performance timer."""
    performance_monitor.stop_timer(timer_id)

def increment_counter(counter_name: str, value: int = 1):
    """Increment a performance counter."""
    performance_monitor.increment_counter(counter_name, value)

def record_metric(metric_name: str, value: float):
    """Record a performance metric."""
    performance_monitor.record_metric(metric_name, value)

def log_performance_summary():
    """Log performance summary."""
    performance_monitor.log_performance_summary()