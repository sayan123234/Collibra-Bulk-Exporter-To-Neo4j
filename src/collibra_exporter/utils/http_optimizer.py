"""
HTTP Optimizer Module

This module provides advanced HTTP connection optimization including
intelligent session reuse, connection pooling, request batching, and
adaptive retry strategies.
"""

import time
import json
import logging
import threading
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .performance_monitor import start_timer, stop_timer, increment_counter, record_metric

logger = logging.getLogger(__name__)

@dataclass
class RequestMetrics:
    """Metrics for HTTP request performance tracking."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_response_time: float = 0.0
    min_response_time: float = float('inf')
    max_response_time: float = 0.0
    retry_count: int = 0
    connection_reuse_count: int = 0
    
    def add_request(self, response_time: float, success: bool, retries: int = 0, connection_reused: bool = False):
        """Add a request to the metrics."""
        self.total_requests += 1
        self.total_response_time += response_time
        self.min_response_time = min(self.min_response_time, response_time)
        self.max_response_time = max(self.max_response_time, response_time)
        self.retry_count += retries
        
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            
        if connection_reused:
            self.connection_reuse_count += 1
    
    @property
    def average_response_time(self) -> float:
        """Calculate average response time."""
        return self.total_response_time / self.total_requests if self.total_requests > 0 else 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        return (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0.0
    
    @property
    def connection_reuse_rate(self) -> float:
        """Calculate connection reuse rate as percentage."""
        return (self.connection_reuse_count / self.total_requests * 100) if self.total_requests > 0 else 0.0

@dataclass
class BatchRequest:
    """Represents a batched HTTP request."""
    url: str
    method: str = 'POST'
    headers: Dict[str, str] = field(default_factory=dict)
    data: Any = None
    json_data: Any = None
    params: Dict[str, str] = field(default_factory=dict)
    timeout: Optional[float] = None
    callback: Optional[Callable] = None
    request_id: str = field(default_factory=lambda: str(time.time()))

class AdaptiveRetryStrategy:
    """Adaptive retry strategy that adjusts based on error patterns."""
    
    def __init__(self):
        self.error_patterns = {}
        self.success_patterns = {}
        self.lock = threading.Lock()
    
    def get_retry_config(self, url: str, error_history: List[int]) -> Retry:
        """Get adaptive retry configuration based on URL and error history."""
        with self.lock:
            # Base retry configuration
            base_config = {
                'total': 3,
                'backoff_factor': 1.0,
                'status_forcelist': [429, 500, 502, 503, 504],
                'allowed_methods': ["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
            }
            
            # Adjust based on error patterns for this URL
            url_errors = self.error_patterns.get(url, [])
            if len(url_errors) > 5:  # If we have enough data
                recent_errors = url_errors[-5:]
                error_rate = len([e for e in recent_errors if e >= 500]) / len(recent_errors)
                
                if error_rate > 0.6:  # High server error rate
                    base_config['total'] = 5
                    base_config['backoff_factor'] = 2.0
                elif error_rate < 0.2:  # Low error rate
                    base_config['total'] = 2
                    base_config['backoff_factor'] = 0.5
            
            # Adjust based on recent error history
            if error_history:
                recent_5xx = len([e for e in error_history[-3:] if e >= 500])
                if recent_5xx >= 2:
                    base_config['backoff_factor'] *= 1.5
            
            return Retry(**base_config)
    
    def record_response(self, url: str, status_code: int):
        """Record response for adaptive learning."""
        with self.lock:
            if url not in self.error_patterns:
                self.error_patterns[url] = []
            
            self.error_patterns[url].append(status_code)
            
            # Keep only last 20 responses
            if len(self.error_patterns[url]) > 20:
                self.error_patterns[url] = self.error_patterns[url][-20:]

class OptimizedHTTPAdapter(HTTPAdapter):
    """Optimized HTTP adapter with enhanced connection pooling."""
    
    def __init__(self, pool_connections=20, pool_maxsize=50, max_retries=None, 
                 pool_block=False, **kwargs):
        # Store configuration for use in init_poolmanager
        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize
        self._pool_block = pool_block
        
        # Filter kwargs to only pass parameters that HTTPAdapter accepts
        # Remove our custom parameters that the parent class doesn't understand
        parent_kwargs = {k: v for k, v in kwargs.items() 
                        if k not in ['pool_connections', 'pool_maxsize', 'pool_block']}
        
        # Call parent constructor with only compatible parameters
        super().__init__(max_retries=max_retries, **parent_kwargs)
    
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        """Initialize pool manager with optimized settings."""
        # Use our stored configuration values, with safe fallbacks
        pool_maxsize = getattr(self, '_pool_maxsize', 50)
        pool_connections = getattr(self, '_pool_connections', 20)
        pool_block = getattr(self, '_pool_block', False)
        
        # Override with our optimized values
        connections = pool_connections
        maxsize = pool_maxsize
        block = pool_block
        
        # Add additional optimization settings
        optimization_settings = {
            'retries': False,  # We handle retries at session level
            'timeout': 30,
        }
        
        # Only add socket_options if supported (some urllib3 versions may not support it)
        try:
            import socket
            optimization_settings['socket_options'] = [
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),  # SO_KEEPALIVE
            ]
        except (ImportError, AttributeError):
            # Skip socket options if not supported
            pass
        
        # Add optimization settings that don't conflict
        for key, value in optimization_settings.items():
            if key not in pool_kwargs:
                pool_kwargs[key] = value
        
        # Remove any conflicting parameters that might be passed as both positional and keyword arguments
        conflicting_params = ['connections', 'maxsize', 'block']
        for param in conflicting_params:
            pool_kwargs.pop(param, None)
        
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)

class HTTPOptimizer:
    """Advanced HTTP optimizer with session reuse and connection pooling."""
    
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
            self.sessions = {}
            self.session_lock = threading.RLock()
            self.metrics = RequestMetrics()
            self.metrics_lock = threading.Lock()
            self.retry_strategy = AdaptiveRetryStrategy()
            self.batch_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="HTTPBatch")
            self.request_queue = []
            self.queue_lock = threading.Lock()
            self.initialized = True
            logger.info("HTTP Optimizer initialized with advanced connection management")
    
    def _create_optimized_session(self, base_url: str) -> requests.Session:
        """Create an optimized session with advanced configuration."""
        session = requests.Session()
        
        # Configure optimized adapter
        retry_config = self.retry_strategy.get_retry_config(base_url, [])
        adapter = OptimizedHTTPAdapter(
            pool_connections=20,    # Increased connection pools
            pool_maxsize=50,        # Increased max connections per pool
            max_retries=retry_config,
            pool_block=False
        )
        
        # Mount adapter for both HTTP and HTTPS
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Configure session defaults
        session.timeout = (10, 30)  # (connect_timeout, read_timeout)
        session.headers.update({
            'Connection': 'keep-alive',
            'Keep-Alive': 'timeout=30, max=100',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'Collibra-Bulk-Exporter/2.0 (Optimized)'
        })
        
        # Enable connection pooling optimizations
        session.trust_env = False  # Disable proxy detection for performance
        
        return session
    
    def get_optimized_session(self, base_url: str) -> requests.Session:
        """Get or create an optimized session for the given base URL."""
        with self.session_lock:
            if base_url not in self.sessions:
                logger.info(f"Creating optimized HTTP session for {base_url}")
                self.sessions[base_url] = self._create_optimized_session(base_url)
                increment_counter("http_optimized_sessions_created")
            else:
                logger.debug(f"Reusing optimized HTTP session for {base_url}")
                increment_counter("http_optimized_sessions_reused")
            
            return self.sessions[base_url]
    
    def make_optimized_request(self, url: str, method: str = 'POST', 
                             headers: Optional[Dict[str, str]] = None,
                             **kwargs) -> requests.Response:
        """Make an optimized HTTP request with advanced error handling."""
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        session = self.get_optimized_session(base_url)
        
        # Merge headers
        request_headers = session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # Track request metrics
        timer_id = start_timer(f"http_optimized_request_{method.lower()}")
        start_time = time.time()
        retries = 0
        last_error = None
        
        try:
            # Update retry strategy based on URL
            adapter = session.get_adapter(url)
            if hasattr(adapter, 'max_retries'):
                adapter.max_retries = self.retry_strategy.get_retry_config(url, [])
            
            response = getattr(session, method.lower())(
                url=url, 
                headers=request_headers, 
                **kwargs
            )
            
            response_time = time.time() - start_time
            
            # Record metrics
            with self.metrics_lock:
                connection_reused = hasattr(response.raw, '_original_response') and \
                                  response.raw._original_response is not None
                self.metrics.add_request(response_time, True, retries, connection_reused)
            
            # Record response for adaptive learning
            self.retry_strategy.record_response(url, response.status_code)
            
            # Update performance counters
            increment_counter("http_optimized_requests_successful")
            record_metric("http_optimized_response_time", response_time)
            
            response.raise_for_status()
            return response
            
        except requests.RequestException as e:
            response_time = time.time() - start_time
            
            # Record failed metrics
            with self.metrics_lock:
                self.metrics.add_request(response_time, False, retries)
            
            # Record error for adaptive learning
            status_code = getattr(e.response, 'status_code', 0) if hasattr(e, 'response') else 0
            self.retry_strategy.record_response(url, status_code)
            
            increment_counter("http_optimized_requests_failed")
            logger.error(f"Optimized HTTP request failed: {e}")
            raise
        finally:
            stop_timer(timer_id)
    
    def batch_requests(self, requests_list: List[BatchRequest], 
                      max_concurrent: int = 5) -> List[Tuple[str, requests.Response, Exception]]:
        """Execute multiple requests concurrently with optimized connection reuse."""
        if not requests_list:
            return []
        
        logger.info(f"Executing batch of {len(requests_list)} requests with max_concurrent={max_concurrent}")
        timer_id = start_timer("http_batch_requests")
        
        results = []
        
        def execute_request(batch_req: BatchRequest) -> Tuple[str, requests.Response, Exception]:
            """Execute a single batched request."""
            try:
                response = self.make_optimized_request(
                    url=batch_req.url,
                    method=batch_req.method,
                    headers=batch_req.headers,
                    json=batch_req.json_data,
                    data=batch_req.data,
                    params=batch_req.params,
                    timeout=batch_req.timeout
                )
                
                # Execute callback if provided
                if batch_req.callback:
                    batch_req.callback(response)
                
                return (batch_req.request_id, response, None)
                
            except Exception as e:
                logger.error(f"Batch request {batch_req.request_id} failed: {e}")
                return (batch_req.request_id, None, e)
        
        # Execute requests concurrently
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            future_to_request = {
                executor.submit(execute_request, req): req for req in requests_list
            }
            
            for future in as_completed(future_to_request):
                request_id, response, error = future.result()
                results.append((request_id, response, error))
        
        stop_timer(timer_id)
        increment_counter("http_batch_requests_completed")
        
        successful = len([r for r in results if r[2] is None])
        logger.info(f"Batch execution completed: {successful}/{len(requests_list)} successful")
        
        return results
    
    def get_metrics(self) -> RequestMetrics:
        """Get current HTTP request metrics."""
        with self.metrics_lock:
            return self.metrics
    
    def reset_metrics(self):
        """Reset HTTP request metrics."""
        with self.metrics_lock:
            self.metrics = RequestMetrics()
    
    def log_performance_stats(self):
        """Log detailed HTTP performance statistics."""
        metrics = self.get_metrics()
        
        logger.info("="*60)
        logger.info("HTTP OPTIMIZER PERFORMANCE STATISTICS")
        logger.info("="*60)
        logger.info(f"Total Requests: {metrics.total_requests}")
        logger.info(f"Successful Requests: {metrics.successful_requests}")
        logger.info(f"Failed Requests: {metrics.failed_requests}")
        logger.info(f"Success Rate: {metrics.success_rate:.2f}%")
        logger.info(f"Average Response Time: {metrics.average_response_time:.3f}s")
        logger.info(f"Min Response Time: {metrics.min_response_time:.3f}s")
        logger.info(f"Max Response Time: {metrics.max_response_time:.3f}s")
        logger.info(f"Total Retries: {metrics.retry_count}")
        logger.info(f"Connection Reuse Rate: {metrics.connection_reuse_rate:.2f}%")
        
        # Session statistics
        with self.session_lock:
            logger.info(f"Active Sessions: {len(self.sessions)}")
            for base_url in self.sessions.keys():
                logger.info(f"  - {base_url}")
        
        logger.info("="*60)
    
    def close_all_sessions(self):
        """Close all HTTP sessions."""
        with self.session_lock:
            for base_url, session in self.sessions.items():
                try:
                    session.close()
                    logger.info(f"Closed optimized HTTP session: {base_url}")
                except Exception as e:
                    logger.warning(f"Error closing session {base_url}: {e}")
            self.sessions.clear()
        
        # Shutdown batch executor
        self.batch_executor.shutdown(wait=True)
        logger.info("HTTP Optimizer cleanup completed")

# Global HTTP optimizer instance
http_optimizer = HTTPOptimizer()

def make_optimized_request(url: str, method: str = 'POST', 
                         headers: Optional[Dict[str, str]] = None, **kwargs) -> requests.Response:
    """Make an optimized HTTP request using the global optimizer."""
    return http_optimizer.make_optimized_request(url, method, headers, **kwargs)

def batch_http_requests(requests_list: List[BatchRequest], max_concurrent: int = 5) -> List[Tuple[str, requests.Response, Exception]]:
    """Execute multiple HTTP requests concurrently."""
    return http_optimizer.batch_requests(requests_list, max_concurrent)

def get_http_metrics() -> RequestMetrics:
    """Get current HTTP performance metrics."""
    return http_optimizer.get_metrics()

def log_http_performance():
    """Log HTTP performance statistics."""
    http_optimizer.log_performance_stats()

def cleanup_http_optimizer():
    """Clean up HTTP optimizer resources."""
    http_optimizer.close_all_sessions()