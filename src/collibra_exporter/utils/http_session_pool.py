"""
HTTP Session Pool Module

This module provides a thread-safe HTTP session pool manager with connection pooling and retry logic.
"""

import logging
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .performance_monitor import increment_counter

logger = logging.getLogger(__name__)

class HTTPSessionPool:
    """Thread-safe HTTP session pool manager with connection pooling and retry logic."""
    
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
            self.session_lock = threading.Lock()
            self.initialized = True
    
    def get_session(self, base_url: str = None):
        """
        Get or create an HTTP session with connection pooling and retry logic.
        
        Args:
            base_url: Base URL for the session (optional, used for session key)
            
        Returns:
            requests.Session: Configured session with connection pooling
        """
        session_key = base_url or "default"
        
        with self.session_lock:
            if session_key not in self.sessions:
                logger.info(f"Creating new HTTP session for {session_key}")
                increment_counter("http_sessions_created")
                
                session = requests.Session()
                
                # Configure retry strategy
                retry_strategy = Retry(
                    total=3,
                    backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
                )
                
                # Configure HTTP adapter with connection pooling
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=10,  # Number of connection pools
                    pool_maxsize=20,      # Maximum number of connections per pool
                    pool_block=False      # Don't block when pool is full
                )
                
                # Mount adapter for both HTTP and HTTPS
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                
                # Set default timeout and headers
                session.timeout = 30
                session.headers.update({
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'User-Agent': 'Collibra-Bulk-Exporter/1.0'
                })
                
                self.sessions[session_key] = session
                logger.info(f"HTTP session created with connection pooling and retry logic")
            else:
                logger.debug(f"Reusing existing HTTP session for {session_key}")
                increment_counter("http_sessions_reused")
        
        return self.sessions[session_key]
    
    def close_all(self):
        """Close all sessions in the pool."""
        with self.session_lock:
            for session_key, session in self.sessions.items():
                try:
                    session.close()
                    logger.info(f"Closed HTTP session: {session_key}")
                except Exception as e:
                    logger.warning(f"Error closing session {session_key}: {e}")
            self.sessions.clear()
    
    def __del__(self):
        """Cleanup when the pool is destroyed."""
        self.close_all()