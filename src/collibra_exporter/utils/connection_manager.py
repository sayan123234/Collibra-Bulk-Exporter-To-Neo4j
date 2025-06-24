"""
Connection Manager Module

This module provides centralized connection management and cleanup functionality.
"""

import logging
import atexit
from .http_session_pool import HTTPSessionPool
from ..models.exporter import Neo4jConnectionPool
from .http_optimizer import cleanup_http_optimizer

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Centralized connection manager for all connection pools."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.http_pool = HTTPSessionPool()
            self.neo4j_pool = Neo4jConnectionPool()
            self._register_cleanup()
            ConnectionManager._initialized = True
            logger.info("Connection manager initialized")
    
    def _register_cleanup(self):
        """Register cleanup function to run at application exit."""
        atexit.register(self.cleanup_all_connections)
    
    def cleanup_all_connections(self):
        """Clean up all connection pools."""
        logger.info("Cleaning up all connection pools...")
        
        try:
            self.http_pool.close_all()
            logger.info("HTTP session pool cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up HTTP session pool: {e}")
        
        try:
            cleanup_http_optimizer()
            logger.info("HTTP optimizer cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up HTTP optimizer: {e}")
        
        try:
            self.neo4j_pool.close_all()
            logger.info("Neo4j connection pool cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up Neo4j connection pool: {e}")
        
        logger.info("All connection pools cleaned up")
    
    def get_http_session(self, base_url: str = None):
        """Get HTTP session from pool."""
        return self.http_pool.get_session(base_url)
    
    def get_neo4j_driver(self, uri: str, username: str, password: str, database: str = "neo4j"):
        """Get Neo4j driver from pool."""
        return self.neo4j_pool.get_driver(uri, username, password, database)

# Global connection manager instance
connection_manager = ConnectionManager()

def cleanup_connections():
    """Public function to manually trigger connection cleanup."""
    connection_manager.cleanup_all_connections()