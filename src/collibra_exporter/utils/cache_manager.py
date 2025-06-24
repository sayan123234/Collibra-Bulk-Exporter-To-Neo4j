"""
Cache Manager Module

This module provides comprehensive caching functionality with multiple cache layers,
intelligent eviction policies, and performance optimization.
"""

import time
import json
import hashlib
import logging
import threading
from typing import Any, Dict, Optional, Tuple, List
from collections import OrderedDict
from functools import wraps
from dataclasses import dataclass, asdict
from .performance_monitor import start_timer, stop_timer, increment_counter, record_metric

logger = logging.getLogger(__name__)

@dataclass
class CacheEntry:
    """Represents a cache entry with metadata."""
    value: Any
    timestamp: float
    access_count: int = 0
    last_access: float = 0
    ttl: Optional[float] = None
    size_bytes: int = 0
    
    def __post_init__(self):
        if self.last_access == 0:
            self.last_access = self.timestamp
        if self.size_bytes == 0:
            self.size_bytes = self._calculate_size()
    
    def _calculate_size(self) -> int:
        """Estimate the size of the cached value in bytes."""
        try:
            if isinstance(self.value, (str, bytes)):
                return len(self.value)
            elif isinstance(self.value, (list, dict)):
                return len(json.dumps(self.value, default=str))
            else:
                return len(str(self.value))
        except:
            return 100  # Default estimate
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.timestamp > self.ttl
    
    def access(self):
        """Record an access to this cache entry."""
        self.access_count += 1
        self.last_access = time.time()

class LRUCache:
    """Thread-safe LRU cache with size and TTL management."""
    
    def __init__(self, max_size: int = 1000, default_ttl: Optional[float] = None):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache = OrderedDict()
        self.lock = threading.RLock()
        self.total_size = 0
        self.max_memory_mb = 100  # 100MB default limit
    
    def _evict_expired(self):
        """Remove expired entries."""
        current_time = time.time()
        expired_keys = []
        
        for key, entry in self.cache.items():
            if entry.is_expired():
                expired_keys.append(key)
        
        for key in expired_keys:
            self._remove_entry(key)
            increment_counter("cache_expired_evictions")
    
    def _evict_lru(self):
        """Evict least recently used entries if cache is full."""
        while len(self.cache) >= self.max_size:
            key, entry = self.cache.popitem(last=False)
            self.total_size -= entry.size_bytes
            increment_counter("cache_lru_evictions")
    
    def _evict_by_memory(self):
        """Evict entries if memory usage is too high."""
        max_bytes = self.max_memory_mb * 1024 * 1024
        
        while self.total_size > max_bytes and self.cache:
            # Remove least recently used
            key, entry = self.cache.popitem(last=False)
            self.total_size -= entry.size_bytes
            increment_counter("cache_memory_evictions")
    
    def _remove_entry(self, key: str):
        """Remove an entry and update size tracking."""
        if key in self.cache:
            entry = self.cache.pop(key)
            self.total_size -= entry.size_bytes
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        with self.lock:
            self._evict_expired()
            
            if key not in self.cache:
                increment_counter("cache_misses")
                return None
            
            entry = self.cache[key]
            if entry.is_expired():
                self._remove_entry(key)
                increment_counter("cache_misses")
                increment_counter("cache_expired_on_access")
                return None
            
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            entry.access()
            increment_counter("cache_hits")
            
            return entry.value
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Put a value in the cache."""
        with self.lock:
            # Use default TTL if not specified
            if ttl is None:
                ttl = self.default_ttl
            
            # Create cache entry
            entry = CacheEntry(
                value=value,
                timestamp=time.time(),
                ttl=ttl
            )
            
            # Check if entry would exceed memory limit
            if entry.size_bytes > self.max_memory_mb * 1024 * 1024:
                logger.warning(f"Cache entry too large ({entry.size_bytes} bytes), skipping")
                return False
            
            # Remove existing entry if present
            if key in self.cache:
                self._remove_entry(key)
            
            # Add new entry
            self.cache[key] = entry
            self.total_size += entry.size_bytes
            
            # Evict if necessary
            self._evict_lru()
            self._evict_by_memory()
            
            increment_counter("cache_puts")
            record_metric("cache_size", len(self.cache))
            record_metric("cache_memory_mb", self.total_size / (1024 * 1024))
            
            return True
    
    def clear(self):
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self.total_size = 0
            increment_counter("cache_clears")
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'memory_mb': self.total_size / (1024 * 1024),
                'max_memory_mb': self.max_memory_mb,
                'total_accesses': sum(entry.access_count for entry in self.cache.values()),
                'avg_access_count': sum(entry.access_count for entry in self.cache.values()) / len(self.cache) if self.cache else 0
            }

class CacheManager:
    """Comprehensive cache manager with multiple cache layers."""
    
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
            # Different cache layers for different types of data
            self.asset_type_cache = LRUCache(max_size=500, default_ttl=3600)  # 1 hour TTL
            self.graphql_response_cache = LRUCache(max_size=1000, default_ttl=300)  # 5 minutes TTL
            self.nested_data_cache = LRUCache(max_size=2000, default_ttl=600)  # 10 minutes TTL
            self.auth_cache = LRUCache(max_size=10, default_ttl=1800)  # 30 minutes TTL
            self.metadata_cache = LRUCache(max_size=100, default_ttl=7200)  # 2 hours TTL
            
            self.initialized = True
            logger.info("Cache manager initialized with multiple cache layers")
    
    def _generate_cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate a consistent cache key from arguments."""
        # Create a string representation of all arguments
        key_data = {
            'args': args,
            'kwargs': sorted(kwargs.items()) if kwargs else {}
        }
        
        # Create hash of the key data
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        key_hash = hashlib.md5(key_string.encode()).hexdigest()
        
        return f"{prefix}:{key_hash}"
    
    def get_asset_type_cache(self) -> LRUCache:
        """Get the asset type cache."""
        return self.asset_type_cache
    
    def get_graphql_cache(self) -> LRUCache:
        """Get the GraphQL response cache."""
        return self.graphql_response_cache
    
    def get_nested_data_cache(self) -> LRUCache:
        """Get the nested data cache."""
        return self.nested_data_cache
    
    def get_auth_cache(self) -> LRUCache:
        """Get the authentication cache."""
        return self.auth_cache
    
    def get_metadata_cache(self) -> LRUCache:
        """Get the metadata cache."""
        return self.metadata_cache
    
    def clear_all_caches(self):
        """Clear all cache layers."""
        caches = [
            self.asset_type_cache,
            self.graphql_response_cache,
            self.nested_data_cache,
            self.auth_cache,
            self.metadata_cache
        ]
        
        for cache in caches:
            cache.clear()
        
        logger.info("All caches cleared")
    
    def get_cache_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all cache layers."""
        return {
            'asset_type_cache': self.asset_type_cache.stats(),
            'graphql_response_cache': self.graphql_response_cache.stats(),
            'nested_data_cache': self.nested_data_cache.stats(),
            'auth_cache': self.auth_cache.stats(),
            'metadata_cache': self.metadata_cache.stats()
        }
    
    def log_cache_stats(self):
        """Log cache statistics."""
        stats = self.get_cache_stats()
        
        logger.info("="*60)
        logger.info("CACHE STATISTICS")
        logger.info("="*60)
        
        for cache_name, cache_stats in stats.items():
            logger.info(f"{cache_name}:")
            logger.info(f"  Size: {cache_stats['size']}/{cache_stats['max_size']}")
            logger.info(f"  Memory: {cache_stats['memory_mb']:.2f}/{cache_stats['max_memory_mb']:.2f} MB")
            logger.info(f"  Total Accesses: {cache_stats['total_accesses']}")
            logger.info(f"  Avg Access Count: {cache_stats['avg_access_count']:.2f}")
        
        logger.info("="*60)

def cached(cache_type: str = "graphql", ttl: Optional[float] = None, key_prefix: str = ""):
    """
    Decorator for caching function results.
    
    Args:
        cache_type: Type of cache to use ('asset_type', 'graphql', 'nested_data', 'auth', 'metadata')
        ttl: Time to live for cache entry (overrides default)
        key_prefix: Prefix for cache key
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_manager = CacheManager()
            
            # Select appropriate cache
            cache_map = {
                'asset_type': cache_manager.get_asset_type_cache(),
                'graphql': cache_manager.get_graphql_cache(),
                'nested_data': cache_manager.get_nested_data_cache(),
                'auth': cache_manager.get_auth_cache(),
                'metadata': cache_manager.get_metadata_cache()
            }
            
            cache = cache_map.get(cache_type, cache_manager.get_graphql_cache())
            
            # Generate cache key
            prefix = key_prefix or f"{func.__module__}.{func.__name__}"
            cache_key = cache_manager._generate_cache_key(prefix, *args, **kwargs)
            
            # Try to get from cache
            timer_id = start_timer("cache_lookup")
            cached_result = cache.get(cache_key)
            stop_timer(timer_id)
            
            if cached_result is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached_result
            
            # Cache miss - execute function
            logger.debug(f"Cache miss for {func.__name__}")
            timer_id = start_timer(f"function_execution_{func.__name__}")
            try:
                result = func(*args, **kwargs)
                stop_timer(timer_id)
                
                # Store in cache
                timer_id = start_timer("cache_store")
                cache.put(cache_key, result, ttl)
                stop_timer(timer_id)
                
                return result
            except Exception as e:
                stop_timer(timer_id)
                raise
        
        return wrapper
    return decorator

# Global cache manager instance
cache_manager = CacheManager()

def clear_all_caches():
    """Clear all caches."""
    cache_manager.clear_all_caches()

def log_cache_stats():
    """Log cache statistics."""
    cache_manager.log_cache_stats()