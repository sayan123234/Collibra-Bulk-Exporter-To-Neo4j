"""
Cache Warmer Module

This module provides intelligent cache warming functionality to pre-populate
caches with frequently accessed data.
"""

import logging
import threading
from typing import List, Dict, Any
from ..utils.cache_manager import cache_manager
from ..utils.asset_type import get_asset_type_name, get_available_asset_type
from ..utils.performance_monitor import start_timer, stop_timer, increment_counter

logger = logging.getLogger(__name__)

class CacheWarmer:
    """Intelligent cache warming for frequently accessed data."""
    
    def __init__(self):
        self.warming_in_progress = False
        self.warming_lock = threading.Lock()
    
    def warm_asset_type_cache(self, asset_type_ids: List[str]) -> int:
        """
        Warm the asset type cache with the provided asset type IDs.
        
        Args:
            asset_type_ids: List of asset type IDs to warm
            
        Returns:
            Number of asset types successfully cached
        """
        if not asset_type_ids:
            return 0
        
        logger.info(f"Warming asset type cache with {len(asset_type_ids)} asset types")
        timer_id = start_timer("cache_warming_asset_types")
        
        cached_count = 0
        for asset_type_id in asset_type_ids:
            try:
                # This will cache the asset type name
                name = get_asset_type_name(asset_type_id)
                if name:
                    cached_count += 1
                    logger.debug(f"Cached asset type: {asset_type_id} -> {name}")
                else:
                    logger.warning(f"Could not get name for asset type: {asset_type_id}")
            except Exception as e:
                logger.error(f"Error warming cache for asset type {asset_type_id}: {e}")
        
        stop_timer(timer_id)
        increment_counter("cache_warming_asset_types_completed")
        
        logger.info(f"Asset type cache warming completed: {cached_count}/{len(asset_type_ids)} cached")
        return cached_count
    
    def warm_metadata_cache(self) -> bool:
        """
        Warm the metadata cache with available asset types.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Warming metadata cache with available asset types")
        timer_id = start_timer("cache_warming_metadata")
        
        try:
            # This will cache the available asset types
            available_types = get_available_asset_type()
            
            stop_timer(timer_id)
            increment_counter("cache_warming_metadata_completed")
            
            if available_types and 'results' in available_types:
                count = len(available_types['results'])
                logger.info(f"Metadata cache warming completed: {count} asset types cached")
                return True
            else:
                logger.warning("No asset types found during metadata cache warming")
                return False
                
        except Exception as e:
            stop_timer(timer_id)
            logger.error(f"Error warming metadata cache: {e}")
            return False
    
    def warm_caches_for_asset_types(self, asset_type_ids: List[str]) -> Dict[str, Any]:
        """
        Comprehensive cache warming for the provided asset type IDs.
        
        Args:
            asset_type_ids: List of asset type IDs to warm caches for
            
        Returns:
            Dictionary with warming results
        """
        with self.warming_lock:
            if self.warming_in_progress:
                logger.warning("Cache warming already in progress, skipping")
                return {'status': 'skipped', 'reason': 'already_in_progress'}
            
            self.warming_in_progress = True
        
        try:
            logger.info("="*60)
            logger.info("STARTING COMPREHENSIVE CACHE WARMING")
            logger.info("="*60)
            
            overall_timer = start_timer("cache_warming_comprehensive")
            results = {
                'status': 'completed',
                'asset_types_cached': 0,
                'metadata_cached': False,
                'total_time': 0,
                'errors': []
            }
            
            # Warm metadata cache first
            try:
                results['metadata_cached'] = self.warm_metadata_cache()
            except Exception as e:
                error_msg = f"Metadata cache warming failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            
            # Warm asset type cache
            try:
                results['asset_types_cached'] = self.warm_asset_type_cache(asset_type_ids)
            except Exception as e:
                error_msg = f"Asset type cache warming failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            
            stop_timer(overall_timer)
            
            # Log cache statistics after warming
            cache_manager.log_cache_stats()
            
            logger.info("="*60)
            logger.info("CACHE WARMING COMPLETED")
            logger.info(f"Asset types cached: {results['asset_types_cached']}")
            logger.info(f"Metadata cached: {results['metadata_cached']}")
            logger.info(f"Errors: {len(results['errors'])}")
            logger.info("="*60)
            
            return results
            
        finally:
            with self.warming_lock:
                self.warming_in_progress = False
    
    def warm_caches_async(self, asset_type_ids: List[str]) -> threading.Thread:
        """
        Start cache warming in a background thread.
        
        Args:
            asset_type_ids: List of asset type IDs to warm caches for
            
        Returns:
            Thread object for the warming process
        """
        def warming_worker():
            try:
                self.warm_caches_for_asset_types(asset_type_ids)
            except Exception as e:
                logger.error(f"Background cache warming failed: {e}")
        
        thread = threading.Thread(target=warming_worker, name="CacheWarmer")
        thread.daemon = True
        thread.start()
        
        logger.info("Started background cache warming")
        return thread
    
    def is_warming_in_progress(self) -> bool:
        """Check if cache warming is currently in progress."""
        with self.warming_lock:
            return self.warming_in_progress

# Global cache warmer instance
cache_warmer = CacheWarmer()

def warm_caches(asset_type_ids: List[str]) -> Dict[str, Any]:
    """Warm caches for the provided asset type IDs."""
    return cache_warmer.warm_caches_for_asset_types(asset_type_ids)

def warm_caches_async(asset_type_ids: List[str]) -> threading.Thread:
    """Start cache warming in a background thread."""
    return cache_warmer.warm_caches_async(asset_type_ids)

def is_cache_warming_in_progress() -> bool:
    """Check if cache warming is currently in progress."""
    return cache_warmer.is_warming_in_progress()