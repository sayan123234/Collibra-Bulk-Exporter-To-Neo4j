"""
Data Fetcher Module

This module provides functionality for fetching data from the Collibra API.
"""

import json
import time
import logging
from typing import List, Dict, Any
import requests
from .oauth_auth import get_auth_header
from . import get_query, get_nested_query
from ..utils.performance_monitor import start_timer, stop_timer, increment_counter
from ..utils.cache_manager import cached, cache_manager
from ..utils.http_optimizer import make_optimized_request, HTTPOptimizer, BatchRequest, batch_http_requests

logger = logging.getLogger(__name__)

def make_request(url, method='post', **kwargs):
    """
    Make a request with automatic token refresh handling and optimized connection management.
    
    Args:
        url: The URL to make the request to
        method: The HTTP method to use (default: 'post')
        **kwargs: Additional arguments to pass to the request
        
    Returns:
        requests.Response: The response from the server
        
    Raises:
        requests.RequestException: If the request fails
    """
    timer_id = start_timer(f"http_request_{method}")
    try:
        # Always get fresh headers before making a request
        headers = get_auth_header()
        if 'headers' in kwargs:
            kwargs['headers'].update(headers)
        else:
            kwargs['headers'] = headers

        # Use the optimized HTTP request system
        response = make_optimized_request(url=url, method=method, **kwargs)
        
        increment_counter("http_requests_successful")
        return response
    except requests.RequestException as error:
        increment_counter("http_requests_failed")
        logger.error(f"Request failed: {str(error)}")
        raise
    finally:
        stop_timer(timer_id)

def fetch_data(base_url, asset_type_id, paginate, limit, nested_offset=0, nested_limit=50):
    """
    Fetch initial data batch with basic nested limits and intelligent caching.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: The ID of the asset type to fetch
        paginate: Pagination token or None for first page
        limit: Maximum number of assets to fetch
        nested_offset: Offset for nested fields
        nested_limit: Limit for nested fields
        
    Returns:
        dict: The response data, or None if the request fails
    """
    # Create cache key for this specific request
    cache_key = f"graphql_data:{asset_type_id}:{paginate}:{limit}:{nested_offset}:{nested_limit}"
    
    # Try to get from cache first (shorter TTL for paginated data)
    cache = cache_manager.get_graphql_cache()
    cached_result = cache.get(cache_key)
    
    if cached_result is not None:
        logger.debug(f"Cache hit for GraphQL request: {asset_type_id}")
        increment_counter("graphql_cache_hits")
        return cached_result
    
    logger.debug(f"Cache miss for GraphQL request: {asset_type_id}")
    increment_counter("graphql_cache_misses")
    
    try:
        query = get_query(asset_type_id, f'"{paginate}"' if paginate else 'null', nested_offset, nested_limit)
        variables = {'limit': limit}
        logger.debug(f"Sending GraphQL request for asset_type_id: {asset_type_id}, paginate: {paginate}, nested_offset: {nested_offset}")

        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        start_time = time.time()
        
        response = make_request(
            url=graphql_url,
            json={
                'query': query,
                'variables': variables
            }
        )
        
        response_time = time.time() - start_time
        logger.debug(f"GraphQL request completed in {response_time:.2f} seconds")

        data = response.json()
        
        if 'errors' in data:
            logger.error(f"GraphQL errors received: {data['errors']}")
            return None
        
        # Cache the successful response with appropriate TTL
        # Use shorter TTL for paginated requests (they change more frequently)
        ttl = 60 if paginate else 300  # 1 minute for paginated, 5 minutes for first page
        cache.put(cache_key, data, ttl)
        increment_counter("graphql_responses_cached")
        
        return data
    except requests.RequestException as error:
        logger.exception(f"Request failed for asset_type_id {asset_type_id}: {str(error)}")
        return None
    except json.JSONDecodeError as error:
        logger.exception(f"Failed to parse JSON response: {str(error)}")
        return None

def fetch_nested_data(base_url, asset_type_id, asset_id, field_name, nested_limit=20000):
    """
    Fetch all nested data for a field with intelligent caching, using pagination if necessary.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: ID of the asset type
        asset_id: ID of the specific asset
        field_name: Name of the nested field to fetch
        nested_limit: Initial limit for number of nested items
    
    Returns:
        list: List of all nested items for the field or None if an error occurs
    """
    # Create cache key for nested data
    cache_key = f"nested_data:{asset_type_id}:{asset_id}:{field_name}:{nested_limit}"
    
    # Try to get from cache first
    cache = cache_manager.get_nested_data_cache()
    cached_result = cache.get(cache_key)
    
    if cached_result is not None:
        logger.debug(f"Cache hit for nested data: {asset_id}:{field_name}")
        increment_counter("nested_data_cache_hits")
        return cached_result
    
    logger.debug(f"Cache miss for nested data: {asset_id}:{field_name}")
    increment_counter("nested_data_cache_misses")
    
    try:
        # First attempt with maximum limit to see if pagination is needed
        query = get_nested_query(asset_type_id, asset_id, field_name, 0, nested_limit)
        
        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        start_time = time.time()
        
        response = make_request(
            url=graphql_url,
            json={'query': query}
        )
        
        response_time = time.time() - start_time
        logger.debug(f"Nested GraphQL request completed in {response_time:.2f} seconds")

        data = response.json()
        if 'errors' in data:
            logger.error(f"GraphQL errors in nested query: {data['errors']}")
            return None
            
        if not data['data']['assets']:
            logger.error(f"No asset found in nested query response")
            return None
            
        initial_results = data['data']['assets'][0][field_name]
        
        # If we hit the limit, use pagination to fetch all results
        if len(initial_results) == nested_limit:
            logger.info(f"Hit nested limit of {nested_limit} for {field_name}, switching to pagination")
            
            # Use pagination to fetch all items
            all_items = []
            offset = 0
            batch_number = 1
            batch_size = nested_limit

            # Add initial results to our collection
            all_items.extend(initial_results)
            offset += nested_limit
            
            # Continue fetching batches until we get fewer items than requested
            while True:
                logger.info(f"Fetching batch {batch_number} for {field_name} (offset: {offset})")
                
                # Check cache for individual batch
                batch_cache_key = f"nested_batch:{asset_type_id}:{asset_id}:{field_name}:{offset}:{batch_size}"
                cached_batch = cache.get(batch_cache_key)
                
                if cached_batch is not None:
                    logger.debug(f"Cache hit for nested batch: {asset_id}:{field_name}:{offset}")
                    current_items = cached_batch
                    increment_counter("nested_batch_cache_hits")
                else:
                    logger.debug(f"Cache miss for nested batch: {asset_id}:{field_name}:{offset}")
                    increment_counter("nested_batch_cache_misses")
                    
                    query = get_nested_query(asset_type_id, asset_id, field_name, offset, batch_size)
                    
                    try:
                        response = make_request(
                            url=f"https://{base_url}/graphql/knowledgeGraph/v1",
                            json={'query': query}
                        )
                        
                        data = response.json()
                        
                        if 'errors' in data:
                            logger.error(f"GraphQL errors in nested query: {data['errors']}")
                            break
                            
                        if not data['data']['assets']:
                            logger.error(f"No asset found in nested query response")
                            break
                            
                        current_items = data['data']['assets'][0][field_name]
                        
                        # Cache the batch result
                        cache.put(batch_cache_key, current_items, ttl=600)  # 10 minutes TTL
                        increment_counter("nested_batches_cached")
                        
                    except Exception as e:
                        logger.exception(f"Failed to fetch batch {batch_number} for {field_name}: {str(e)}")
                        break
                
                current_batch_size = len(current_items)
                all_items.extend(current_items)
                logger.info(f"Retrieved {current_batch_size} items in batch {batch_number}")
                
                # If we got fewer items than the batch size, we've reached the end
                if current_batch_size < batch_size:
                    break
                    
                offset += batch_size
                batch_number += 1

            logger.info(f"Completed fetching {field_name}. Total items: {len(all_items)}")
            
            # Cache the complete result
            cache.put(cache_key, all_items, ttl=600)  # 10 minutes TTL
            increment_counter("nested_data_complete_cached")
            
            return all_items
            
        # If we didn't hit the limit, cache and return the initial results
        cache.put(cache_key, initial_results, ttl=600)  # 10 minutes TTL
        increment_counter("nested_data_simple_cached")
        
        return initial_results
    except Exception as e:
        logger.exception(f"Failed to fetch nested data for {field_name}: {str(e)}")
        return None

def fetch_nested_data_batch(base_url, requests_data: List[Dict[str, Any]], max_concurrent: int = 5) -> Dict[str, Any]:
    """
    Fetch multiple nested data requests concurrently for optimal performance.
    
    Args:
        base_url: The base URL of the Collibra instance
        requests_data: List of request data dictionaries containing:
            - asset_type_id: ID of the asset type
            - asset_id: ID of the specific asset
            - field_name: Name of the nested field to fetch
            - nested_limit: Limit for number of nested items (optional)
        max_concurrent: Maximum number of concurrent requests
        
    Returns:
        dict: Dictionary mapping request keys to results
    """
    if not requests_data:
        return {}
    
    logger.info(f"Fetching {len(requests_data)} nested data requests concurrently")
    timer_id = start_timer("fetch_nested_data_batch")
    
    # Prepare batch requests
    batch_requests = []
    request_mapping = {}
    
    for i, req_data in enumerate(requests_data):
        asset_type_id = req_data['asset_type_id']
        asset_id = req_data['asset_id']
        field_name = req_data['field_name']
        nested_limit = req_data.get('nested_limit', 20000)
        
        # Create cache key for this request
        cache_key = f"nested_data:{asset_type_id}:{asset_id}:{field_name}:{nested_limit}"
        request_key = f"{asset_id}:{field_name}"
        
        # Check cache first
        cache = cache_manager.get_nested_data_cache()
        cached_result = cache.get(cache_key)
        
        if cached_result is not None:
            logger.debug(f"Cache hit for batch nested data: {request_key}")
            increment_counter("nested_data_batch_cache_hits")
            # Store cached result directly
            request_mapping[request_key] = cached_result
            continue
        
        logger.debug(f"Cache miss for batch nested data: {request_key}")
        increment_counter("nested_data_batch_cache_misses")
        
        # Create GraphQL query
        query = get_nested_query(asset_type_id, asset_id, field_name, 0, nested_limit)
        
        # Create batch request
        batch_req = BatchRequest(
            url=f"https://{base_url}/graphql/knowledgeGraph/v1",
            method='POST',
            json_data={'query': query},
            request_id=request_key
        )
        
        batch_requests.append(batch_req)
        request_mapping[request_key] = None  # Placeholder
    
    # Execute batch requests if any
    if batch_requests:
        logger.info(f"Executing {len(batch_requests)} concurrent nested data requests")
        batch_results = batch_http_requests(batch_requests, max_concurrent)
        
        # Process batch results
        for request_id, response, error in batch_results:
            if error:
                logger.error(f"Batch nested data request {request_id} failed: {error}")
                request_mapping[request_id] = None
                continue
            
            try:
                data = response.json()
                
                if 'errors' in data:
                    logger.error(f"GraphQL errors in batch nested query {request_id}: {data['errors']}")
                    request_mapping[request_id] = None
                    continue
                
                if not data['data']['assets']:
                    logger.error(f"No asset found in batch nested query response for {request_id}")
                    request_mapping[request_id] = None
                    continue
                
                # Extract field name from request_id
                field_name = request_id.split(':', 1)[1]
                result = data['data']['assets'][0][field_name]
                
                # Cache the result
                asset_id = request_id.split(':', 1)[0]
                # Find the original request data to get cache parameters
                orig_req = next((r for r in requests_data if f"{r['asset_id']}:{r['field_name']}" == request_id), None)
                if orig_req:
                    cache_key = f"nested_data:{orig_req['asset_type_id']}:{orig_req['asset_id']}:{orig_req['field_name']}:{orig_req.get('nested_limit', 20000)}"
                    cache = cache_manager.get_nested_data_cache()
                    cache.put(cache_key, result, ttl=600)
                    increment_counter("nested_data_batch_cached")
                
                request_mapping[request_id] = result
                
            except Exception as e:
                logger.error(f"Error processing batch nested data response {request_id}: {e}")
                request_mapping[request_id] = None
    
    stop_timer(timer_id)
    
    successful = len([v for v in request_mapping.values() if v is not None])
    logger.info(f"Batch nested data fetch completed: {successful}/{len(requests_data)} successful")
    
    return request_mapping

def fetch_graphql_batch(base_url, queries_data: List[Dict[str, Any]], max_concurrent: int = 5) -> Dict[str, Any]:
    """
    Fetch multiple GraphQL queries concurrently for optimal performance.
    
    Args:
        base_url: The base URL of the Collibra instance
        queries_data: List of query data dictionaries containing:
            - query: GraphQL query string
            - variables: Query variables (optional)
            - cache_key: Cache key for the query (optional)
            - ttl: Cache TTL (optional)
        max_concurrent: Maximum number of concurrent requests
        
    Returns:
        dict: Dictionary mapping query indices to results
    """
    if not queries_data:
        return {}
    
    logger.info(f"Fetching {len(queries_data)} GraphQL queries concurrently")
    timer_id = start_timer("fetch_graphql_batch")
    
    # Prepare batch requests
    batch_requests = []
    request_mapping = {}
    
    for i, query_data in enumerate(queries_data):
        query = query_data['query']
        variables = query_data.get('variables', {})
        cache_key = query_data.get('cache_key')
        ttl = query_data.get('ttl', 300)
        
        request_key = str(i)
        
        # Check cache if cache_key provided
        if cache_key:
            cache = cache_manager.get_graphql_cache()
            cached_result = cache.get(cache_key)
            
            if cached_result is not None:
                logger.debug(f"Cache hit for batch GraphQL query {i}")
                increment_counter("graphql_batch_cache_hits")
                request_mapping[request_key] = cached_result
                continue
        
        logger.debug(f"Cache miss for batch GraphQL query {i}")
        increment_counter("graphql_batch_cache_misses")
        
        # Create batch request
        batch_req = BatchRequest(
            url=f"https://{base_url}/graphql/knowledgeGraph/v1",
            method='POST',
            json_data={'query': query, 'variables': variables},
            request_id=request_key
        )
        
        batch_requests.append(batch_req)
        request_mapping[request_key] = None  # Placeholder
    
    # Execute batch requests if any
    if batch_requests:
        logger.info(f"Executing {len(batch_requests)} concurrent GraphQL requests")
        batch_results = batch_http_requests(batch_requests, max_concurrent)
        
        # Process batch results
        for request_id, response, error in batch_results:
            if error:
                logger.error(f"Batch GraphQL request {request_id} failed: {error}")
                request_mapping[request_id] = None
                continue
            
            try:
                data = response.json()
                
                if 'errors' in data:
                    logger.error(f"GraphQL errors in batch query {request_id}: {data['errors']}")
                    request_mapping[request_id] = None
                    continue
                
                # Cache the result if cache_key was provided
                query_index = int(request_id)
                if query_index < len(queries_data):
                    query_data = queries_data[query_index]
                    cache_key = query_data.get('cache_key')
                    ttl = query_data.get('ttl', 300)
                    
                    if cache_key:
                        cache = cache_manager.get_graphql_cache()
                        cache.put(cache_key, data, ttl)
                        increment_counter("graphql_batch_cached")
                
                request_mapping[request_id] = data
                
            except Exception as e:
                logger.error(f"Error processing batch GraphQL response {request_id}: {e}")
                request_mapping[request_id] = None
    
    stop_timer(timer_id)
    
    successful = len([v for v in request_mapping.values() if v is not None])
    logger.info(f"Batch GraphQL fetch completed: {successful}/{len(queries_data)} successful")
    
    return request_mapping
