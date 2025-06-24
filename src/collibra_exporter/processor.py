"""
Data Processor Module

This module provides the core functionality for processing Collibra assets and exporting to Neo4j.
"""

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .api import fetch_data, fetch_nested_data
from .utils import get_asset_type_name
from .models import flatten_json
from .models.exporter import export_flattened_data_to_neo4j, export_batch_to_neo4j

logger = logging.getLogger(__name__)

def process_data(base_url, asset_type_id, limit=94, initial_nested_limit=50):
    """
    Process assets with optimized nested field handling.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: The ID of the asset type to process
        limit: Maximum number of assets to fetch per batch
        initial_nested_limit: Initial limit for nested fields
        
    Returns:
        list: A list of processed assets
    """
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info("="*60)
    logger.info(f"Starting data processing for asset type: {asset_type_name} (ID: {asset_type_id})")
    logger.info(f"Configuration - Batch Size: {limit}, Initial Nested Limit: {initial_nested_limit}")
    logger.info("="*60)
    
    all_assets = []
    paginate = None
    batch_count = 0
    start_time = time.time()

    while True:
        batch_count += 1
        batch_start_time = time.time()
        logger.info(f"\n[Batch {batch_count}] Starting new batch for {asset_type_name}")
        logger.debug(f"[Batch {batch_count}] Pagination token: {paginate}")
        
        # Get initial batch with small nested limits
        initial_response = fetch_data(
            base_url,
            asset_type_id, 
            paginate, 
            limit, 
            0, 
            initial_nested_limit
        )
        
        if not initial_response or 'data' not in initial_response or 'assets' not in initial_response['data']:
            logger.error(f"[Batch {batch_count}] Failed to fetch initial data")
            break

        current_assets = initial_response['data']['assets']
        if not current_assets:
            logger.info(f"[Batch {batch_count}] No more assets to fetch")
            break

        logger.info(f"[Batch {batch_count}] Processing {len(current_assets)} assets")

        # Process each asset
        processed_assets = []
        for asset_idx, asset in enumerate(current_assets, 1):
            asset_id = asset['id']
            asset_name = asset.get('displayName', 'Unknown Name')
            logger.info(f"\n[Batch {batch_count}][Asset {asset_idx}/{len(current_assets)}] Processing: {asset_name}")
            
            complete_asset = asset.copy()
            
            # Define nested fields to check
            nested_fields = [
                'stringAttributes',
                'multiValueAttributes',
                'numericAttributes',
                'dateAttributes',
                'booleanAttributes',
                'outgoingRelations',
                'incomingRelations',
                'responsibilities'
            ]

            # Check each nested field
            for field in nested_fields:
                if field not in asset:
                    continue
                    
                initial_data = asset[field]
                
                # If we hit the initial limit, fetch all data in one big query
                if len(initial_data) == initial_nested_limit:
                    logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] Requires full fetch")
                    
                    complete_data = fetch_nested_data(
                        base_url,
                        asset_type_id,
                        asset_id,
                        field
                    )
                    
                    if complete_data:
                        complete_asset[field] = complete_data
                        logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                  f"Retrieved {len(complete_data)} items")
                    else:
                        logger.warning(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                     f"Failed to fetch complete data, using initial data")
                        complete_asset[field] = initial_data
                else:
                    complete_asset[field] = initial_data

            processed_assets.append(complete_asset)
            logger.info(f"[Batch {batch_count}][Asset {asset_idx}] Completed processing")

        all_assets.extend(processed_assets)
        
        if len(current_assets) < limit:
            logger.info(f"[Batch {batch_count}] Retrieved fewer assets than limit, ending pagination")
            break
            
        paginate = current_assets[-1]['id']
        batch_time = time.time() - batch_start_time
        logger.info(f"\n[Batch {batch_count}] Completed batch in {batch_time:.2f}s")
        logger.info(f"Total assets processed so far: {len(all_assets)}")

    total_time = time.time() - start_time
    logger.info("\n" + "="*60)
    logger.info(f"[DONE] Completed processing {asset_type_name}")
    logger.info(f"Total assets processed: {len(all_assets)}")
    logger.info(f"Total batches processed: {batch_count}")
    logger.info(f"Total time taken: {total_time:.2f} seconds")
    avg_time = total_time/len(all_assets) if all_assets else 0
    logger.info(f"Average time per asset: {avg_time:.2f} seconds")
    logger.info("="*60)
    
    return all_assets

def process_data_streaming(base_url, asset_type_id, batch_size=10, limit=94, initial_nested_limit=50):
    """
    Stream process assets with optimized memory usage by yielding batches.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: The ID of the asset type to process
        batch_size: Number of assets to yield in each batch
        limit: Maximum number of assets to fetch per API call
        initial_nested_limit: Initial limit for nested fields
        
    Yields:
        list: Batches of processed assets
    """
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info("="*60)
    logger.info(f"Starting streaming data processing for asset type: {asset_type_name} (ID: {asset_type_id})")
    logger.info(f"Configuration - API Batch Size: {limit}, Neo4j Batch Size: {batch_size}, Initial Nested Limit: {initial_nested_limit}")
    logger.info("="*60)
    
    paginate = None
    api_batch_count = 0
    total_processed = 0
    current_batch = []

    while True:
        api_batch_count += 1
        batch_start_time = time.time()
        logger.info(f"\n[API Batch {api_batch_count}] Fetching new batch for {asset_type_name}")
        
        # Get initial batch with small nested limits
        initial_response = fetch_data(
            base_url,
            asset_type_id, 
            paginate, 
            limit, 
            0, 
            initial_nested_limit
        )
        
        if not initial_response or 'data' not in initial_response or 'assets' not in initial_response['data']:
            logger.error(f"[API Batch {api_batch_count}] Failed to fetch initial data")
            break

        current_assets = initial_response['data']['assets']
        if not current_assets:
            logger.info(f"[API Batch {api_batch_count}] No more assets to fetch")
            break

        logger.info(f"[API Batch {api_batch_count}] Processing {len(current_assets)} assets")

        # Process each asset in the API batch
        for asset_idx, asset in enumerate(current_assets, 1):
            asset_id = asset['id']
            asset_name = asset.get('displayName', 'Unknown Name')
            logger.debug(f"[API Batch {api_batch_count}][Asset {asset_idx}] Processing: {asset_name}")
            
            complete_asset = asset.copy()
            
            # Define nested fields to check
            nested_fields = [
                'stringAttributes',
                'multiValueAttributes', 
                'numericAttributes',
                'dateAttributes',
                'booleanAttributes',
                'outgoingRelations',
                'incomingRelations',
                'responsibilities'
            ]

            # Check each nested field
            for field in nested_fields:
                if field not in asset:
                    continue
                    
                initial_data = asset[field]
                
                # If we hit the initial limit, fetch all data
                if len(initial_data) == initial_nested_limit:
                    logger.debug(f"[API Batch {api_batch_count}][Asset {asset_idx}][{field}] Requires full fetch")
                    
                    complete_data = fetch_nested_data(
                        base_url,
                        asset_type_id,
                        asset_id,
                        field
                    )
                    
                    if complete_data:
                        complete_asset[field] = complete_data
                        logger.debug(f"[API Batch {api_batch_count}][Asset {asset_idx}][{field}] "
                                   f"Retrieved {len(complete_data)} items")
                    else:
                        logger.warning(f"[API Batch {api_batch_count}][Asset {asset_idx}][{field}] "
                                     f"Failed to fetch complete data, using initial data")
                        complete_asset[field] = initial_data
                else:
                    complete_asset[field] = initial_data

            current_batch.append(complete_asset)
            total_processed += 1
            
            # Yield batch when it reaches the desired size
            if len(current_batch) >= batch_size:
                logger.info(f"Yielding batch of {len(current_batch)} assets (Total processed: {total_processed})")
                yield current_batch
                current_batch = []

        # Check if we should continue pagination
        if len(current_assets) < limit:
            logger.info(f"[API Batch {api_batch_count}] Retrieved fewer assets than limit, ending pagination")
            break
            
        paginate = current_assets[-1]['id']
        batch_time = time.time() - batch_start_time
        logger.info(f"[API Batch {api_batch_count}] Completed in {batch_time:.2f}s")

    # Yield any remaining assets in the final batch
    if current_batch:
        logger.info(f"Yielding final batch of {len(current_batch)} assets (Total processed: {total_processed})")
        yield current_batch
    
    logger.info(f"Streaming processing completed for {asset_type_name}. Total assets: {total_processed}")

def process_asset_type(base_url, asset_type_id, batch_size=10):
    """
    Process a single asset type by ID and export to Neo4j with optimized batch processing.
    
    This function:
    1. Gets the asset type name
    2. Processes assets in streaming batches to reduce memory usage
    3. Flattens and exports assets in batches to Neo4j for better performance
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: The ID of the asset type to process
        batch_size: Number of assets to process in each Neo4j batch
        
    Returns:
        tuple: (processing_time, successful_exports, failed_exports)
    """
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info(f"Processing asset type: {asset_type_name} with batch size: {batch_size}")

    successful_exports = 0
    failed_exports = 0
    total_processed = 0

    # Use streaming processing instead of loading all assets into memory
    for asset_batch in process_data_streaming(base_url, asset_type_id, batch_size):
        if not asset_batch:
            continue
            
        logger.info(f"Processing batch of {len(asset_batch)} assets for {asset_type_name}")
        
        # Prepare flattened assets for batch export
        flattened_batch = []
        for asset in asset_batch:
            try:
                flattened_asset = flatten_json(asset, asset_type_name)
                flattened_batch.append(flattened_asset)
            except Exception as e:
                failed_exports += 1
                logger.error(f"Error flattening asset: {str(e)}")
        
        # Batch export to Neo4j
        if flattened_batch:
            batch_success, batch_failed = export_batch_to_neo4j(flattened_batch, asset_type_name)
            successful_exports += batch_success
            failed_exports += batch_failed
            total_processed += len(asset_batch)
            
            logger.info(f"Batch completed - Success: {batch_success}, Failed: {batch_failed}, "
                       f"Total processed: {total_processed}")

    end_time = time.time()
    elapsed_time = end_time - start_time

    if total_processed > 0:
        logger.info(f"Completed {asset_type_name}: {elapsed_time:.2f}s, "
                   f"Total processed: {total_processed}, Exported: {successful_exports}, Failed: {failed_exports}")
        return elapsed_time, successful_exports, failed_exports
    else:
        logger.critical(f"No assets found for asset type: {asset_type_name}")
        return 0, 0, 0

def process_all_asset_types(base_url, asset_type_ids, max_workers=5):
    """
    Process multiple asset types in parallel and export to Neo4j.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_ids: A list of asset type IDs to process
        max_workers: Maximum number of worker threads to use
        
    Returns:
        tuple: (total_successful_exports, total_failed_exports, total_time)
    """
    logger.info(f"Starting Collibra Bulk Exporter to Neo4j")
    logger.info(f"Number of asset types to process: {len(asset_type_ids)}")

    total_start_time = time.time()
    total_successful_exports = 0
    total_failed_exports = 0
    processed_asset_types = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_asset = {
            executor.submit(
                process_asset_type, 
                base_url, 
                asset_type_id
            ): asset_type_id for asset_type_id in asset_type_ids
        }
        
        for future in as_completed(future_to_asset):
            asset_type_id = future_to_asset[future]
            try:
                elapsed_time, successful_exports, failed_exports = future.result()
                total_successful_exports += successful_exports
                total_failed_exports += failed_exports
                
                if successful_exports > 0 or failed_exports > 0:
                    processed_asset_types += 1
                    logger.info(f"Asset type ID {asset_type_id}: "
                              f"Time: {elapsed_time:.2f}s, "
                              f"Exported: {successful_exports}, "
                              f"Failed: {failed_exports}")
                else:
                    logger.warning(f"No assets processed for asset type ID: {asset_type_id}")
                    
            except Exception as e:
                logger.exception(f"Error processing asset type ID {asset_type_id}: {str(e)}")
    
    total_end_time = time.time()
    total_time = total_end_time - total_start_time
    
    logger.info(f"\n" + "="*60)
    logger.info(f"EXPORT SUMMARY")
    logger.info(f"="*60)
    logger.info(f"Total asset types processed: {processed_asset_types}/{len(asset_type_ids)}")
    logger.info(f"Total successful exports to Neo4j: {total_successful_exports}")
    logger.info(f"Total failed exports: {total_failed_exports}")
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    logger.info(f"="*60)
    
    return total_successful_exports, total_failed_exports, total_time