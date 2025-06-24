#!/usr/bin/env python3
"""
Collibra Bulk Exporter to Neo4j

This script exports assets from Collibra based on asset type IDs and loads them into Neo4j.
"""

import os
import json
import sys
from dotenv import load_dotenv
from collibra_exporter import (
    setup_logging,
    process_all_asset_types,
    clear_all_caches,
    log_cache_stats,
    warm_caches,
    log_performance_summary,
    log_http_performance,
    cleanup_connections
)
from collibra_exporter.models.exporter import create_neo4j_exporter_from_env

def test_neo4j_connection():
    """Test Neo4j connection before starting the export process."""
    try:
        with create_neo4j_exporter_from_env() as exporter:
            if exporter.test_connection():
                print("[SUCCESS] Neo4j connection successful")
                return True
            else:
                print("[FAILURE] Neo4j connection failed")
                return False
    except Exception as e:
        print(f"[ERROR] Neo4j connection error: {str(e)}")
        return False

def main():
    """Main entry point for the Collibra Bulk Exporter."""
    # Setup logging
    logger = setup_logging()
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Get configuration from environment
        base_url = os.getenv('COLLIBRA_INSTANCE_URL')
        if not base_url:
            logger.error("COLLIBRA_INSTANCE_URL environment variable is not set")
            sys.exit(1)
        
        # Test Neo4j connection first
        logger.info("Testing Neo4j database connection...")
        if not test_neo4j_connection():
            logger.error("Failed to connect to Neo4j database. Please check your Neo4j configuration.")
            logger.error("Required environment variables: NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD")
            sys.exit(1)
        
        logger.info("Neo4j connection verified successfully")
        
        # Load asset type IDs from configuration file
        config_path = os.getenv('CONFIG_PATH', 'config/Collibra_Asset_Type_Id_Manager.json')
        try:
            with open(config_path, 'r') as file:
                config = json.load(file)
                asset_type_ids = config.get('ids', [])
                
            if not asset_type_ids:
                logger.error("No asset type IDs found in configuration file")
                sys.exit(1)
                
            logger.info(f"Loaded {len(asset_type_ids)} asset type IDs from {config_path}")
                
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading configuration file: {str(e)}")
            sys.exit(1)
        
        # Warm caches for better performance
        logger.info("Warming caches for optimal performance...")
        cache_warming_results = warm_caches(asset_type_ids)
        logger.info(f"Cache warming completed - Asset types: {cache_warming_results['asset_types_cached']}, "
                   f"Metadata: {cache_warming_results['metadata_cached']}")
        
        # Process all asset types and export to Neo4j
        logger.info("Starting Collibra to Neo4j export process...")
        successful_exports, failed_exports, total_time = process_all_asset_types(
            base_url,
            asset_type_ids
        )
        
        # Log final summary
        logger.info("\n" + "="*60)
        logger.info("FINAL EXPORT SUMMARY")
        logger.info("="*60)
        logger.info(f"Total assets successfully exported to Neo4j: {successful_exports}")
        logger.info(f"Total assets failed to export: {failed_exports}")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
        if successful_exports > 0:
            logger.info("Export completed successfully!")
            logger.info("Your Collibra data is now available in Neo4j database")
        else:
            logger.warning("⚠ No assets were successfully exported")
        
        logger.info("="*60)
        
        # Log performance, cache, and HTTP summaries
        log_performance_summary()
        log_cache_stats()
        log_http_performance()
        
        # Return exit code based on success/failure
        if failed_exports > 0 and successful_exports == 0:
            return 1  # Complete failure
        elif failed_exports > 0:
            return 2  # Partial failure
        return 0  # Success
        
    except KeyboardInterrupt:
        logger.info("Export process interrupted by user")
        return 1
    except Exception as e:
        logger.exception("Fatal error in main program")
        return 1
    finally:
        # Clean up all connections and caches before exit
        logger.info("Cleaning up connections and caches...")
        cleanup_connections()
        clear_all_caches()

if __name__ == "__main__":
    sys.exit(main())