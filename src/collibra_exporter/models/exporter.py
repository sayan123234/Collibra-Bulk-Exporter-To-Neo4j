"""
Neo4j Exporter Module

This module provides functionality for exporting flattened Collibra data to Neo4j database.
"""

import os
import logging
import threading
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
from dotenv import load_dotenv
import re
from contextlib import contextmanager
from ..utils.performance_monitor import start_timer, stop_timer, increment_counter

logger = logging.getLogger(__name__)

class Neo4jConnectionPool:
    """Thread-safe Neo4j connection pool manager."""
    
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
            self.drivers = {}
            self.driver_lock = threading.Lock()
            self.initialized = True
    
    def get_driver(self, uri: str, username: str, password: str, database: str = "neo4j"):
        """
        Get or create a Neo4j driver with connection pooling.
        
        Args:
            uri: Neo4j connection URI
            username: Neo4j username
            password: Neo4j password
            database: Neo4j database name
            
        Returns:
            Neo4j driver instance
        """
        driver_key = f"{uri}:{username}:{database}"
        
        with self.driver_lock:
            if driver_key not in self.drivers:
                logger.info(f"Creating new Neo4j driver for {uri}")
                increment_counter("neo4j_drivers_created")
                
                self.drivers[driver_key] = GraphDatabase.driver(
                    uri, 
                    auth=(username, password),
                    max_connection_lifetime=3600,  # 1 hour
                    max_connection_pool_size=50,   # Increased pool size
                    connection_acquisition_timeout=60,  # 60 seconds timeout
                    keep_alive=True
                )
                logger.info(f"Neo4j driver created with connection pooling")
            else:
                logger.debug(f"Reusing existing Neo4j driver for {uri}")
                increment_counter("neo4j_drivers_reused")
        
        return self.drivers[driver_key], database
    
    def close_all(self):
        """Close all drivers in the pool."""
        with self.driver_lock:
            for driver_key, driver in self.drivers.items():
                try:
                    driver.close()
                    logger.info(f"Closed Neo4j driver: {driver_key}")
                except Exception as e:
                    logger.warning(f"Error closing driver {driver_key}: {e}")
            self.drivers.clear()
    
    def __del__(self):
        """Cleanup when the pool is destroyed."""
        self.close_all()

class Neo4jExporter:
    """Handles exporting flattened Collibra data to Neo4j database with connection pooling."""
    
    def __init__(self, uri: str, username: str, password: str, database: str = "neo4j"):
        """
        Initialize Neo4j connection using connection pool.
        
        Args:
            uri: Neo4j connection URI
            username: Neo4j username
            password: Neo4j password
            database: Neo4j database name
        """
        self.pool = Neo4jConnectionPool()
        self.driver, self.database = self.pool.get_driver(uri, username, password, database)
        self.uri = uri
        self.username = username
        self.password = password
        
    def close(self):
        """Connection pool manages connections, so this is a no-op."""
        # Connection pool manages the lifecycle, so we don't close individual connections
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @contextmanager
    def get_session(self):
        """
        Get a database session with proper resource management.
        
        Yields:
            Neo4j session
        """
        session = None
        try:
            session = self.driver.session(database=self.database)
            yield session
        except Exception as e:
            logger.error(f"Error with Neo4j session: {e}")
            raise
        finally:
            if session:
                session.close()
    
    def _sanitize_property_name(self, name: str) -> str:
        """
        Sanitize property names for Neo4j compatibility.
        
        Args:
            name: Original property name
            
        Returns:
            str: Sanitized property name
        """
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r'[^\w]', '_', name)
        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        return sanitized
    
    def _extract_relation_info(self, property_name: str) -> Optional[tuple]:
        """
        Extract relation information from property name using the pattern:
        {asset_type_name}__{role_type}__{target_type}
        
        Args:
            property_name: Property name containing relation information
            
        Returns:
            tuple or None: (role_type, target_node_type) if relation property
        """
        # Split by double underscores
        parts = property_name.split('__')
        
        if len(parts) == 3:
            # parts[0] = asset_type_name (source)
            # parts[1] = role_type (relationship)
            # parts[2] = target_type (target node type)
            role_type = parts[1]
            target_type = parts[2]
            return role_type, target_type
        
        return None
    
    def _is_relation_property(self, property_name: str) -> bool:
        """
        Check if a property represents a relation by looking for the pattern:
        {asset_type_name}__{role_type}__{target_type}_Full Name
        
        Args:
            property_name: Property name to check
            
        Returns:
            bool: True if it's a relation property
        """
        # Check if it ends with "_Full Name" and has the double underscore pattern
        if property_name.endswith("_Full Name"):
            # Remove "_Full Name" and check if base has the relation pattern
            base_property = property_name[:-10]  # Remove "_Full Name"
            relation_info = self._extract_relation_info(base_property)
            return relation_info is not None
        return False
    
    def _parse_responsibilities(self, flattened_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse responsibility data from flattened dictionary.
        
        Args:
            flattened_data: Flattened asset data
            
        Returns:
            List of responsibility dictionaries
        """
        responsibilities = []
        
        # Find responsibility keys (case-insensitive)
        user_name_key = None
        user_role_key = None
        user_email_key = None
        
        for key in flattened_data.keys():
            key_lower = key.lower()
            if 'user name against' in key_lower:
                user_name_key = key
            elif 'user role against' in key_lower:
                user_role_key = key
            elif 'user email against' in key_lower:
                user_email_key = key
        
        if user_name_key and user_role_key and user_email_key:
            names_str = flattened_data.get(user_name_key, '')
            roles_str = flattened_data.get(user_role_key, '')
            emails_str = flattened_data.get(user_email_key, '')
            
            # Handle None values
            if names_str is None:
                names_str = ''
            if roles_str is None:
                roles_str = ''
            if emails_str is None:
                emails_str = ''
            
            names = [name.strip() for name in str(names_str).split(',') if name.strip()]
            roles = [role.strip() for role in str(roles_str).split(',') if role.strip()]
            emails = [email.strip() for email in str(emails_str).split(',') if email.strip()]
            
            # Ensure all lists have the same length
            max_len = max(len(names), len(roles), len(emails)) if any([names, roles, emails]) else 0
            names.extend([''] * (max_len - len(names)))
            roles.extend([''] * (max_len - len(roles)))
            emails.extend([''] * (max_len - len(emails)))
            
            for name, role, email in zip(names, roles, emails):
                if name.strip() or role.strip() or email.strip():
                    responsibilities.append({
                        'name': name.strip(),
                        'role': role.strip(),
                        'email': email.strip()
                    })
        
        return responsibilities
    
    def _create_or_update_node(self, tx, node_name: str, node_label: str, properties: Dict[str, Any]):
        """
        Create or update a node in Neo4j.
        
        Args:
            tx: Neo4j transaction
            node_name: Name of the node (used as unique identifier)
            node_label: Label for the node
            properties: Properties to set on the node
        """
        # Sanitize the label
        sanitized_label = self._sanitize_property_name(node_label)
        
        # Sanitize property names and filter out None/empty values
        sanitized_properties = {}
        for k, v in properties.items():
            if v is not None and str(v).strip():
                sanitized_key = self._sanitize_property_name(k)
                sanitized_properties[sanitized_key] = v
        
        # Always include the name property
        sanitized_properties['name'] = node_name
        
        # Create or update node - only add new properties, don't overwrite existing ones
        query = f"""
        MERGE (n:{sanitized_label} {{name: $node_name}})
        SET n += $properties
        """
        
        try:
            tx.run(query, node_name=node_name, properties=sanitized_properties)
            logger.debug(f"Created/updated node: {node_name} with label: {sanitized_label}")
        except Exception as e:
            logger.error(f"Error creating/updating node {node_name}: {str(e)}")
            raise
    
    def _create_relationship(self, tx, source_node: str, source_label: str, 
                          target_node: str, target_label: str, relationship_type: str):
        """
        Create a relationship between two nodes if it doesn't already exist.
        
        Args:
            tx: Neo4j transaction
            source_node: Source node name
            source_label: Source node label
            target_node: Target node name
            target_label: Target node label
            relationship_type: Type of relationship
        """
        # Sanitize labels and relationship type
        sanitized_source_label = self._sanitize_property_name(source_label)
        sanitized_target_label = self._sanitize_property_name(target_label)
        sanitized_rel_type = self._sanitize_property_name(relationship_type).upper()
        
        # First ensure both nodes exist
        try:
            # Create source node if it doesn't exist
            tx.run(f"MERGE (n:{sanitized_source_label} {{name: $name}})", name=source_node)
            # Create target node if it doesn't exist  
            tx.run(f"MERGE (n:{sanitized_target_label} {{name: $name}})", name=target_node)
        except Exception as e:
            logger.warning(f"Could not ensure nodes exist: {str(e)}")
        
        # Create relationship if it doesn't exist
        query = f"""
        MATCH (source:{sanitized_source_label} {{name: $source_node}})
        MATCH (target:{sanitized_target_label} {{name: $target_node}})
        MERGE (source)-[r:{sanitized_rel_type}]->(target)
        """
        
        try:
            tx.run(query, source_node=source_node, target_node=target_node)
            logger.debug(f"Created relationship: {source_node} -[{sanitized_rel_type}]-> {target_node}")
        except Exception as e:
            logger.warning(f"Could not create relationship {source_node} -[{sanitized_rel_type}]-> {target_node}: {str(e)}")
    
    def _create_user_relationships(self, tx, main_node_name: str, main_node_label: str, 
                                 responsibilities: List[Dict[str, Any]]):
        """
        Create relationships between main node and user nodes based on responsibilities.
        
        Args:
            tx: Neo4j transaction
            main_node_name: Name of the main asset node
            main_node_label: Label of the main asset node
            responsibilities: List of responsibility data
        """
        for resp in responsibilities:
            if resp['name']:
                # Create or update user node
                user_properties = {
                    'name': resp['name']
                }
                if resp['email']:
                    user_properties['email'] = resp['email']
                
                self._create_or_update_node(tx, resp['name'], 'User', user_properties)
                
                # Create relationship with role as relationship type
                if resp['role']:
                    self._create_relationship(
                        tx, resp['name'], 'User',
                        main_node_name, main_node_label, resp['role']
                    )
    
    def export_to_neo4j(self, flattened_data: Dict[str, Any], asset_type_name: str) -> bool:
        """
        Export flattened data to Neo4j database.
        
        Args:
            flattened_data: Flattened asset data dictionary
            asset_type_name: Name of the asset type
            
        Returns:
            bool: True if export successful, False otherwise
        """
        try:
            with self.get_session() as session:
                return session.execute_write(self._export_transaction, flattened_data, asset_type_name)
        except Exception as e:
            logger.error(f"Error exporting to Neo4j: {str(e)}")
            return False
    
    def _export_transaction(self, tx, flattened_data: Dict[str, Any], asset_type_name: str) -> bool:
        """
        Execute the export transaction.
        
        Args:
            tx: Neo4j transaction
            flattened_data: Flattened asset data
            asset_type_name: Asset type name
            
        Returns:
            bool: True if successful
        """
        try:
            # Get the main node name (Full Name)
            full_name_key = f"{asset_type_name} Full Name"
            main_node_name = flattened_data.get(full_name_key)
            
            if not main_node_name:
                logger.error(f"No full name found for asset type: {asset_type_name}")
                return False
            
            # Separate properties and relations
            node_properties = {}
            relation_full_names = {}
            
            for key, value in flattened_data.items():
                if value is None or str(value).strip() == '':
                    continue
                
                # Skip responsibility properties - these should not be node properties
                key_lower = key.lower()
                if ('user name against' in key_lower or 
                    'user role against' in key_lower or 
                    'user email against' in key_lower):
                    continue
                
                # Check if this is a relation property ending with "_Full Name"
                if self._is_relation_property(key):
                    # This contains the actual node names for the relation
                    base_key = key[:-10]  # Remove "_Full Name"
                    relation_full_names[base_key] = value
                elif key.endswith(" Full Name") or key.endswith("_Full Name"):
                    # This is a Full Name property but not a relation (like the main node's Full Name)
                    continue
                else:
                    # Check if this property has a corresponding "_Full Name" property
                    full_name_key_check = f"{key}_Full Name"
                    if full_name_key_check in flattened_data:
                        # This is a relation property, skip it (we'll use the Full Name version)
                        continue
                    else:
                        # Regular node property
                        node_properties[key] = value
            
            # Create or update the main node
            self._create_or_update_node(tx, main_node_name, asset_type_name, node_properties)
            
            # Process relations
            for relation_key, target_names_str in relation_full_names.items():
                relation_info = self._extract_relation_info(relation_key)
                if relation_info:
                    role_type, target_node_type = relation_info
                    
                    if target_names_str:  # Only process if not None or empty
                        # Split target names by comma and process each
                        target_names = [name.strip() for name in str(target_names_str).split(',') if name.strip()]
                        
                        for target_name in target_names:
                            # Create target node with minimal properties (just name)
                            self._create_or_update_node(tx, target_name, target_node_type, {'name': target_name})
                            
                            # Create relationship from main node to target node
                            self._create_relationship(
                                tx, main_node_name, asset_type_name,
                                target_name, target_node_type, role_type
                            )
            
            # Process responsibilities (user relationships)
            responsibilities = self._parse_responsibilities(flattened_data)
            if responsibilities:
                self._create_user_relationships(tx, main_node_name, asset_type_name, responsibilities)
            
            logger.info(f"Successfully exported {main_node_name} to Neo4j")
            return True
            
        except Exception as e:
            logger.error(f"Error in export transaction: {str(e)}")
            return False
    
    def export_batch_to_neo4j(self, flattened_batch: List[Dict[str, Any]], asset_type_name: str) -> tuple:
        """
        Export a batch of flattened data to Neo4j database in a single transaction.
        
        Args:
            flattened_batch: List of flattened asset data dictionaries
            asset_type_name: Name of the asset type
            
        Returns:
            tuple: (successful_exports, failed_exports)
        """
        if not flattened_batch:
            return 0, 0
            
        try:
            with self.get_session() as session:
                return session.execute_write(self._export_batch_transaction, flattened_batch, asset_type_name)
        except Exception as e:
            logger.error(f"Error exporting batch to Neo4j: {str(e)}")
            return 0, len(flattened_batch)
    
    def _export_batch_transaction(self, tx, flattened_batch: List[Dict[str, Any]], asset_type_name: str) -> tuple:
        """
        Execute the batch export transaction.
        
        Args:
            tx: Neo4j transaction
            flattened_batch: List of flattened asset data
            asset_type_name: Asset type name
            
        Returns:
            tuple: (successful_exports, failed_exports)
        """
        successful_exports = 0
        failed_exports = 0
        
        logger.info(f"Starting batch export of {len(flattened_batch)} assets for {asset_type_name}")
        
        for idx, flattened_data in enumerate(flattened_batch, 1):
            try:
                if self._export_single_asset_in_transaction(tx, flattened_data, asset_type_name):
                    successful_exports += 1
                    logger.debug(f"[{idx}/{len(flattened_batch)}] Successfully exported asset in batch")
                else:
                    failed_exports += 1
                    logger.debug(f"[{idx}/{len(flattened_batch)}] Failed to export asset in batch")
            except Exception as e:
                failed_exports += 1
                logger.error(f"[{idx}/{len(flattened_batch)}] Error in batch export: {str(e)}")
        
        logger.info(f"Batch export completed - Success: {successful_exports}, Failed: {failed_exports}")
        return successful_exports, failed_exports
    
    def _export_single_asset_in_transaction(self, tx, flattened_data: Dict[str, Any], asset_type_name: str) -> bool:
        """
        Export a single asset within an existing transaction.
        
        Args:
            tx: Neo4j transaction
            flattened_data: Flattened asset data
            asset_type_name: Asset type name
            
        Returns:
            bool: True if successful
        """
        try:
            # Get the main node name (Full Name)
            full_name_key = f"{asset_type_name} Full Name"
            main_node_name = flattened_data.get(full_name_key)
            
            if not main_node_name:
                logger.error(f"No full name found for asset type: {asset_type_name}")
                return False
            
            # Separate properties and relations
            node_properties = {}
            relation_full_names = {}
            
            for key, value in flattened_data.items():
                if value is None or str(value).strip() == '':
                    continue
                
                # Skip responsibility properties - these should not be node properties
                key_lower = key.lower()
                if ('user name against' in key_lower or 
                    'user role against' in key_lower or 
                    'user email against' in key_lower):
                    continue
                
                # Check if this is a relation property ending with "_Full Name"
                if self._is_relation_property(key):
                    # This contains the actual node names for the relation
                    base_key = key[:-10]  # Remove "_Full Name"
                    relation_full_names[base_key] = value
                elif key.endswith(" Full Name") or key.endswith("_Full Name"):
                    # This is a Full Name property but not a relation (like the main node's Full Name)
                    continue
                else:
                    # Check if this property has a corresponding "_Full Name" property
                    full_name_key_check = f"{key}_Full Name"
                    if full_name_key_check in flattened_data:
                        # This is a relation property, skip it (we'll use the Full Name version)
                        continue
                    else:
                        # Regular node property
                        node_properties[key] = value
            
            # Create or update the main node
            self._create_or_update_node(tx, main_node_name, asset_type_name, node_properties)
            
            # Process relations
            for relation_key, target_names_str in relation_full_names.items():
                relation_info = self._extract_relation_info(relation_key)
                if relation_info:
                    role_type, target_node_type = relation_info
                    
                    if target_names_str:  # Only process if not None or empty
                        # Split target names by comma and process each
                        target_names = [name.strip() for name in str(target_names_str).split(',') if name.strip()]
                        
                        for target_name in target_names:
                            # Create target node with minimal properties (just name)
                            self._create_or_update_node(tx, target_name, target_node_type, {'name': target_name})
                            
                            # Create relationship from main node to target node
                            self._create_relationship(
                                tx, main_node_name, asset_type_name,
                                target_name, target_node_type, role_type
                            )
            
            # Process responsibilities (user relationships)
            responsibilities = self._parse_responsibilities(flattened_data)
            if responsibilities:
                self._create_user_relationships(tx, main_node_name, asset_type_name, responsibilities)
            
            return True
            
        except Exception as e:
            logger.error(f"Error in single asset export transaction: {str(e)}")
            return False

    def test_connection(self) -> bool:
        """
        Test the Neo4j database connection.
        
        Returns:
            bool: True if connection successful
        """
        try:
            with self.get_session() as session:
                result = session.run("RETURN 1 as test")
                return result.single()["test"] == 1
        except Exception as e:
            logger.error(f"Neo4j connection test failed: {str(e)}")
            return False


def create_neo4j_exporter_from_env() -> Neo4jExporter:
    """
    Create Neo4j exporter instance from environment variables.
    
    Returns:
        Neo4jExporter: Configured exporter instance
    """
    load_dotenv()
    
    uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    username = os.getenv('NEO4J_USERNAME', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'password')
    database = os.getenv('NEO4J_DATABASE', 'neo4j')
    
    return Neo4jExporter(uri, username, password, database)


# Example usage and integration with existing code
def export_flattened_data_to_neo4j(flattened_data: Dict[str, Any], asset_type_name: str) -> bool:
    """
    Export flattened data to Neo4j database.
    
    Args:
        flattened_data: Flattened asset data dictionary
        asset_type_name: Name of the asset type
        
    Returns:
        bool: True if export successful
    """
    try:
        with create_neo4j_exporter_from_env() as exporter:
            # Test connection first
            if not exporter.test_connection():
                logger.error("Failed to connect to Neo4j database")
                return False
            
            # Export the data
            return exporter.export_to_neo4j(flattened_data, asset_type_name)
            
    except Exception as e:
        logger.error(f"Error exporting to Neo4j: {str(e)}")
        return False


def export_batch_to_neo4j(flattened_batch: List[Dict[str, Any]], asset_type_name: str) -> tuple:
    """
    Export a batch of flattened data to Neo4j database in a single transaction.
    
    Args:
        flattened_batch: List of flattened asset data dictionaries
        asset_type_name: Name of the asset type
        
    Returns:
        tuple: (successful_exports, failed_exports)
    """
    if not flattened_batch:
        return 0, 0
        
    try:
        with create_neo4j_exporter_from_env() as exporter:
            # Test connection first
            if not exporter.test_connection():
                logger.error("Failed to connect to Neo4j database")
                return 0, len(flattened_batch)
            
            # Export the batch
            return exporter.export_batch_to_neo4j(flattened_batch, asset_type_name)
            
    except Exception as e:
        logger.error(f"Error exporting batch to Neo4j: {str(e)}")
        return 0, len(flattened_batch)


# Integration function to be called from your existing transformer
def integrate_neo4j_export(asset, asset_type_name):
    """
    Integration function that flattens JSON and exports to Neo4j.
    
    Args:
        asset: The nested asset JSON structure
        asset_type_name: The name of the asset type
        
    Returns:
        bool: True if export successful
    """
    # Import the flatten_json function from your transformer module
    from transformer import flatten_json
    
    # Flatten the JSON data
    flattened_data = flatten_json(asset, asset_type_name)
    
    # Export to Neo4j
    return export_flattened_data_to_neo4j(flattened_data, asset_type_name)