import requests
import os
import logging
import time
from dotenv import load_dotenv
from functools import lru_cache
# Removed module-level imports to avoid circular dependencies
# These will be imported locally where needed

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

session = requests.Session()

class OAuthTokenManager:
    def __init__(self):
        self._token = None
        self._expiration_time = 0
        # Add buffer time (30 seconds) to refresh before actual expiration
        self._refresh_buffer = 30

    def get_valid_token(self):
        """Get a valid OAuth token, refreshing if necessary."""
        # Import locally to avoid circular dependencies
        from ..utils.cache_manager import cache_manager
        from ..utils.performance_monitor import increment_counter
        
        current_time = time.time()
        
        # Check cache first
        auth_cache = cache_manager.get_auth_cache()
        cached_token = auth_cache.get("oauth_token")
        
        if cached_token and cached_token.get('expiration_time', 0) > (current_time + self._refresh_buffer):
            increment_counter("auth_cache_hits")
            self._token = cached_token['token']
            self._expiration_time = cached_token['expiration_time']
            return self._token
        
        increment_counter("auth_cache_misses")
        
        # Check if token is expired or will expire soon
        if not self._token or current_time >= (self._expiration_time - self._refresh_buffer):
            self._fetch_new_token()
            
        return self._token

    def _fetch_new_token(self):
        """Fetch a new OAuth token from the server."""
        # Import locally to avoid circular dependencies
        from ..utils.cache_manager import cache_manager
        from ..utils.performance_monitor import increment_counter
        
        client_id = os.getenv('CLIENT_ID')
        client_secret = os.getenv('CLIENT_SECRET')
        base_url = os.getenv('COLLIBRA_INSTANCE_URL')
        
        url = f"https://{base_url}/rest/oauth/v2/token"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        payload = f'client_id={client_id}&grant_type=client_credentials&client_secret={client_secret}'
        
        try:
            response = session.post(url=url, data=payload, headers=headers)
            response.raise_for_status()
            token_data = response.json()
            
            self._token = token_data["access_token"]
            # Set expiration time based on server response
            self._expiration_time = time.time() + token_data["expires_in"]
            
            # Cache the token
            auth_cache = cache_manager.get_auth_cache()
            token_cache_data = {
                'token': self._token,
                'expiration_time': self._expiration_time
            }
            # Cache with TTL slightly less than actual expiration
            cache_ttl = token_data["expires_in"] - self._refresh_buffer - 10
            auth_cache.put("oauth_token", token_cache_data, ttl=cache_ttl)
            increment_counter("auth_tokens_cached")
            
            logging.info("Successfully obtained and cached new OAuth token")
            
        except requests.RequestException as e:
            logging.error(f"Error obtaining OAuth token: {e}")
            raise

# Create a singleton instance
token_manager = OAuthTokenManager()

def get_oauth_token():
    """Get a valid OAuth token."""
    return token_manager.get_valid_token()

def get_auth_header():
    """Get the authorization header with a valid token."""
    return {'Authorization': f'Bearer {get_oauth_token()}'}
