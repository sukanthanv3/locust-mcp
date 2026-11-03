import os

def get_auth_token(env_var_name: str = "LOCUST_API_TOKEN") -> str:
    """Fetch token dynamically at execution time, not import time."""
    token = os.getenv(env_var_name)
    if not token:
        raise ValueError(
            f"Environment variable '{env_var_name}' is not set. "
            "Configure it in your MCP client settings."
        )
    return token