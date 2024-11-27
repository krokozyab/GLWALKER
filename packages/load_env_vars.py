import os
import sys

from dotenv import load_dotenv


def load_environment_variables():
    """
    Loads environment variables from a .env file.
    """
    load_dotenv()  # Load variables from .env into environment


def get_env_variable(var_name, required=True):
    """
    Retrieves an environment variable and handles missing variables.

    Parameters:
    - var_name (str): The name of the environment variable.
    - required (bool): Whether the variable is required.

    Returns:
    - str: The value of the environment variable.

    Raises:
    - SystemExit: If a required environment variable is missing.
    """
    value = os.getenv(var_name)
    if required and not value:
        print(f"Error: The environment variable '{var_name}' is missing.")
        sys.exit(1)
    return value
