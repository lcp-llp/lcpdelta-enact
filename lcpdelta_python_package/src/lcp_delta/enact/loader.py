import os
import importlib

def get_base_endpoints():
    env = os.getenv("ENACT_ENV", "default")  # Read from an environment variable
    module_name = f".configs.base_endpoints_{env}"
    try:
        return importlib.import_module(module_name, package=__package__)
    except ModuleNotFoundError:
        raise ImportError(f"Could not load endpoint configuration: {module_name}")