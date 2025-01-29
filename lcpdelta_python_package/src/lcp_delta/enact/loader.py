import os
import importlib

def load_endpoints():
    env = os.getenv("ENACT_ENV", "default")  # Read from an environment variable
    print(env)
    module_name = f".configs.endpoints_{env}" if env != "default" else ".endpoints"

    try:
        return importlib.import_module(module_name, package=__package__)
    except ModuleNotFoundError:
        raise ImportError(f"Could not load endpoint configuration: {module_name}")

endpoints = load_endpoints()
