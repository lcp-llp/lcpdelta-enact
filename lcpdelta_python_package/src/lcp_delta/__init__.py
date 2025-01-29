import importlib
import sys

module_prefix = __name__ + "."  # "lcp_delta."

submodules_to_reload = [
    mod for mod in sys.modules if mod.startswith(module_prefix) and sys.modules[mod]
]

for mod in submodules_to_reload:
    importlib.reload(sys.modules[mod])

from . import enact
