import json
import enum
import inspect
import sys
import importlib
from pathlib import Path
from franzmq.data_contracts.base import Payload

# Monkey-patch JSONEncoder to handle enum serialization
_original_encoder = json.JSONEncoder.default


def _patched_encoder(self, o):
    if isinstance(o, enum.Enum):
        return str(o)
    return _original_encoder(self, o)


json.JSONEncoder.default = _patched_encoder

# Import all modules in this directory to ensure they're loaded
_current_dir = Path(__file__).parent
_module_name = __name__

for _py_file in _current_dir.glob("*.py"):
    if _py_file.name == "__init__.py":
        continue
    _mod_name = _py_file.stem
    try:
        importlib.import_module(f"{_module_name}.{_mod_name}")
    except ImportError:
        pass

# Discover all Payload classes from all loaded modules in this package
PAYLOAD_CLASSES = {}
for _loaded_module_name, _loaded_module in sys.modules.items():
    if _loaded_module_name.startswith(_module_name) and _loaded_module is not None:
        for _name, _obj in inspect.getmembers(_loaded_module, inspect.isclass):
            if (
                issubclass(_obj, Payload)
                and _obj != Payload
                and _obj.__module__ == _loaded_module_name
            ):
                identifier = _obj.get_identifier()
                PAYLOAD_CLASSES[identifier] = _obj
