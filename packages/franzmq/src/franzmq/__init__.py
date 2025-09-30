from importlib.metadata import entry_points

def load_extensions():
    eps = entry_points(group="franzmq.extensions")
    return [ep.load()() for ep in eps]
