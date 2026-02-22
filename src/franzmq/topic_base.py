import sys
from typing import List, Union

from franzmq.topic import Topic
from franzmq.data_contracts.base import Payload, Metric


class classproperty:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        return self.func(owner)


def _snake_to_pascal(snake_str: str) -> str:
    return ''.join(word.capitalize() for word in snake_str.split('_'))


class TopicBase(Topic):
    """Base class for hierarchical topic building.

    Projects extend this to define their own topic hierarchies using
    ``classproperty`` descriptors and ``_topic()`` composition.
    """
    _parent_class_name: str = None
    _prefix: List[str] = None

    @classmethod
    def _get_parent_class(cls):
        if cls._parent_class_name:
            module = sys.modules[cls.__module__]
            return getattr(module, cls._parent_class_name)
        return None

    @classmethod
    def _topic(cls, context_suffix: List[Union[str, int]], payload_type: type[Payload] = Metric):
        parent_class = cls._get_parent_class()
        if parent_class and cls._prefix is not None:
            return parent_class._topic(cls._prefix + list(context_suffix), payload_type)
        return Topic(
            prefix=cls.prefix,
            version=cls.version,
            payload_type=payload_type,
            context=cls.context + tuple(str(c) for c in context_suffix)
        )

    @classmethod
    def get_from_snake_case(cls, key: str, forbidden_keys: List[str] = None) -> Topic:
        forbidden_keys = forbidden_keys or []
        if key in forbidden_keys:
            raise ValueError(f"Cannot use get_from_snake_case for {key}. Use the appropriate method instead.")
        pascal_name = _snake_to_pascal(key)
        if pascal_name not in cls.__dict__:
            raise AttributeError(f"'{cls.__name__}' has no attribute '{pascal_name}' (from snake_case '{key}')")
        attr = cls.__dict__[pascal_name]
        if not isinstance(attr, classproperty):
            raise AttributeError(f"'{pascal_name}' is not a classproperty of {cls.__name__} (from snake_case '{key}')")
        return getattr(cls, pascal_name)
