import types
from dataclasses import dataclass as py_dataclass, field, fields, asdict, is_dataclass
import yaml

from .db import (
    store_object,
    find_objects,
    find_history,
    delete_all,
    load_object,
)

_SIMPLE_SERIALIZABLE_TYPES = (bool, str, int, float)


def dataclass(cls=None, keep_history=False):
    def _dataclass(cls, *args, **kwargs):
        cls = py_dataclass(cls, *args, **kwargs)

        class Wrapper(cls):
            __keep_history__ = keep_history

            @classmethod
            def is_complex(cls):
                return any(
                    f.type not in _SIMPLE_SERIALIZABLE_TYPES for f in fields(cls)
                )

            @classmethod
            def fields(cls):
                return fields(cls)

            @classmethod
            def id_fields(cls):
                return [f for f in fields(cls) if f.metadata.get("_id") is True]

            def id(self) -> str:
                id_attrs = {
                    f.name: getattr(self, f.name) for f in self.__class__.id_fields()
                }
                return "_".join(f"{k}={v}" for k, v in id_attrs.items())

            def serialize(self):
                return (
                    yaml.dump(asdict(self))
                    if self.__class__.is_complex()
                    else asdict(self)
                )

            @classmethod
            def deserialize(cls, raw_data):
                instance = (
                    cls(**yaml.safe_load(raw_data))
                    if isinstance(raw_data, str)
                    else cls(**raw_data)
                )
                # instance = cls(**raw_data)
                for f in fields(instance):
                    if is_dataclass(f.type):
                        setattr(
                            instance,
                            f.name,
                            f.type.deserialize(getattr(instance, f.name)),
                        )
                    elif isinstance(f.type, types.GenericAlias) and is_dataclass(
                        f.type.__args__[0]
                    ):
                        cls = f.type.__args__[0]
                        setattr(
                            instance,
                            f.name,
                            [cls.deserialize(v) for v in getattr(instance, f.name)],
                        )
                return instance

            @classmethod
            def collection_name(cls):
                return f"{cls.__module__}_{cls.__name__}"

            def save(self):
                store_object(self)

            @classmethod
            def load(cls, **kwargs):
                id_fields = [f.name for f in cls.id_fields()]
                for f in fields(cls):
                    if f.name not in kwargs and f.name not in id_fields:
                        kwargs[f.name] = None

                object_id = cls(**kwargs).id()
                return load_object(cls, object_id)

            @classmethod
            def find(cls, **kwargs):
                id_keys = [f.name for f in cls.id_fields()]
                if any(k not in id_keys for k in kwargs):
                    raise RuntimeError("Wrong find keys")
                return find_objects(cls, **kwargs)

            def history(self):
                return find_history(self)

            @classmethod
            def delete_all(cls):
                delete_all(cls)

        Wrapper.__name__ = cls.__name__
        Wrapper.__qualname__ = cls.__qualname__
        return Wrapper

    # See if we're being called as @dataclass or @dataclass().
    if cls is None:
        # We're called with parens.
        return _dataclass

    # We're called as @dataclass without parens.
    return _dataclass(cls)


def id_field(*args, **kwargs):
    return field(*args, **kwargs, metadata={"_id": True})
