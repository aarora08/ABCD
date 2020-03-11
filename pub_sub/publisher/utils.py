from dataclasses import _MISSING_TYPE
from typing import Any, Dict


def model_builder(args: Dict, tuple_type, additional=None) -> Any:
    """
    Automagically determine the arguments for a given dataclass and return an instantiated object.

    Parameters
    ----------
    args
    tuple_type
    additional

    Returns
    -------

    """
    data = dict()
    for field_name, field_obj in tuple_type.__dataclass_fields__.items():
        if not field_obj.init:
            continue
        if type(field_obj.default_factory) != _MISSING_TYPE:
            data[field_name] = field_obj.default_factory
        else:
            value = args.get(field_name, None)
            data[field_name] = (
                field_obj.type(value)
                if type(value) == str and type(value) != field_obj.type
                else value
            )
    if additional:
        for key, value in additional.items():
            data[key] = value
    return tuple_type(**data)
