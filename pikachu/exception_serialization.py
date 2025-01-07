"""
Exceptions raised in a worker process of a ProcessPool have to be serializable (and deserializable) to get passed back to the main process.
Otherwise, the pool can break completely.
Here is some logic for making sure all passed eceptions are serializable.
The NonserializableExceptionWrapper is raised and the Python's exception chaining logic makes sure the original exception info is included.
"""

import pickle


def ensure_exceptions_are_serializable(message_function, message_json, models):
    try:
        return message_function(message_json, models)
    except Exception as e:
        if not _is_serializable_and_deserializable(e):
            raise NonserializableExceptionWrapper() from e

        raise e


def _is_serializable_and_deserializable(obj):
    try:
        serialized_obj = pickle.dumps(obj)
        pickle.loads(serialized_obj)
        return True
    except (pickle.PicklingError, TypeError, AttributeError):
        return False


class NonserializableExceptionWrapper(Exception): ...
