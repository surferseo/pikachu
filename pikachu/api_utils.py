import os


def is_server():
    return os.getenv("ANSIBLE_ENV", "missing") in ["production", "staging"]
