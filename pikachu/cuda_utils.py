import os


def is_server():
    return os.getenv("ANSIBLE_ENV", "missing") in ["production", "staging"]


def assert_is_cuda_available():
    import torch

    if is_server():
        assert torch.cuda.is_available()


def is_oom_cuda_error(e):
    return "CUDA out of memory" in str(e)


def is_cuda_error(e):
    return "CUDA" in str(e)
