from pikachu.api_utils import is_server


def assert_cuda_is_available():
    import torch

    if is_server():
        assert torch.cuda.is_available()


def device():
    """
    We require of server to have CUDA.
    This way we avoid running on CPU when CUDA is not available, what happens frequently due to https://github.com/NVIDIA/nvidia-docker/issues/1671.
    """
    import torch

    use_cuda = is_server() or torch.cuda.is_available()
    return "cuda" if use_cuda else "cpu"


def dtype():
    import torch

    return torch.float16 if device() == "cuda" else torch.float32
