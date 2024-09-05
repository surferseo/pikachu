def is_unknown_cuda_error(e):
    return _is_cuda_error(e) and not _is_oom_cuda_error(e)


def _is_cuda_error(e):
    return "CUDA" in str(e)


def _is_oom_cuda_error(e):
    return "CUDA out of memory" in str(e)
