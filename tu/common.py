from tqdm import tqdm as tqdm_


def tqdm(*args, **kwargs):
    return tqdm_(*args, **kwargs, delay=5, disable=None)


STATUSES = ['running', 'allocating', 'success', 'failed', 'killed']
