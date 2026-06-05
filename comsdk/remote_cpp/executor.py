import threading

from comsdk.remote_cpp.client import RemoteCppClient, normalize_workers


DEFAULT_MAX_POINTS = 1_000_000

_client_lock = threading.Lock()
_client_cache = {}


def get_remote_cpp_client(data):
    workers = tuple(normalize_workers(data.get("cpp_workers", [])))
    timeout_ms = int(data.get("timeout_ms", 3000))
    retries = int(data.get("retries", 1))
    load_balancing = data.get("load_balancing", "round_robin")
    encoding = data.get("cpp_encoding", "binary-f64")
    max_points = int(data.get("cpp_max_points", DEFAULT_MAX_POINTS))

    key = (
        workers,
        timeout_ms,
        retries,
        load_balancing,
        encoding,
        max_points,
    )

    with _client_lock:
        if key not in _client_cache:
            _client_cache[key] = RemoteCppClient(
                workers=workers,
                timeout_ms=timeout_ms,
                retries=retries,
                load_balancing=load_balancing,
                encoding=encoding,
                max_points=max_points,
            )

        return _client_cache[key]


def execute_remote_cpp(data, operation, input_key, output_key):
    if input_key not in data:
        raise KeyError(
            f"Remote C++ operation '{operation}' requires input key '{input_key}'"
        )

    client = get_remote_cpp_client(data)
    data[output_key] = client.compute(operation, data[input_key])
    return data
