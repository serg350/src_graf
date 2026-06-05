import array
import math
import threading
import time

from comsdk.remote_cpp.client import RemoteCppClient, normalize_workers


_client_lock = threading.Lock()
_client_cache = {}


def build_remote_client(data):
    workers = tuple(normalize_workers(data.get("cpp_workers", [])))
    timeout_ms = int(data.get("timeout_ms", 3000))
    retries = int(data.get("retries", 1))
    load_balancing = data.get("load_balancing", "round_robin")
    encoding = data.get("cpp_encoding", "binary-f64")

    key = (workers, timeout_ms, retries, load_balancing, encoding)

    with _client_lock:
        if key not in _client_cache:
            _client_cache[key] = RemoteCppClient(
                workers=workers,
                timeout_ms=timeout_ms,
                retries=retries,
                load_balancing=load_balancing,
                encoding=encoding,
            )

        return _client_cache[key]

def remote_compute(data, operation, result_key):
    client = build_remote_client(data)
    result = client.compute(operation, data.get("x", []))
    data[result_key] = result
    return data


def log_branch(name, phase):
    print(
        f"[{time.perf_counter():.6f}] "
        f"{name} {phase} "
        f"thread={threading.current_thread().name}"
    )


def always_true(data):
    return True


def prepare_input(data):
    x_start = float(data.get("X_start", 0.0))
    x_end = float(data.get("X_end", 1.0))
    points = int(data.get("points", 100))

    if points <= 1:
        data["x"] = array.array("d", [x_start])
        return data

    step = (x_end - x_start) / (points - 1)
    data["x"] = array.array("d", (x_start + i * step for i in range(points)))
    data["branch_results"] = {}

    return data


#def remote_sin_batch(data):
#    log_branch("SIN", "start")
#    #time.sleep(2)
#
#    x = data.get("x", [])
#    data["sin_result"] = [math.sin(value) for value in x]
#
#    log_branch("SIN", "end")
#    return data
#
#
#def remote_cos_batch(data):
#    log_branch("COS", "start")
#    #time.sleep(2)
#    x = data.get("x", [])
#    data["cos_result"] = [math.cos(value) for value in x]
#    log_branch("COS", "end")
#    return data
#
#
#def remote_exp_batch(data):
#    log_branch("EXP", "start")
#    #time.sleep(2)
#    x = data.get("x", [])
#    data["exp_result"]  = [math.exp(value) for value in x]
#    log_branch("EXP", "end")
#    return data


def remote_sin_batch(data):
    return remote_compute(data, "sin", "sin_result")


def remote_cos_batch(data):
    return remote_compute(data, "cos", "cos_result")


def remote_exp_batch(data):
    return remote_compute(data, "exp", "exp_result")


def merge_results(data):
    data["merged_result"] = {
        "x_count": len(data.get("x", [])),
        "has_sin": "sin_result" in data,
        "has_cos": "cos_result" in data,
        "has_exp": "exp_result" in data,
    }
    return data


def save_result(data):
    data["saved"] = True
    print("PARALLEL_CPP_MATH result:", data.get("merged_result"))
    return data
