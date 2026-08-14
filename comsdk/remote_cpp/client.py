import array
import json
import logging
import re
import sys
import threading
import time
import urllib.request

logger = logging.getLogger(__name__)
NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
OPERATION_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def safe_len(value):
    try:
        return len(value)
    except TypeError:
        return None


def _to_float_or_none(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int_or_none(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def normalize_worker_url(value):
    return str(value).strip().strip('"').strip("'")


def normalize_workers(workers):
    if workers is None:
        return []

    if isinstance(workers, str):
        value = workers.strip()

        if value.startswith("{") and value.endswith("}"):
            value = value[1:-1]

        if value.startswith("[") and value.endswith("]"):
            value = value[1:-1]

        raw_items = value.replace(",", ";").split(";")
    else:
        raw_items = workers

    return [
        normalize_worker_url(item)
        for item in raw_items
        if normalize_worker_url(item)
    ]


def to_float64_array(values):
    if isinstance(values, array.array) and values.typecode == "d":
        return values

    return array.array("d", values)


def to_little_endian_float64_bytes(values):
    values_array = to_float64_array(values)
    if sys.byteorder == "little":
        return values_array.tobytes(), len(values_array)

    little_endian = array.array("d", values_array)
    little_endian.byteswap()
    return little_endian.tobytes(), len(little_endian)


def from_little_endian_float64_bytes(body):
    if len(body) % 8 != 0:
        raise RuntimeError(f"Invalid binary C++ worker response size: {len(body)}")

    result = array.array("d")
    result.frombytes(body)

    if sys.byteorder != "little":
        result.byteswap()

    return result


class RemoteCppClient:
    def __init__(
        self,
        workers,
        timeout_ms=3000,
        retries=1,
        load_balancing="round_robin",
        encoding="binary-f64",
        max_points=1_000_000,
    ):
        self.workers = normalize_workers(workers)
        self.timeout = timeout_ms / 1000
        self.retries = retries
        self.load_balancing = load_balancing
        self.encoding = encoding
        self.max_points = int(max_points) if max_points is not None else None
        self._lock = threading.Lock()
        self._thread_local = threading.local()
        self._rr_index = 0
        self._in_flight = {worker: 0 for worker in self.workers}
        logger.debug("C++ workers normalized: %s", self.workers)

    def get_last_response_metadata(self):
        return getattr(self._thread_local, "response_metadata", {})

    def _set_last_response_metadata(self, metadata):
        self._thread_local.response_metadata = {
            key: value
            for key, value in (metadata or {}).items()
            if value is not None and value != ""
        }

    def compute(self, operation, x):
        operation = str(operation).strip()
        if not OPERATION_RE.fullmatch(operation):
            raise ValueError(f"Invalid remote C++ operation name: {operation!r}")

        last_error = None
        points_count = safe_len(x)
        if (
            self.max_points is not None
            and points_count is not None
            and points_count > self.max_points
        ):
            raise ValueError(
                f"Remote C++ request has {points_count} points, "
                f"which exceeds cpp_max_points={self.max_points}. "
                "Split the data into batches or use a data-plane reference."
            )

        attempts_count = self.retries + 1

        for attempt in range(1, attempts_count + 1):
            worker = self._reserve_worker()
            started_at = time.perf_counter()

            logger.info(
                "[CPP] start operation=%s worker=%s encoding=%s points=%s attempt=%s/%s timeout_s=%.3f",
                operation,
                worker,
                self.encoding,
                points_count,
                attempt,
                attempts_count,
                self.timeout,
            )

            try:
                result = self._send_compute(worker, operation, x)
                metadata = self.get_last_response_metadata()
                duration = time.perf_counter() - started_at
                logger.info(
                    "[CPP] done operation=%s worker=%s encoding=%s points=%s result_points=%s duration_s=%.3f attempt=%s/%s",
                    operation,
                    worker,
                    self.encoding,
                    points_count,
                    safe_len(result),
                    duration,
                    attempt,
                    attempts_count,
                )
                self._set_last_response_metadata({
                    **metadata,
                    "operation": metadata.get("operation") or operation,
                    "worker_id": metadata.get("worker_id") or worker,
                    "elapsed_ms": metadata.get("elapsed_ms") or duration * 1000.0,
                })
                return result
            except Exception as exc:
                duration = time.perf_counter() - started_at
                last_error = exc
                logger.warning(
                    "[CPP] error operation=%s worker=%s encoding=%s points=%s duration_s=%.3f attempt=%s/%s error=%s",
                    operation,
                    worker,
                    self.encoding,
                    points_count,
                    duration,
                    attempt,
                    attempts_count,
                    exc,
                )
            finally:
                self._mark_finished(worker)

        raise RuntimeError(f"Remote C++ compute failed: {last_error}")

    def compute_task(self, operation, payload):
        operation = str(operation).strip()
        if not OPERATION_RE.fullmatch(operation):
            raise ValueError(f"Invalid remote C++ operation name: {operation!r}")

        particles_count = None
        if isinstance(payload, dict):
            particles_count = safe_len(payload.get("particles", []))

        last_error = None
        attempts_count = self.retries + 1

        for attempt in range(1, attempts_count + 1):
            worker = self._reserve_worker()
            started_at = time.perf_counter()
            logger.info(
                "[CPP] task start operation=%s worker=%s load_balancing=%s particles=%s attempt=%s/%s timeout_s=%.3f",
                operation,
                worker,
                self.load_balancing,
                particles_count,
                attempt,
                attempts_count,
                self.timeout,
            )

            try:
                result = self._send_task_json(worker, operation, payload)
                if isinstance(result, dict):
                    self._set_last_response_metadata({
                        "operation": result.get("operation") or operation,
                        "worker_id": result.get("worker_id") or worker,
                        "elapsed_ms": result.get("elapsed_ms"),
                        "cpu_percent": result.get("cpu_percent"),
                        "memory_bytes": result.get("memory_bytes"),
                        "memory_mb": result.get("memory_mb"),
                    })
                duration = time.perf_counter() - started_at
                result_count = (
                    safe_len(result.get("results", []))
                    if isinstance(result, dict)
                    else None
                )
                logger.info(
                    "[CPP] task done operation=%s worker=%s load_balancing=%s particles=%s results=%s duration_s=%.3f attempt=%s/%s",
                    operation,
                    worker,
                    self.load_balancing,
                    particles_count,
                    result_count,
                    duration,
                    attempt,
                    attempts_count,
                )
                return result
            except Exception as exc:
                duration = time.perf_counter() - started_at
                last_error = exc
                logger.warning(
                    "[CPP] task error operation=%s worker=%s load_balancing=%s particles=%s duration_s=%.3f attempt=%s/%s error=%s",
                    operation,
                    worker,
                    self.load_balancing,
                    particles_count,
                    duration,
                    attempt,
                    attempts_count,
                    exc,
                )
            finally:
                self._mark_finished(worker)

        raise RuntimeError(f"Remote C++ task failed: {last_error}")

    def _send_task_json(self, worker, operation, payload):
        body = json.dumps({
            "operation": operation,
            "payload": payload,
        }).encode("utf-8")

        request = urllib.request.Request(
            f"{worker.rstrip('/')}/task",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with NO_PROXY_OPENER.open(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def _reserve_worker(self):
        if not self.workers:
            raise RuntimeError("No remote C++ workers configured")

        with self._lock:
            if self.load_balancing == "least_in_flight":
                worker = min(self.workers, key=lambda item: self._in_flight.get(item, 0))
            else:
                worker = self.workers[self._rr_index % len(self.workers)]
                self._rr_index += 1
            self._in_flight[worker] = self._in_flight.get(worker, 0) + 1
            return worker

    def _mark_finished(self, worker):
        with self._lock:
            self._in_flight[worker] = max(0, self._in_flight.get(worker, 0) - 1)

    def _send_compute(self, worker, operation, x):
        if self.encoding == "binary-f64":
            return self._send_compute_binary(worker, operation, x)
        if self.encoding == "json-f64":
            return self._send_compute_json(worker, operation, x)

        raise RuntimeError(f"Unsupported remote C++ encoding: {self.encoding}")

    def _send_compute_binary(self, worker, operation, x):
        body, _ = to_little_endian_float64_bytes(x)

        request = urllib.request.Request(
            f"{worker.rstrip('/')}/compute-binary/{operation}",
            data=body,
            headers={"Content-Type": "application/octet-stream"},
            method="POST",
        )

        with NO_PROXY_OPENER.open(request, timeout=self.timeout) as response:
            headers = response.info()
            self._set_last_response_metadata({
                "operation": headers.get("X-Operation") or operation,
                "worker_id": headers.get("X-Worker-Id") or worker,
                "elapsed_ms": _to_float_or_none(headers.get("X-Elapsed-Ms")),
                "cpu_percent": _to_float_or_none(headers.get("X-Cpu-Percent")),
                "memory_bytes": _to_int_or_none(headers.get("X-Memory-Bytes")),
                "memory_mb": _to_float_or_none(headers.get("X-Memory-Mb")),
            })
            return from_little_endian_float64_bytes(response.read())

    def _send_compute_json(self, worker, operation, x):
        body = json.dumps({
            "operation": operation,
            "x": list(x),
            "dtype": "float64",
            "encoding": "json-f64",
        }).encode("utf-8")

        request = urllib.request.Request(
            f"{worker.rstrip('/')}/compute",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with NO_PROXY_OPENER.open(request, timeout=self.timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))

        if "result" not in payload:
            raise RuntimeError(f"Invalid C++ worker response: {payload}")

        self._set_last_response_metadata({
            "operation": payload.get("operation") or operation,
            "worker_id": payload.get("worker_id"),
            "elapsed_ms": payload.get("elapsed_ms"),
            "cpu_percent": payload.get("cpu_percent"),
            "memory_bytes": payload.get("memory_bytes"),
            "memory_mb": payload.get("memory_mb"),
        })

        return payload["result"]
