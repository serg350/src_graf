# План перехода на gRPC + Arrow Flight

Цель: разделить команды и большие данные.

- gRPC передает команды: какую операцию выполнить, где лежат входные данные, куда положить результат, timeout, cancel, health.
- Arrow Flight передает массивы и таблицы: `float64` batches, stream upload/download, tickets, endpoints.
- Runtime графа хранит не большие массивы, а ссылки `DataRef`.

## 0. Что оставить на время миграции

Текущий HTTP `binary-f64` транспорт нужно оставить как fallback, пока новый транспорт не пройдет тесты:

- `comsdk/remote_cpp/client.py`
- `cpp_worker/main.cpp`
- `cpp_encoding=binary-f64`

Новый режим добавляем рядом как `transport=grpc-flight` или `cpp_transport=grpc-flight`.

## 1. Ввести DataRef

Сначала надо договориться о формате ссылки на данные. Минимальный вариант:

```json
{
  "id": "session/<session_id>/x",
  "flight_uri": "grpc://arrow-flight:8815",
  "ticket": "session/<session_id>/x",
  "format": "arrow",
  "dtype": "float64",
  "length": 10000000,
  "shape": [10000000]
}
```

Где менять:

- `test_funcs/remote_cpp_math.py`: `prepare_input` должен уметь создавать не только `x`, но и `x_ref`.
- `comsdk/graph.py`: event summarizer должен красиво показывать `DataRef`, не разворачивая массив.
- `comwpc/execution_history.py`: историю лучше хранить с кратким summary, а не с содержимым больших данных.

Критерий готовности: граф может пройти маленький тест, где `x` заменен на `x_ref`, даже если пока `DataRef` хранится локально в Python-словаре.

## 2. Добавить Python Data Plane интерфейс

Сделать модуль, который скрывает конкретный транспорт:

```text
comsdk/data_plane/
  __init__.py
  refs.py
  local_store.py
  arrow_flight_store.py
```

Интерфейс:

```python
class DataPlane:
    def put_array(self, values, *, session_id, name) -> DataRef:
        ...

    def get_array(self, ref: DataRef):
        ...

    def delete(self, ref: DataRef) -> None:
        ...
```

Сначала реализовать `local_store.py`, чтобы не тащить Arrow сразу во все места. Потом заменить на `arrow_flight_store.py`.

Критерий готовности: `prepare_input` кладет массив через `DataPlane.put_array`, а `remote_sin_batch` получает `DataRef`.

## 3. Поднять Arrow Flight сервер

Добавить отдельный сервис:

```text
arrow-flight
  python server
  port 8815
  stores Arrow RecordBatch by ticket
```

Для первой версии можно сделать in-memory store. Потом заменить на disk/object storage.

Docker Compose:

```yaml
arrow-flight:
  build: .
  command: python -m comsdk.data_plane.arrow_flight_server
  ports:
    - "8815:8815"
```

Python зависимости:

```text
pyarrow
grpcio
grpcio-tools
```

Критерий готовности:

- `put_array([0.0, 1.0])` возвращает `DataRef`;
- `get_array(ref)` возвращает тот же массив;
- тест работает из контейнера `celery` и из контейнера C++ worker.

## 4. Описать gRPC control plane

Создать proto:

```text
proto/compute_control.proto
```

Минимальная схема:

```proto
syntax = "proto3";

package graphvislab.compute;

message DataRef {
  string id = 1;
  string flight_uri = 2;
  string ticket = 3;
  string dtype = 4;
  uint64 length = 5;
}

message RunUnaryRequest {
  string operation = 1;
  DataRef input = 2;
  map<string, string> params = 3;
}

message RunUnaryReply {
  DataRef output = 1;
  string worker_id = 2;
  double elapsed_ms = 3;
}

message HealthRequest {}

message HealthReply {
  string worker_id = 1;
  string status = 2;
}

service ComputeControl {
  rpc RunUnary(RunUnaryRequest) returns (RunUnaryReply);
  rpc Health(HealthRequest) returns (HealthReply);
}
```

Критерий готовности: Python-клиент может вызвать `Health` у C++ worker.

## 5. Переписать C++ worker на gRPC control

Новый C++ worker должен:

1. Принять `RunUnary(operation, input_ref)`.
2. Через Arrow Flight скачать входной batch по `input_ref.ticket`.
3. Посчитать `sin/cos/exp`.
4. Через Arrow Flight загрузить результат.
5. Вернуть `output_ref`.

Важно: worker должен быть конкурентным. Старый `accept -> handle_client -> close` фактически обслуживает запросы по одному на процесс. Для gRPC лучше использовать async/callback API или thread pool.

Критерий готовности: три параллельных gRPC запроса `sin/cos/exp` реально стартуют одновременно и используют разные C++ потоки/worker-ы.

## 6. Добавить Python ComputeClient

Структура:

```text
comsdk/remote_cpp/
  client.py              # старый HTTP fallback
  grpc_client.py         # новый gRPC control client
  transport.py           # фабрика клиента по настройке
```

Фабрика:

```python
def create_compute_client(data):
    transport = data.get("cpp_transport", "http-binary")
    if transport == "grpc-flight":
        return GrpcFlightComputeClient(...)
    return RemoteCppClient(...)
```

Критерий готовности: `remote_cpp_math.py` не знает деталей транспорта, а только вызывает `client.compute(operation, input_ref_or_array)`.

## 7. Изменить пример parallel_cpp_math

В `examples/parallel_cpp_math.aini` добавить:

```ini
-cpp_transport=[grpc-flight]{http-binary|grpc-flight}
-flight_uri=[grpc://arrow-flight:8815]
-grpc_workers={cpp-worker-1:9100; cpp-worker-2:9100}
```

Старые параметры `cpp_workers` и `cpp_encoding` оставить для fallback.

## 8. Не возвращать огромные массивы в Python без необходимости

Для `parallel_cpp_math` лучше хранить:

```json
{
  "x_ref": "...",
  "sin_ref": "...",
  "cos_ref": "...",
  "exp_ref": "..."
}
```

`merge_results` должен объединять ссылки и summary, а не скачивать все данные обратно.

`save_result` может:

- сохранить только refs;
- скачать маленькую preview-выборку;
- скачать все данные только если явно указано `materialize_result=true`.

## 9. Что чистить после успешной миграции

Удалять только после прохождения тестов `grpc-flight`:

- HTTP endpoint `/compute-binary/<operation>` в `cpp_worker/main.cpp`;
- JSON fallback `/compute`;
- `cpp_encoding=json-f64`;
- frontend/backend SSE fallback, если WebSocket стабилен и SSE больше не нужен;
- debug `print` в `comsdk/parser.py`, `comsdk/graph.py`, `comwpc/views.py`.

## 10. Минимальный порядок работ

1. Добавить `DataRef` и local `DataPlane`.
2. Перевести `parallel_cpp_math` на refs внутри Python без Arrow.
3. Добавить Arrow Flight server и Python client.
4. Проверить upload/download массива из Celery.
5. Добавить `.proto` и Python gRPC client.
6. Добавить C++ gRPC worker `Health`.
7. Добавить C++ `RunUnary` + Arrow Flight download/upload.
8. Подключить `cpp_transport=grpc-flight` в aINI.
9. Прогнать `sin/cos/exp` на 1k, 1M, 10M точек.
10. Только потом удалять старый HTTP/binary путь.
