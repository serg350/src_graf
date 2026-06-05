# GraphVisLab

GraphVisLab - веб-система для импорта, просмотра и запуска вычислительных графов, описанных в формате aDOT/aINI.

Проект состоит из Django backend, React frontend, Celery worker-а для запуска графов, Redis для очередей/событий и отдельных C++ worker-ов для тяжелых численных расчетов.

## Текущая Архитектура

```text
React/Vite UI
    |
    | HTTP API, WebSocket/SSE events
    v
Django + Channels
    |
    | Celery task
    v
Celery worker
    |
    | comsdk parser/runtime
    v
Graph execution engine
    |
    | remote C++ calls
    v
cpp-worker-1, cpp-worker-2, ...
```

Основные части:

- `comsdk/` - парсер aDOT/aINI и runtime обхода графа.
- `comwpc/` - Django-приложение: модели графов, импорт, API, история запусков, события выполнения.
- `frontend/` - React-интерфейс на Vite, React Flow и Dagre.
- `config/` - Django/Celery/Channels настройки.
- `cpp_worker/` - простой C++ HTTP worker для удаленных численных вычислений.
- `test_funcs/remote_cpp_math.py` - пример функций графа, которые вызывают C++ worker-ы.
- `examples/parallel_cpp_math.adot` и `examples/parallel_cpp_math.aini` - пример графа с параллельными ветками `sin/cos/exp`.

## Запуск

Нужны:

- Docker Desktop / Docker Compose
- Node.js 18+ для frontend
- Python 3.10 для локальных проверок, если запускаете команды вне Docker

### 1. Backend, Redis, Celery и C++ worker-ы

Из корня проекта:

```powershell
docker compose up -d --build
```

Проверить, что сервисы поднялись:

```powershell
docker compose ps
```

Применить миграции:

```powershell
docker compose exec web python manage.py migrate
```

Создать администратора, если нужен доступ в Django admin:

```powershell
docker compose exec web python manage.py createsuperuser
```

Backend будет доступен:

```text
http://127.0.0.1:8000
```

Django admin:

```text
http://127.0.0.1:8000/admin/
```

C++ worker-ы проброшены наружу:

```text
http://127.0.0.1:9001/health
http://127.0.0.1:9002/health
```

### 2. Frontend

В отдельном терминале:

```powershell
cd frontend
npm install
npm run dev
```

Интерфейс будет доступен:

```text
http://127.0.0.1:5173
```

Важно: страницы React Router вида `/graphs/78` открываются через Vite frontend. Django API остается на `:8000`, а Vite проксирует нужные API-запросы.



## Импорт И Запуск Графа

1. Откройте `http://127.0.0.1:5173`.
2. Импортируйте aDOT/aINI через кнопку импорта графа.
3. Для теста можно использовать:

```text
examples/parallel_cpp_math.adot
examples/parallel_cpp_math.aini
examples/composite_pso.adot
examples/composite_pso.aini
```

4. Откройте импортированный граф.
5. Нажмите запуск.
6. Следите за подсветкой состояний и событиями выполнения.

В `parallel_cpp_math.adot` важная строка:

```dot
PREPARED [parallelism=threading, comment="Run sin/cos/exp branches in parallel"]
```

Она включает параллельный запуск веток:

```text
PREPARED -> SIN_DONE
PREPARED -> COS_DONE
PREPARED -> EXP_DONE
```

## Remote C++ Расчеты

Пример `parallel_cpp_math` вызывает C++ worker-ы для операций:

- `sin`
- `cos`
- `exp`

Настройки лежат в `examples/parallel_cpp_math.aini`:

```ini
-cpp_workers={http://cpp-worker-1:9000; http://cpp-worker-2:9000}
-load_balancing=[least_in_flight]{round_robin|least_in_flight}
-cpp_encoding=[binary-f64]{binary-f64|json-f64}
-timeout_ms=[3000; 100:60000; 100]
-retries=[1; 0:5; 1]
```

Рекомендуемый режим сейчас:

```text
cpp_encoding = binary-f64
load_balancing = least_in_flight
```

`binary-f64` передает массивы как raw little-endian `float64`, без JSON-списков. Это сильно снижает накладные расходы на сериализацию больших массивов.

## Как Читать Логи Параллельности

В Celery-логах есть строки вида:

```text
[CPP] start operation=sin worker=http://cpp-worker-1:9000 encoding=binary-f64 points=10000000
[CPP] done operation=sin worker=http://cpp-worker-1:9000 encoding=binary-f64 duration_s=...
```

Если `sin`, `cos`, `exp` стартуют близко по времени, значит параллельность графа работает.

Если операции идут одна за другой, проверьте:

- есть ли `PREPARED[parallelism=threading]` в aDOT;
- импортирован ли свежий aDOT после изменения;
- перезапущен ли Celery;
- не выбран ли последовательный вариант графа.

Строка:

```text
implicit_parallelization_info: None
```

не означает, что `parallelism=threading` не работает. Это другой механизм старой неявной параллелизации по массивам.

## Текущие Ограничения

- SQLite используется как dev-БД. (временно)
- C++ worker сейчас простой HTTP-сервер, без полноценного transport.
- Даже `binary-f64` все еще гоняет большие массивы туда и обратно по сети.
- Если одна операция возвращает 10 млн `float64`, это около 80 MB только на один результат.
- Python-часть графа пока хранит результаты веток в памяти процесса Celery.
- Транспорт команд и транспорт данных пока смешаны в одном HTTP-вызове.

## Что Доделывать По Распараллеливанию

Подробный пошаговый план перехода лежит в `docs/grpc_arrow_flight_migration.md`.

### 1. Разделить команды и данные

Сейчас worker получает данные прямо в запросе. В будущем лучше разделить:

```text
control plane: команда, операция, параметры, data_id
data plane: бинарные чанки массивов
```

Команды можно передавать через gRPC, а большие данные через streaming chunks или Arrow Flight.

### 2. Перейти от HTTP binary к gRPC streaming

Следующий практичный шаг:

- описать `.proto` для `ComputeWorker`;
- добавить unary-команды для малых задач;
- добавить streaming-вызовы для больших массивов;
- передавать `float64` чанками как `bytes`;
- сохранить timeout/deadline/retry на уровне клиента.

Пример направления:

```proto
service ComputeWorker {
  rpc Compute(ComputeRequest) returns (ComputeResult);
  rpc ComputeStream(stream ComputeChunk) returns (stream ComputeChunk);
}
```

### 3. Рассмотреть Apache Arrow / Arrow Flight

Arrow Flight подходит, когда данные становятся не просто байтами, а полноценными массивами/таблицами:

- `float64` колонки;
- батчи;
- схемы;
- меньше лишних копирований;
- нормальная связка Python/C++.

Ориентир: gRPC для команд, Arrow Flight для больших массивов.

### 4. ZeroMQ Рассматривать Как Низкоуровневый Вариант

ZeroMQ может быть полезен для собственного worker-pool протокола:

```text
frame 1: metadata
frame 2: binary payload
```

Но retry, schema, auth, health-checks и versioning придется делать вручную. Поэтому сначала лучше gRPC/Arrow.

### 5. Не Передавать `x`, Если Worker Может Его Сгенерировать

Для `parallel_cpp_math` входной массив можно не отправлять вообще. Достаточно передать:

```text
x_start
x_end
points
operation
```

Worker сам построит `x` и посчитает результат. Это быстрее любого транспорта, потому что входные 80 MB не передаются по сети.

### 6. Не Возвращать Большие Результаты В Python Без Необходимости

Если дальше нужен только summary, файл или визуализация, worker должен возвращать не массив, а ссылку:

```json
{
  "result_id": "sin_123",
  "dtype": "float64",
  "shape": [10000000],
  "location": "worker/storage/path"
}
```

Тогда Python-граф передает дальше `DataRef`, а не сам массив.

### 7. Масштабировать C++ Worker-ы

Планы:

- сделать worker pool на нескольких машинах;
- добавить registry/health-check worker-ов;
- выбирать worker по загрузке;
- ограничивать число одновременных задач на worker;
- добавить метрики: время передачи, время вычисления, размер payload, очередь.

## Полезные Команды

Логи backend:

```powershell
docker compose logs -f web
```

Логи Celery:

```powershell
docker compose logs -f celery
```

Логи C++ worker-ов:

```powershell
docker compose logs -f cpp-worker-1 cpp-worker-2
```

Пересборка C++ worker-ов:

```powershell
docker compose up -d --build cpp-worker-1 cpp-worker-2
```

Проверка frontend build:

```powershell
cd frontend
npm run build
```

## Куда Смотреть В Коде

- `comsdk/parser.py` - парсинг aDOT/aINI в runtime-граф.
- `comsdk/graph.py` - обход графа, события, `ThreadParallelizationPolicy`.
- `comsdk/edge.py` - выполнение морфизмов/предикатов.
- `config/tasks.py` - Celery task запуска графа.
- `comwpc/execution_history.py` - запись истории событий.
- `comwpc/consumers.py` - WebSocket-события.
- `frontend/src/services/executionEvents.js` - прием live-событий.
- `frontend/src/components/GraphView.jsx` - визуализация графа.
- `comsdk/remote_cpp/client.py` - Python-клиент C++ worker-а.
- `cpp_worker/main.cpp` - C++ worker.



