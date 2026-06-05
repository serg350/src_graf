# comsdk: состояние реализации aDOT и runtime-графов

Дата ревизии: 2026-06-05.

## 1. Что такое comsdk в этом проекте

`comsdk` - это Python-слой, который превращает описание вычислительного графа в формате
aDOT в исполняемый объект `Graph` и затем обходит его как граф состояний.

Основные файлы:

- `comsdk/parser.py` - регулярный парсер aDOT, сборка `Graph`, `State`, `Edge`, `Func`,
  подключение селекторов, подграфов и политики `ThreadParallelizationPolicy`.
- `comsdk/graph.py` - runtime графа: `Graph.run`, `State.run`, селекторы, ожидание join-точек,
  события исполнения, serial/threading policies.
- `comsdk/edge.py` - ребро вычисления: предикат, морфизм, optional `InOutMapping`,
  программные Edge-классы для локальных/удаленных задач.
- `comsdk/communication.py`, `comsdk/distributed_storage.py`, `comsdk/research.py` -
  старый инфраструктурный слой для локального/SSH/qsub/research-сценариев.
- `comsdk/executors.py`, `comsdk/remote_cpp/*` - новое расширение для вызова внешнего
  C++ worker-а через HTTP.

Важно: aINI в проекте поддерживается, но на другом уровне. `comwpc` использует aINI как
описание входных параметров запуска: парсит файл, строит UI-схему и передает в `comsdk`
уже готовый словарь `data`. Сам `comsdk.Parser` не получает `raw_aini` и не использует
aINI-секции как `config_file_section` внутри aDOT-атрибутов.

## 2. Как идет выполнение графа

Типовой путь выполнения:

1. `Parser.parse_file(path)` читает UTF-8 `.adot`.
2. Парсер удаляет пробелы вне кавычек, убирает `digraph`, `{}`, однострочные `//` комментарии.
3. Строки вида `NAME[...]` сохраняются как entities.
4. Строки топологии `A -> B[...]` или `A => B[...]` создают состояния и связи.
5. `GraphFactory.build()` создает `Graph(__BEGIN__, __END__)`, инициализирует граф,
   подключает селекторы, подграфы и `parallelism=threading`.
6. `Graph.run(data)` запускает обход:
   - вызывает `init_graph`;
   - на каждом состоянии вызывает `State.run`;
   - `State.run` проверяет готовность join-точек, вызывает selector, затем predicate-ы;
   - выбранные переходы передаются в текущую политику распараллеливания;
   - `Edge.morph` вызывает Python-функцию и мутирует общий `data`.

`__BEGIN__` и `__END__` ожидаются явно. Если их нет, сборка графа упадет.

## 3. Что реально поддерживает parser.py

### Граф, состояния и функции

Поддержано:

- `digraph NAME { ... }` без обязательных DOT-точек с запятой.
- Однострочные комментарии `//`.
- Entity-строки `NAME [attr=value, ...]`.
- Библиотечные функции через `module` и `entry_func`.
- Морфизмы через `predicate=...` и `function=...`.
- Optional predicate: если в морфизме нет `predicate`, используется dummy predicate.
- Узлы создаются не только явным `NODE[...]`, но и неявно из топологии.
- `comment="..."` сохраняется для функций, морфизмов и части ребер/узлов.

Ограничения:

- Парсер регулярный, не полноценный DOT/aDOT parser. Он чувствителен к символам вне `\w`,
  кавычкам, запятым внутри сложных значений и не поддерживает общий DOT-синтаксис.
- Комментарии в кавычках обрабатываются как первый quoted-текст в атрибутах. Это подходит
  для `comment="..."`, но ломает сценарии вроде `subgraph="file.adot"`.
- `keys_mapping` в entity принимается синтаксически, но не подключается к `Edge`.

### Топология

Поддержано:

- Обычное ребро `A -> B`.
- Запись `A => B` распознается парсером.
- Один вход - один выход.
- Один вход - несколько выходов: `A -> B, C`.
- Несколько входов - один выход: `A, B -> C`.
- Множественные морфизмы в локальном синтаксисе проекта:
  `A -> B, C [morphism=(EDGE_B, EDGE_C)]`.
- Локальный атрибут `order`, который сортирует исходящие ребра в `idle_run`.

Не поддержано или отличается от PDF:

- Многие входы - многие выходы запрещены как неоднозначные.
- Спецификационный атрибут `morphisms=(...)` не поддержан. Парсер принимает только
  локальный вариант `morphism=(...)`.
- `edge_index` из PDF принимается как атрибут, но не используется для порядка ребра.
- `=>` не включает threading сам по себе и дальше исполняется как `->`.

## 4. Селекторы и ветвления

Селектор можно задать двумя способами:

- На узле: `NODE [selector=SELECTOR]`.
- На one-to-many ребре: `NODE -> LEFT, RIGHT [selector=SELECTOR]`.

Второй вариант является удобным локальным расширением: парсер переносит selector с ребра
на исходное состояние.

Runtime ожидает, что selector вернет список boolean-значений той же длины, что и список
исходящих переходов состояния. Далее каждый выбранный переход дополнительно проверяется
predicate-ом ребра.

Если selector вернул пустой список, список неправильной длины или после predicate-ов не
осталось ни одного перехода, выполнение завершается ошибкой `GraphUnexpectedTermination`.

## 5. Подграфы

Узел с атрибутом `subgraph=path/to/file.adot` заменяется на подграф:

```adot
PREPROCESS [subgraph=./tests/test_aDOT/test_adot_files/preprocess.adot]
```

Механизм:

- `GraphFactory.build()` создает новый `Parser(subgraph=True)`.
- Подграф парсится отдельно.
- Текущее состояние получает `_proxy_state`, который указывает на `init_state` подграфа.
- `term_state` подграфа получает исходящие переходы заменяемого состояния.

Ограничения:

- Путь передается как есть. Надежного разрешения относительно файла родительского `.adot`
  в `comsdk.Parser` нет.
- Кавычки вокруг пути не поддержаны надежно.
- Визуальная/модельная рекурсия подграфов дополнительно реализуется уже в `comwpc`, не в
  `comsdk`.

## 6. Морфизмы, Edge и данные

`Edge` содержит:

- `pred_f` - функция-предикат.
- `morph_f` - функция-обработчик.
- `_io_mapping` - программный `InOutMapping`.
- `preprocess` и `postprocess` - callable hooks, по умолчанию no-op lambda.
- `order`, `comment`, `mandatory_keys`.

При выполнении:

1. `predicate(data)` строит proxy data через `_io_mapping` и вызывает `pred_f.func`.
2. `morph(data)` строит proxy data, вызывает `preprocess`, проверяет mandatory keys,
   вызывает `morph_f.func`, затем `postprocess`.

Важно: из aDOT сейчас не создаются `InOutMapping`, `preprocess`, `postprocess` и
специализированные `ExecutableProgramEdge`. Эти возможности есть в Python-коде, но не
связаны с текущим aDOT-парсером.

## 7. Распараллеливание

В `comsdk` есть два разных механизма, которые легко спутать.

### 7.1. Явное threading-распараллеливание из aDOT

Включается только атрибутом состояния:

```adot
PREPARED [parallelism=threading]
PREPARED -> SIN_DONE [morphism=SIN]
PREPARED -> COS_DONE [morphism=COS]
PREPARED -> EXP_DONE [morphism=EXP]
```

Парсер ставит на состояние `ThreadParallelizationPolicy`. В runtime:

- `State.run` выбирает исходящие переходы через selector и predicate-ы.
- Если выбранных переходов меньше двух, policy откатывается к serial.
- Если есть `array_keys_mapping`, policy тоже откатывается к serial.
- Иначе создается `ThreadPoolExecutor(max_workers=len(transfers))`.
- Каждый выбранный `Transfer` запускается в отдельном Python thread.
- После завершения всех futures runtime последовательно продвигает ветки дальше к join-точке.

Следствие: параллельно выполняются именно морфизмы исходящих ребер из состояния с
`parallelism=threading`. Длинные цепочки после этих ребер уже согласуются в основном
последовательно. Для текущих примеров это нормально, потому что тяжелые операции стоят
непосредственно на ребрах из параллельного состояния.

### 7.2. Join-точки

Состояние с несколькими входящими ребрами ждет, пока активируются все нужные входы.
Счетчик хранится в `activated_input_edges_number`. Для циклов учитывается
`looped_edges_number`, чтобы обратные ребра не блокировали выполнение.

Для терминального состояния есть специальное поведение: если выполнение приходит в
`__END__` без implicit parallelization, достаточно одного входа. Это позволяет альтернативным
веткам завершать граф без ожидания всех потенциальных входящих ребер.

### 7.3. Implicit parallelization

В `graph.py` есть старый программный механизм `array_keys_mapping` и
`ImplicitParallelizationInfo`.

Он:

- создается через Python-конструкторы `State(..., array_keys_mapping=...)`;
- не парсится из aDOT;
- разбивает данные по индексам массива через `ProxyDict` и `ArrayItemGetter`;
- выполняется через serial policy;
- требует один исходящий переход из implicit-parallel state.

Это не тот же механизм, что `parallelism=threading`.

### 7.4. Remote C++ worker

В проект добавлено локальное расширение aDOT для морфизмов с executor:

```adot
SIN [executor=remote_cpp, operation=sin, input_key=x, output_key=sin_x]
```

`comsdk.executors.build_executor_function` поддерживает только `executor=remote_cpp`.
`RemoteCppClient`:

- нормализует список worker URL;
- поддерживает `round_robin` и `least_in_flight`;
- защищает выбор worker-а lock-ом;
- отправляет синхронный HTTP-запрос в `binary-f64` или `json-f64`;
- кешируется по настройкам worker-а, timeout, retries, encoding и load balancing.

Параллельность C++ вызовов появляется, когда несколько aDOT-веток запускаются через
`parallelism=threading` и каждая ветка синхронно вызывает свой remote C++ request.

### 7.5. Риски threading-модели

- Все ветки получают общий mutable `data`.
- Копирования данных по веткам нет.
- Lock-ов вокруг пользовательских morph-функций нет.
- Безопасный паттерн - писать результаты параллельных веток в разные ключи, а затем
  объединять их отдельным merge-морфизмом.
- Если две ветки пишут в один ключ, результат зависит от порядка завершения потоков.
- Полноценного unit-теста на фактическую одновременность сейчас нет:
  `TestGraphExecution.test_parallel_execution` является заглушкой.

## 8. Проверка, выполненная сейчас

Существующие тесты:

```powershell
.venv\Scripts\python.exe -m unittest tests.test_aDOT.unit_test_aDOT
```

Результат: 9 тестов, OK.

Покрытые сценарии:

- последовательный граф;
- цикл;
- ветвление и join;
- selector на one-to-many ребре;
- подграфы;
- `->` и `=>` как допустимые стрелки;
- часть атрибутов узлов/ребер;
- альтернативное завершение через терминальное состояние.

Диагностический прогон спорных конструкций дал такие результаты:

- `morphisms=(EDGE_A, EDGE_B)` - ошибка `Unknown parameter: morphisms`.
- `edge_index=7` - парсится, но `edge.order` остается `0`.
- `__BEGIN__ => STEP` без `parallelism=threading` - policy остается `SerialParallelizationPolicy`.
- `STEP [parallelism=threading]` - policy становится `ThreadParallelizationPolicy`.
- `parallelism=pseudo-parallelism` - policy остается `SerialParallelizationPolicy`.
- `keys_mapping=(local=global)` на функции - парсится, но `Edge._io_mapping._keys_mapping` пустой.
- `preprocessor=...` и `postprocessor=...` на ребре - парсятся, но hooks остаются lambda no-op.

## 9. Что сделано по aDOT, а что нет

### Сделано или в целом работает

| Возможность aDOT | Статус в comsdk | Комментарий |
| --- | --- | --- |
| `digraph ID { ... }` | Сделано частично | Работает для ограниченного синтаксиса без общего DOT-парсинга. |
| `//` комментарии | Сделано | Удаляются перед разбором. |
| Узлы-состояния | Сделано | Создаются явно из `NODE[...]` и неявно из топологии. |
| `module`, `entry_func` | Сделано | Импорт Python-функций через `importlib`. |
| `predicate`, `function` в морфизме | Сделано | Predicate optional, по умолчанию dummy true. |
| `comment="..."` | Частично | Работает для типового comment, но парсер хрупок к другим quoted-значениям. |
| `selector` на узле | Сделано | Selector возвращает boolean-вектор по исходящим ребрам. |
| `selector` на one-to-many ребре | Сделано как расширение | Переносится на исходный узел. |
| `subgraph=...` | Частично | Работает для bare path; нет надежного relative path и quoted path. |
| `parallelism=threading` | Сделано | Включает `ThreadParallelizationPolicy` на состоянии. |
| `A -> B` | Сделано | Обычная топология. |
| `A -> B, C` | Сделано | One-to-many. |
| `A, B -> C` | Сделано | Many-to-one join. |
| Циклы | Сделано частично | Есть учет looped edges, покрыто тестами. |

### Сделано с отклонением от PDF

| Возможность | Как в PDF | Как сейчас в comsdk |
| --- | --- | --- |
| Несколько морфизмов | `morphisms=(A, B)` | Работает только локальное `morphism=(A, B)`. |
| Индекс ребра | `edge_index=NUM` | Используется локальный `order=NUM`; `edge_index` игнорируется. |
| Multi-thread стрелка | `=>` означает multi threading | `=>` только распознается как стрелка, но не включает threading. |
| Параллельность | В PDF есть `threading` и `pseudo-parallelism` | Реально работает только `threading`; pseudo игнорируется. |
| IOMapping | `keys_mapping=(local=global)` | Класс `InOutMapping` есть, но aDOT-парсер его не подключает. |

### Не сделано по текущей реализации

| Возможность aDOT | Статус |
| --- | --- |
| Спецификационный `morphisms=(...)` | Не поддержан. |
| Семантика `edge_index` | Не реализована. |
| Семантика `=>` как multi-thread edge | Не реализована. |
| `parallelism=pseudo-parallelism` | Не реализован. |
| `preprocessor=...` на ребре | Не подключается к `Edge.preprocess`. |
| `postprocessor=...` на ребре | Не подключается к `Edge.postprocess`. |
| `keys_mapping` из aDOT | Не подключается к `InOutMapping`; ссылки на aINI-секции не читаются. |
| `executable_parameters` | Не создает `ExecutableProgramEdge`; aINI-секции параметров запуска executable не используются. |
| `connection_data` | Не создает remote/SSH communication из aDOT; aINI-секции подключения не используются. |
| `config_file_section` из aINI внутри aDOT | Не интегрирован в `comsdk.Parser`; это не отменяет поддержку aINI как формы входных данных в `comwpc`. |
| Многие входы - многие выходы | Запрещено как неоднозначное. |
| Полноценный parser DOT/aDOT | Не реализован, используется regex-парсер. |
| Unit-тест на реальную одновременность threading | Нет, тест является заглушкой. |

Итог: текущий `comsdk` исполняет рабочий практический поднабор aDOT для Python-функций,
селекторов, подграфов, циклов, join-точек и явного `parallelism=threading`. Полного
соответствия PDF-формату aDOT пока нет: основные пробелы находятся вокруг `morphisms`,
`edge_index`, `=>`, `keys_mapping`, pre/post-процессоров и запуска внешних программ через
`executable_parameters/connection_data`. Поддержка aINI сейчас вынесена в `comwpc` и
работает как подготовка входных данных запуска, а не как конфигуратор aDOT-объектов.
