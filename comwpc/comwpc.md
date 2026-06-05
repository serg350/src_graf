# comwpc: Django-слой графов, aINI, запуска и визуализации

Дата ревизии: 2026-06-05.

Документ описывает текущее состояние Django-приложения `comwpc`: что оно делает вокруг
`comsdk`, как поддерживает aDOT/aINI, что реально сохраняется в БД, как устроен запуск
графа и какие части пока являются частичными или legacy.

Сверка сделана по:

- `comwpc/models.py`, `comwpc/views.py`, `comwpc/graph_payloads.py`.
- `comwpc/aini/aini_parser.py`, `comwpc/execution_inputs.py`.
- `comwpc/execution_history.py`, `comwpc/consumers.py`, `comwpc/events.py`.
- `comwpc/admin_visualization/*`, `config/tasks.py`, `comwpc/api/urls.py`, `config/urls.py`.
- `comwpc/tests.py`.
- PDF `comfrm_dsl_FormatAINI.pdf` и `comfrm_dsl_FormatAINI_figs_ru.pdf`.

## 1. Что такое comwpc

`comwpc` - это Django-приложение верхнего уровня над `comsdk`.

Его зоны ответственности:

- прием и валидация загруженных `.adot` и optional `.aini`;
- запуск `comsdk.Parser` при импорте и сохранение графа в Django-модели;
- хранение `Graph`, `State`, `Edge`, `Transfer`, `GraphExecutionSession`,
  `GraphExecutionEvent`;
- построение JSON payload для React-визуализации;
- построение формы запуска по aINI;
- запуск обхода графа через Celery;
- live-события выполнения через Channels WebSocket;
- сохранение истории запусков;
- legacy/admin SVG-визуализация через Graphviz.

Важно: `comwpc` не исполняет граф сам. При запуске он заново передает `raw_dot` в
`comsdk.Parser`, получает runtime-граф `comsdk.Graph` и вызывает `comsdk_graph.run(data)`.

## 2. Модель данных

### Graph

Поля:

- `name` - имя из `digraph`.
- `raw_dot` - исходный текст импортированного aDOT/DOT.
- `raw_aini` - исходный текст aINI, хранится только на основном графе.
- `parent_graph` - связь подграфа с родителем.
- `is_subgraph` - признак подграфа.

Есть constraint `unique_graph_name_per_type`: имя уникально в паре `(name, is_subgraph)`.
То есть основной граф и подграф с одним именем технически могут сосуществовать, но два
основных графа с одним именем - нет.

### State

Поля:

- `name`;
- `graph`;
- `is_terminal`;
- `subgraph` - ссылка на `Graph`, если состояние является узлом-подграфом;
- `comment`;
- `array_keys_mapping`;
- `is_subgraph_node`.

Не хранятся:

- selector состояния;
- policy `parallelism=threading`;
- полная информация о количестве входов/looped edges из runtime.

### Edge и Transfer

`Edge` хранит только:

- `comment`;
- `pred_module`, `pred_func`;
- `morph_module`, `morph_func`.

`Transfer` хранит:

- `source`;
- `target`;
- `edge`;
- `order`;
- `graph`.

Не хранятся:

- исходный тип стрелки `->` или `=>`;
- `selector`;
- `keys_mapping`;
- `preprocessor`, `postprocessor`;
- `connection_data`, `executable_parameters`;
- executor-параметры кроме того, что итоговая runtime-функция может попасть в
  `morph_module/morph_func`.

## 3. Импорт aDOT и подграфов

Импорт идет через:

- API endpoint `POST /api/comwpc/graph/import-dot/`;
- legacy/admin endpoint `POST /import-dot/` через `import_dot`;
- форму `DotImportForm`, где `.aini` optional.

Путь импорта:

1. `_read_and_validate_aini` читает `raw_aini` из POST или `aini_file`.
2. Если aINI передан, он валидируется через `parse_aini`.
3. `_import_graph_from_upload` сохраняет `.adot` во временную директорию.
4. `comsdk.Parser().parse_file(main_temp_path)` строит runtime-граф.
5. Если основной `Graph(name, is_subgraph=False)` уже есть, импорт возвращает конфликт.
6. `process_graph_recursively` создает Django `Graph`, `State`, `Edge`, `Transfer`.

Подграфы:

- ищутся по `subgraph`-атрибуту entity из `comsdk.Parser`;
- относительный путь ищется через `settings.SUBGRAPH_BASE_DIR`;
- путь относительно загруженного родительского файла сейчас закомментирован;
- найденный файл копируется во временную директорию и парсится отдельным `Parser`;
- уже существующий подграф с тем же `name` и `is_subgraph=True` переиспользуется;
- `raw_aini` подграфам не присваивается.

Ограничения импорта:

- импорт сохраняет состояние БД как представление для просмотра, а не как полноценный
  исполняемый IR;
- execution всегда использует `Graph.raw_dot`, а не пересобирает runtime из таблиц БД;
- повторный импорт основного графа с тем же именем не обновляет существующий граф;
- подграфы переиспользуются по имени и не обновляются автоматически;
- service states `__BEGIN__` и `__END__` сохраняются в БД, но скрываются в JSON payload;
- ошибки отсутствующего подграфа в некоторых ветках только печатаются в лог и состояние
  может быть пропущено/создано без подграфа.

## 4. aINI в comwpc

В `comwpc` aINI работает как описание входных параметров запуска, а не как механизм
конфигурирования `comsdk.Parser`.

### Поддерживаемый синтаксис aINI

Секции:

```aini
[Input]
```

Параметры:

```aini
*TaskName=ElasticResearch
-Pressure=34 [[MPa]]
OutputFilename=@TaskName@_@Pressure@.res
```

Префиксы:

- `*` - обязательный параметр;
- `-` - optional параметр;
- без префикса - обычный параметр без признака required/optional.

Поддерживаемые типы значений:

| Тип | Пример | Runtime-значение |
| --- | --- | --- |
| text/scalar | `TaskName=Demo` | строка, int, float или bool после базовой нормализации |
| bool | `Copy=[1]{0|1}` | `True` / `False` |
| dim | `Pressure=34 [[MPa]]` | `34`, unit сохраняется в schema |
| interval | `Range=[0.4;0.1:0.6;0.05]` | `0.4`, min/max/step сохраняются в schema |
| dimensional interval | `E=[120.0;1:300;1] [[GPa]]` | `120.0`, unit/min/max/step в schema |
| combobox | `Mode=[auNO]{auNO|auLCS}` | текущая опция |
| file_ref | `GeoFile=[geometry.geo]` | строка `geometry.geo` |
| dbtable_ref | `Author=[ivanov]$sys.users` | структура `{record_id, schema, table}` |
| array | `Axes=((1;0;0);(0;1;0))` | вложенные списки |
| set | `Workers={w1;w2}` | список |
| sub_parameter_id | `1$name=EGLASS` | имя параметра `1$name` |

Дополнительно:

- поддерживаются inline-комментарии `//`, если они не внутри кавычек/скобок;
- поддерживаются шаблоны `@Name@`, они разрешаются в `build_initial_data`;
- шаблоны разрешаются до 8 проходов.

### Что делает execution_input_schema

`build_execution_input_schema(raw_aini)` возвращает список UI-полей:

- `name`, `label`, `section`, `required`, `optional`, `comment`;
- `value_type` из aINI;
- `input_type`: `text`, `number`, `checkbox`, `select`;
- `initial_value`;
- `initial_value_label`, включая единицы измерения;
- `min`, `max`, `step`, `options`.

React получает эту схему через payload графа и показывает модальное окно параметров запуска.

### Как initial_data попадает в comsdk

`start_execution` вызывает:

1. `parse_execution_request_data(request)`;
2. `prepare_execution_initial_data(graph, request_data)`;
3. `create_execution_session(..., initial_data=initial_data)`;
4. `execute_graph_task.delay(graph.raw_dot, session_id, initial_data)`.

Важное поведение: сервер не смешивает автоматически все default/runtime-значения из
`build_initial_data(raw_aini)` в payload запуска. Если у графа есть `raw_aini`, сервер:

- валидирует required-поля;
- приводит типы только для полей, которые пришли в request;
- добавляет extra-ключи, которых нет в aINI;
- не добавляет omitted optional/default-поля сам.

На практике frontend строит форму с `initial_value` и отправляет заполненные значения.
Но API-клиент, который вызовет `/graph/<id>/start/` напрямую и опустит optional поле из
aINI, не получит это поле в `initial_data`.

## 5. Что НЕ означает поддержка aINI

Это ключевое уточнение к `comsdk.md`.

В PDF aDOT есть атрибуты вида:

```adot
FUNC [module=lib, entry_func=f, keys_mapping=some.aini:SECTION]
FUNC [executable_parameters=params.aini:RUN]
FUNC [connection_data=params.aini:SSH]
```

То есть `config_file_section` из aINI внутри aDOT должен служить источником настроек для
`keys_mapping`, запуска внешнего executable или SSH/remote connection.

В текущем проекте это не реализовано ни в `comsdk.Parser`, ни в `comwpc`:

- aINI хранится как `Graph.raw_aini`;
- aINI парсится для UI-формы запуска и подготовки `initial_data`;
- `comsdk.Parser` не получает `raw_aini`;
- `keys_mapping=...`, `executable_parameters=...`, `connection_data=...` не читают секции
  из aINI;
- SSH/remote communication из aDOT+aINI не создается.

Поэтому корректная формулировка:

> aINI как файл входных параметров запуска поддержан на уровне `comwpc`.  
> aINI как `config_file_section` внутри aDOT-атрибутов `keys_mapping`,
> `executable_parameters`, `connection_data` не интегрирован в `comsdk.Parser`.

## 6. API для React/frontend

Маршруты:

| Endpoint | Назначение |
| --- | --- |
| `GET /api/graphs/` | список графов со счетчиками и датой последнего запуска |
| `GET /api/graphs/<id>/` | один граф без рекурсивных подграфов |
| `GET /api/graphs/<id>/deep/` | граф с рекурсивными payload подграфов |
| `GET /api/graphs/<id>/executions/` | история запусков графа |
| `POST /api/comwpc/graph/import-dot/` | импорт aDOT/DOT и optional aINI |
| `POST /graph/<id>/start/` | запуск обхода графа |
| `WS /ws/execution/<session_id>/` | live-события выполнения |

`build_graph_payload` скрывает `__BEGIN__` и `__END__`. В edges для React передается
только `id`, `source`, `target`, `label`, `comment`; технические данные морфизма и
predicate-а не передаются.

## 7. Запуск и события

`execute_graph_task`:

- создает временный `.adot` из `raw_dot`;
- парсит его через `comsdk.Parser`;
- регистрирует listener;
- публикует события в Channels group `execution_<session_id>`;
- сохраняет часть событий в историю;
- вызывает `comsdk_graph.run(initial_data)`;
- при `False` или exception пишет error-событие.

Persisted event types:

- `state_enter`;
- `complete`;
- `error`.

В WebSocket отправляются все события, которые приходят в listener, включая `state_exit`,
`edge_enter`, `edge_exit`, `edge_error`, если они возникают в `comsdk`.

Ограничения:

- SSE endpoint `/execution/events/<session_id>/` и `comwpc/events.py` используют Redis
  pub/sub legacy-сервис, но текущий `execute_graph_task` публикует через Channels group,
  а не через `ExecutionEventService.publish`.
- Поэтому актуальный live-путь - WebSocket; SSE-путь выглядит устаревшим/неподключенным.
- В `execute_graph_task.process_subgraphs` проверяется `state.subgraph`, но у runtime
  `comsdk.State` подграф находится через `_proxy_state`; этот helper, вероятно, устарел.
  События вложенного исполнения все равно проходят через `observer`, который передается
  в `State.run`.

## 8. История запусков

История хранится в:

- `GraphExecutionSession`;
- `GraphExecutionEvent`.

При старте создается session со статусом `pending`.
При записи событий:

- `state_enter` переводит session в `running`;
- `complete` переводит в `completed` и ставит `finished_at`;
- `error` переводит в `failed`, ставит `error_message` и `finished_at`;
- события получают монотонный `sequence`;
- `inf` и `nan` в payload заменяются на `null`.

Запись событий batch-ится в Celery по 25 событий или при `complete/error`.

## 9. Визуализация

Есть два слоя визуализации.

### React payload

`graph_payloads.py` готовит JSON для React Flow:

- nodes без service states;
- edges без service edges;
- `subgraph` ID на узлах;
- recursive `subgraphs` в `/deep/`;
- `execution_input_schema` и `execution_input_error`.

### Legacy/admin Graphviz

`comwpc/admin_visualization` строит SVG через Python `graphviz`:

- admin preview в карточке `Graph`;
- отдельная интерактивная страница;
- чистый SVG endpoint `/graph-svg/<id>/`;
- подсветка узлов/переходов JS-кодом;
- модальное открытие подграфов;
- форма запуска по aINI в legacy-шаблоне.

Ограничения:

- SVG постобрабатывается regex-заменами, что хрупко к изменениям формата Graphviz SVG;
- визуализация не показывает selector/predicate/parallelism как отдельные first-class
  атрибуты модели;
- основные frontend-сценарии сейчас идут через React, legacy/admin слой частично дублирует
  функциональность.

## 10. Что покрыто тестами

`comwpc/tests.py` покрывает:

- распознавание типов aINI;
- `build_initial_data` и шаблоны `@Name@`;
- schema формы запуска;
- required-поля при запуске;
- то, что server-side payload запуска содержит только присланные aINI-поля и extra-ключи;
- создание session при запуске;
- запись истории, статусы, error message;
- batch-запись событий и sanitizing `inf/nan`;
- failure path `execute_graph_task`, если `comsdk_graph.run` вернул `False`.

Не покрыто или покрыто слабо:

- реальный импорт aDOT через HTTP endpoint;
- рекурсивная обработка подграфов;
- актуальный WebSocket-путь;
- legacy SSE-путь;
- Graphviz/admin visualization;
- связка aDOT+aINI `config_file_section`;
- обновление уже импортированных графов.

## 11. Что поддерживается, а что нет

### Поддерживается

| Возможность | Статус |
| --- | --- |
| Импорт основного aDOT/DOT | Работает через `comsdk.Parser`. |
| Optional загрузка `.aini` при импорте | Работает, валидируется и сохраняется в `Graph.raw_aini`. |
| Хранение структуры графа в БД | Работает как представление для UI. |
| Рекурсивные подграфы | Частично работает через `subgraph` и `SUBGRAPH_BASE_DIR`. |
| JSON API для списка/графа/deep-графа | Работает. |
| aINI schema для формы запуска | Работает. |
| Required-поля aINI при запуске | Работает. |
| Типизация bool/int/float/select/number | Работает в пределах `execution_inputs.py`. |
| Celery-запуск `comsdk` | Работает. |
| WebSocket live events | Работает через Channels group. |
| История запусков и событий | Работает для `state_enter`, `complete`, `error`. |
| React payload с подграфами и schema | Работает. |
| Legacy Graphviz preview/admin visualization | Работает при наличии Graphviz. |

### Частично поддерживается

| Возможность | Что есть | Ограничение |
| --- | --- | --- |
| aINI defaults | UI получает `initial_value` | Backend не добавляет omitted defaults сам. |
| aINI `dbtable_ref` | Парсится в структуру | Нет обращения к БД по этой ссылке. |
| aINI `file_ref` | Парсится как имя файла | Нет загрузки/проверки/резолва файла. |
| aINI `set`/`array` | Парсятся | В UI сложные значения сериализуются строкой. |
| Подграфы | Сохраняются как Graph + State.subgraph | Поиск относительных путей ограничен `SUBGRAPH_BASE_DIR`. |
| События подграфов | Runtime observer проходит в `comsdk` | Helper `process_subgraphs` в Celery выглядит устаревшим. |
| Визуализация параллельности | Можно увидеть структуру веток | `parallelism=threading` не хранится отдельным полем модели. |
| Edge details | Хранятся module/func/comment | Не хранится selector, mapping, pre/post, стрелка `=>`. |

### Не поддерживается

| Возможность | Комментарий |
| --- | --- |
| aINI как `config_file_section` внутри aDOT | Не интегрировано с `comsdk.Parser`. |
| `connection_data` из aDOT+aINI | Не создает SSH/remote communication. |
| `executable_parameters` из aDOT+aINI | Не создает executable edge. |
| `keys_mapping` из aDOT+aINI | Не создает `InOutMapping`. |
| Server-side merge всех defaults из aINI при запуске | Omitted optional/default поля не попадают в `initial_data`. |
| Обновление существующего основного графа при повторном импорте | Возвращается конфликт. |
| Полное восстановление исполняемого graph runtime из БД | Запуск идет из `raw_dot`. |
| Актуальный SSE live stream | Есть legacy endpoint, но текущий publisher использует WebSocket/Channels. |
| Полное покрытие импорта/подграфов/WS тестами | Тесты есть в основном для aINI, запуска и истории. |

Итог: `comwpc` действительно поддерживает aINI, но как веб-слой входных параметров запуска.
Это отдельная рабочая возможность. Она не закрывает требования aDOT про использование
aINI-секций как конфигурационных секций для `connection_data`, `executable_parameters`
или `keys_mapping` внутри `comsdk.Parser`.
