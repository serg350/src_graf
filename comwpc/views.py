import json
import os
import queue
import shutil
import tempfile
import time
import uuid
from collections import deque
from typing import Any, Dict

from django.contrib import messages
from django.db.models import Count, Max
from django.http import JsonResponse, StreamingHttpResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods, require_POST

from config import settings
from config.tasks import execute_graph_task
from comsdk.parser import Parser
from .aini.aini_parser import parse_aini
from .execution_history import (
    create_execution_session,
    list_graph_execution_sessions,
    serialize_execution_sessions,
)
from .execution_inputs import (
    build_execution_input_schema as _build_execution_input_schema,
    parse_execution_request_data as _parse_execution_request_data,
    prepare_execution_initial_data as _prepare_execution_initial_data,
)
from .events import get_event_service
from .forms import DotImportForm
from .graph_payloads import build_graph_list_payload, build_graph_payload
from .models import Edge, Graph, State, Transfer


def graph_deep_json(request, graph_id):
    """
    Что делает: API просмотра графа в React-приложении, режим с раскрытием подграфов.
    Место: API просмотра графа в React-приложении, режим с раскрытием подграфов.
    Вход: HTTP GET и graph_id из URL /api/graphs/<id>/deep/.
    Выход: JsonResponse с графом, ребрами, состояниями, схемой запуска и вложенными подграфами.

    Полный endpoint: возвращает граф + все вложенные подграфы рекурсивно.
    GET /api/graphs/<id>/deep/
    """
    graph = get_object_or_404(Graph, pk=graph_id)
    return JsonResponse(build_graph_payload(graph, include_subgraphs=True), safe=True)


def graph_json(request, graph_id):
    """
    Что делает: API просмотра одного графа без рекурсивной загрузки подграфов.
    Место: API просмотра одного графа без рекурсивной загрузки подграфов.
    Вход: HTTP GET и graph_id из URL /api/graphs/<id>/.
    Выход: JsonResponse с базовым payload графа.
    """
    graph = get_object_or_404(Graph, pk=graph_id)
    return JsonResponse(build_graph_payload(graph), safe=True)


def graphs_list_json(request):
    """
    Что делает: API стартовой страницы со списком графов.
    Место: API стартовой страницы со списком графов.
    Вход: HTTP GET /api/graphs/.
    Выход: JsonResponse со списком графов, счетчиками узлов/ребер и краткой историей запусков.

    GET /api/graphs/
    Возвращает список всех графов (без вложенных структур)
    """

    graphs = (
        Graph.objects
        .annotate(
            nodes_count=Count("state", distinct=True),
            edges_count=Count("transfer", distinct=True),
            execution_count=Count("execution_sessions", distinct=True),
            last_execution_at=Max("execution_sessions__created_at"),
        )
        .order_by("name")
    )

    return JsonResponse(build_graph_list_payload(graphs), safe=False)


# -------------------------------------------------------------------


def graph_execution_history_json(request, graph_id):
    """
    Что делает: API правой панели истории обходов графа.
    Место: API правой панели истории обходов графа.
    Вход: HTTP GET, graph_id и optional query-параметр limit.
    Выход: JsonResponse со списком последних сессий исполнения и их событиями.
    """
    graph = get_object_or_404(Graph, pk=graph_id)

    try:
        limit = int(request.GET.get("limit", 12))
    except (TypeError, ValueError):
        limit = 12

    limit = max(1, min(limit, 50))
    sessions = list_graph_execution_sessions(graph, limit=limit)
    return JsonResponse(serialize_execution_sessions(sessions), safe=False)


def _save_uploaded_file(uploaded_file, target_dir):
    """
    Что делает: служебный шаг импорта DOT/aDOT файлов.
    Место: служебный шаг импорта DOT/aDOT файлов.
    Вход: Django UploadedFile и путь временной директории.
    Выход: полный путь сохраненного файла на диске.
    """
    target_path = os.path.join(target_dir, uploaded_file.name)
    with open(target_path, "wb+") as destination:
        for chunk in uploaded_file.chunks():
            destination.write(chunk)
    return target_path


def _read_and_validate_aini(request):
    """
    Что делает: общий парсер aINI для API и admin-импорта графов.
    Место: общий парсер aINI для API и admin-импорта графов.
    Вход: HTTP-запрос с raw_aini в POST или aini_file в FILES.
    Выход: строка aINI; при невалидном UTF-8 или формате бросает ValueError.
    """
    raw_aini = request.POST.get("raw_aini", "") or ""
    aini_file = request.FILES.get("aini_file")
    if aini_file is not None:
        try:
            raw_aini = aini_file.read().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("aINI file must be UTF-8 encoded") from exc

    if raw_aini:
        parse_aini(raw_aini)

    return raw_aini


def _collect_graph_stats(django_graph):
    """
    Что делает: формирование краткой статистики после импорта графа.
    Место: формирование краткой статистики после импорта графа.
    Вход: созданная или найденная модель Graph.
    Выход: словарь с числом состояний, ребер и подграфов.
    """
    return {
        "states": django_graph.state_set.count(),
        "edges": Edge.objects.filter(transfer__graph=django_graph).count(),
        "subgraphs": Graph.objects.filter(parent_graph=django_graph).count(),
    }


def _import_graph_from_upload(dot_file, raw_aini=""):
    """
    Что делает: общий сценарий импорта загруженного DOT/aDOT из API и admin-формы.
    Место: общий сценарий импорта загруженного DOT/aDOT из API и admin-формы.
    Вход: UploadedFile с графом и optional сырой aINI.
    Выход: tuple (Graph|None, stats|None, existing_graph|None).
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        main_temp_path = _save_uploaded_file(dot_file, temp_dir)
        parser = Parser()
        comsdk_graph = parser.parse_file(main_temp_path)
        graph_name = parser.fact.name

        existing_main_graph = Graph.objects.filter(name=graph_name, is_subgraph=False).first()
        if existing_main_graph:
            return None, None, existing_main_graph

        processed_graphs = {}
        django_graph = process_graph_recursively(
            parser=parser,
            comsdk_graph=comsdk_graph,
            dot_path=main_temp_path,
            temp_dir=temp_dir,
            processed_graphs=processed_graphs,
            parent_graph=None,
            raw_aini=raw_aini,
        )

        return django_graph, _collect_graph_stats(django_graph), None


@csrf_exempt
@require_POST
def import_dot_api(request):
    """
    Что делает: REST endpoint импорта графа из React-страницы импорта.
    Место: REST endpoint импорта графа из React-страницы импорта.
    Вход: multipart POST с dot_file и optional raw_aini/aini_file.
    Выход: JsonResponse с success, graph_id, graph_name и stats либо ошибкой.
    """
    form = DotImportForm(request.POST, request.FILES)

    if not form.is_valid():
        return JsonResponse({
            "success": False,
            "error": "Invalid file format",
        }, status=400)

    try:
        dot_file = request.FILES["dot_file"]
        raw_aini = _read_and_validate_aini(request)
        django_graph, stats, existing_main_graph = _import_graph_from_upload(dot_file, raw_aini=raw_aini)

        if existing_main_graph:
            return JsonResponse({
                "success": False,
                "error": f'Graph "{existing_main_graph.name}" already exists',
                "graph_id": existing_main_graph.id,
            }, status=409)

        return JsonResponse({
            "success": True,
            "graph_id": django_graph.id,
            "graph_name": django_graph.name,
            "stats": stats,
        })
    except ValueError as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=500)


@csrf_exempt
@require_http_methods(["GET", "POST"])
def import_dot(request):
    """
    Что делает: legacy/admin endpoint импорта графа через HTML/AJAX форму.
    Место: legacy/admin endpoint импорта графа через HTML/AJAX форму.
    Вход: GET для формы или POST multipart с dot_file и optional aINI.
    Выход: HTML-форма или JsonResponse для AJAX-импорта.
    """
    if request.method == "POST":
        form = DotImportForm(request.POST, request.FILES)
        if not form.is_valid():
            return JsonResponse({
                "success": False,
                "error": "Invalid file format",
            })

        dot_file = request.FILES["dot_file"]

        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            try:
                raw_aini = _read_and_validate_aini(request)
                django_graph, stats, existing_main_graph = _import_graph_from_upload(dot_file, raw_aini=raw_aini)

                if existing_main_graph:
                    return JsonResponse({
                        "success": False,
                        "error": f'Graph "{existing_main_graph.name}" already exists (ID: {existing_main_graph.id})',
                    })

                return JsonResponse({
                    "success": True,
                    "redirect_url": reverse("admin:comwpc_graph_change", args=[django_graph.id]),
                    "stats": stats,
                })
            except ValueError as exc:
                return JsonResponse({
                    "success": False,
                    "error": str(exc),
                })
            except Exception as exc:
                return JsonResponse({
                    "success": False,
                    "error": str(exc),
                })

        messages.info(request, "Use AJAX form for import")

    form = DotImportForm()
    return render(request, "admin/import_dot.html", {"form": form})


def import_progress(request):
    """
    Что делает: демонстрационный SSE endpoint прогресса импорта.
    Место: демонстрационный SSE endpoint прогресса импорта.
    Вход: HTTP GET.
    Выход: StreamingHttpResponse text/event-stream с искусственными progress-событиями.
    """
    def event_stream():
        """
        Что делает: генератор сообщений import_progress.
        Место: генератор сообщений import_progress.
        Вход: нет; использует локальный счетчик.
        Выход: строки SSE data с progress/message.
        """
        # Эмуляция прогресса
        for i in range(1, 101):
            time.sleep(0.5)
            yield f"data: {{\"progress\": {i}, \"message\": \"Обработано {i}%\"}}\n\n"

    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    return response


def process_graph_recursively(parser, comsdk_graph, dot_path, temp_dir, processed_graphs, parent_graph=None, raw_aini=""):
    """
    Что делает: ядро импорта DOT/aDOT в Django-модели Graph/State/Edge/Transfer.
    Место: ядро импорта DOT/aDOT в Django-модели Graph/State/Edge/Transfer.
    Вход: comsdk Parser/Graph, путь к DOT, temp_dir, cache processed_graphs, optional parent_graph/raw_aini.
    Выход: модель Graph; рекурсивно создает или переиспользует подграфы, состояния и переходы.
    """
    """Рекурсивно обрабатывает граф и его подграфы"""
    if dot_path in processed_graphs:
        return processed_graphs[dot_path]

    print(f"Обработка графа: {dot_path}")

    # Создаем базовую директорию для поиска файлов
    BASE_SEARCH_DIR = getattr(settings, 'SUBGRAPH_BASE_DIR', '/app')
    # base_dir = os.path.dirname(dot_path)

    # Читаем содержимое DOT-файла
    with open(dot_path, 'r', encoding='utf-8') as f:
        dot_content = f.read()

    # Проверяем существование графа
    graph_name = parser.fact.name
    # is_subgraph = parent_graph is not None
    existing_graph = Graph.objects.filter(name=graph_name, is_subgraph=parent_graph is not None, ).first()

    if existing_graph:
        graph = existing_graph
        print(f"Используем существующий граф: {graph.name} (ID: {graph.id})")
    else:
        # Создаем новый граф
        graph = Graph.objects.create(
            name=graph_name,
            raw_dot=dot_content,
            raw_aini=raw_aini if parent_graph is None else "",
            is_subgraph=parent_graph is not None,
            parent_graph=parent_graph
        )
        print(f"Создан новый граф: {graph.name} (ID: {graph.id}), is_subgraph={parent_graph is not None,}")

    processed_graphs[dot_path] = graph

    # Получаем все сущности графа
    entities = getattr(parser.fact, 'entities', {})
    print(f"Entities in graph: {list(entities.keys())}")

    state_mapping = {}
    queue = deque([comsdk_graph.init_state])
    visited = set()

    while queue:
        current = queue.popleft()
        if current.name in visited:
            continue
        visited.add(current.name)
        print(f"Обработка состояния: {current.name}")

        # Ищем информацию о состоянии в entities
        state_entity = entities.get(current.name)
        subgraph_path = None

        # Если нашли сущность для этого состояния и у нее есть subgraph
        if state_entity and hasattr(state_entity, 'subgraph'):
            subgraph_path = state_entity.subgraph
            print(f"Найден подграф для состояния {current.name}: {subgraph_path}")

        # Обработка подграфа
        subgraph_obj = None
        if subgraph_path:
            # Обрабатываем относительные пути
            if not os.path.isabs(subgraph_path):
                # Пытаемся найти файл в разных возможных местах
                possible_paths = [
                    # os.path.join(os.path.dirname(dot_path), subgraph_path),
                    os.path.join(BASE_SEARCH_DIR, subgraph_path),
                    # os.path.join(BASE_SEARCH_DIR, 'tests', 'test_aDOT', 'test_adot_files',
                    # os.path.basename(subgraph_path))
                ]

                found = False
                for path in possible_paths:
                    if os.path.exists(path):
                        subgraph_path = path
                        found = True
                        print(f"Нашли файл подграфа по пути: {subgraph_path}")
                        break

                if not found:
                    print(f"Файл подграфа не найден ни по одному из путей: {possible_paths}")
                    continue
            else:
                if not os.path.exists(subgraph_path):
                    print(f"Файл подграфа не найден: {subgraph_path}")
                    continue

            # Копируем файл подграфа во временную директорию
            subgraph_filename = os.path.basename(subgraph_path)
            temp_subgraph_path = os.path.join(temp_dir, subgraph_filename)

            if os.path.exists(subgraph_path):
                shutil.copy2(subgraph_path, temp_subgraph_path)
                print(f"Скопирован файл подграфа: {subgraph_path} -> {temp_subgraph_path}")
            else:
                print(f"Файл подграфа не найден: {subgraph_path}")
                continue

            if os.path.exists(temp_subgraph_path):
                try:
                    print(f"Обработка подграфа: {temp_subgraph_path}")
                    # Парсим подграф
                    sub_parser = Parser()
                    sub_comsdk_graph = sub_parser.parse_file(temp_subgraph_path)

                    # Получаем имя подграфа
                    subgraph_name = sub_parser.fact.name

                    # Проверяем существование подграфа
                    existing_subgraph = Graph.objects.filter(
                        name=subgraph_name,
                        is_subgraph=True
                    ).first()

                    if existing_subgraph:
                        print(f"Используем существующий подграф: {existing_subgraph.name} (ID: {existing_subgraph.id})")
                        subgraph_obj = existing_subgraph
                    else:
                        # Рекурсивно обрабатываем подграф
                        subgraph_obj = process_graph_recursively(
                            parser=sub_parser,
                            comsdk_graph=sub_comsdk_graph,
                            dot_path=temp_subgraph_path,
                            temp_dir=temp_dir,
                            processed_graphs=processed_graphs,
                            parent_graph=graph
                        )
                        print(f"Создан новый подграф: {subgraph_obj.name} (ID: {subgraph_obj.id})")
                except Exception as e:
                    print(f"Ошибка обработки подграфа: {str(e)}")
            else:
                print(f"Файл подграфа не найден после копирования: {temp_subgraph_path}")

        # Создаем состояние
        if current.name not in state_mapping:
            django_state, created = State.objects.get_or_create(
                name=current.name,
                graph=graph,
                defaults={
                    'is_terminal': current.is_term_state,
                    'subgraph': subgraph_obj,
                    'array_keys_mapping': current.array_keys_mapping,
                    'is_subgraph_node': subgraph_obj is not None
                }
            )

            # Если состояние уже существует - обновляем его подграф
            if not created and django_state.subgraph != subgraph_obj:
                django_state.subgraph = subgraph_obj
                django_state.is_subgraph_node = subgraph_obj is not None
                django_state.save()
                print(f"Обновлен подграф для состояния {django_state.name}")

            state_mapping[current.name] = django_state
            if subgraph_obj:
                print(f"Установлен подграф для состояния {current.name}: {subgraph_obj.name} (ID: {subgraph_obj.id})")
            else:
                print(f"Состояние {current.name} без подграфа")
        else:
            django_state = state_mapping[current.name]
            # Обновляем подграф даже если состояние уже было в маппинге
            if django_state.subgraph != subgraph_obj:
                django_state.subgraph = subgraph_obj
                django_state.is_subgraph_node = subgraph_obj is not None
                django_state.save()
                print(f"Обновлен подграф для существующего состояния {django_state.name}")

        print(f"Создано состояние: {django_state.name} (ID: {django_state.id}), subgraph={subgraph_obj is not None}")

        # Обработка переходов
        for transfer in getattr(current, 'transfers', []):
            target = transfer.output_state
            target_name = target.name

            if target_name not in state_mapping:
                # Ищем информацию о целевом состоянии в entities
                target_entity = entities.get(target.name)
                target_subgraph_path = None

                if target_entity and hasattr(target_entity, 'subgraph'):
                    target_subgraph_path = target_entity.subgraph
                    print(f"Найден подграф для целевого состояния {target.name}: {target_subgraph_path}")

                target_subgraph_obj = None
                if target_subgraph_path:
                    # Обрабатываем относительные пути
                    if not os.path.isabs(target_subgraph_path):
                        # Пытаемся найти файл в разных возможных местах
                        possible_paths = [
                            os.path.join(BASE_SEARCH_DIR, target_subgraph_path),
                        ]
                        found = False
                        for path in possible_paths:
                            if os.path.exists(path):
                                target_subgraph_path = path
                                found = True
                                print(f"Нашли файл подграфа по пути: {target_subgraph_path}")
                                break
                        if not found:
                            print(f"Файл подграфа цели не найден ни по одному из путей: {possible_paths}")
                            continue
                    else:
                        if not os.path.exists(target_subgraph_path):
                            print(f"Файл подграфа цели не найден: {target_subgraph_path}")
                            continue

                    # Копируем файл подграфа во временную директорию
                    target_subgraph_filename = os.path.basename(target_subgraph_path)
                    temp_target_subgraph_path = os.path.join(temp_dir, target_subgraph_filename)

                    if os.path.exists(target_subgraph_path):
                        shutil.copy2(target_subgraph_path, temp_target_subgraph_path)
                        print(f"Скопирован файл подграфа цели: {target_subgraph_path} -> {temp_target_subgraph_path}")
                    else:
                        print(f"Файл подграфа цели не найден: {target_subgraph_path}")
                        continue

                    if os.path.exists(temp_target_subgraph_path):
                        try:
                            target_sub_parser = Parser()
                            target_sub_comsdk_graph = target_sub_parser.parse_file(temp_target_subgraph_path)

                            # Получаем имя подграфа
                            target_subgraph_name = target_sub_parser.fact.name

                            # Проверяем, существует ли подграф в базе данных
                            existing_target_subgraph = Graph.objects.filter(
                                name=target_subgraph_name,
                                is_subgraph=True
                            ).first()

                            if existing_target_subgraph:
                                print(
                                    f"Используем существующий подграф: {existing_target_subgraph.name} (ID: {existing_target_subgraph.id})")
                                target_subgraph_obj = existing_target_subgraph
                            else:
                                target_subgraph_obj = process_graph_recursively(
                                    parser=target_sub_parser,
                                    comsdk_graph=target_sub_comsdk_graph,
                                    dot_path=temp_target_subgraph_path,
                                    temp_dir=temp_dir,
                                    processed_graphs=processed_graphs,
                                    parent_graph=graph
                                )
                        except Exception as e:
                            print(f"Ошибка обработки подграфа цели: {str(e)}")
                    else:
                        print(f"Файл подграфа цели не найден после копирования: {temp_target_subgraph_path}")

                # Создаем целевое состояние
                target_state, created = State.objects.get_or_create(
                    name=target_name,
                    graph=graph,
                    defaults={
                        'is_terminal': target.is_term_state,
                        'subgraph': target_subgraph_obj,
                        'array_keys_mapping': target.array_keys_mapping,
                        'is_subgraph_node': target_subgraph_obj is not None
                    }
                )

                # Обновляем подграф если состояние уже существовало
                if not created and target_state.subgraph != target_subgraph_obj:
                    target_state.subgraph = target_subgraph_obj
                    target_state.is_subgraph_node = target_subgraph_obj is not None
                    target_state.save()
                    print(f"Обновлен подграф для целевого состояния {target_state.name}")

                state_mapping[target_name] = target_state
                queue.append(target)
                print(f"Создано целевое состояние: {target_state.name} (ID: {target_state.id})")

            # Создаем Edge и Transfer
            edge = Edge.objects.create(
                comment=transfer.edge.comment or "",
                pred_module=transfer.edge.pred_f.module or "",
                pred_func=transfer.edge.pred_f.name or "",
                morph_module=transfer.edge.morph_f.module or "",
                morph_func=transfer.edge.morph_f.name or ""
            )

            transfer_obj = Transfer.objects.create(
                source=django_state,
                edge=edge,
                target=state_mapping[target_name],
                graph=graph,
                order=transfer.edge.order
            )
            print(f"Создан переход: {edge.comment} (ID: {transfer_obj.id})")

    return graph

event_service = get_event_service()


#@login_required
#@csrf_exempt
#@require_http_methods(["POST", "OPTIONS"])
def start_execution(request, graph_id):
    """
    Что делает: endpoint запуска обхода графа из React/legacy UI.
    Место: endpoint запуска обхода графа из React/legacy UI.
    Вход: HTTP POST, graph_id и JSON/POST data с параметрами запуска.
    Выход: JsonResponse с session_id; создает историю запуска и отправляет Celery-задачу.
    """
    graph = get_object_or_404(Graph, pk=graph_id)
    session_id = str(uuid.uuid4())

    try:
        request_data = _parse_execution_request_data(request)
        initial_data = _prepare_execution_initial_data(graph, request_data)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    create_execution_session(graph, session_id=session_id, initial_data=initial_data)
    execute_graph_task.delay(
        graph.raw_dot,
        session_id,
        initial_data
    )

    return JsonResponse({
        "session_id": session_id,
    })


def execution_events(request, session_id):
    """
    Что делает: SSE endpoint live-событий конкретной сессии исполнения.
    Место: SSE endpoint live-событий конкретной сессии исполнения.
    Вход: HTTP GET и session_id из URL /execution/events/<session_id>/.
    Выход: StreamingHttpResponse text/event-stream с событиями графа и keep-alive ping.
    """
    def event_generator():
        """
        Что делает: адаптер event_stream к Django StreamingHttpResponse.
        Место: адаптер event_stream к Django StreamingHttpResponse.
        Вход: замыкание session_id.
        Выход: генератор строк SSE; тихо завершается при отключении клиента.
        """
        try:
            for event in event_stream(session_id):
                yield event
        except GeneratorExit:
            # Клиент отключился
            pass

    response = StreamingHttpResponse(event_generator(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    #response['Connection'] = 'keep-alive'
    return response

def event_stream(session_id):
    """
    Что делает: мост между Redis pub/sub событиями исполнения и SSE-ответом Django.
    Место: мост между Redis pub/sub событиями исполнения и SSE-ответом Django.
    Вход: session_id сессии исполнения.
    Выход: генератор строк SSE data/ping; управляет подпиской и отпиской от event_service.
    """
    # Создаем очередь для получения событий
    event_queue = queue.Queue()
    # Колбэк, который будет помещать события в очередь
    def event_handler(event):
        """
        Что делает: callback подписки event_service внутри event_stream.
        Место: callback подписки event_service внутри event_stream.
        Вход: dict события исполнения из Redis.
        Выход: None; помещает событие в потокобезопасную очередь SSE-генератора.
        """
        event_queue.put(event)
    # Подписываемся на события
    event_service.subscribe(session_id, event_handler)
    print(f"[SSE] yielding ping for session: {session_id}")
    try:
        while True:
            try:
                # Ждем событие с таймаутом для проверки прерывания
                event = event_queue.get(timeout=5)
                yield f"data: {json.dumps(event)}\n\n"
            except queue.Empty:
                # Проверяем, нужно ли завершить поток
                #if threading.current_thread().stopped:
                #    break
                # Отправляем keep-alive комментарий
                yield ": ping\n\n"
                #yield ":keep-alive\n\n"
    finally:
        # Отписываемся при завершении
        event_service.unsubscribe(session_id, event_handler)
