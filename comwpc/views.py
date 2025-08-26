import json
import queue
import re
import shutil
import time

from django.core.exceptions import PermissionDenied
from django.shortcuts import render, get_object_or_404
from django.urls import reverse
from django.utils.safestring import mark_safe

from comwpc.models import Graph
import graphviz
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.contrib.admin.views.decorators import staff_member_required

from django.contrib.auth.decorators import login_required

from config import settings

execution_status = {}


@staff_member_required
def graph_interactive_view(request, graph_id):
    graph = get_object_or_404(Graph, pk=graph_id)
    session_id = request.GET.get('session')

    dot = graphviz.Digraph()
    dot.attr('node', shape='box')
    #dot.attr(rankdir='LR')
    dot.attr(rankdir='TB')
    dot.attr('node', shape='rect', style='rounded,filled', fontname='Roboto')

    # Добавляем состояния с атрибутом data-name
    for state in graph.state_set.all():
        attrs = {
            'data-name': state.name,
            'data-id': str(state.id)
        }

        #if state.subgraph:
        #    attrs.update({
        #        'shape': 'folder',
        #        'color': 'orange',
        #        'style': 'filled',
        #        'fillcolor': 'moccasin',
        #        'URL': f"javascript:openSubgraph({state.subgraph.id})"
        #    })
        if state.subgraph:
            base_name = state.subgraph.name
            if re.match(r'^.*\d+$', base_name):
                base_name = re.sub(r'\d+$', '', base_name)

            dot.node(
                str(state.id),
                label=state.name,
                shape='folder',
                color='orange',
                style='rounded,filled',
                fillcolor='moccasin',
                URL=f"javascript:openSubgraph({state.subgraph.id}, '{base_name}')"
            )
        else:
            color = 'green' if state.is_terminal else 'blue'
            attrs.update({
                'color': color,
                'style': 'rounded,filled' if state.is_terminal else '',
                'fillcolor': 'lightgreen' if state.is_terminal else 'lightblue'
            })
        dot.node(
            str(state.id),
            label=state.name,
            **{
                'data-name': state.name,  # Явное указание атрибутов
                'data-id': str(state.id),
                'attributes': json.dumps(attrs)
            }
        )
    # Добавляем переходы
    for transfer in graph.transfer_set.all():
        dot.edge(
            str(transfer.source.id),
            str(transfer.target.id),
            label=transfer.edge.comment
        )

    svg_bytes = dot.pipe(format='svg')

    # Добавляем JavaScript для интерактивности
    svg_str = svg_bytes.decode('utf-8')
    zoom_script = """
    <script>
    function enableZoom(svgElement) {
        let viewBox = svgElement.viewBox.baseVal;
        let width = viewBox.width;
        let height = viewBox.height;

        svgElement.addEventListener('wheel', function(e) {
            e.preventDefault();

            let zoom = e.deltaY > 0 ? 1.1 : 0.9;
            let mouseX = e.clientX - svgElement.getBoundingClientRect().left;
            let mouseY = e.clientY - svgElement.getBoundingClientRect().top;

            let newWidth = viewBox.width * zoom;
            let newHeight = viewBox.height * zoom;

            // Ограничиваем минимальный и максимальный масштаб
            if (newWidth < width/10 || newWidth > width*10) return;

            // Вычисляем новые координаты viewBox
            let newX = viewBox.x - (mouseX / svgElement.clientWidth) * (newWidth - viewBox.width);
            let newY = viewBox.y - (mouseY / svgElement.clientHeight) * (newHeight - viewBox.height);

            viewBox.x = newX;
            viewBox.y = newY;
            viewBox.width = newWidth;
            viewBox.height = newHeight;
        });

        // Добавляем обработчик для сброса масштаба по двойному клику
        svgElement.addEventListener('dblclick', function(e) {
            e.preventDefault();
            viewBox.x = 0;
            viewBox.y = 0;
            viewBox.width = width;
            viewBox.height = height;
        });
    }

    document.addEventListener('DOMContentLoaded', function() {
        let svgElement = document.querySelector('svg');
        enableZoom(svgElement);
    });
    </script>
    """

    svg_str = svg_str.replace('<svg ', '<svg style="max-width: 100%; height: auto;" ')

    # Модифицируем SVG для добавления data-атрибутов
    svg_str = add_data_attributes(svg_str, graph)

    return render(request, 'comwpc/graph_interactive.html', {
        'graph': graph,
        'execution_session': session_id,
        'svg_content': mark_safe(svg_str + zoom_script),
        'is_main_graph': not graph.is_subgraph
    })


@staff_member_required
def get_transitions(request, graph_id):
    graph = get_object_or_404(Graph, pk=graph_id)
    transitions = {}

    for transfer in Transfer.objects.filter(graph=graph):
        key = f"{transfer.source.name}-{transfer.target.name}"
        transitions[key] = transfer.edge.comment

    return JsonResponse(transitions)


def add_data_attributes(svg_str, graph):
    """Добавляет data-атрибуты в SVG для интерактивности"""
    # Создаем маппинг id состояния -> имя
    state_mapping = {str(state.id): state.name for state in graph.state_set.all()}

    # Создаем маппинг для подграфов
    subgraph_mapping = {}
    for state in graph.state_set.all():
        if state.subgraph:
            subgraph_mapping[str(state.id)] = {
                'graph_id': state.subgraph.id,
                'graph_name': state.subgraph.name
            }

    # Функция для замены узлов
    def node_replacer(match):
        full_node_id = match.group(1)
        title = match.group(2)
        state_name = state_mapping.get(title, title)

        # Добавляем атрибуты для подграфов
        attrs = f'data-name="{state_name}" data-id="{title}"'
        if title in subgraph_mapping:
            subgraph_info = subgraph_mapping[title]
            attrs += f' data-graph-id="{subgraph_info["graph_id"]}" data-graph-name="{subgraph_info["graph_name"]}"'

        return f'<g id="{full_node_id}" class="node" {attrs}>'

    # Функция для замены ребер
    def edge_replacer(match):
        full_edge_id = match.group(1)
        title = match.group(2)
        parts = title.split('->')
        if len(parts) == 2:
            source_id, target_id = parts
            source_name = state_mapping.get(source_id.strip(), source_id)
            target_name = state_mapping.get(target_id.strip(), target_id)
            return f'<g id="{full_edge_id}" class="edge" data-source="{source_name}" data-target="{target_name}">'
        return match.group(0)

    # Заменяем узлы
    svg_str = re.sub(
        r'<g id="(node\d+)" class="node">\s*<title>([^<]+)<\/title>',
        node_replacer,
        svg_str
    )

    # Заменяем ребра
    svg_str = re.sub(
        r'<g id="(edge\d+)" class="edge">\s*<title>([^<]+)<\/title>',
        edge_replacer,
        svg_str
    )

    return svg_str


@staff_member_required
def graph_interactive_content(request, graph_id):
    """Представление для загрузки только содержимого графа (без шаблона)"""
    graph = get_object_or_404(Graph, pk=graph_id)

    # Генерация SVG аналогична основной функции
    dot = graphviz.Digraph()
    dot.attr('node', shape='box')
    dot.attr(rankdir='LR')
    dot.attr('node', shape='rect', style='rounded,filled', fontname='Roboto')

    # Добавляем состояния
    for state in graph.state_set.all():
        if state.subgraph:
            dot.node(
                str(state.id),
                label=state.name,
                shape='folder',
                color='orange',
                style='rounded,filled',
                fillcolor='moccasin',
                URL=f"javascript:openSubgraph({state.subgraph.id})"
            )
        else:
            color = 'green' if state.is_terminal else 'blue'
            dot.node(
                str(state.id),
                label=state.name,
                color=color,
                style='rounded,filled' if state.is_terminal else '',
                fillcolor='lightgreen' if state.is_terminal else 'lightblue'
            )

    # Добавляем переходы
    for transfer in graph.transfer_set.all():
        dot.edge(
            str(transfer.source.id),
            str(transfer.target.id),
            label=transfer.edge.comment
        )

    svg_bytes = dot.pipe(format='svg')
    svg_str = svg_bytes.decode('utf-8')
    svg_str = svg_str.replace('<svg ', '<svg style="max-width: 100%; height: auto;" ')

    return render(request, 'comwpc/graph_content.html', {
        'graph': graph,
        'svg_content': mark_safe(svg_str)
    })


@staff_member_required
def graph_svg_view(request, graph_id):
    graph = get_object_or_404(Graph, pk=graph_id)
    dot = graphviz.Digraph()
    dot.attr('node', shape='box')
    dot.attr(rankdir='TB')

    # Устанавливаем единые стили для всех узлов
    dot.attr('node',
             shape='rect',
             style='rounded,filled',
             fontname='Roboto',
             fontsize='12',
             width='1.5',
             height='0.8')

    for state in graph.state_set.all():
        if state.subgraph:
            base_name = state.subgraph.name
            if re.match(r'^.*\d+$', base_name):
                base_name = re.sub(r'\d+$', '', base_name)

            dot.node(
                str(state.id),
                label=state.name,
                shape='folder',
                color='#e67e22',  # Оранжевый
                style='rounded,filled',
                fillcolor='#fff4e5'  # Светло-оранжевый
            )
        else:
            if state.is_terminal:
                # Терминальный узел - зеленый
                dot.node(
                    str(state.id),
                    label=state.name,
                    color='#28a745',  # Зеленый
                    style='rounded,filled',
                    fillcolor='#e7f5e9'  # Светло-зеленый
                )
            else:
                # Обычный узел - синий
                dot.node(
                    str(state.id),
                    label=state.name,
                    color='#417690',  # Синий
                    style='rounded,filled',
                    fillcolor='#f0f7ff'  # Светло-голубой
                )

    # Добавляем переходы
    for transfer in graph.transfer_set.all():
        dot.edge(
            str(transfer.source.id),
            str(transfer.target.id),
            label=transfer.edge.comment
        )

    # Генерируем SVG
    svg_bytes = dot.pipe(format='svg')
    svg_str = svg_bytes.decode('utf-8')

    # Добавляем data-атрибуты
    svg_str = add_data_attributes(svg_str, graph)

    svg_str = re.sub(
        r'<svg ',
        f'<svg data-graph-name="{graph.name}" ',
        svg_str
    )

    return HttpResponse(svg_str, content_type='image/svg+xml')

# Для парсинга графа
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from .forms import DotImportForm
from .models import Graph, State, Edge, Transfer
from comsdk.parser import Parser
from comsdk.graph import Graph as ComsdkGraph, State as ComsdkState
import tempfile
import os
from collections import deque


@staff_member_required
def import_dot(request):
    if request.method == 'POST':
        form = DotImportForm(request.POST, request.FILES)
        if form.is_valid():
            dot_file = request.FILES['dot_file']

            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                try:
                    temp_dir = tempfile.mkdtemp()
                    main_temp_path = os.path.join(temp_dir, dot_file.name)

                    with open(main_temp_path, 'wb+') as destination:
                        for chunk in dot_file.chunks():
                            destination.write(chunk)

                    parser = Parser()
                    comsdk_graph = parser.parse_file(main_temp_path)
                    graph_name = parser.fact.name

                    # Проверяем существование основного графа
                    existing_main_graph = Graph.objects.filter(
                        name=graph_name,
                        is_subgraph=False
                    ).first()

                    if existing_main_graph:
                        return JsonResponse({
                            'success': False,
                            'error': f'Граф "{graph_name}" уже существует (ID: {existing_main_graph.id})'
                        })

                    # Обрабатываем граф рекурсивно
                    processed_graphs = {}
                    django_graph = process_graph_recursively(
                        parser=parser,
                        comsdk_graph=comsdk_graph,
                        dot_path=main_temp_path,
                        temp_dir=temp_dir,
                        processed_graphs=processed_graphs,
                        parent_graph=None
                    )

                    stats = {
                        'states': django_graph.state_set.count(),
                        'edges': Edge.objects.filter(transfer__graph=django_graph).count(),
                        'subgraphs': Graph.objects.filter(parent_graph=django_graph).count()
                    }

                    return JsonResponse({
                        'success': True,
                        'redirect_url': reverse('admin:comwpc_graph_change', args=[django_graph.id]),
                        'stats': stats
                    })
                except Exception as e:
                    return JsonResponse({
                        'success': False,
                        'error': str(e)
                    })
            else:
                # Обработка для обычных запросов
                try:
                    form = DotImportForm()
                # ... (код для обычных запросов)
                except Exception as e:
                    messages.error(request, f'Ошибка импорта: {str(e)}')
        else:
            return JsonResponse({
                'success': False,
                'error': 'Неверный формат файла'
            })

    form = DotImportForm()
    return render(request, 'admin/import_dot.html', {'form': form})

def import_progress(request):
    def event_stream():
        # Эмуляция прогресса
        for i in range(1, 101):
            time.sleep(0.5)
            yield f"data: {{\"progress\": {i}, \"message\": \"Обработано {i}%\"}}\n\n"

    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    return response


def process_graph_recursively(parser, comsdk_graph, dot_path, temp_dir, processed_graphs, parent_graph=None):
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

from .events import get_event_service
from django.http import JsonResponse
import uuid
import threading

event_service = get_event_service()

from config.tasks import execute_graph_task


@login_required
def start_execution(request, graph_id):
    graph = get_object_or_404(Graph, pk=graph_id)
    session_id = str(uuid.uuid4())

    initial_data = json.loads(request.POST.get('data', '{}'))
    initial_data.setdefault("a", 10)

    # ТОЛЬКО вызов Celery задачи
    execute_graph_task.delay(
        graph.raw_dot,
        session_id,
        initial_data
    )

    return JsonResponse({
        'session_id': session_id,
    })

def execution_events(request, session_id):
    def event_generator():
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
    # Создаем очередь для получения событий
    event_queue = queue.Queue()
    # Колбэк, который будет помещать события в очередь
    def event_handler(event):
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
