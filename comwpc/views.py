import shutil
import time

from django.core.exceptions import PermissionDenied
from django.shortcuts import render, get_object_or_404
from django.urls import reverse
from django.utils.safestring import mark_safe

from comwpc.models import Graph
import graphviz
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse

from django.contrib.auth.decorators import login_required

@login_required
def graph_interactive_view(request, graph_id):
    graph = get_object_or_404(Graph, pk=graph_id)

    dot = graphviz.Digraph()
    dot.attr('node', shape='box')
    dot.attr(rankdir='LR')

    # Добавляем состояния
    for state in graph.state_set.all():
        if state.subgraph:
            dot.node(
                str(state.id),
                label=state.name,
                shape='folder',
                color='orange',
                style='filled',
                fillcolor='moccasin',
                URL=f"javascript:openSubgraph({state.subgraph.id})"  # JavaScript для открытия модального окна
            )
        else:
            color = 'green' if state.is_terminal else 'blue'
            dot.node(
                str(state.id),
                label=state.name,
                color=color,
                style='filled' if state.is_terminal else '',
                fillcolor='lightgreen' if state.is_terminal else 'lightblue'
            )

    # Остальной код без изменений...

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

    return render(request, 'comwpc/graph_interactive.html', {
        'graph': graph,
        'svg_content': mark_safe(svg_str + zoom_script)
    })

@login_required
def graph_interactive_content(request, graph_id):
    """Представление для загрузки только содержимого графа (без шаблона)"""
    graph = get_object_or_404(Graph, pk=graph_id)

    # Генерация SVG аналогична основной функции
    dot = graphviz.Digraph()
    dot.attr('node', shape='box')
    dot.attr(rankdir='LR')

    # Добавляем состояния
    for state in graph.state_set.all():
        if state.subgraph:
            dot.node(
                str(state.id),
                label=state.name,
                shape='folder',
                color='orange',
                style='filled',
                fillcolor='moccasin',
                URL=f"javascript:openSubgraph({state.subgraph.id})"
            )
        else:
            color = 'green' if state.is_terminal else 'blue'
            dot.node(
                str(state.id),
                label=state.name,
                color=color,
                style='filled' if state.is_terminal else '',
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

            # Для AJAX-запросов возвращаем JSON
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                try:
                    temp_dir = tempfile.mkdtemp()
                    main_temp_path = os.path.join(temp_dir, dot_file.name)

                    # Сохраняем основной файл
                    with open(main_temp_path, 'wb+') as destination:
                        for chunk in dot_file.chunks():
                            destination.write(chunk)

                    # Парсим основной файл
                    parser = Parser()
                    comsdk_graph = parser.parse_file(main_temp_path)

                    # Обрабатываем граф рекурсивно
                    processed_graphs = {}  # Кеш для уже обработанных графов
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

                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return JsonResponse({
                            'success': True,
                            'redirect_url': reverse('admin:comwpc_graph_change', args=[django_graph.id]),
                            'stats': stats
                        })
                    else:
                        messages.success(request, 'Импорт завершен!')
                        return redirect('admin:comwpc_graph_change', django_graph.id)

                except Exception as e:
                    return JsonResponse({
                        'success': False,
                        'error': str(e)
                    })
            else:
                # Обработка для обычных запросов
                try:
                    form = DotImportForm()
                except Exception as e:
                    messages.error(request, f'Ошибка импорта: {str(e)}')
        else:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return JsonResponse({
                    'success': False,
                    'error': 'Неверный формат файла'
                })

    # Для GET-запросов
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
    """Рекурсивно обрабатывает граф и его подграфы с использованием parser.fact.entities"""
    if dot_path in processed_graphs:
        return processed_graphs[dot_path]

    print(f"Обработка графа: {dot_path}")

    with open(dot_path, 'r', encoding='utf-8') as f:
        dot_content = f.read()

    # Проверяем, существует ли граф с таким именем
    graph_name = parser.fact.name
    existing_graph = Graph.objects.filter(name=graph_name, is_subgraph=(parent_graph is not None)).first()

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
        print(f"Создан новый граф: {graph.name} (ID: {graph.id}), is_subgraph={parent_graph is not None}")

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
                base_dir = os.path.dirname(dot_path)
                subgraph_path = os.path.join(base_dir, subgraph_path)

            # Формируем путь во временной директории
            subgraph_filename = os.path.basename(subgraph_path)
            temp_subgraph_path = os.path.join(temp_dir, subgraph_filename)

            # Копируем файл подграфа, если его нет
            if not os.path.exists(temp_subgraph_path) and os.path.exists(subgraph_path):
                shutil.copy2(subgraph_path, temp_subgraph_path)

            if os.path.exists(temp_subgraph_path):
                try:
                    print(f"Обработка подграфа: {temp_subgraph_path}")
                    # Парсим подграф
                    sub_parser = Parser()
                    sub_comsdk_graph = sub_parser.parse_file(temp_subgraph_path)

                    # Получаем имя подграфа
                    subgraph_name = sub_parser.fact.name

                    # Проверяем, существует ли подграф в базе данных
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
                print(f"Файл подграфа не найден: {temp_subgraph_path}")

        # Создаем состояние
        if current.name not in state_mapping:
            django_state = State.objects.create(
                name=current.name,
                graph=graph,
                is_terminal=current.is_term_state,
                subgraph=subgraph_obj,
                array_keys_mapping=current.array_keys_mapping,
                is_subgraph_node=subgraph_obj is not None
            )
            state_mapping[current.name] = django_state
        else:
            django_state = state_mapping[current.name]
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
                    if not os.path.isabs(target_subgraph_path):
                        base_dir = os.path.dirname(dot_path)
                        target_subgraph_path = os.path.join(base_dir, target_subgraph_path)

                    target_subgraph_filename = os.path.basename(target_subgraph_path)
                    temp_target_subgraph_path = os.path.join(temp_dir, target_subgraph_filename)

                    # Копируем файл подграфа, если его нет
                    if not os.path.exists(temp_target_subgraph_path) and os.path.exists(target_subgraph_path):
                        shutil.copy2(target_subgraph_path, temp_target_subgraph_path)

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
                        print(f"Файл подграфа цели не найден: {temp_target_subgraph_path}")

                # Создаем целевое состояние
                target_state = State.objects.create(
                    name=target_name,
                    graph=graph,
                    is_terminal=target.is_term_state,
                    subgraph=target_subgraph_obj,
                    array_keys_mapping=target.array_keys_mapping,
                    is_subgraph_node=target_subgraph_obj is not None
                )
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