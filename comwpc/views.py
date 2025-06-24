import shutil

from django.core.exceptions import PermissionDenied
from django.shortcuts import render, get_object_or_404
from django.urls import reverse
from django.utils.safestring import mark_safe

from comwpc.models import Graph
import graphviz
from django.http import HttpResponse

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

            try:
                # Создаем временную директорию
                temp_dir = tempfile.mkdtemp()
                temp_path = os.path.join(temp_dir, dot_file.name)

                # Сохраняем файл
                with open(temp_path, 'wb+') as destination:
                    for chunk in dot_file.chunks():
                        destination.write(chunk)

                # Парсим файл
                parser = Parser()
                comsdk_graph = parser.parse_file(temp_path)

                # Преобразуем в Django-модель
                django_graph = convert_comsdk_to_django(parser, comsdk_graph, temp_path)

                messages.success(request, 'Граф успешно импортирован!')
                return redirect('admin:comwpc_graph_change', django_graph.id)

            except Exception as e:
                messages.error(request, f'Ошибка импорта: {str(e)}')
            finally:
                # Удаляем временную директорию
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
    else:
        form = DotImportForm()

    return render(request, 'admin/import_dot.html', {'form': form})


def convert_comsdk_to_django(parser, comsdk_graph, dot_path=None):
    # Создаем объект Graph
    with open(dot_path, 'r', encoding='utf-8') as f:
        dot_content = f.read()

    # Создаем объект Graph
    graph = Graph.objects.create(
        name=parser.fact.name,
        raw_dot=dot_content
    )

    # Словарь для сопоставления состояний
    state_mapping = {}
    queue = deque([comsdk_graph.init_state])
    visited = set()

    print(f'queue={queue}')

    while queue:
        current = queue.popleft()
        if current.name in visited:
            continue
        print(f'current={current}')
        visited.add(current.name)
        #todo
        # Делал для обработки подграфов, не сработало
        #subgraph = None
        #if hasattr(current, 'subgraph') and current.subgraph:
        #    # Создаем или получаем подграф
        #    subgraph, _ = Graph.objects.get_or_create(
        #        name=current.subgraph.init_state.name,
        #        defaults={
        #            'raw_dot': current.subgraph.raw_dot if hasattr(current.subgraph, 'raw_dot') else '',
        #            'is_subgraph': True,
        #            'parent_graph': graph
        #        }
        #    )

        # Создаем Django State
        if current.name not in state_mapping:
            django_state = State.objects.create(
                name=current.name,
                graph=graph,
                is_terminal=current.is_term_state,
                array_keys_mapping=current.array_keys_mapping
            )
            state_mapping[current.name] = django_state
        else:
            django_state = state_mapping[current.name]

        # Обрабатываем переходы
        print(f'current.transfers={current.transfers}')
        for transfer in current.transfers:
            print(f'transfer={transfer}')
            target = transfer.output_state
            target_name = target.name
            print(state_mapping)
            # Создаем целевое состояние если нужно
            print(f'target_name={target_name}')
            if target_name not in state_mapping:
                #todo
                # Делал для обработки подграфов, не сработало
                #target_subgraph = None
                #if hasattr(target, 'subgraph') and target.subgraph:
                #    target_subgraph, _ = Graph.objects.get_or_create(
                #        name=target.subgraph.init_state.name,
                #        defaults={
                #            'raw_dot': target.subgraph.raw_dot if hasattr(target.subgraph, 'raw_dot') else '',
                #            'is_subgraph': True,
                #            'parent_graph': graph
                #        }
                #    )
                state_mapping[target_name] = State.objects.create(
                    name=target_name,
                    graph=graph,
                    is_terminal=target.is_term_state,
                    array_keys_mapping=target.array_keys_mapping
                )
                queue.append(target)

            # Создаем Edge
            edge = Edge.objects.create(
                comment=transfer.edge.comment or "",
                pred_module=transfer.edge.pred_f.module or "",
                pred_func=transfer.edge.pred_f.name or "",
                morph_module=transfer.edge.morph_f.module or "",
                morph_func=transfer.edge.morph_f.name or ""
            )

            # Создаем Transfer
            Transfer.objects.create(
                source=django_state,
                edge=edge,
                target=state_mapping[target_name],
                graph=graph,
                order=transfer.edge.order
            )

    return graph