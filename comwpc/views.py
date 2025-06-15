# views.py
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, get_object_or_404
from django.urls import reverse
from django.utils.safestring import mark_safe

from .models import Graph
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