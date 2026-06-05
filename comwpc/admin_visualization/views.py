import json
import re

import graphviz
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils.safestring import mark_safe

from comwpc.execution_inputs import (
    build_execution_input_context as _get_execution_input_context,
)
from comwpc.models import Graph, Transfer


SERVICE_STATES = ("__BEGIN__", "__END__")


@staff_member_required
def graph_interactive_view(request, graph_id):
    """
    Что делает: legacy/admin HTML-страница интерактивной SVG-визуализации графа.
    Место: legacy/admin HTML-страница интерактивной SVG-визуализации графа.
    Вход: HTTP GET, graph_id и optional query session для привязки live-событий.
    Выход: HTML-страница comwpc/admin_visualization/graph_interactive.html с SVG, схемой aINI и JS-визуализацией.
    """
    graph = get_object_or_404(Graph, pk=graph_id)
    session_id = request.GET.get("session")
    execution_input_schema, execution_input_error = _get_execution_input_context(graph.raw_aini)

    dot = graphviz.Digraph()
    dot.attr("node", shape="box")
    dot.attr(rankdir="TB")
    dot.attr("node", shape="rect", style="rounded,filled", fontname="Roboto")

    for state in graph.state_set.all():
        if state.name in SERVICE_STATES:
            continue

        attrs = {
            "data-name": state.name,
            "data-id": str(state.id),
        }

        if state.subgraph:
            base_name = state.subgraph.name
            if re.match(r"^.*\d+$", base_name):
                base_name = re.sub(r"\d+$", "", base_name)

            dot.node(
                str(state.id),
                label=state.name,
                shape="folder",
                color="orange",
                style="rounded,filled",
                fillcolor="moccasin",
                URL=f"javascript:openSubgraph({state.subgraph.id}, '{base_name}')",
            )
        else:
            color = "green" if state.is_terminal else "blue"
            attrs.update({
                "color": color,
                "style": "rounded,filled" if state.is_terminal else "",
                "fillcolor": "lightgreen" if state.is_terminal else "lightblue",
            })

        dot.node(
            str(state.id),
            label=state.name,
            **{
                "data-name": state.name,
                "data-id": str(state.id),
                "attributes": json.dumps(attrs),
            },
        )

    for transfer in graph.transfer_set.all():
        if transfer.source.name in SERVICE_STATES or transfer.target.name in SERVICE_STATES:
            continue

        dot.edge(
            str(transfer.source.id),
            str(transfer.target.id),
            label=transfer.edge.comment,
        )

    svg_str = dot.pipe(format="svg").decode("utf-8")
    svg_str = svg_str.replace("<svg ", '<svg style="max-width: 100%; height: auto;" ')
    svg_str = add_data_attributes(svg_str, graph)

    return render(request, "comwpc/admin_visualization/graph_interactive.html", {
        "graph": graph,
        "execution_session": session_id,
        "svg_content": mark_safe(svg_str + _ZOOM_SCRIPT),
        "is_main_graph": not graph.is_subgraph,
        "execution_input_schema": execution_input_schema,
        "execution_input_error": execution_input_error,
    })


@staff_member_required
def get_transitions(request, graph_id):
    """
    Что делает: вспомогательный admin endpoint для получения подписей переходов графа.
    Место: вспомогательный admin endpoint для получения подписей переходов графа.
    Вход: HTTP GET и graph_id.
    Выход: JsonResponse map "source-target" -> comment ребра.
    """
    graph = get_object_or_404(Graph, pk=graph_id)
    transitions = {}

    for transfer in Transfer.objects.filter(graph=graph):
        key = f"{transfer.source.name}-{transfer.target.name}"
        transitions[key] = transfer.edge.comment

    return JsonResponse(transitions)


def add_data_attributes(svg_str, graph):
    """
    Что делает: постобработка SVG Graphviz для интерактивной подсветки и раскрытия подграфов.
    Место: постобработка SVG Graphviz для интерактивной подсветки и раскрытия подграфов.
    Вход: SVG как строка и модель Graph.
    Выход: SVG-строка с data-name/data-id/data-source/data-target атрибутами.
    """
    state_mapping = {}
    for state in graph.state_set.all():
        if state.name not in SERVICE_STATES:
            state_mapping[str(state.id)] = state.name

    subgraph_mapping = {}
    for state in graph.state_set.all():
        if state.subgraph:
            subgraph_mapping[str(state.id)] = {
                "graph_id": state.subgraph.id,
                "graph_name": state.subgraph.name,
            }

    def node_replacer(match):
        """
        Что делает: callback регулярной замены узлов внутри add_data_attributes.
        Место: callback регулярной замены узлов внутри add_data_attributes.
        Вход: regex match для SVG-группы node.
        Выход: строка открывающего тега g с data-атрибутами состояния.
        """
        full_node_id = match.group(1)
        title = match.group(2)
        state_name = state_mapping.get(title, title)

        attrs = f'data-name="{state_name}" data-id="{title}"'
        if title in subgraph_mapping:
            subgraph_info = subgraph_mapping[title]
            attrs += (
                f' data-graph-id="{subgraph_info["graph_id"]}"'
                f' data-graph-name="{subgraph_info["graph_name"]}"'
            )

        return f'<g id="{full_node_id}" class="node" {attrs}>'

    def edge_replacer(match):
        """
        Что делает: callback регулярной замены ребер внутри add_data_attributes.
        Место: callback регулярной замены ребер внутри add_data_attributes.
        Вход: regex match для SVG-группы edge.
        Выход: строка открывающего тега g с data-source/data-target или исходный текст.
        """
        full_edge_id = match.group(1)
        title = match.group(2)
        parts = title.split("->")
        if len(parts) == 2:
            source_id, target_id = parts
            source_name = state_mapping.get(source_id.strip(), source_id)
            target_name = state_mapping.get(target_id.strip(), target_id)
            return (
                f'<g id="{full_edge_id}" class="edge"'
                f' data-source="{source_name}" data-target="{target_name}">'
            )
        return match.group(0)

    svg_str = re.sub(
        r'<g id="(node\d+)" class="node">\s*<title>([^<]+)</title>',
        node_replacer,
        svg_str,
    )
    svg_str = re.sub(
        r'<g id="(edge\d+)" class="edge">\s*<title>([^<]+)</title>',
        edge_replacer,
        svg_str,
    )

    return svg_str


@staff_member_required
def graph_interactive_content(request, graph_id):
    """
    Что делает: legacy endpoint для подгрузки только HTML-фрагмента графа без полной страницы.
    Место: legacy endpoint для подгрузки только HTML-фрагмента графа без полной страницы.
    Вход: HTTP GET и graph_id.
    Выход: HTML-фрагмент comwpc/admin_visualization/graph_content.html с SVG графа.
    """
    graph = get_object_or_404(Graph, pk=graph_id)

    dot = graphviz.Digraph()
    dot.attr("node", shape="box")
    dot.attr(rankdir="LR")
    dot.attr("node", shape="rect", style="rounded,filled", fontname="Roboto")

    for state in graph.state_set.all():
        if state.name in SERVICE_STATES:
            continue

        if state.subgraph:
            dot.node(
                str(state.id),
                label=state.name,
                shape="folder",
                color="orange",
                style="rounded,filled",
                fillcolor="moccasin",
                URL=f"javascript:openSubgraph({state.subgraph.id})",
            )
        else:
            color = "green" if state.is_terminal else "blue"
            dot.node(
                str(state.id),
                label=state.name,
                color=color,
                style="rounded,filled" if state.is_terminal else "",
                fillcolor="lightgreen" if state.is_terminal else "lightblue",
            )

    for transfer in graph.transfer_set.all():
        if transfer.source.name in SERVICE_STATES or transfer.target.name in SERVICE_STATES:
            continue

        dot.edge(
            str(transfer.source.id),
            str(transfer.target.id),
            label=transfer.edge.comment,
        )

    svg_str = dot.pipe(format="svg").decode("utf-8")
    svg_str = svg_str.replace("<svg ", '<svg style="max-width: 100%; height: auto;" ')

    return render(request, "comwpc/admin_visualization/graph_content.html", {
        "graph": graph,
        "svg_content": mark_safe(svg_str),
    })


@staff_member_required
def graph_svg_view(request, graph_id):
    """
    Что делает: endpoint получения чистого SVG графа для legacy-интерфейса и модальных подграфов.
    Место: endpoint получения чистого SVG графа для legacy-интерфейса и модальных подграфов.
    Вход: HTTP GET и graph_id.
    Выход: HttpResponse image/svg+xml с data-атрибутами для интерактивности.
    """
    graph = get_object_or_404(Graph, pk=graph_id)
    dot = graphviz.Digraph()
    dot.attr("node", shape="box")
    dot.attr(rankdir="TB")
    dot.attr(
        "node",
        shape="rect",
        style="rounded,filled",
        fontname="Roboto",
        fontsize="12",
        width="1.5",
        height="0.8",
    )

    for state in graph.state_set.all():
        if state.name in SERVICE_STATES:
            continue

        if state.subgraph:
            dot.node(
                str(state.id),
                label=state.name,
                shape="folder",
                color="#e67e22",
                style="rounded,filled",
                fillcolor="#fff4e5",
            )
        elif state.is_terminal:
            dot.node(
                str(state.id),
                label=state.name,
                color="#28a745",
                style="rounded,filled",
                fillcolor="#e7f5e9",
            )
        else:
            dot.node(
                str(state.id),
                label=state.name,
                color="#417690",
                style="rounded,filled",
                fillcolor="#f0f7ff",
            )

    for transfer in graph.transfer_set.all():
        if transfer.source.name in SERVICE_STATES or transfer.target.name in SERVICE_STATES:
            continue

        dot.edge(
            str(transfer.source.id),
            str(transfer.target.id),
            label=transfer.edge.comment,
        )

    svg_str = dot.pipe(format="svg").decode("utf-8")
    svg_str = add_data_attributes(svg_str, graph)
    svg_str = re.sub(r"<svg ", f'<svg data-graph-name="{graph.name}" ', svg_str)

    return HttpResponse(svg_str, content_type="image/svg+xml")


_ZOOM_SCRIPT = """
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

        if (newWidth < width/10 || newWidth > width*10) return;

        let newX = viewBox.x - (mouseX / svgElement.clientWidth) * (newWidth - viewBox.width);
        let newY = viewBox.y - (mouseY / svgElement.clientHeight) * (newHeight - viewBox.height);

        viewBox.x = newX;
        viewBox.y = newY;
        viewBox.width = newWidth;
        viewBox.height = newHeight;
    });

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
