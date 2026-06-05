import graphviz
from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from django.utils.safestring import mark_safe

from comwpc.models import Transfer


@admin.display(description="Визуализация графа")
def graph_preview_html(obj):
    """
    Что делает: read-only поле карточки Graph в Django admin.
    Место: read-only поле карточки Graph в Django admin.
    Вход: объект Graph из админки.
    Выход: HTML/SVG-превью графа или сообщение об ошибке/отсутствующем объекте.
    """
    if not obj.pk:
        return "Сначала создайте граф"

    dot = graphviz.Digraph()
    dot.attr("node", shape="rect", style="rounded,filled", fontname="Roboto")
    dot.attr(rankdir="LR")

    for state in obj.state_set.all():
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

    for transfer in Transfer.objects.filter(source__graph=obj):
        dot.edge(
            str(transfer.source.id),
            str(transfer.target.id),
            label=transfer.edge.comment,
        )

    try:
        svg_str = dot.pipe(format="svg").decode("utf-8")
        svg_str = svg_str.replace("<svg ", '<svg class="graphviz" ')
        return mark_safe(f"""
        <div style="width: 100%; overflow: auto; border: 1px solid #ddd;">
            {svg_str}
            {_PREVIEW_ZOOM_SCRIPT}
        </div>
        """)
    except Exception as exc:
        return format_html(
            "<div style='color: red;'>Ошибка визуализации: {}</div>",
            str(exc),
        )


@admin.display(description="Интерактивный просмотр")
def graph_interactive_link(obj):
    """
    Что делает: read-only поле Graph admin с переходом в интерактивную визуализацию.
    Место: read-only поле Graph admin с переходом в интерактивную визуализацию.
    Вход: объект Graph из админки.
    Выход: HTML-ссылка на graph_visualization или подсказка создать граф.
    """
    if not obj.pk:
        return "Сначала создайте граф"
    return format_html(
        '<a href="{}" class="button">Открыть визуализацию</a>',
        reverse("graph_visualization", args=[obj.id]),
    )


class GraphVisualizationAdminMixin:
    """Подключает поля admin-превью графа к GraphAdmin без хранения SVG-логики в admin.py."""

    @admin.display(description="Визуализация графа")
    def graph_preview(self, obj):
        """
        Что делает: read-only поле GraphAdmin, подключаемое через mixin.
        Место: read-only поле GraphAdmin, подключаемое через mixin.
        Вход: self GraphAdmin и объект Graph.
        Выход: HTML/SVG-превью графа.
        """
        return graph_preview_html(obj)

    @admin.display(description="Интерактивный просмотр")
    def graph_interactive(self, obj):
        """
        Что делает: read-only поле GraphAdmin, подключаемое через mixin.
        Место: read-only поле GraphAdmin, подключаемое через mixin.
        Вход: self GraphAdmin и объект Graph.
        Выход: HTML-ссылка на интерактивную visualisation-страницу.
        """
        return graph_interactive_link(obj)


_PREVIEW_ZOOM_SCRIPT = """
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
    let svgElements = document.querySelectorAll('svg.graphviz');
    svgElements.forEach(enableZoom);
});
</script>
"""
