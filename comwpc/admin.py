# admin.py
from django.contrib import admin
from .models import Graph, State, Edge, Transfer
from django.utils.html import format_html
import graphviz
from io import BytesIO
import base64
from django.urls import reverse, path
from django.utils.safestring import mark_safe

from .views import import_dot


class TransferInline(admin.TabularInline):
    model = Transfer
    extra = 1
    fields = ('source', 'edge', 'target', 'order')
    autocomplete_fields = ['source', 'target']


@admin.register(Graph)
class GraphAdmin(admin.ModelAdmin):
    list_display = ('name', 'created_at', 'graph_preview')
    inlines = [TransferInline]
    readonly_fields = ('graph_preview', 'graph_interactive')
    fields = ('name', 'parent_graph', 'is_subgraph', 'raw_dot', 'graph_preview', 'graph_interactive')
    search_fields = ('name',)  # Добавлено для автозаполнения

    def graph_preview(self, obj):
        if not obj.pk:
            return "Сначала создайте граф"

        dot = graphviz.Digraph()
        dot.attr('node', shape='box')
        dot.attr(rankdir='LR')

        # Добавляем состояния
        for state in obj.state_set.all():
            if state.subgraph:
                dot.node(
                    str(state.id),
                    label=state.name,
                    shape='folder',
                    color='orange',
                    style='filled',
                    fillcolor='moccasin',
                    URL=reverse('admin:comwpc_graph_change', args=[state.subgraph.id])
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
        for transfer in Transfer.objects.filter(source__graph=obj):
            dot.edge(
                str(transfer.source.id),
                str(transfer.target.id),
                label=transfer.edge.comment
            )

        try:
            svg_bytes = dot.pipe(format='svg')
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

            svg_str = svg_str.replace('<svg ', '<svg class="graphviz" ')
            return mark_safe(f"""
            <div style="width: 100%; overflow: auto; border: 1px solid #ddd;">
                {svg_str}
                {zoom_script}
            </div>
            """)
        except Exception as e:
            return format_html(f"<div style='color: red;'>Ошибка визуализации: {str(e)}</div>")

    graph_preview.short_description = "Визуализация графа"

    def graph_interactive(self, obj):
        if not obj.pk:
            return "Сначала создайте граф"
        return format_html(
            '<a href="{}" class="button">Открыть визуализацию</a>',
            reverse('graph_visualization', args=[obj.id])
        )

    graph_interactive.short_description = "Интерактивный просмотр"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('import-dot/',
                 self.admin_site.admin_view(import_dot),
                 name='import_dot'  # Используем простое имя
            ),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        # Формируем URL без указания приложения
        extra_context['import_dot_url'] = reverse('admin:import_dot')
        return super().changelist_view(request, extra_context=extra_context)


@admin.register(State)
class StateAdmin(admin.ModelAdmin):
    list_display = ('name', 'graph', 'is_terminal', 'has_subgraph')
    list_filter = ('graph',)
    search_fields = ('name',)
    autocomplete_fields = ['subgraph', 'graph']  # Добавлен graph для автозаполнения

    def has_subgraph(self, obj):
        return obj.subgraph is not None

    has_subgraph.boolean = True
    has_subgraph.short_description = "Имеет подграф"


@admin.register(Edge)
class EdgeAdmin(admin.ModelAdmin):
    list_display = ('comment', 'pred_func', 'morph_func')
    search_fields = ('comment',)

    def pred_func(self, obj):
        return f"{obj.pred_module}.{obj.pred_func}"

    def morph_func(self, obj):
        return f"{obj.morph_module}.{obj.morph_func}"


@admin.register(Transfer)
class TransferAdmin(admin.ModelAdmin):
    list_display = ('source', 'edge', 'target', 'graph')
    list_filter = ('graph',)
    search_fields = ('source__name', 'target__name')