from django.contrib import admin
from .models import Graph, State, Edge, Transfer
from django.utils.html import format_html
import graphviz
from io import BytesIO
import base64


class TransferInline(admin.TabularInline):
    model = Transfer
    extra = 1
    fields = ('source', 'edge', 'target', 'order')
    autocomplete_fields = ['source', 'target']


@admin.register(Graph)
class GraphAdmin(admin.ModelAdmin):
    list_display = ('name', 'created_at', 'graph_preview')
    inlines = [TransferInline]
    readonly_fields = ('graph_preview',)
    def graph_preview(self, obj):
        if not obj.pk:
            return "Сначала создайте граф"
        # Создаем граф
        dot = graphviz.Digraph()
        # Настройки графа
        dot.attr('node', shape='box')
        dot.attr(rankdir='LR')  # Горизонтальная ориентация
        # Добавляем состояния
        for state in obj.state_set.all():
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
        # Генерируем изображение
        try:
            png_bytes = dot.pipe(format='png')
            img_base64 = base64.b64encode(png_bytes).decode('utf-8')
            return format_html(
                '<img src="data:image/png;base64,{}" style="max-width: 100%; height: auto;"/>',
                img_base64
            )
        except Exception as e:
            return format_html(f"<div style='color: red;'>Ошибка визуализации: {str(e)}</div>")
    graph_preview.short_description = "Визуализация графа"


@admin.register(State)
class StateAdmin(admin.ModelAdmin):
    list_display = ('name', 'graph', 'is_terminal')
    list_filter = ('graph',)
    search_fields = ('name',)


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