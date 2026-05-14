# admin.py
from django.contrib import admin
from .models import Graph, State, Edge, Transfer
from django.urls import reverse, path

from .admin_visualization.admin_fields import GraphVisualizationAdminMixin
from .views import import_dot


admin.site.site_header = "Проект GCD"  # Заголовок в шапке
admin.site.site_title = "Проект GCD"  # Текст для вкладки браузера
admin.site.index_title = "Добро пожаловать в панель GCD"  # Заголовок на главной


class TransferInline(admin.TabularInline):
    model = Transfer
    extra = 1
    fields = ('source', 'edge', 'target', 'order')
    autocomplete_fields = ['source', 'target']


@admin.register(Graph)
class GraphAdmin(GraphVisualizationAdminMixin, admin.ModelAdmin):
    list_display = ('name', 'created_at', 'graph_preview')
    inlines = [TransferInline]
    readonly_fields = ('graph_preview', 'graph_interactive')
    fields = ('name', 'parent_graph', 'is_subgraph', 'raw_dot', 'raw_aini', 'graph_preview', 'graph_interactive')
    search_fields = ('name',)  # Добавлено для автозаполнения

    def get_urls(self):
        """
        Что делает: расширение маршрутов Django admin для модели Graph.
        Место: расширение маршрутов Django admin для модели Graph.
        Вход: self GraphAdmin.
        Выход: список URL, где import-dot добавлен перед стандартными admin URL.
        """
        urls = super().get_urls()
        custom_urls = [
            path('import-dot/',
                 self.admin_site.admin_view(import_dot),
                 name='import_dot'  # Используем простое имя
            ),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        """
        Что делает: страница списка графов в Django admin.
        Место: страница списка графов в Django admin.
        Вход: HTTP-запрос админки и optional extra_context.
        Выход: стандартный admin response с добавленным import_dot_url.
        """
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
        """
        Что делает: boolean-колонка State admin.
        Место: boolean-колонка State admin.
        Вход: объект State.
        Выход: True, если состояние связано с подграфом.
        """
        return obj.subgraph is not None

    has_subgraph.boolean = True
    has_subgraph.short_description = "Имеет подграф"


@admin.register(Edge)
class EdgeAdmin(admin.ModelAdmin):
    list_display = ('comment', 'pred_func', 'morph_func')
    search_fields = ('comment',)

    def pred_func(self, obj):
        """
        Что делает: вычисляемая колонка Edge admin для функции-предиката.
        Место: вычисляемая колонка Edge admin для функции-предиката.
        Вход: объект Edge.
        Выход: строка module.function для предиката.
        """
        return f"{obj.pred_module}.{obj.pred_func}"

    def morph_func(self, obj):
        """
        Что делает: вычисляемая колонка Edge admin для функции-морфизма.
        Место: вычисляемая колонка Edge admin для функции-морфизма.
        Вход: объект Edge.
        Выход: строка module.function для морфизма.
        """
        return f"{obj.morph_module}.{obj.morph_func}"


@admin.register(Transfer)
class TransferAdmin(admin.ModelAdmin):
    list_display = ('source', 'edge', 'target', 'graph')
    list_filter = ('graph',)
    search_fields = ('source__name', 'target__name')
