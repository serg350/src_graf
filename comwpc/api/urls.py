from django.urls import path

from .. import views
from ..views import graph_json, graph_deep_json, graphs_list_json

urlpatterns = [
    path("graphs/", graphs_list_json, name="graphs-list"),
    path("graphs/<int:graph_id>/", graph_json, name="graph-json"),
    path("graphs/<int:graph_id>/deep/", graph_deep_json, name="graph-json-deep"),
    path('comwpc/graph/import-dot/',
        views.import_dot_api,
        name='comwpc_import_dot'
     ),
]
