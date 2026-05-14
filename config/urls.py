from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from comwpc import views
from comwpc.admin_visualization import views as admin_visualization_views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('import-progress/', views.import_progress, name='import_progress'),
    path('graph-visualization/<int:graph_id>/',
         admin_visualization_views.graph_interactive_view,
         name='graph_visualization'),
    path('graph-svg/<int:graph_id>/',
         admin_visualization_views.graph_svg_view,
         name='graph_svg'),
    path('graph-visualization/<int:graph_id>/content/',
         admin_visualization_views.graph_interactive_content,
         name='graph_content'),
    path('graph/<int:graph_id>/start/', views.start_execution, name='start-execution'),
    path('execution/events/<str:session_id>/', views.execution_events, name='execution-events'),
    path('graph/<int:graph_id>/visualize/', admin_visualization_views.graph_interactive_view, name='graph-visualization'),
    path('api/transitions/<int:graph_id>/', admin_visualization_views.get_transitions, name='get_transitions'),
    path("api/", include("comwpc.api.urls")),
    ]

urlpatterns += static(
        settings.STATIC_URL, document_root=settings.STATIC_ROOT
    )
