from django.urls import path
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from comwpc import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('import-progress/', views.import_progress, name='import_progress'),
    path('admin/comwpc/graph/import-dot/',
         views.import_dot,
         name='comwpc_import_dot'
         ),
    path('graph-visualization/<int:graph_id>/',
         views.graph_interactive_view,
         name='graph_visualization'),
    path('graph-visualization/<int:graph_id>/content/',
         views.graph_interactive_content,
         name='graph_content'),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)