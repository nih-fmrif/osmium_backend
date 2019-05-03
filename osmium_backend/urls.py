from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # path('bromine/admin/', admin.site.urls),
    path('api/account/', include('fmrif_base.urls', namespace='base')),
    path('api/', include('fmrif_archive.urls', namespace='archive')),
]
