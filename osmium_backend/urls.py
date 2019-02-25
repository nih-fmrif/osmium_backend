from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # path('bromine/admin/', admin.site.urls),
    path('api/', include('fmrif_archive.urls', namespace='archive')),
]
