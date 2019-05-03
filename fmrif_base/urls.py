from django.urls import path

from fmrif_base.views import FMRIFUserView

app_name = 'fmrif_base'

urlpatterns = [
    path('user/', FMRIFUserView.as_view()),
]
