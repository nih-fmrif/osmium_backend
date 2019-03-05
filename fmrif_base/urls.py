from django.urls import path

from fmrif_base.views import FMRIFUserView

app_name = 'fmrif_base'

urlpatterns = [
    path('account/user/', FMRIFUserView.as_view()),
]
