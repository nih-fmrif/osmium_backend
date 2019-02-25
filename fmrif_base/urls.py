from django.urls import path

from fmrif_base.views import UserAccount

app_name = 'fmrif_archive'

urlpatterns = [
    path('account/', UserAccount.as_view()),
]
