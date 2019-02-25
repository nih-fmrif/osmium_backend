from django.urls import path

from fmrif_archive.views import (
    BasicSearchView,
    ExamView,
    MRScanView,
)

app_name = 'fmrif_archive'

urlpatterns = [
    path('basic_search/', BasicSearchView.as_view()),
    path('exam/<str:exam_id>/mr_scan/<str:scan_name>/', MRScanView.as_view()),
    path('exam/<str:exam_id>/', ExamView.as_view()),
]