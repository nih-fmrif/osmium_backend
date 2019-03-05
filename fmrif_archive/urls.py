from django.urls import path

from fmrif_archive.views import (
    BasicSearchView,
    ExamView,
    DICOMInstanceList,
    # MRScanView,
    # TestView,
)

app_name = 'fmrif_archive'

urlpatterns = [
    path('basic_search/', BasicSearchView.as_view()),
    path('exam/<str:exam_id>/revision/<int:revision>/scan/<str:scan_name>/instances/', DICOMInstanceList.as_view()),
    path('exam/<str:exam_id>/scan/<str:scan_name>/instances/', DICOMInstanceList.as_view()),
    path('exam/<str:exam_id>/revision/<int:revision>/', ExamView.as_view()),
    path('exam/<str:exam_id>/', ExamView.as_view()),
    # path('', TestView.as_view()),
]
