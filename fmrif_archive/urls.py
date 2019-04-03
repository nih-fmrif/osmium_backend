from django.urls import path

from fmrif_archive.views import (
    BasicSearchView,
    AdvancedSearchView,
    ExamView,
    MRScanView,
    FileCollectionView,
)

app_name = 'fmrif_archive'

urlpatterns = [
    path('basic_search/', BasicSearchView.as_view()),
    path('advanced_search/', AdvancedSearchView.as_view()),
    path('exam/<str:exam_id>/revision/<int:revision>/file_collection/<str:collection_name>/',
         FileCollectionView.as_view()),
    path('exam/<str:exam_id>/file_collection/<str:collection_name>/', FileCollectionView.as_view()),
    path('exam/<str:exam_id>/revision/<int:revision>/scan/<str:scan_name>/', MRScanView.as_view()),
    path('exam/<str:exam_id>/scan/<str:scan_name>/', MRScanView.as_view()),
    path('exam/<str:exam_id>/revision/<int:revision>/', ExamView.as_view()),
    path('exam/<str:exam_id>/', ExamView.as_view()),
]
