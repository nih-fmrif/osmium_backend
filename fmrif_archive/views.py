import os

from django.http import HttpResponse
from fmrif_archive.models import Exam, MRScan
from fmrif_archive.serializers import (
    ExamPreviewSerializer,
    ExamSerializer,
    MRScanSerializer,
)
from fmrif_archive.pagination import ExamSearchResultTablePagination
from rest_framework import generics
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.filters import OrderingFilter
from django_filters.rest_framework import DjangoFilterBackend
from django.http import Http404
from datetime import datetime
from functools import reduce
from django.db.models import Q
from fmrif_base.permissions import HasActiveAccount

import logging
from django.conf import settings


################################## IMPORTANT #################################################
#                                                                                            #
# Ensure all the view classes defined here contain permission_classes = (HasActiveAccount, ) #
# where HasActiveAccount is defined in fmrif_auth.permissions                                #
#                                                                                            #
##############################################################################################


class BasicSearchView(generics.ListAPIView):

    permission_classes = (HasActiveAccount,)

    serializer_class = ExamPreviewSerializer
    filter_backends = (
        DjangoFilterBackend,
        OrderingFilter,
    )
    queryset = Exam.objects.all()
    pagination_class = ExamSearchResultTablePagination
    ordering_fields = (
        'name',
        'study_date',
        'study_time',
        'station_name',
    )
    ordering = (
        '-study_date',
        '-study_time',
    )

    def get_queryset(self):

        queryset = Exam.objects.all()

        patient_last_name = self.request.query_params.get('patient_last_name', None)
        if patient_last_name:
            queryset = queryset.filter(patient_last_name__istartswith=patient_last_name)

        patient_first_name = self.request.query_params.get('patient_first_name', None)
        if patient_first_name:
            queryset = queryset.filter(patient_first_name__istartswith=patient_first_name)

        scanners = self.request.query_params.get('scanners', None)
        if scanners:
            pass

        study_date = self.request.query_params.get('study_date', None)
        if study_date:
            study_date = datetime.strptime(study_date, '%Y-%m-%d').date()
            queryset = queryset.filter(study_date=study_date)

        scanners = self.request.query_params.get('scanners', None)
        if scanners:
            scanners = scanners.split(",")
            query = reduce(lambda q, scanner: q | Q(station_name=scanner), scanners, Q())
            queryset = queryset.filter(query)

        return queryset


class ExamView(APIView):

    permission_classes = (HasActiveAccount,)

    def get_object(self, exam_id, revision=None):

        if not revision:
            exam = Exam.objects.filter(exam_id=exam_id).order_by('-revision').first()
        else:
            exam = Exam.objects.filter(exam_id=exam_id, revision=revision).first()

        if not exam:
            raise Http404

        return exam

    def get(self, request, exam_id, revision=None):

        logger = logging.getLogger('django.request')

        exam = self.get_object(exam_id=exam_id, revision=revision)

        # download = request.query_params.get('download', None)
        #
        # if exam and (download == 'full'):
        #
        #     response = HttpResponse()
        #     response["Content-Disposition"] = "attachment; filename={}.tgz".format(exam.filename)
        #     response['Content-Type'] = "application/gzip"
        #
        #     archive_fpath = os.path.join(
        #         exam.station_name,
        #         str(exam.study_date.year),
        #         str(exam.study_date.month).rjust(2, '0'),
        #         str(exam.study_date.day).rjust(2, '0'),
        #         exam.filename,
        #         'archives',
        #         '{}.tgz'.format(exam.filename)
        #     )
        #
        #     response['X-Accel-Redirect'] = os.path.join(
        #         '/protected',
        #         archive_fpath
        #     )
        #
        #     response['Content-Length'] = os.path.getsize(os.path.join(settings.ARCHIVE_BASE_PATH, archive_fpath))
        #
        #     logger.debug('x-accel-redir: {}'.format(response['X-Accel-Redirect']))
        #
        #     return response

        serializer = ExamSerializer(exam)

        return Response(serializer.data)


# class MRScanView(APIView):
#
#     permission_classes = (HasActiveAccount,)
#
#     def get_object(self, exam_id, scan_name, version=None):
#
#         if not version:
#             exam = Exam.objects.filter(exam_id=exam_id).latest('version').prefetch_related('file_collections')
#         else:
#             exam = Exam.objects.filter(exam_id=exam_id, version=version).prefetch_related('file_collections')
#
#         if not exam:
#             raise Http404
#
#         scan = exam.first().file_collections.filter(Q(MRScan__name=scan_name))
#
#         if not scan:
#             raise Http404
#
#         return scan
#
#     def get(self, request, exam_id, scan_name):
#         version = request.query_params.get('version', None)
#         scan = self.get_object(exam_id=exam_id, scan_name=scan_name, version=version)
#         serializer = MRScanSerializer(scan)
#         return Response(serializer.data)


# class TestView(APIView):
#
#     permission_classes = (HasActiveAccount,)
#
#     def get(self, request):
#
#         user_data = {
#             'username': request.user.username,
#             'employee_id': request.user.employee_id,
#             'mail': request.user.mail,
#             'first_name': request.user.first_name,
#             'last_name': request.user.last_name,
#             'user_principal_name': request.user.user_principal_name
#         }
#
#         return Response(user_data)
