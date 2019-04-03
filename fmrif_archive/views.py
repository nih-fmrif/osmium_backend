import os

from django.http import HttpResponse
from fmrif_archive.models import Exam, MRScan, FileCollection
from fmrif_archive.serializers import (
    ExamPreviewSerializer,
    ExamSerializer,
    FileCollectionSerializer,
    MRScanSerializer,
)
from fmrif_archive.pagination import ExamSearchResultTablePagination
from rest_framework import generics
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.filters import OrderingFilter
from rest_framework.exceptions import ParseError
from django_filters.rest_framework import DjangoFilterBackend
from django.http import Http404
from datetime import datetime
from functools import reduce
from django.db.models import Q
from fmrif_base.permissions import HasActiveAccount
from pathlib import Path
from fmrif_archive.utils import get_fmrif_scanner
from collections import OrderedDict
import rapidjson as json

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

        last_name = self.request.query_params.get('last_name', None)
        if last_name:
            queryset = queryset.filter(last_name__istartswith=last_name)

        first_name = self.request.query_params.get('first_name', None)
        if first_name:
            queryset = queryset.filter(first_name__istartswith=first_name)

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


class AdvancedSearchView(APIView):

    permission_classes = (HasActiveAccount,)

    def mongo_query(self, query, page_num=1, page_size=10, count=None, new_query=True):

        mongo_client = settings.MONGO_CLIENT
        collection = mongo_client.image_archive.mr_scans

        if page_size > 100:
            page_size = 100

        if page_num < 1:
            page_num = 1

        count_query = [
            {
                "$match": query,
            },
            # {
            #     "$sort": {
            #         "$_metadata.study_date": -1
            #     }
            # },
            {
                '$limit': 500,
            },
            {
                "$group":
                {
                    "_id": {"exam_id": "$_metadata.exam_id"},
                    "revision": {"$push": "$_metadata.revision"},
                    "scan_name": {"$push": "$_metadata.scan_name"},
                    "scanner": {"$first": "$_metadata.scanner"},
                    "patient_first_name": {"$first": "$_metadata.patient_first_name"},
                    "patient_last_name": {"$first": "$_metadata.patient_last_name"},
                    "patient_id": {"$first": "$_metadata.patient_id"},
                    "patient_sex": {"$first": "$_metadata.patient_sex"},
                    "patient_birth_date": {"$first": "$_metadata.patient_birth_date"},
                    "study_id": {"$first": "$_metadata.study_id"},
                    "study_description": {"$first": "$_metadata.study_description"},
                    "study_datetime": {"$first": "$_metadata.study_datetime"},
                    "protocol": {"$first": "$_metadata.protocol"},
                }
            },
            {
                "$project":
                {
                    "revision_scan_pairs": {
                        "$zip": {
                            "inputs": ["$revision", "$scan_name"]
                        }
                    },
                    "_id": 0,
                    "exam_id": "$_id.exam_id",
                    "scanner": 1,
                    "patient_first_name": 1,
                    "patient_last_name": 1,
                    "patient_id": 1,
                    "patient_sex": 1,
                    "patient_birth_date": 1,
                    "study_id": 1,
                    "study_description": 1,
                    "study_datetime": 1,
                    "protocol": 1,
                }
            },
            {
                "$count": "count"
            }
        ]

        aggregation_query = [
            {
                "$match": query,
            },
            # {
            #     "$sort": {
            #         "$_metadata.study_date": -1
            #     }
            # },
            {
                '$limit': 500,
            },
            {
                "$group":
                {
                    "_id": {"exam_id": "$_metadata.exam_id"},
                    "revision": {"$push": "$_metadata.revision"},
                    "scan_name": {"$push": "$_metadata.scan_name"},
                    "scanner": {"$first": "$_metadata.scanner"},
                    "patient_first_name": {"$first": "$_metadata.patient_first_name"},
                    "patient_last_name": {"$first": "$_metadata.patient_last_name"},
                    "patient_id": {"$first": "$_metadata.patient_id"},
                    "patient_sex": {"$first": "$_metadata.patient_sex"},
                    "patient_birth_date": {"$first": "$_metadata.patient_birth_date"},
                    "study_id": {"$first": "$_metadata.study_id"},
                    "study_description": {"$first": "$_metadata.study_description"},
                    "study_datetime": {"$first": "$_metadata.study_datetime"},
                    "protocol": {"$first": "$_metadata.protocol"},
                }
            },
            {
                "$project":
                {
                    "revision_scan_pairs": {
                        "$zip": {
                            "inputs": ["$revision", "$scan_name"]
                        }
                    },
                    "_id": 0,
                    "exam_id": "$_id.exam_id",
                    "scanner": 1,
                    "patient_first_name": 1,
                    "patient_last_name": 1,
                    "patient_id": 1,
                    "patient_sex": 1,
                    "patient_birth_date": 1,
                    "study_id": 1,
                    "study_description": 1,
                    "study_datetime": 1,
                    "protocol": 1,
                }
            },
            {
                "$skip": page_size*(page_num - 1),
            },
            {
                "$limit": page_size,
            }
        ]

        if new_query or not count:
            count = collection.aggregate(count_query)

        cursor = collection.aggregate(aggregation_query)

        results = [res for res in cursor]

        for res in results:

            try:
                pt_name = res['PatientName'][0]['Alphabetic']
                res['PatientName'] = [pt_name]
            except (KeyError, IndexError):
                pass

            try:
                scanner = res['StationName'][0]
                res['StationName'] = [get_fmrif_scanner(scanner)]
            except(KeyError, IndexError):
                pass

            scans = {}
            try:

                revision_scan_pairs = res['revision_scan_pairs']

                for pair in revision_scan_pairs:

                    revision, curr_scan = pair

                    if revision not in scans.keys():
                        scans[revision] = [curr_scan]
                    else:
                        scans[revision].append(curr_scan)

            except (KeyError, IndexError):
                pass

            res.pop('revision_scan_pairs')
            res['scans'] = OrderedDict(sorted(scans.items()))

        return {
            'pagination': {
                'page': page_num,
                'page_size': page_size,
                'last_page': 0,
                'count': count,
                'has_next_page': True,
                'has_prev_page': False if page_num == 1 else True,
            },
            'results': results,
            'current_query': json.dumps(query)
        }

    def get(self, request):

        query = request.query_params.get('query', '{}')

        try:
            query = json.loads(query)
        except:
            raise ParseError("Invalid query")

        page_num = request.query_params.get('page_num', 1)
        page_size = request.query_params.get('page_size', 10)

        count = query.get('_count', None)
        query.pop('_count', None)
        new_query = query.get('_new_query', True)
        query.pop('_new_query', None)

        results = self.mongo_query(query=query, page_num=page_num, page_size=page_size, count=count,
                                   new_query=new_query)

        return Response(results)


class ExamView(APIView):

    permission_classes = (HasActiveAccount,)

    def get_object(self, exam_id, revision=None):

        if not revision:
            exam = Exam.objects.filter(exam_id=exam_id).order_by('-revision').prefetch_related(
                'mr_scans', 'other_data').first()
        else:
            exam = Exam.objects.filter(exam_id=exam_id, revision=revision).prefetch_related(
                'mr_scans', 'other_data').first()

        if not exam:
            raise Http404

        return exam

    def get(self, request, exam_id, revision=None):

        exam = self.get_object(exam_id=exam_id, revision=revision)

        download = request.query_params.get('download', None)

        if exam and (download == 'dicom'):

            response = HttpResponse()
            response["Content-Disposition"] = 'attachment; filename="{}"'.format(Path(exam.filepath).name)
            response['Content-Type'] = "application/gzip"

            archive_fpath = Path(settings.ARCHIVE_BASE_PATH) / exam.filepath

            response['X-Sendfile'] = str(archive_fpath)

            response['Content-Length'] = os.path.getsize(str(archive_fpath))

            return response

        serializer = ExamSerializer(exam)

        return Response(serializer.data)


class MRScanView(APIView):

    permission_classes = (HasActiveAccount,)

    def get_object(self, exam_id, scan_name, revision=None):

        if not revision:
            exam = Exam.objects.filter(exam_id=exam_id).order_by('-revision').first()
        else:
            exam = Exam.objects.filter(exam_id=exam_id, revision=revision).first()

        if not exam:
            raise Http404

        scan = MRScan.objects.filter(parent_exam=exam,
                                     name=scan_name).prefetch_related('dicom_files').first()

        if not scan:
            raise Http404

        return scan

    def get(self, request, exam_id, scan_name, revision=None):
        scan = self.get_object(exam_id=exam_id, scan_name=scan_name, revision=revision)
        serializer = MRScanSerializer(scan)
        return Response(serializer.data)


class FileCollectionView(APIView):

    permission_classes = (HasActiveAccount,)

    def get_object(self, exam_id, collection_name, revision=None):

        if not revision:
            exam = Exam.objects.filter(exam_id=exam_id).order_by('-revision').first()
        else:
            exam = Exam.objects.filter(exam_id=exam_id, revision=revision).first()

        if not exam:
            raise Http404

        file_collection = FileCollection.objects.filter(parent_exam=exam,
                                                        name=collection_name).prefetch_related('files').first()

        if not file_collection:
            raise Http404

        return file_collection

    def get(self, request, exam_id, collection_name, revision=None):
        file_collection = self.get_object(exam_id=exam_id, collection_name=collection_name, revision=revision)
        serializer = FileCollectionSerializer(file_collection)
        return Response(serializer.data)



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
