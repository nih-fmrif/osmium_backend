import os
import rapidjson as json

from django.http import HttpResponse
from fmrif_archive.models import Exam, MRScan, FileCollection, MRBIDSAnnotation
from fmrif_archive.serializers import (
    ExamPreviewSerializer,
    ExamSerializer,
    FileCollectionSerializer,
    MRScanSerializer
)
from fmrif_archive.pagination import ExamSearchResultTablePagination
from rest_framework import generics
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.filters import OrderingFilter
from rest_framework.exceptions import ParseError, NotFound, ValidationError
from django_filters.rest_framework import DjangoFilterBackend
from datetime import datetime
from functools import reduce
from django.db.models import Q
from fmrif_base.permissions import HasActiveAccount
from pathlib import Path
from fmrif_archive.utils import get_fmrif_scanner
from collections import OrderedDict
from django.db import Error

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

        scanners = self.request.query_params.getlist('scanners', None)
        if scanners:
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
            #     '$limit': 500,
            # },
            {
                "$sort": {
                    "_metadata.study_datetime": -1
                }
            },
            {
                "$group":
                {
                    "_id": {"exam_id": "$_metadata.exam_id", "revision": "$_metadata.revision"},
                    "scan_name": {"$push": "$_metadata.scan_name"},
                    "exam_id": {"$first": "$_metadata.exam_id"},
                    "revision": {"$first": "$_metadata.revision"},
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
                    "_id": 0,
                }
            },
            {
                "$sort": {
                    "study_datetime": -1
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
            #     '$limit': 500,
            # },
            {
                "$sort": {
                    "_metadata.study_datetime": -1
                }
            },
            {
                "$group":
                {
                    "_id": {"exam_id": "$_metadata.exam_id", "revision": "$_metadata.revision"},
                    "scan_name": {"$push": "$_metadata.scan_name"},
                    "exam_id": {"$first": "$_metadata.exam_id"},
                    "revision": {"$first": "$_metadata.revision"},
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
                    "_id": 0,
                }
            },
            {
                "$skip": page_size * (page_num - 1)
            },
            {
                "$limit": page_size
            }
        ]

        if new_query or not count:

            count = list(collection.aggregate(count_query, allowDiskUse=True))

            try:
                count = count[0].get('count', 0)
            except (IndexError, AttributeError):
                count = 0

        cursor = collection.aggregate(aggregation_query, allowDiskUse=True)

        results = [res for res in cursor]

        cursor.close()

        # for res in results:
        #
        #     try:
        #         pt_name = res['PatientName'][0]['Alphabetic']
        #         res['PatientName'] = [pt_name]
        #     except (KeyError, IndexError):
        #         pass
        #
        #     try:
        #         scanner = res['StationName'][0]
        #         res['StationName'] = [get_fmrif_scanner(scanner)]
        #     except(KeyError, IndexError):
        #         pass
        #
        #     scans = {}
        #     try:
        #
        #         revision_scan_pairs = res['revision_scan_pairs']
        #
        #         for pair in revision_scan_pairs:
        #
        #             revision, curr_scan = pair
        #
        #             if revision not in scans.keys():
        #                 scans[revision] = [curr_scan]
        #             else:
        #                 scans[revision].append(curr_scan)
        #
        #     except (KeyError, IndexError):
        #         pass
        #
        #     res.pop('revision_scan_pairs')
        #     res['scans'] = OrderedDict(sorted(scans.items()))

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
            raise NotFound

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
            raise NotFound

        scan = MRScan.objects.filter(parent_exam=exam,
                                     name=scan_name).prefetch_related('dicom_files').first()

        if not scan:
            raise NotFound

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
            raise NotFound

        file_collection = FileCollection.objects.filter(parent_exam=exam,
                                                        name=collection_name).prefetch_related('files').first()

        if not file_collection:
            raise NotFound

        return file_collection

    def get(self, request, exam_id, collection_name, revision=None):
        file_collection = self.get_object(exam_id=exam_id, collection_name=collection_name, revision=revision)
        serializer = FileCollectionSerializer(file_collection)
        return Response(serializer.data)


class MRBIDSAnnotationView(APIView):

    permission_classes = (HasActiveAccount,)

    def get_mr_scan(self, exam_id, revision, scan_name):

        try:
            scan = MRScan.objects.get(parent_exam__exam_id=exam_id, parent_exam__revision=revision,
                                      name=scan_name)
        except MRScan.DoesNotExist:
            raise NotFound

        return scan

    def post(self, request, exam_id, revision, scan_name):

        scan = self.get_mr_scan(exam_id, revision, scan_name)

        if hasattr(scan, 'bids_annotation'):
            raise ValidationError("Annotations for this scan already exist. Use PUT to update instead.")

        post_params = json.loads(request.body.decode('utf-8'))

        scan_type = post_params.get('scan_type', None)
        modality = post_params.get('modality', None)
        acquisition_label = post_params.get('acquisition_label', None)
        contrast_enhancement_label = post_params.get('contrast_enhancement_label', None)
        reconstruction_label = post_params.get('reconstruction_label', None)
        is_defacemask = post_params.get('is_defacemask', None)
        task_label = post_params.get('task_label', None)
        phase_encoding_direction = post_params.get('phase_encoding_direction', None)
        echo_number = post_params.get('echo_number', None)
        is_sbref = post_params.get('is_sbref', None)

        try:

            MRBIDSAnnotation.objects.create(
                parent_scan=scan,
                scan_type=scan_type,
                modality=modality,
                acquisition_label=acquisition_label,
                contrast_enhancement_label=contrast_enhancement_label,
                reconstruction_label=reconstruction_label,
                is_defacemask=is_defacemask,
                task_label=task_label,
                phase_encoding_direction=phase_encoding_direction,
                echo_number=echo_number,
                is_sbref=is_sbref
            )

        except Error as e:

            if settings.DEBUG:
                raise e

            raise ValidationError("Unable to annotate scan. If this problem persists, please contact support.")

        return Response({"msg": "BIDS annotation added successfully."}, status=201)

    def put(self, request, exam_id, revision, scan_name):

        scan = self.get_mr_scan(exam_id, revision, scan_name)

        if not hasattr(scan, 'bids_annotation'):
            raise ValidationError("Annotations for this scan do not exist. Use POST to create "
                                  "initial annotations instead.")

        post_params = json.loads(request.body.decode('utf-8'))

        scan.bids_annotation.scan_type = post_params.get('scan_type', None)
        scan.bids_annotation.modality = post_params.get('modality', None)
        scan.bids_annotation.acquisition_label = post_params.get('acquisition_label', None)
        scan.bids_annotation.contrast_enhancement_label = post_params.get('contrast_enhancement_label', None)
        scan.bids_annotation.reconstruction_label = post_params.get('reconstruction_label', None)
        scan.bids_annotation.is_defacemask = post_params.get('is_defacemask', None)
        scan.bids_annotation.task_label = post_params.get('task_label', None)
        scan.bids_annotation.phase_encoding_direction = post_params.get('phase_encoding_direction', None)
        scan.bids_annotation.echo_number = post_params.get('echo_number', None)
        scan.bids_annotation.is_sbref = post_params.get('is_sbref', None)

        try:
            scan.bids_annotation.save()
        except Error as e:

            if settings.DEBUG:
                raise e

            raise ValidationError("Unable to annotate scan. If this problem persists, please contact support.")

        return Response({"msg": "BIDS annotation added successfully."}, status=201)

    def delete(self, request, exam_id, revision, scan_name):

        scan = self.get_mr_scan(exam_id, revision, scan_name)

        if not hasattr(scan, 'bids_annotation'):
            raise ValidationError("Annotations for this scan do not exist.")

        bids_annotation = scan.bids_annotation

        try:
            bids_annotation.delete()
        except Error as e:

            if settings.DEBUG:
                raise e

            raise ValidationError("Unable to delete bids annotations for this scan. "
                                  "If this problem persists, please contact support.")

        return Response({"msg": "BIDS annotation deleted successfully."}, status=200)
