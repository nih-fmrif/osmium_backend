from rest_framework import serializers
from fmrif_archive.models import (
    Exam,
    FileCollection,
    MRScan,
    File,
    DICOMInstance,
)
from pathlib import Path
from django.conf import settings


class FileInstanceSerializer(serializers.ModelSerializer):

    class Meta:

        model = File

        fields = (
            'filename',
            'checksum',
        )

        read_only_fields = (
            'type',
            'checksum',
        )


class DICOMInstanceSerializer(serializers.ModelSerializer):

    class Meta:

        model = DICOMInstance

        fields = (
            'sop_instance_uid',
            'filename',
            'checksum',
        )

        read_only_fields = (
            'sop_instance_uid',
            'filename',
            'checksum',
        )


class MRScanPreviewSerializer(serializers.ModelSerializer):

    class Meta:

        model = MRScan

        fields = (
            'id',
            'name',
            'num_files',
            'series_description',
        )

        read_only_fields = (
            'id',
            'name',
            'num_files',
            'series_description',
        )


class MRScanSerializer(serializers.ModelSerializer):

    exam_id = serializers.CharField(source="parent_exam.exam_id", read_only=True)
    exam_revision = serializers.IntegerField(source="parent_exam.revision", read_only=True)
    exam_patient_name = serializers.CharField(source="parent_exam.name", read_only=True)
    exam_study_id = serializers.CharField(source="parent_exam.study_id", read_only=True)
    exam_filename = serializers.CharField(source="parent_exam.filepath", read_only=True)
    dicom_files = DICOMInstanceSerializer(many=True, read_only=True)

    class Meta:

        model = MRScan

        fields = (
            'id',
            'exam_id',
            'exam_revision',
            'exam_patient_name',
            'exam_study_id',
            'exam_filename',
            'name',
            'num_files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
            'scan_sequence',
            'dicom_files',
        )

        read_only_fields = (
            'id',
            'exam_id',
            'exam_revision',
            'exam_patient_name',
            'exam_study_id',
            'exam_filename',
            'name',
            'num_files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
            'scan_sequence',
            'dicom_files',
        )

    # def to_representation(self, instance):
    #
    #     data = super().to_representation(instance)
    #
    #     # Add dicom metadata from mongo db database
    #     mongo_client = settings.MONGO_CLIENT
    #     collection = mongo_client.image_archive.mr_scans
    #
    #     query = {
    #         "_metadata.exam_id": data['exam_id'],
    #         "_metadata.revision": data['exam_revision'],
    #         "_metadata.scan_name": data['name'],
    #     }
    #
    #     mr_scan = collection.find_one(query)
    #
    #     mr_scan.pop('_id')
    #     mr_scan.pop('_metadata')
    #
    #     data['dicom_metadata'] = mr_scan if mr_scan else None
    #
    #     return data


class FileCollectionPreviewSerializer(serializers.ModelSerializer):

    class Meta:

        model = FileCollection

        fields = (
            'id',
            'name',
            'num_files',
        )

        read_only_fields = (
            'id',
            'name',
            'num_files',
        )


class FileCollectionSerializer(serializers.ModelSerializer):

    exam_id = serializers.CharField(source="parent_exam.exam_id", read_only=True)
    exam_revision = serializers.IntegerField(source="parent_exam.revision", read_only=True)
    exam_patient_name = serializers.CharField(source="parent_exam.name", read_only=True)
    exam_study_id = serializers.CharField(source="parent_exam.study_id", read_only=True)
    files = FileInstanceSerializer(many=True, read_only=True)

    class Meta:

        model = FileCollection

        fields = (
            'id',
            'exam_id',
            'exam_revision',
            'exam_patient_name',
            'exam_study_id',
            'name',
            'num_files',
            'files'
        )

        read_only_fields = (
            'id',
            'exam_id',
            'exam_revision',
            'name',
            'num_files',
            'files'
        )


class ExamSerializer(serializers.ModelSerializer):

    mr_scans = MRScanPreviewSerializer(many=True, read_only=True)
    other_data = FileCollectionPreviewSerializer(many=True, read_only=True)

    class Meta:

        model = Exam

        fields = (
            "exam_id",
            "revision",
            "created_on",
            "station_name",
            "study_instance_uid",
            "study_id",
            "study_date",
            "study_time",
            "study_description",
            "protocol",
            "name",
            "last_name",
            "first_name",
            "patient_id",
            "sex",
            "birth_date",
            "mr_scans",
            "other_data",
        )

        read_only_fields = (
            "exam_id",
            "revision",
            "created_on",
            "station_name",
            "study_instance_uid",
            "study_id",
            "study_date",
            "study_time",
            "study_description",
            "protocol",
            "name",
            "last_name",
            "first_name",
            "patient_id",
            "sex",
            "birth_date",
            "mr_scans",
            "other_data",
        )

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data['filename'] = Path(instance.filepath).name
        return data


class ExamPreviewSerializer(serializers.ModelSerializer):

    class Meta:

        model = Exam

        fields = (
            "exam_id",
            "revision",
            "station_name",
            "study_instance_uid",
            "study_id",
            "study_date",
            "study_time",
            "study_description",
            "protocol",
            "first_name",
            "last_name",
        )

        read_only_fields = (
            "exam_id",
            "revision",
            "station_name",
            "study_instance_uid",
            "study_id",
            "study_date",
            "study_time",
            "study_description",
            "protocol",
            "first_name",
            "last_name",
        )
