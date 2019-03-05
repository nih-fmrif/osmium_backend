from rest_framework import serializers
from fmrif_archive.models import (
    Exam,
    FileCollection,
    MRScan,
    File,
    DICOMInstance,
)
from pathlib import Path


class FileSerializer(serializers.ModelSerializer):

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


class MRScanSerializer(serializers.ModelSerializer):

    class Meta:

        model = MRScan

        fields = (
            'id',
            'name',
            'num_files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
        )

        read_only_fields = (
            'id',
            'name',
            'num_files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
        )


class FileCollectionSerializer(serializers.ModelSerializer):

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


class ExamSerializer(serializers.ModelSerializer):

    mr_scans = MRScanSerializer(many=True, read_only=True)
    other_data = FileCollectionSerializer(many=True, read_only=True)

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
