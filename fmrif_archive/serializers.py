from rest_framework import serializers
from fmrif_archive.models import (
    Exam,
    BaseFileCollection,
    FileCollection,
    MRScan,
    BaseFile,
    File,
    DICOMInstance,
)


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


class BaseFileSerializer(serializers.ModelSerializer):

    class Meta:

        model = BaseFile

    def to_representation(self, obj):

        if isinstance(obj, File):
            return FileSerializer(obj, context=self.context).to_representation(obj)
        elif isinstance(obj, DICOMInstance):
            return DICOMInstanceSerializer(obj, context=self.context).to_representation(obj)


class FileCollectionSerializer(serializers.ModelSerializer):

    files = BaseFileSerializer(many=True, read_only=True)

    class Meta:

        model = FileCollection

        fields = (
            'name',
            'num_files',
            'files',
        )

        read_only_fields = (
            'name',
            'num_files',
            'files',
        )


class GEMinimalMetaSerializer(serializers.ModelSerializer):

    class Meta:

        fields = (
            'slice_indexes',
            'image_position_patient',
            'num_indices',
            'num_slices',
        )

        read_only_fields = (
            'slice_indexes',
            'image_position_patient',
            'num_indices',
            'num_slices',
        )


class MRScanSerializer(serializers.ModelSerializer):

    dicom_files = DICOMInstanceSerializer(many=True, read_only=True)

    class Meta:

        model = MRScan

        fields = (
            'name',
            'num_files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
            'dicom_files',
        )

        read_only_fields = (
            'name',
            'num_files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
            'dicom_files',
        )


class BaseFileCollectionSerializer(serializers.ModelSerializer):

    class Meta:

        model = BaseFileCollection

    def to_representation(self, obj):

        if isinstance(obj, FileCollection):
            return FileCollectionSerializer(obj, context=self.context).to_representation(obj)
        elif isinstance(obj, MRScan):
            return MRScanSerializer(obj, context=self.context).to_representation(obj)


class ExamSerializer(serializers.ModelSerializer):

    mr_scans = False
    other_data = False
    file_collections = BaseFileCollectionSerializer(many=True, read_only=True)

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
            "name",
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
            "name",
        )

