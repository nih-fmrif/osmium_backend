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

    files = BaseFileSerializer(many=True, read_only=True)
    minimal_metadata = GEMinimalMetaSerializer(read_only=True)

    class Meta:

        model = MRScan

        fields = (
            'name',
            'num_files',
            'files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
            'minimal_metadata',
        )

        read_only_fields = (
            'name',
            'num_files',
            'files',
            'series_date',
            'series_time',
            'series_description',
            'sop_class_uid',
            'series_instance_uid',
            'series_number',
            'minimal_metadata',
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

    file_collections = BaseFileCollectionSerializer(many=True, read_only=True)

    class Meta:

        model = Exam

        fields = (
            "exam_id",
            "version",
            "last_modified",
            "oxygen_filename",
            "oxygen_checksum",
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
            "weight",
            "size",
            "birth_date",
            "file_collections",
        )

        read_only_fields = (
            "exam_id",
            "version",
            "last_modified",
            "oxygen_filename",
            "oxygen_checksum",
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
            "weight",
            "size",
            "birth_date",
            "file_collections",
        )
