from rest_framework import serializers
from fmrif_archive.models import (
    Exam,
    FileCollection,
    MRScan,
    File,
    DICOMInstance,
    MRBIDSAnnotation,
)
from fmrif_archive.mappings.json_mappings import DICOM_TAG_TO_NAME
from pathlib import Path


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


class MRBIDSAnnotationSerializer(serializers.ModelSerializer):

    class Meta:

        model = MRBIDSAnnotation

        fields = (
            'scan_type',
            'modality',
            'acquisition_label',
            'contrast_enhancement_label',
            'reconstruction_label',
            'is_defacemask',
            'task_label',
            'phase_encoding_direction',
            'echo_number',
            'is_sbref',
        )

        read_only_fields = (
            'scan_type',
            'modality',
            'acquisition_label',
            'contrast_enhancement_label',
            'reconstruction_label',
            'is_defacemask',
            'task_label',
            'phase_encoding_direction',
            'echo_number',
            'is_sbref',
        )


class MRScanSerializer(serializers.ModelSerializer):

    exam_id = serializers.CharField(source="parent_exam.exam_id", read_only=True)
    exam_revision = serializers.IntegerField(source="parent_exam.revision", read_only=True)
    exam_patient_name = serializers.CharField(source="parent_exam.name", read_only=True)
    exam_study_id = serializers.CharField(source="parent_exam.study_id", read_only=True)
    exam_filename = serializers.CharField(source="parent_exam.filepath", read_only=True)
    dicom_files = DICOMInstanceSerializer(many=True, read_only=True)
    bids_annotation = MRBIDSAnnotationSerializer(read_only=True)

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
            'dicom_metadata',
            'private_dicom_metadata',
            'dicom_files',
            'bids_annotation',
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
            'dicom_metadata',
            'private_dicom_metadata',
            'dicom_files',
            'bids_annotation',
        )

    def to_representation(self, instance):

        data = super().to_representation(instance)

        if not hasattr(instance, 'dicom_metadata'):
            data['dicom_metadata'] = {}
            return data

        dicom_metadata = instance.dicom_metadata

        for tag, attrs in dicom_metadata.items():

            value = attrs.get('Value', None)
            if value:
                value = ", ".join([str(v) for v in value])

            curr_tag = DICOM_TAG_TO_NAME.get(tag, None)
            name = curr_tag.get('name', None) if curr_tag else None

            dicom_metadata[tag] = {
                'name': name,
                'value': value,
            }

        data['dicom_metadata'] = dicom_metadata

        if not hasattr(instance, 'bids_annotation'):

            data['bids_annotation'] = {
                "scan_type": "",
                "modality": "",
                "acquisition_label": "",
                "contrast_enhancement_label": "",
                "reconstruction_label": "",
                "is_defacemask": False,
                "task_label": "",
                "phase_encoding_direction": "",
                "echo_number": 1,
                "is_sbref": False,
                "has_bids_annotation": False
            }

        return data


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
            'exam_patient_name',
            'exam_study_id',
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
