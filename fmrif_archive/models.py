from django.db import models
from fmrif_base.models import Protocol
from django.utils import timezone
from django.contrib.postgres.fields import ArrayField, JSONField
from django.core.exceptions import ValidationError


class Exam(models.Model):
    """Representation of minimal metadata for an MR exam collected at the FMRIF"""

    SCANNER_CHOICES = (
        ('crada3t', 'crada3t'),
        ('fmrif3t1', 'fmrif3t1'),
        ('fmrif3ta', 'fmrif3ta'),
        ('fmrif3tc', 'fmrif3tc'),
        ('fmrif7t', 'fmrif7t'),
        ('ldrr3t', 'ldrr3t'),
        ('fmrif1p5t', 'fmrif1p5t'),
        ('fmrif3t2', 'fmrif3t2'),
        ('fmrif3tb', 'fmrif3tb'),
        ('fmrif3td', 'fmrif3td'),
        ('fmrif7tptx', 'fmrif7tptx'),
        ('nmrf3t', 'nmrf3t'),
    )

    PT_SEX_CHOICES = (
        ('M', 'M'),
        ('F', 'F'),
        ('O', 'O'),
    )

    # Version tracking parameters
    # Exam ID is the checksum of the ***ORIGINAL*** exam archive in TGZ format as obtained from
    # Oxygen. Any derivatives of the exam would have the SAME exam id as the original, but different
    # version number. "exam_id" and "version" together should be unique when considered as a pair.
    exam_id = models.CharField(max_length=64, editable=False)
    revision = models.PositiveSmallIntegerField(default=1, editable=False)
    created_on = models.DateTimeField(default=timezone.now, editable=False)
    parser_version = models.CharField(max_length=10, editable=False)

    # Original filename and MD5 checksum of TGZ archive as stored in Oxygen/Gold
    filepath = models.TextField(unique=True, editable=False)
    checksum = models.CharField(max_length=32, editable=False)

    # Basic exam metadata
    station_name = models.CharField(max_length=10, blank=True, null=True, choices=SCANNER_CHOICES)
    study_instance_uid = models.CharField(max_length=64, null=True)
    study_id = models.CharField(max_length=16, null=True)
    study_date = models.DateField(null=True)
    study_time = models.TimeField(null=True)
    study_description = models.CharField(max_length=64, null=True)
    protocol = models.ForeignKey(Protocol, on_delete=models.PROTECT, null=True, related_name='protocol_exams')
    accession_number = models.CharField(max_length=16, null=True)

    # Basic patient metadata for basic search functionality
    name = models.CharField(max_length=324, null=True)  # Alphabetic name as stored in DICOM header
    last_name = models.CharField(max_length=64, null=True)
    first_name = models.CharField(max_length=64, null=True)
    patient_id = models.CharField(max_length=64, null=True)
    sex = models.CharField(max_length=1, null=True, choices=PT_SEX_CHOICES)
    birth_date = models.DateField(null=True)

    class Meta:
        unique_together = (
            'revision',
            'exam_id',
        )
        ordering = ['-revision']


class BaseFileCollection(models.Model):
    """Abstract base model for representing a collection of files such as scans or RT data"""

    name = models.CharField(max_length=255, editable=False)
    num_files = models.PositiveIntegerField()

    class Meta:
        abstract = True
        ordering = ['name']


class MRScan(BaseFileCollection):
    """Representation of minimal metadata for an MRI scan conducted during an exam at the FMRIF"""

    parent_exam = models.ForeignKey(Exam, related_name='mr_scans', on_delete=models.PROTECT)

    # Fields like sop_class_uid and series_instance_uid accept more characters than the DICOM standard
    # allows for because there are files with non-compliant number of characters (i.e. if they've been modified
    # in the console, or some other situations).
    series_date = models.DateField(null=True)
    series_time = models.TimeField(null=True)
    series_description = models.CharField(max_length=255, null=True)
    sop_class_uid = models.CharField(max_length=255, null=True)
    series_instance_uid = models.CharField(max_length=255, null=True)
    series_number = models.CharField(max_length=50, null=True)
    scan_sequence = models.CharField(max_length=255, null=True, blank=True)


class FileCollection(BaseFileCollection):

    parent_exam = models.ForeignKey(Exam, related_name='other_data', on_delete=models.PROTECT)


class BaseFile(models.Model):

    FILE_TYPES = (
        ('other', 'other'),
        ('dicom', 'dicom'),
        ('nifti', 'nifti')
    )

    file_type = models.CharField(max_length=5, choices=FILE_TYPES, default='other')
    filename = models.CharField(max_length=255)
    checksum = models.CharField(max_length=32, null=True)

    class Meta:
        abstract = True
        ordering = ['filename']


class DICOMInstance(BaseFile):

    sop_instance_uid = models.CharField(max_length=255, null=True)

    # Minimal metadata for GE scans
    echo_number = models.PositiveSmallIntegerField(null=True)
    slice_index = models.PositiveIntegerField(null=True)
    image_position_patient = JSONField(null=True)

    parent_scan = models.ForeignKey('MRScan', related_name='dicom_files', on_delete=models.PROTECT)


class File(BaseFile):

    parent_collection = models.ForeignKey('FileCollection', related_name='files', on_delete=models.PROTECT)


class DICOMValueRepresentation(models.Model):

    JSON_TYPES = (
        ('string', 'string'),
        ('number', 'number'),
        ('json', 'json'),  # json encoded saved as string
        ('b64', 'b64'),  # base64 encoded string
    )
    vr = models.CharField(max_length=2, primary_key=True)
    name = models.CharField(max_length=255)
    json_type = models.CharField(max_lenght=6, choices=JSON_TYPES)


class DICOMTag(models.Model):

    tag = models.CharField(max_length=4)
    name = models.CharField(max_length=255, null=True, blank=True)
    keyword = models.CharField(max_length=255, null=True, blank=True)
    vr = models.ForeignKey(DICOMValueRepresentation, on_delete=models.PROTECT, related_name='dicom_tags', null=True)
    is_multival = models.NullBooleanField(null=True, default=False)  # Private fields saved as multival fields
    is_retired = models.NullBooleanField(null=True, default=False)
    can_query = models.NullBooleanField(default=True)

    class Meta:

        unique_together = (
            'tag',
            'vr',
        )


class DICOMFieldInstance(models.Model):

    dicom_tag = models.ForeignKey(DICOMTag, on_delete=models.PROTECT, related_name='field_instances')
    parent_scan = models.ForeignKey(MRScan, on_delete=models.PROTECT, related_name='dicom_fields')

    string_single = models.CharField(max_length=400, blank=True, null=True)
    number_single = models.FloatField(null=True)
    b64_single = models.TextField(blank=True, null=True)
    json_single = JSONField(blank=True, null=True)
    string_multi = ArrayField(models.CharField(max_length=400, blank=True, null=True), null=True)
    number_multi = ArrayField(models.FloatField(null=True), null=True)
    b64_multi = ArrayField(models.TextField(blank=True, null=True), null=True)
    json_multi = ArrayField(JSONField(blank=True, null=True), null=True)

    def save(self, *args, **kwargs):

        is_multival = self.dicom_tag.is_multival
        json_type = self.dicom_tag.vr.json_type

        if is_multival:

            if json_type == "string":

                if (self.string_single or self.number_single or self.b64_single or self.json_single
                        or self.number_multi or self.b64_multi or self.json_multi):

                    raise ValidationError("Only the field string_multi is allowed to be populated.")

            elif json_type == "number":

                if (self.string_single or self.number_single or self.b64_single or self.json_single
                        or self.string_multi or self.b64_multi or self.json_multi):

                    raise ValidationError("Only the field number_multi is allowed to be populated.")

            elif json_type == "json":

                if (self.string_single or self.number_single or self.b64_single or self.json_single
                        or self.string_multi or self.number_multi or self.b64_multi):

                    raise ValidationError("Only the field json_multi is allowed to be populated.")

            elif json_type == "b64":

                if (self.string_single or self.number_single or self.b64_single or self.json_single
                        or self.string_multi or self.number_multi or self.json_multi):

                    raise ValidationError("Only the field b64_multi is allowed to be populated.")

        else:

            if json_type == "string":

                if (self.number_single or self.b64_single or self.json_single
                        or self.string_multi or self.number_multi or self.b64_multi or self.json_multi):

                    raise ValidationError("Only the field string_single is allowed to be populated.")

            elif json_type == "number":

                if (self.string_single or self.b64_single or self.json_single
                        or self.string_multi or self.number_multi or self.b64_multi or self.json_multi):

                    raise ValidationError("Only the field number_single is allowed to be populated.")

            elif json_type == "json":

                if (self.string_single or self.number_single or self.b64_single
                        or self.string_multi or self.number_multi or self.json_multi or self.b64_multi):

                    raise ValidationError("Only the field json_single is allowed to be populated.")

            elif json_type == "b64":

                if (self.string_single or self.number_single or self.json_single
                        or self.string_multi or self.number_multi or self.json_multi or self.b64_multi):

                    raise ValidationError("Only the field b64_single is allowed to be populated.")
