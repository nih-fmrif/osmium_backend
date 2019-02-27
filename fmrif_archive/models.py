from django.db import models
from django.contrib.postgres.fields import JSONField
from fmrif_base.models import Protocol
from django.utils import timezone


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
    checksum = models.CharField(max_length=32, unique=True, editable=False)

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
    patient_id = models.CharField(max_length=64)
    sex = models.CharField(max_length=1, null=True, choices=PT_SEX_CHOICES)
    birth_date = models.DateField(null=True)

    class Meta:
        unique_together = (
            'revision',
            'exam_id',
        )


class BaseFileCollection(models.Model):
    """Abstract base model for representing a collection of files such as scans or RT data"""

    name = models.CharField(max_length=25, editable=False)
    num_files = models.PositiveIntegerField()

    class Meta:
        abstract = True


class MRScan(BaseFileCollection):
    """Representation of minimal metadata for an MRI scan conducted during an exam at the FMRIF"""

    parent_exam = models.ForeignKey(Exam, related_name='mr_scans', on_delete=models.PROTECT)

    series_date = models.DateField(null=True)
    series_time = models.TimeField(null=True)
    series_description = models.CharField(max_length=64, null=True)
    sop_class_uid = models.CharField(max_length=64, null=True)
    series_instance_uid = models.CharField(max_length=64, null=True)
    series_number = models.CharField(max_length=12, null=True)


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


class DICOMInstance(BaseFile):

    sop_instance_uid = models.CharField(max_length=64, null=True)

    # Minimal metadata for GE scans
    slice_indexes = JSONField(null=True)
    image_position_patient = JSONField(null=True)
    num_indices = models.IntegerField(null=True)
    num_slices = models.IntegerField(null=True)

    parent_scan = models.ForeignKey('MRScan', related_name='dicom_files', on_delete=models.PROTECT)


class File(BaseFile):

    parent_collection = models.ForeignKey('FileCollection', related_name='files', on_delete=models.PROTECT)
