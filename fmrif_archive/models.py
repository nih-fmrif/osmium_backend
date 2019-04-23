import itertools

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
    dicom_metadata = JSONField(null=True, blank=True)
    private_dicom_metadata = JSONField(null=True, blank=True)


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


class MRBIDSAnnotation(models.Model):

    SCAN_TYPE_CHOICES = (
        ('anat', 'Anatomical'),
        ('func', 'Functional'),
        ('dwi', 'Diffusion'),
        ('fmap', 'Fieldmap'),
    )

    MODALITY_CHOICES_BY_SCAN_TYPE = {
        "anat": (
            ("T1w", "T1 weighted"),
            ("T2w", "T2 weighted"),
            ("T1rho", "T1 rho"),
            ("T1map", "T1 map"),
            ("T2map", "T2 map"),
            ("T2star", "T2*"),
            ("FLAIR", "FLAIR"),
            ("FLASH", "FLASH"),
            ("PD", "Proton density"),
            ("PDmap", "Proton density map"),
            ("PDT2", "Combined PD/T2"),
            ("inplaneT1", "Inplane T1"),
            ("inplaneT2", "Inplane T2"),
            ("angio", "Angiography"),
        ),
        "func": (
            ("bold", "BOLD"),
            ("cbv", "CBV"),
            ("phase", "Phase"),
        ),
        "dwi": (
            ("dwi", "DWI"),
            ("sbref", "Single-band reference image"),
        ),
        "fmap": (
            ("phase_epi", "Phase Encoding EPI (PEpolar)"),
            ("phasediff", "Phase Difference"),
            ("magnitude1", "Magnitude Image 1"),
            ("magnitude2", "Magnitude Image 2"),
            ("phase1", "Phase Image 1"),
            ("phase2", "Phase Image 2"),
        )
    }

    POOLED_MODALITY_CHOICES = (
        ("T1w", "T1 weighted"),
        ("T2w", "T2 weighted"),
        ("T1rho", "T1 rho"),
        ("T1map", "T1 map"),
        ("T2map", "T2 map"),
        ("T2star", "T2*"),
        ("FLAIR", "FLAIR"),
        ("FLASH", "FLASH"),
        ("PD", "Proton density"),
        ("PDmap", "Proton density map"),
        ("PDT2", "Combined PD/T2"),
        ("inplaneT1", "Inplane T1"),
        ("inplaneT2", "Inplane T2"),
        ("angio", "Angiography"),
        ("bold", "BOLD"),
        ("cbv", "CBV"),
        ("phase", "Phase"),
        ("dwi", "DWI"),
        ("sbref", "Single-band reference image"),
        ("phase_epi", "Phase Encoding EPI (PEpolar)"),
        ("phasediff", "Phase Difference"),
        ("magnitude1", "Magnitude Image 1"),
        ("magnitude2", "Magnitude Image 2"),
        ("phase1", "Phase Image 1"),
        ("phase2", "Phase Image 2"),
    )

    PHASE_ENC_DIRS_CHOICES = (
        ("i", "i"),
        ("j", "j"),
        ("k", "k"),
        ("i-", "i-"),
        ("j-", "j-"),
        ("k-", "k-"),
    )

    parent_scan = models.OneToOneField('MRScan', related_name='bids_annotation', on_delete=models.PROTECT)

    scan_type = models.CharField(max_length=4, choices=SCAN_TYPE_CHOICES, blank=True)
    modality = models.CharField(max_length=15, choices=POOLED_MODALITY_CHOICES, blank=True)
    acquisition_label = models.CharField(max_length=255, blank=True, null=True)
    contrast_enhancement_label = models.CharField(max_length=255, blank=True, null=True)
    reconstruction_label = models.CharField(max_length=255, blank=True, null=True)
    is_defacemask = models.BooleanField(null=True)
    task_label = models.CharField(max_length=255, blank=True, null=True)
    phase_encoding_direction = models.CharField(max_length=2, blank=True, null=True, choices=PHASE_ENC_DIRS_CHOICES)
    echo_number = models.PositiveSmallIntegerField(null=True)
    is_sbref = models.BooleanField(null=True)

    def save(self, *args, **kwargs):

        if self.scan_type and not self.modality:
            raise ValidationError("If a Scan Type is specified, a Modality is required.")

        if self.modality in [modality[0] for modality in self.MODALITY_CHOICES_BY_SCAN_TYPE['anat']]:

            if self.task_label or self.phase_encoding_direction or self.echo_number or self.is_sbref:
                raise ValidationError("Anatomical scans only support the following fields: "
                                      "'Scan Type', 'Modality', 'Acquisition Label', 'Contrast Enhancement Label' "
                                      "'Reconstruction Label', and 'Is de-facing mask?'")

        elif self.modality in [modality[0] for modality in self.MODALITY_CHOICES_BY_SCAN_TYPE['func']]:

            if self.is_defacemask:
                raise ValidationError("Functional scans only support the following fields: "
                                      "'Scan Type', 'Modality', 'Task Label', 'Acquisition Label', "
                                      "'Contrast Enhancement Label', 'Reconstruction Label', "
                                      "'Phase Encoding Direction', 'Echo Number', and 'Is single-band "
                                      "reference scan?'")

        elif self.modality in [modality[0] for modality in self.MODALITY_CHOICES_BY_SCAN_TYPE['dwi']]:

            if (self.contrast_enhancement_label or self.reconstruction_label or self.task_label or self.is_defacemask
                    or self.echo_number):

                raise ValidationError("Diffusion scans only support the following fields: "
                                      "'Scan Type', 'Modality', 'Acquisition Label', "
                                      "'Phase Encoding Direction', and 'Is single-band reference scan?'")
        elif self.scan_type == "fmap":

            if self.modality == "phase_epi":

                if (self.reconstruction_label or self.is_defacemask or self.task_label or self.echo_number or
                        self.is_sbref):

                    raise ValidationError("Phase-encoding EPI fieldmap scans only support the following fields: "
                                          "'Scan Type', 'Modality', 'Acquisition Label', 'Contrast Enhancement Label, "
                                          "and 'Phase Encoding Direction'")
            else:

                if (self.contrast_enhancement_label or self.reconstruction_label or self.is_defacemask or
                        self.task_label or self.phase_encoding_direction or self.echo_number or self.is_sbref):

                    raise ValidationError("Non-PEpolar fieldmap scans only support the following fields: "
                                          "'Scan Type', 'Modality', and 'Acquisition Label'")

        super().save(*args, **kwargs)


class DICOMValueRepresentation(models.Model):

    JSON_TYPES = (
        ('string', 'string'),
        ('number', 'number'),
        ('json', 'json'),  # json encoded saved as string
        ('b64', 'b64'),  # base64 encoded string
    )
    symbol = models.CharField(max_length=2, primary_key=True)
    type = models.CharField(max_length=255)
    json_type = models.CharField(max_length=6, choices=JSON_TYPES)


class DICOMTag(models.Model):

    tag = models.CharField(max_length=8)
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

    string_single = models.TextField(blank=True, null=True)
    number_single = models.FloatField(null=True)
    b64_single = models.TextField(blank=True, null=True)
    string_multi = ArrayField(models.TextField(blank=True, null=True), null=True)
    number_multi = ArrayField(models.FloatField(null=True), null=True)
    b64_multi = ArrayField(models.TextField(blank=True, null=True), null=True)
    json_data = JSONField(blank=True, null=True)

    def save(self, *args, **kwargs):

        is_multival = self.dicom_tag.is_multival
        json_type = self.dicom_tag.vr.json_type

        if json_type == "json":

            if (self.string_single or self.number_single or self.b64_single
                    or self.string_multi or self.number_multi or self.b64_multi):
                raise ValidationError("Only the field json_data is allowed to be populated.")

        elif is_multival:

            if json_type == "string":

                if (self.string_single or self.number_single or self.b64_single or
                        self.number_multi or self.b64_multi):

                    raise ValidationError("Only the field string_multi is allowed to be populated.")

            elif json_type == "number":

                if (self.string_single or self.number_single or self.b64_single or
                        self.string_multi or self.b64_multi):

                    raise ValidationError("Only the field number_multi is allowed to be populated.")

            elif json_type == "b64":

                if (self.string_single or self.number_single or self.b64_single or
                        self.string_multi or self.number_multi):

                    raise ValidationError("Only the field b64_multi is allowed to be populated.")

        else:

            if json_type == "string":

                if (self.number_single or self.b64_single
                        or self.string_multi or self.number_multi or self.b64_multi):

                    raise ValidationError("Only the field string_single is allowed to be populated.")

            elif json_type == "number":

                if (self.string_single or self.b64_single
                        or self.string_multi or self.number_multi or self.b64_multi):

                    raise ValidationError("Only the field number_single is allowed to be populated.")

            elif json_type == "b64":

                if (self.string_single or self.number_single
                        or self.string_multi or self.number_multi or self.b64_multi):

                    raise ValidationError("Only the field b64_single is allowed to be populated.")

        super().save(*args, **kwargs)
