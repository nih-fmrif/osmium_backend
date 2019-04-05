import rapidjson as json
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import Error as DjangoDBError
from psycopg2 import Error as PgError
from psycopg2 import Warning as PgWarning
from pathlib import Path
from fmrif_archive.models import (
    Exam,
    MRScan,
    DICOMValueRepresentation,
    DICOMTag,
    DICOMFieldInstance
)


class Command(BaseCommand):

    help = 'Load study and scan metadata obtained from Oxygen/Gold archives'

    def add_arguments(self, parser):

        parser.add_argument("--data", type=str, default=settings.PARSED_DATA_PATH)

        parser.add_argument("--scanners", nargs="*", type=str, default=[])

        parser.add_argument("--years", nargs="*", type=str, default=[])

        parser.add_argument("--months", nargs="*", type=str, default=[])

        parser.add_argument("--days", nargs="*", type=str, default=[])

    def handle(self, *args, **options):

        scanners = options['scanners']
        years = options['years']
        months = options['months']
        days = options['days']

        parsed_data_path = Path(options['data'])

        if not scanners:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir() if scanner_path.is_dir()]
        else:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir()
                             if (scanner_path.is_dir() and scanner_path.name in scanners)]

        for scanner_path in sorted(scanner_paths):

            if not years:
                year_paths = [year_path for year_path in scanner_path.iterdir() if year_path.is_dir()]
            else:
                year_paths = [year_path for year_path in scanner_path.iterdir()
                              if (year_path.is_dir() and year_path.name in years)]

            for year_path in sorted(year_paths):

                if not months:
                    month_paths = [month_path for month_path in year_path.iterdir() if month_path.is_dir()]
                else:
                    month_paths = [month_path for month_path in year_path.iterdir()
                                   if (month_path.is_dir() and month_path.name in months)]

                for month_path in sorted(month_paths):

                    if not days:
                        day_paths = [day_path for day_path in month_path.iterdir() if day_path.is_dir()]
                    else:
                        day_paths = [day_path for day_path in month_path.iterdir()
                                     if (day_path.is_dir() and day_path.name in days)]

                    for day_path in sorted(day_paths):

                        for exam_id in sorted([e for e in day_path.iterdir() if e.is_dir()]):

                            for pt_dir in sorted([p for p in exam_id.iterdir() if p.is_dir()]):

                                for session_dir in sorted([s for s in pt_dir.iterdir() if s.is_dir()]):

                                    study_metadata_files = list(session_dir.glob("study_*_metadata.txt"))

                                    if not study_metadata_files:
                                        self.stdout.write("Error: No study metadata found in {}".format(session_dir))
                                        continue

                                    if len(study_metadata_files) > 1:
                                        self.stdout.write("Error: Multiple study metadata "
                                                          "files for {} found".format(session_dir))
                                        continue

                                    study_meta_file = study_metadata_files[0]

                                    if not study_meta_file.is_file():
                                        self.stdout.write("Error: Cannot load file {}".format(study_meta_file))
                                        continue

                                    self.stdout.write("Loading data from {}".format(str(study_meta_file)))

                                    try:
                                        with open(str(study_meta_file), "rt") as sm:
                                            study_metadata = json.load(sm)
                                    except ValueError:
                                        self.stdout.write("Error: Cannot load file {}".format(study_meta_file))
                                        continue

                                    metadata = study_metadata['metadata']
                                    data = study_metadata['data']

                                    mr_scans = []

                                    for subdir in data:
                                        if subdir.get('dicom_data', None):
                                            mr_scans.append(subdir)

                                    for scan in mr_scans:

                                        try:
                                            exam_id = metadata['exam_id']
                                            revision = 1
                                            scan_metadata = scan['metadata']
                                            scan_dicom_data = scan['dicom_data']
                                            scan_name = scan_metadata['gold_scan_dir']

                                        except KeyError:

                                            self.stdout.write("Error: Missing mandatory scan metadata, "
                                                              "omitting scan from exam {}".format(study_meta_file))
                                            continue

                                        parent_exam = Exam.objects.get(
                                            exam_id=exam_id,
                                            revision=revision
                                        )

                                        parent_scan = MRScan.objects.get(
                                            parent_exam=parent_exam,
                                            name=scan_name
                                        )

                                        if scan_dicom_data and (type(scan_dicom_data) == dict):

                                            for tag, attrs in scan_dicom_data.items():

                                                if not attrs.get('vr', None):
                                                    self.stdout.write(
                                                        "WARNING: VR field missing in "
                                                        "metadata for tag {} in scan {} of "
                                                        "exam {}. Skipping".format(tag, scan_name, study_meta_file)
                                                    )
                                                    continue

                                                current_vr = list(
                                                    DICOMValueRepresentation.objects.filter(symbol=attrs['vr'])
                                                )

                                                if not current_vr:
                                                    self.stdout.write(
                                                        "WARNING: VR type object not found for tag {} in scan {} of "
                                                        "exam {}. Skipping".format(tag, scan_name, study_meta_file)
                                                    )
                                                    continue

                                                current_vr = current_vr[0]

                                                current_tag = list(
                                                    DICOMTag.objects.filter(tag=tag, vr=current_vr)
                                                )

                                                if not current_tag:

                                                    self.stdout.write(
                                                        "WARNING: Tag {} with VR {} not found on scan {} of "
                                                        "exam {}. "
                                                        "Creating one...".format(tag, current_vr.symbol,
                                                                                 scan_name, study_meta_file)
                                                    )

                                                    current_tag = DICOMTag(
                                                        tag=tag,
                                                        name=None,
                                                        keyword=None,
                                                        vr=current_vr,
                                                        is_multival=True,
                                                        is_retired=False,
                                                        can_query=False
                                                    )
                                                    current_tag.save()
                                                else:
                                                    current_tag = current_tag[0]

                                                string_single = True if (
                                                        (not current_tag.is_multival)
                                                        and current_vr.json_type == 'string'
                                                ) else False
                                                string_multi = True if (
                                                        current_tag.is_multival
                                                        and current_vr.json_type == 'string'
                                                ) else False
                                                number_single = True if (
                                                        (not current_tag.is_multival)
                                                        and current_vr.json_type == 'number'
                                                ) else False
                                                number_multi = True if (
                                                        current_tag.is_multival
                                                        and current_vr.json_type == 'number'
                                                ) else False
                                                b64_single = True if (
                                                        (not current_tag.is_multival)
                                                        and current_vr.json_type == 'b64'
                                                ) else False
                                                b64_multi = True if (
                                                        current_tag.is_multival
                                                        and current_vr.json_type == 'b64'
                                                ) else False

                                                json_data = True if current_vr.json_type == 'json' else False

                                                orig_val = attrs.get('Value', None)

                                                if orig_val:

                                                    if current_vr.symbol == 'PN':

                                                        if current_tag.is_multival:

                                                            value = []

                                                            for val in orig_val:

                                                                if type(val) == dict:
                                                                    value.append(val.get('Alphabetic', None))
                                                                elif type(val) == str:
                                                                    value.append(val)
                                                                else:
                                                                    value.append(None)
                                                        else:

                                                            if type(orig_val[0]) == dict:
                                                                value = orig_val[0].get('Alphabetic', None)
                                                            elif type(orig_val[0]) == str:
                                                                value = orig_val[0]
                                                            else:
                                                                value = None

                                                    else:  # Orig Value but NOT PN

                                                        if current_tag.is_multival:

                                                            value = orig_val

                                                        else:

                                                            value = orig_val[0]

                                                else:  # No original val

                                                    if current_tag.is_multival:
                                                        value = []
                                                    else:
                                                        value = None
                                                try:

                                                    new_field_instance = DICOMFieldInstance(
                                                        dicom_tag=current_tag,
                                                        parent_scan=parent_scan,
                                                        string_single=value if string_single else None,
                                                        number_single=value if number_single else None,
                                                        b64_single=value if b64_single else None,
                                                        string_multi=value if string_multi else None,
                                                        number_multi=value if number_multi else None,
                                                        b64_multi=value if b64_multi else None,
                                                        json_data=value if json_data else None,
                                                    )

                                                    new_field_instance.save()

                                                except (DjangoDBError, PgError) as e:

                                                    self.stdout.write("Error: Unable to create MRScan models "
                                                                      "for exam {}".format(study_meta_file))
                                                    self.stdout.write("Scan: {}".format(scan_name))
                                                    self.stdout.write("Tag: {}".format(tag))
                                                    self.stdout.write("Original Value: {}".format(orig_val))
                                                    self.stdout.write("Processed Value: {}".format(value))
                                                    self.stdout.write(e)
                                                    self.stdout.write(traceback.format_exc())

                                                    raise Exception

                                                except PgWarning as w:

                                                    self.stdout.write("Warning: Postgres warning processing "
                                                                      "MRScan models for "
                                                                      "exam {}".format(study_meta_file))
                                                    self.stdout.write(w)
                                                    self.stdout.write(traceback.format_exc())

                                            else:
                                                self.stdout.write("Warning: No dicom data found for scan {}"
                                                                  "of exam {}".format(scan_name, study_meta_file))
                                                continue
