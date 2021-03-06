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
    DICOMInstance,
    FileCollection,
    File
)


def process_dicom_instances(parent_exam, instance_files):

    checksum_file = instance_files[0]
    metadata_file = instance_files[1]

    scan_name = checksum_file.name.replace("_checksum.txt", "").split("_scan_")[-1]

    try:
        curr_scan = MRScan.objects.get(parent_exam=parent_exam, name=scan_name)
    except MRScan.DoesNotExist:
        return "Error opening MRScan model for metadata file {}".format(metadata_file)

    instances_data = {}

    with open(checksum_file, "rt") as checksums:

        for line in checksums:

            checksum, filename = line.rstrip("\n").split("  ")
            filename = filename.lstrip("./")

            if "readme" not in filename.lower():
                instances_data[filename] = {
                    'checksum': checksum,
                    'metadata': None,
                }

    with open(metadata_file, "rt") as meta_file:

        for line in meta_file:

            filename, instance_meta = line.rstrip("\n").split("\t")
            filename = filename.lstrip("./")

            if "readme" not in filename.lower():
                instances_data[filename]['metadata'] = json.loads(instance_meta)

    new_dicom_instances = []

    for filename, data in instances_data.items():

        echo_number = None
        sop_instance_uid = None
        slice_index = None
        image_position_patient = None

        if data['metadata']:
            echo_number = data['metadata'].get('echo_number', None)

            sop_instance_uid = data['metadata'].get('sop_instance_uid', None)

            slice_index = data['metadata'].get('raw_data_run_number', None)

            image_position_patient = data['metadata'].get('image_position_patient',
                                                          None)
        
        try:

            inst = DICOMInstance.objects.filter(
                parent_scan=curr_scan,
                file_type='dicom',
                filename=filename,
                checksum=data['checksum'],
                echo_number=echo_number,
                sop_instance_uid=sop_instance_uid,
                slice_index=slice_index,
                image_position_patient=image_position_patient
            )
            
            if inst:
                continue
        
        except DICOMInstance.DoesNotExist:
            pass

        new_dicom_instances.append(
            DICOMInstance(
                parent_scan=curr_scan,
                file_type='dicom',
                filename=filename,
                checksum=data['checksum'],
                echo_number=echo_number,
                sop_instance_uid=sop_instance_uid,
                slice_index=slice_index,
                image_position_patient=image_position_patient
            )
        )

    return new_dicom_instances


def process_file_instances(parent_exam, checksum_file):

    scan_name = checksum_file.name.replace("_checksum.txt", "").split("_scan_")[-1]

    try:
        curr_subdir = FileCollection.objects.get(parent_exam=parent_exam, name=scan_name)
    except FileCollection.DoesNotExist:
        return "Error opening FileCollection model for checksum file {}".format(checksum_file)

    subdir_data = {}

    with open(checksum_file, "rt") as checksums:

        for line in checksums:

            checksum, filename = line.rstrip("\n").split("  ")
            filename = filename.lstrip("./")

            if "readme" not in filename.lower():
                subdir_data[filename] = {
                    'checksum': checksum
                }

    new_file_instances = []

    for filename, data in subdir_data.items():

        try:
            
            f = File.objects.filter(
                parent_collection=curr_subdir,
                file_type='other',
                filename=filename,
                checksum=data['checksum']
            )

            if f:
                continue
        
        except File.DoesNotExist:
            pass

        new_file_instances.append(
            File(
                parent_collection=curr_subdir,
                file_type='other',
                filename=filename,
                checksum=data['checksum']
            )
        )

    return new_file_instances


class Command(BaseCommand):

    help = 'Load metadata and checksum for individual DICOM files obtained from Oxygen/Gold archives'

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

                                    self.stdout.write("Loading instances for exam  {}".format(str(study_meta_file)))

                                    exam_id = study_meta_file.name.replace("study_", "").replace("_metadata.txt", "")

                                    try:

                                        parent_exam = Exam.objects.get(exam_id=exam_id)

                                    except Exam.DoesNotExist:
                                        self.stdout.write("Error: Cannot load Exam model for "
                                                          "study {}".format(study_meta_file))
                                        continue

                                    # Get checksum and metadata files for the current exam
                                    metadata_files = list(session_dir.glob("*_scan_*_metadata.txt"))
                                    checksum_files = list(session_dir.glob("*_scan_*_checksum.txt"))

                                    # Pair metadata files with corresponding checksum files, if there is a match

                                    dicom_instances = []
                                    used_checksums = []

                                    for mf in metadata_files:

                                        wanted_checksum = mf.name.replace("_metadata.txt", "_checksum.txt")

                                        matching_checksum = list(filter(lambda cf: cf.name == wanted_checksum,
                                                                        checksum_files))

                                        if not matching_checksum:
                                            self.stdout.write("Error: Cannot find matching checksum file for metadata"
                                                              "file {} in study {}".format(mf, study_meta_file))
                                            continue

                                        if len(matching_checksum) > 1:
                                            self.stdout.write("Error: More than one checksum file retrieved for "
                                                              "metadata file {} in "
                                                              "study {}".format(mf, study_meta_file))
                                            continue

                                        used_checksums.append(matching_checksum[0])
                                        dicom_instances.append((matching_checksum[0], mf))

                                    # Get the remaining checksum files which dont match any metadata, to create
                                    # File instances
                                    non_dicom_checksums = list(filter(lambda cf: cf not in used_checksums,
                                                                      checksum_files))

                                    file_instances = []

                                    for cf in non_dicom_checksums:
                                        file_instances.append(cf)

                                    if dicom_instances:

                                        dicom_instances_to_create = []

                                        for dicom_instance in dicom_instances:

                                            result = process_dicom_instances(parent_exam, dicom_instance)

                                            if type(result) == str:

                                                self.stdout.write(result)

                                            else:

                                                dicom_instances_to_create.extend(result)

                                        try:

                                            self.stdout.write("Writing DICOMInstance "
                                                              "objects for exam {}".format(study_meta_file))

                                            DICOMInstance.objects.bulk_create(dicom_instances_to_create)

                                        except (DjangoDBError, PgError) as e:

                                            self.stdout.write("Warning: Unable to write "
                                                              "DICOMInstance objects for "
                                                              "exam {}".format(study_meta_file))
                                            self.stdout.write(e)
                                            self.stdout.write(traceback.format_exc())

                                        except PgWarning as w:

                                            self.stdout.write("Warning: Postgres warning creating "
                                                              "DICOMInstance objects for "
                                                              "exam {}".format(study_meta_file))
                                            self.stdout.write(w)
                                            self.stdout.write(traceback.format_exc())

                                    if file_instances:

                                        file_instances_to_create = []

                                        for file_instance in file_instances:

                                            result = process_file_instances(parent_exam, file_instance)

                                            if type(result) == str:

                                                self.stdout.write(result)

                                            else:

                                                file_instances_to_create.extend(result)

                                        try:

                                            self.stdout.write("Writing File objects for "
                                                              "exam {}".format(study_meta_file))

                                            File.objects.bulk_create(file_instances_to_create)

                                        except (DjangoDBError, PgError) as e:

                                            self.stdout.write("Warning: Unable to create "
                                                              "File objects for exam {}".format(study_meta_file))
                                            self.stdout.write(e)
                                            self.stdout.write(traceback.format_exc())

                                        except PgWarning as w:

                                            self.stdout.write("Warning: Postgres warning creating "
                                                              "File objects for exam {}".format(study_meta_file))
                                            self.stdout.write(w)
                                            self.stdout.write(traceback.format_exc())
