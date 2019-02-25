import rapidjson as json
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import Error, transaction
from pathlib import Path
from fmrif_archive.models import (
    Exam,
    MRScan,
    DICOMInstance,
    FileCollection,
    File
)
from datetime import datetime
from fmrif_archive.utils import parse_pn, get_fmrif_scanner


class Command(BaseCommand):

    help = 'Loads metadata obtained from Oxygen/Gold archives'

    def add_arguments(self, parser):

        parser.add_argument("--scanners", nargs="*", type="str", default=[])

        parser.add_argument("--years", nargs="*", type="str", default=[])

        parser.add_argument("--months", nargs="*", type="str", default=[])

        parser.add_argument("--days", nargs="*", type="str", default=[])

    def handle(self, *args, **options):

        scanners = options['scanners']
        years = options['years']
        months = options['months']
        days = options['days']

        parsed_data_path = Path(settings.PARSED_DATA_PATH)

        if not scanners:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir() if scanner_path.is_dir()]
        else:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir()
                             if (scanner_path.is_dir() and scanner_path.name in scanners)]

        for scanner_path in scanner_paths:

            if not years:
                year_paths = [year_path for year_path in scanner_path.iterdir() if year_path.is_dir()]
            else:
                year_paths = [year_path for year_path in scanner_path.iterdir()
                              if (year_path.is_dir() and year_path.name in years)]

            for year_path in year_paths:

                if not months:
                    month_paths = [month_path for month_path in year_path.iterdir() if month_path.is_dir()]
                else:
                    month_paths = [month_path for month_path in year_path.iterdir()
                                   if (month_path.is_dir() and month_path.name in months)]

                for month_path in month_paths:

                    if not days:
                        day_paths = [day_path for day_path in month_path.iterdir() if day_path.is_dir()]
                    else:
                        day_paths = [day_path for day_path in month_path.iterdir()
                                     if (day_path.is_dir() and day_path.name in days)]

                    for day_path in day_paths:

                        for exam_id in [e for e in day_path.iterdir() if e.is_dir()]:

                            for pt_dir in [p for p in exam_id.iterdir() if p.is_dir()]:

                                for session_dir in [s for s in pt_dir.iterdir() if s.is_dir()]:

                                    study_metadata_file = list(session_dir.glob("study_*_metadata.txt"))

                                    if not study_metadata_file:
                                        self.stdout.write("Error: No study metadata found in {}".format(session_dir))
                                        continue

                                    if len(study_metadata_file) > 1:
                                        self.stdout.write("Error: Multiple study metadata "
                                                          "files for {} found".format(session_dir))
                                        continue

                                    study_meta_file = study_metadata_file[0]

                                    self.stdout.write("Loading data from {}".format(str(study_meta_file)))

                                    try:
                                        with open(str(study_meta_file), "rt") as sm:
                                            study_metadata = json.load(sm)
                                    except:
                                        self.stdout.write("Error: Cannot load file {}".format(study_meta_file))
                                        continue

                                    metadata = study_metadata['metadata']
                                    data = study_metadata['data']

                                    dicom_data = None
                                    for subdir in data:
                                        if subdir.get('dicom_data', None):
                                            dicom_data = subdir['dicom_data']
                                            break

                                    if not dicom_data:
                                        self.stdout.write("Error: No DICOM metadata "
                                                          "for exam {}".format(study_meta_file))
                                        continue

                                    try:
                                        exam_id = metadata['exam_id']
                                        revision = 1
                                        parser_version = metadata['parser_version']
                                        filepath = metadata['gold_fpath']
                                        checksum = metadata['gold_archive_checksum']
                                    except KeyError:
                                        self.stdout.write("Error: Required metadata field not "
                                                          "available for exam {}".format(study_meta_file))
                                        continue

                                    try:
                                        station_name = get_fmrif_scanner(dicom_data["00081010"]["Value"][0])
                                    except (KeyError, IndexError):
                                        station_name = None

                                    try:
                                        study_instance_uid = dicom_data["0020000D"]['Value'][0]
                                    except (KeyError, IndexError):
                                        study_instance_uid = None

                                    try:
                                        study_id = dicom_data["00200010"]['Value'][0]
                                    except (KeyError, IndexError):
                                        study_id = None

                                    try:
                                        study_date = dicom_data["00080020"]['Value'][0]
                                        study_date = datetime.strptime(study_date, '%Y%m%d').date()
                                    except (KeyError, IndexError):
                                        study_date = None

                                    try:
                                        study_time = dicom_data["00080030"]['Value'][0]
                                        if "." in study_time:
                                            study_time = datetime.strptime(study_time, '%H%M%S.%f').time()
                                        else:
                                            study_time = datetime.strptime(study_time, '%H%M%S').time()
                                    except (KeyError, IndexError):
                                        study_time = None

                                    try:
                                        study_description = dicom_data["00081030"]['Value'][0]
                                    except (KeyError, IndexError):
                                        study_description = None

                                    protocol = None  # Not implemented yet

                                    try:
                                        accession_number = dicom_data["00080050"]['Value'][0]
                                    except (KeyError, IndexError):
                                        accession_number = None

                                    try:
                                        name = dicom_data["00100010"]['Value'][0]
                                    except (KeyError, IndexError):
                                        name = None

                                    if name:
                                        name_fields = parse_pn(name)
                                        last_name = name_fields['family_name']
                                        first_name = name_fields['given_name']
                                    else:
                                        first_name, last_name = None, None

                                    try:
                                        patient_id = dicom_data["00100020"]['Value'][0]
                                    except (KeyError, IndexError):
                                        patient_id = None

                                    try:
                                        sex = dicom_data["00100040"]['Value'][0]
                                    except (KeyError, IndexError):
                                        sex = None

                                    try:
                                        birth_date = dicom_data["00100030"]['Value'][0]
                                        birth_date = datetime.strptime(birth_date, '%Y%m%d').date()
                                    except (KeyError, IndexError):
                                        birth_date = None

                                    try:

                                        exam = Exam.objects.create(
                                            exam_id=exam_id,
                                            revision=revision,
                                            parser_version=parser_version,
                                            filepath=filepath,
                                            checksum=checksum,
                                            station_name=station_name,
                                            study_instance_uid=study_instance_uid,
                                            study_id=study_id,
                                            study_date=study_date,
                                            study_time=study_time,
                                            study_description=study_description,
                                            protocol=protocol,
                                            accession_number=accession_number,
                                            name=name,
                                            last_name=last_name,
                                            first_name=first_name,
                                            patient_id=patient_id,
                                            sex=sex,
                                            birth_date=birth_date
                                        )

                                    except Error as e:

                                        self.stdout.write("Error: Unable to create exam model "
                                                          "for {}".format(study_meta_file))
                                        self.stdout.write(e)
                                        self.stdout.write(traceback.format_exc())

                                        continue

                                    mr_scans = []
                                    other_data = []

                                    for subdir in data:
                                        if subdir.get('dicom_data', None):
                                            mr_scans.append(subdir)
                                        else:
                                            other_data.append(subdir)

                                    for scan in mr_scans:

                                        parent_exam = exam

                                        try:
                                            scan_metadata = scan['metadata']
                                            scan_dicom_data = scan['dicom_data']
                                            scan_name = scan_metadata['gold_scan_dir']
                                            scan_num_files = scan_metadata['num_files']
                                        except KeyError:

                                            self.stdout.write("Error: Missing mandatory scan metadata, "
                                                              "omitting scan from exam {}".format(study_meta_file))
                                            continue

                                        try:
                                            series_date = scan_dicom_data["00080021"]['Value'][0]
                                            series_date = datetime.strptime(series_date, '%Y%m%d').date()
                                        except (KeyError, IndexError):
                                            series_date = None

                                        try:
                                            series_time = scan_dicom_data["00080031"]['Value'][0]
                                            if "." in series_time:
                                                series_time = datetime.strptime(series_time, '%H%M%S.%f').time()
                                            else:
                                                series_time = datetime.strptime(series_time, '%H%M%S').time()
                                        except (KeyError, IndexError):
                                            series_time = None

                                        try:
                                            series_description = scan_dicom_data["0008103E"]['Value'][0]
                                        except (KeyError, IndexError):
                                            series_description = None

                                        try:
                                            sop_class_uid = scan_dicom_data["00080016"]['Value'][0]
                                        except (KeyError, IndexError):
                                            sop_class_uid = None

                                        try:
                                            series_instance_uid = scan_dicom_data["0020000E"]['Value'][0]
                                        except (KeyError, IndexError):
                                            series_instance_uid = None

                                        try:
                                            series_number = scan_dicom_data["00200011"]['Value'][0]
                                        except (KeyError, IndexError):
                                            series_number = None

                                        try:

                                            new_scan = MRScan.objects.create(
                                                parent_exam=parent_exam,
                                                name=scan_name,
                                                num_files=scan_num_files,
                                                series_date=series_date,
                                                series_time=series_time,
                                                series_description=series_description,
                                                sop_class_uid=sop_class_uid,
                                                series_instance_uid=series_instance_uid,
                                                series_number=series_number
                                            )

                                        except Error as e:

                                            self.stdout.write("Error: Unable to create MRScan model "
                                                              "for scan {} of exam {}".format(scan_name,
                                                                                              study_meta_file))
                                            self.stdout.write(e)
                                            self.stdout.write(traceback.format_exc())

                                            continue

                                        # Get the DICOM Instance Metadata and Checksums for the current scan

                                        instances_to_create = []

                                        instance_checksum = list(session_dir.glob(
                                            "*_scan_{}_checksum.txt".format(scan_name)))

                                        if len(instance_checksum) != 1:
                                            self.stdout.write("Error: Missing or multiple "
                                                              "scan checksum files for scan {}".format(scan_name))
                                            continue

                                        instance_checksum = str(instance_checksum[0])

                                        instance_metadata = list(session_dir.glob(
                                            "*_scan_{}_metadata.txt".format(scan_name)))

                                        if len(instance_metadata) != 1:
                                            self.stdout.write("Warning: Missing or multiple "
                                                              "scan metadata files for scan {}".format(scan_name))
                                            instance_metadata = None
                                        else:
                                            instance_metadata = str(instance_metadata[0])

                                        instance_data = {}

                                        with open(instance_checksum, "rt") as checksums:

                                            for line in checksums:

                                                checksum, fname = line.rstrip("\n").split("  ")
                                                fname = fname.lstrip("./")

                                                if "readme" not in fname.lower():
                                                    instance_data[fname] = {
                                                        'checksum': checksum,
                                                        'metadata': None,
                                                    }

                                        if instance_metadata:

                                            with open(instance_metadata, "rt") as im:

                                                for line in im:

                                                    fname, instance_meta = line.rstrip("\n").split("  ")
                                                    fname = fname.lstrip("./")

                                                    if "readme" not in fname.lower():
                                                        instance_data[fname]['metadata'] = json.loads(instance_meta)

                                        for fname, dat in instance_data.items():

                                            sop_instance_uid = None
                                            slice_indexes = None
                                            image_position_patient = None
                                            num_indices = None
                                            num_slices = None

                                            if dat['metadata']:
                                                sop_instance_uid = dat['metadata'].get('sop_instance_uid', None)
                                                slice_indexes = dat['metadata'].get('slice_indexes', None)
                                                image_position_patient = dat['metadata'].get('image_position_patient',
                                                                                             None)
                                                num_indices = dat['metadata'].get('num_indices', None)
                                                num_slices = dat['metadata'].get('num_slices', None)

                                            instances_to_create.append(DICOMInstance(
                                                file_type='dicom',
                                                filename=fname,
                                                checksum=dat['checksum'],
                                                sop_instance_uid=sop_instance_uid,
                                                slice_indexes=slice_indexes,
                                                image_position_patient=image_position_patient,
                                                num_indices=num_indices,
                                                num_slices=num_slices,
                                            ))

                                        try:

                                            with transaction.atomic():

                                                DICOMInstance.objects.bulk_create(instances_to_create)
                                                new_scan.dicom_files.add(instances_to_create)

                                        except Error as e:

                                            self.stdout.write("Warning: Unable to create "
                                                              "DICOM instances for "
                                                              "scan {} in exam {}".format(scan_name, study_meta_file))
                                            self.stdout.write(e)
                                            self.stdout.write(traceback.format_exc())

                                    for subdir in other_data:

                                        parent_exam = exam

                                        try:
                                            subdir_metadata = subdir['metadata']
                                            subdir_name = subdir_metadata['gold_scan_dir']
                                            subdir_num_files = subdir_metadata['num_files']
                                        except KeyError:
                                            self.stdout.write("Error: Missing mandatory scan metadata, "
                                                              "omitting scan from exam {}".format(study_meta_file))
                                            continue

                                        try:

                                            new_subdir = FileCollection.objects.create(
                                                parent_exam=parent_exam,
                                                name=subdir_name,
                                                num_files=subdir_num_files
                                            )

                                        except Error as e:

                                            self.stdout.write("Error: Unable to create "
                                                              "FileCollection model for subdir "
                                                              "{} of exam {}".format(subdir_name, study_meta_file))
                                            self.stdout.write(e)
                                            self.stdout.write(traceback.format_exc())
                                            continue

                                        subdir_checksum = list(session_dir.glob(
                                            "*_scan_{}_checksum.txt".format(subdir_name)))

                                        if len(subdir_checksum) != 1:
                                            self.stdout.write("Error: Missing or multiple "
                                                              "scan checksum files for scan {}".format(subdir_name))
                                            continue

                                        subdir_checksum = str(subdir_checksum[0])

                                        subdir_data = {}

                                        with open(subdir_checksum, "rt") as checksums:

                                            for line in checksums:

                                                checksum, fname = line.rstrip("\n").split("  ")
                                                fname = fname.lstrip("./")

                                                if "readme" not in fname.lower():
                                                    subdir_data[fname] = {
                                                        'checksum': checksum
                                                    }

                                        subdir_instances_to_create = []

                                        for fname, dat in subdir_data.items():

                                            subdir_instances_to_create.append(File(
                                                file_type='other',
                                                filename=fname,
                                                checksum=dat['checksum']
                                            ))

                                        try:

                                            with transaction.atomic():

                                                File.objects.bulk_create(subdir_instances_to_create)
                                                new_subdir.files.add(subdir_instances_to_create)

                                        except Error as e:

                                            self.stdout.write("Warning: Unable to create "
                                                              "File instances for "
                                                              "subdir {} in exam {}".format(subdir_name,
                                                                                            study_meta_file))
                                            self.stdout.write(e)
                                            self.stdout.write(traceback.format_exc())
