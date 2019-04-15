import rapidjson as json
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from pymongo.errors import PyMongoError
from pymongo import InsertOne
from datetime import datetime
from datetime import time as datetime_time
from fmrif_archive.utils import get_fmrif_scanner, parse_pn


class Command(BaseCommand):

    help = 'Load study and scan metadata obtained from Oxygen/Gold archives'

    def parse_attribute(self, tag, scan_name, attribute):

        values = attribute.get('Value', None)

        new_value = []

        vr = attribute['vr']

        if values:

            if vr == 'PN':
                for val in values:

                    if type(val) == dict:
                        new_value.append(val.get('Alphabetic', None))
                    elif type(val) == str:
                        new_value.append(val)
                    else:
                        new_value.append(None)

            elif vr == 'SQ':

                for val in values:
                    for key, attr in val.items():
                        new_value.append(self.parse_attribute(key, scan_name, attr))
            else:

                new_value = [val for val in values]

        # Check all string values to ensure they dont exceed max mongo indexable size (1024 bytes)
        # Restrict to len(string) < 1024
        for val in new_value:
            if (type(val) == str) and (len(val) >= 1024):
                raise AttributeError

        return {
            'tag': tag,
            'value': new_value,
            'scan_name': scan_name,
        }

    def add_arguments(self, parser):

        parser.add_argument("--data", type=str, default=settings.PARSED_DATA_PATH)

        parser.add_argument("--scanners", nargs="*", type=str, default=[])

        parser.add_argument("--years", nargs="*", type=str, default=[])

        parser.add_argument("--months", nargs="*", type=str, default=[])

        parser.add_argument("--days", nargs="*", type=str, default=[])

        parser.add_argument("--database", type=str, default="image_archive")

        parser.add_argument("--collection", type=str, default="mr_scans")

    def handle(self, *args, **options):

        # Establish MongoDB connection
        client = settings.MONGO_CLIENT
        db = client[options['database']]
        collection = db.get_collection(options['collection'])

        # # Test whether the uniqueness constraint is defined, create it if not (this will only happen when collection
        # # first created)
        # if not collection.index_information().get('scan_uniqueness_constraint', None):
        #
        #     collection.create_index([
        #         ('_metadata.exam_id', DESCENDING),
        #         ('_metadata.revision', DESCENDING),
        #         ('_metadata.scan_name', DESCENDING)
        #     ], unique=True, name="scan_uniqueness_constraint")
        #
        # if not collection.index_information().get('study_date_idx', None):
        #     collection.create_index([
        #         ('_metadata.study_datetime', DESCENDING)
        #     ], name="study_datetime_idx")

        scanners = options['scanners']
        years = options['years']
        months = options['months']
        days = options['days']

        parsed_data_path = Path(options['data'])

        if not scanners:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir() if scanner_path.is_dir()]
        else:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir()
                             if (scanner_path.is_dir() and (scanner_path.name in scanners))]

        for scanner_path in sorted(scanner_paths):

            if not years:
                year_paths = [year_path for year_path in scanner_path.iterdir() if year_path.is_dir()]
            else:
                year_paths = [year_path for year_path in scanner_path.iterdir()
                              if (year_path.is_dir() and (year_path.name in years))]

            for year_path in sorted(year_paths):

                if not months:
                    month_paths = [month_path for month_path in year_path.iterdir() if month_path.is_dir()]
                else:
                    month_paths = [month_path for month_path in year_path.iterdir()
                                   if (month_path.is_dir() and (month_path.name in months))]

                for month_path in sorted(month_paths):

                    if not days:
                        day_paths = [day_path for day_path in month_path.iterdir() if day_path.is_dir()]
                    else:
                        day_paths = [day_path for day_path in month_path.iterdir()
                                     if (day_path.is_dir() and (day_path.name in days))]

                    for day_path in sorted(day_paths):

                        exams_to_create = []

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

                                    if not station_name:
                                        station_name = filepath.split("/")[0]

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

                                    if not study_date:
                                        year, month, day = filepath.split("/")[1:4]
                                        study_date = "{}{}{}".format(year, month, day)
                                        study_date = datetime.strptime(study_date, '%Y%m%d').date()

                                    try:
                                        study_time = dicom_data["00080030"]['Value'][0]
                                        if "." in study_time:
                                            study_time = datetime.strptime(study_time, '%H%M%S.%f').time()
                                        else:
                                            study_time = datetime.strptime(study_time, '%H%M%S').time()
                                    except (KeyError, IndexError):
                                        study_time = None

                                    if study_time:
                                        study_datetime = datetime.combine(study_date, study_time)
                                    else:
                                        study_datetime = datetime.combine(study_date, datetime_time.min)

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
                                        name = dicom_data["00100010"]['Value'][0]['Alphabetic']
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
                                        birth_date = datetime.strptime(birth_date, '%Y%m%d')
                                    except (KeyError, IndexError):
                                        birth_date = None

                                    new_exam = {
                                        'exam_id': exam_id,
                                        'revision': revision,
                                        'parser_version': parser_version,
                                        'filepath': filepath,
                                        'checksum': checksum,
                                        'station_name': station_name,
                                        'study_instance_uid': study_instance_uid,
                                        'study_id': study_id,
                                        'study_datetime': study_datetime,
                                        'study_description': study_description,
                                        'protocol': protocol,
                                        'accession_number': accession_number,
                                        'name': name,
                                        'last_name': last_name,
                                        'first_name': first_name,
                                        'patient_id': patient_id,
                                        'sex': sex,
                                        'birth_date': birth_date,
                                        'dicom_attributes': []
                                    }

                                    study_data = study_metadata['data']

                                    mr_scans = []

                                    for subdir in study_data:
                                        if subdir.get('dicom_data', None):
                                            mr_scans.append(subdir)

                                    self.stdout.write("Found {} mr scans".format(len(mr_scans)))

                                    for scan in mr_scans:

                                        try:

                                            scan_dicom_data = scan['dicom_data']
                                            scan_name = scan['metadata']['gold_scan_dir']

                                        except KeyError:

                                            self.stdout.write("Error: Missing mandatory scan metadata, "
                                                              "omitting scan from exam {}".format(study_meta_file))
                                            continue

                                        for tag, attr in scan_dicom_data.items():

                                            vr = attr.get('vr', None)

                                            if not vr:
                                                self.stdout.write(
                                                    "WARNING: No VR found for tag {} in scan {} "
                                                    "of study {}. Skipping.".format(tag, scan_name,
                                                                                    study_meta_file))
                                                continue

                                            if vr in ['OB', 'OD', 'OF', 'OL', 'OV', 'OW', 'SQ', 'UN']:
                                                self.stdout.write(
                                                    "WARNING: Tag encoding of type B64 or JSON not supported "
                                                    "for querying purposes - Tag {} in scan {} "
                                                    "of study {}. Skipping.".format(tag, scan_name,
                                                                                    study_meta_file))
                                                continue

                                            try:

                                                new_tag = self.parse_attribute(tag, scan_name, attr)
                                                new_exam['dicom_attributes'].append(new_tag)

                                            except AttributeError:
                                                self.stdout.write(
                                                    "Attribute value exceeds indexable size. Skipping. Tag {} in "
                                                    "scan of study {}".format(tag, scan_name, study_meta_file)
                                                )

                                    exams_to_create.append(InsertOne(new_exam))

                        try:

                            res = collection.bulk_write(exams_to_create)

                            self.stdout.write("Inserted {} exams to collection".format(res.inserted_count))

                        except PyMongoError as e:

                            self.stdout.write("Error: Unable to insert scan documents "
                                              "for day ".format(day_path))
                            self.stdout.write(e)
                            self.stdout.write(traceback.format_exc())
