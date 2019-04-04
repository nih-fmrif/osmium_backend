import rapidjson as json
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from fmrif_archive.utils import dicom_json_to_keyword_and_flatten
from pymongo import DESCENDING, InsertOne, WriteConcern
from pymongo.errors import PyMongoError
from datetime import datetime
from fmrif_archive.utils import get_fmrif_scanner, parse_pn


class Command(BaseCommand):

    help = 'Load study and scan metadata obtained from Oxygen/Gold archives'

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

        # Test whether the uniqueness constraint is defined, create it if not (this will only happen when collection
        # first created)
        if not collection.index_information().get('scan_uniqueness_constraint', None):

            collection.create_index([
                ('_metadata.exam_id', DESCENDING),
                ('_metadata.revision', DESCENDING),
                ('_metadata.scan_name', DESCENDING)
            ], unique=True, name="scan_uniqueness_constraint")

        if not collection.index_information().get('study_date_idx', None):
            collection.create_index([
                ('_metadata.study_date', DESCENDING)
            ], name="study_date_idx")

        scanners = options['scanners']
        years = options['years']
        months = options['months']
        days = options['days']

        parsed_data_path = Path(options['data'])

        self.stdout.write("data path: {}".format(parsed_data_path))
        self.stdout.write("years: {}".format(years))
        self.stdout.write("months: {}".format(months))
        self.stdout.write("days: {}".format(days))

        if not scanners:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir() if scanner_path.is_dir()]
        else:
            scanner_paths = [scanner_path for scanner_path in parsed_data_path.iterdir()
                             if (scanner_path.is_dir() and scanner_path.name in scanners)]

        print("scanner_paths: ")
        print(scanner_paths)

        for scanner_path in sorted(scanner_paths):

            if not years:
                year_paths = [year_path for year_path in scanner_path.iterdir() if year_path.is_dir()]
            else:
                year_paths = [year_path for year_path in scanner_path.iterdir()
                              if (year_path.is_dir() and year_path.name in years)]

            print("year_paths: ")
            print(year_paths)

            for year_path in sorted(year_paths):

                if not months:
                    month_paths = [month_path for month_path in year_path.iterdir() if month_path.is_dir()]
                else:
                    month_paths = [month_path for month_path in year_path.iterdir()
                                   if (month_path.is_dir() and month_path.name in months)]

                print("month_paths: ")
                print(month_paths)

                for month_path in sorted(month_paths):

                    if not days:
                        day_paths = [day_path for day_path in month_path.iterdir() if day_path.is_dir()]
                    else:
                        day_paths = [day_path for day_path in month_path.iterdir()
                                     if (day_path.is_dir() and day_path.name in days)]

                    print("day paths:")
                    print(day_paths)

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

                                    study_data = study_metadata['data']

                                    mr_scans = []

                                    for subdir in study_data:
                                        if subdir.get('dicom_data', None):
                                            mr_scans.append(subdir)

                                    self.stdout.write("Found {} mr scans".format(len(mr_scans)))

                                    scan_documents_to_create = []

                                    for scan in mr_scans:

                                        try:

                                            scan_metadata = scan['metadata']
                                            scan_dicom_data = scan['dicom_data']

                                            scan_name = scan_metadata['gold_scan_dir']
                                            num_files = scan_metadata['num_files']
                                            parent_exam_id = scan_metadata['parent_exam_id']
                                            revision = 1

                                        except KeyError:

                                            self.stdout.write("Error: Missing mandatory scan metadata, "
                                                              "omitting scan from exam {}".format(study_meta_file))
                                            continue

                                        try:
                                            study_date = scan_dicom_data["00080020"]['Value'][0]
                                        except (KeyError, IndexError):
                                            study_date = None

                                        if not study_date:
                                            year, month, day = study_metadata['metadata']['gold_fpath'].split("/")[1:4]
                                            study_date = "{}{}{}".format(year, month, day)

                                        try:
                                            study_time = scan_dicom_data["00080030"]['Value'][0]
                                        except (KeyError, IndexError):
                                            study_time = "000000"

                                        if "." in study_time:
                                            fmt = "%Y%m%dT%H%M%S.%f"
                                        else:
                                            fmt = "%Y%m%dT%H%M%S"

                                        datetime_str = "{}T{}".format(study_date, study_time)
                                        study_datetime = datetime.strptime(datetime_str, fmt)

                                        try:
                                            study_id = scan_dicom_data["00200010"]['Value'][0]
                                        except (KeyError, IndexError):
                                            study_id = None

                                        try:
                                            study_description = scan_dicom_data["00081030"]['Value'][0]
                                        except (KeyError, IndexError):
                                            study_description = None

                                        try:
                                            scanner = get_fmrif_scanner(scan_dicom_data["00081010"]["Value"][0])
                                        except (KeyError, IndexError):
                                            scanner = study_metadata['metadata']['gold_fpath'].split("/")[0]

                                        try:
                                            name = scan_dicom_data["00100010"]['Value'][0]['Alphabetic']
                                        except (KeyError, IndexError):
                                            name = None

                                        if name:
                                            name_fields = parse_pn(name)
                                            last_name = name_fields['family_name']
                                            first_name = name_fields['given_name']
                                        else:
                                            first_name, last_name = None, None

                                        try:
                                            patient_id = scan_dicom_data["00100020"]['Value'][0]
                                        except (KeyError, IndexError):
                                            patient_id = None

                                        try:
                                            sex = scan_dicom_data["00100040"]['Value'][0]
                                        except (KeyError, IndexError):
                                            sex = None

                                        try:
                                            birth_date = scan_dicom_data["00100030"]['Value'][0]
                                            birth_date = datetime.strptime(birth_date, '%Y%m%d')
                                        except (KeyError, IndexError):
                                            birth_date = None

                                        scan_document = {
                                            "_metadata": {
                                                "scan_name": scan_name,
                                                "num_files": num_files,
                                                "exam_id": parent_exam_id,
                                                "revision": revision,
                                                "scanner": scanner,
                                                "patient_first_name": first_name,
                                                "patient_last_name": last_name,
                                                "patient_id": patient_id,
                                                "patient_sex": sex,
                                                "patient_birth_date": birth_date,
                                                "study_id": study_id,
                                                "study_description": study_description,
                                                "study_datetime": study_datetime,
                                                "last_modified": datetime.now(),
                                                "protocol": None,
                                            }
                                        }

                                        dicom_readable = dicom_json_to_keyword_and_flatten(scan_dicom_data)

                                        scan_document.update(dicom_readable)

                                        scan_documents_to_create.append(InsertOne(scan_document))

                                    try:

                                        self.stdout.write("Adding {} scan "
                                                          "documents".format(len(scan_documents_to_create)))

                                        res = collection.bulk_write(scan_documents_to_create)

                                        self.stdout.write("Inserted {} scans to collection".format(res.inserted_count))

                                    except PyMongoError as e:

                                        self.stdout.write("Error: Unable to insert scan documents "
                                                          "for study ".format(study_meta_file))
                                        self.stdout.write(e)
                                        self.stdout.write(traceback.format_exc())
