import rapidjson as json
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from fmrif_archive.utils import dicom_json_to_keyword_and_flatten
from pymongo import MongoClient, DESCENDING
from pymongo.errors import PyMongoError


class Command(BaseCommand):

    help = 'Load study and scan metadata obtained from Oxygen/Gold archives'

    def add_arguments(self, parser):

        parser.add_argument("--data", type=str, default=settings.PARSED_DATA_PATH)

        parser.add_argument("--scanners", nargs="*", type=str, default=[])

        parser.add_argument("--years", nargs="*", type=str, default=[])

        parser.add_argument("--months", nargs="*", type=str, default=[])

        parser.add_argument("--days", nargs="*", type=str, default=[])

    def handle(self, *args, **options):

        # Establish MongoDB connection
        client = MongoClient(
            settings.MONGO_DB['mr_scans']['HOST'],
            username=settings.MONGO_DB['mr_scans']['USER'],
            password=settings.MONGO_DB['mr_scans']['PASSWORD'],
            authSource=settings.MONGO_DB['mr_scans']['AUTH_SOURCE'],
            authMechanism=settings.MONGO_DB['mr_scans']['AUTH_MECHANISM']
        )
        db = client[settings.MONGO_DB['mr_scans']['DATABASE']]
        collection = db[settings.MONGO_DB['mr_scans']['COLLECTION']]

        # Test whether the uniqueness constraint is defined, create it if not (this will only happen when collection
        # first created)
        if not collection.index_information().get('scan_uniqueness_constraint', None):

            collection.create_index([
                ('_metadata.exam_id', DESCENDING),
                ('_metadata.revision', DESCENDING),
                ('_metadata.scan_name', DESCENDING)
            ], unique=True, name="scan_uniqueness_constraint")

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

                                    study_data = study_metadata['data']

                                    mr_scans = []

                                    for subdir in study_data:
                                        if subdir.get('dicom_data', None):
                                            mr_scans.append(subdir)

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

                                        scan_document = {
                                            "_metadata": {
                                                "scan_name": scan_name,
                                                "num_files": num_files,
                                                "exam_id": parent_exam_id,
                                                "revision": revision,
                                            }
                                        }

                                        dicom_readable = dicom_json_to_keyword_and_flatten(scan_dicom_data)

                                        scan_document.update(dicom_readable)

                                        scan_documents_to_create.append(scan_document)

                                    try:

                                        collection.insert_many(scan_documents_to_create)

                                    except PyMongoError as e:

                                        self.stdout.write("Error: Unable to insert scan documents "
                                                          "for study ".format(study_meta_file))
                                        self.stdout.write(e)
                                        self.stdout.write(traceback.format_exc())
