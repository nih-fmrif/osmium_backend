import rapidjson as json

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from fmrif_archive.models import (
    MRScan,
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

                        scans_to_update = []

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
                                    except KeyError:
                                        self.stdout.write("Error: Required metadata field not "
                                                          "available for exam {}".format(study_meta_file))
                                        continue

                                    mr_scans = []

                                    for subdir in data:
                                        if subdir.get('dicom_data', None):
                                            mr_scans.append(subdir)

                                    for scan in mr_scans:

                                        try:
                                            scan_metadata = scan['metadata']
                                            scan_dicom_metadata = scan.get('dicom_data', {})
                                            scan_private_dicom_metadata = scan.get('private_data', {})
                                            scan_name = scan_metadata['gold_scan_dir']
                                            curr_scan = MRScan.objects.get(
                                                parent_exam__exam_id=exam_id,
                                                parent_exam__revision=revision,
                                                name=scan_name
                                            )
                                        except KeyError:

                                            self.stdout.write(
                                                "Error: Unable to load scan "
                                                "object for study {}".format(study_meta_file))
                                            continue

                                        curr_scan.dicom_metadata = scan_dicom_metadata
                                        curr_scan.private_dicom_metadata = scan_private_dicom_metadata

                                        scans_to_update.append(curr_scan)

                        MRScan.objects.bulk_update(scans_to_update, ['dicom_metadata', 'private_dicom_metadata'])
