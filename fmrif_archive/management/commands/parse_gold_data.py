import logging
import json
import itertools

from datetime import datetime, timedelta, date
from django.conf import settings as django_settings
from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from fmrif_archive.models import Exam

from fmrif_archive.management.utils.parser_utils import (
    get_checksum,
    get_exam_id,
    parse_metadata,
    uncompress_tgz_files,
)


PARSER_VERSION = "0.2.1"


FMRIF_SCANNERS = [
    'fmrif3ta',
    'fmrif3tb',
    'fmrif3tc',
    'fmrif3td',
    'fmrif7t',
]


class Command(BaseCommand):

    help = "Load study and scan metadata obtained from Gold archives into Osmium's SQL database"

    def add_arguments(self, parser):

        parser.add_argument(
            "--data_dir",
            help="Path to the Gold archives directory",
            default=django_settings.ARCHIVE_BASE_PATH
        )

        parser.add_argument(
            "--from",
            help="Parse from this date. Format: MMDDYYYY",
            type=str,
            default=None
        )

        parser.add_argument(
            "--to",
            default=None,
            type=str,
            help="Parse to this date. Format: MMDDYYYY"
        )

        parser.add_argument(
            "--scanners",
            default=FMRIF_SCANNERS,
            help="Scanners to consider during parsing.",
            choices=FMRIF_SCANNERS,
            nargs="*"
        )

        parser.add_argument(
            "--work_dir",
            help="Path to working directory",
            default=Path(django_settings.PARSED_DATA_PATH) / datetime.today().strftime("%Y%m%d_%H%M%S")
        )

        parser.add_argument(
            "--new_exams_only",
            help="Parse only exams that have not already been added to osmium's DB",
            action='store_true',
        )

        parser.add_argument(
            "--tgz_cores",
            help="Number of cores to use when extracting TGZ files",
            type=int,
            default=6,
        )

        parser.add_argument(
            "--batch_size",
            help="Number of files to uncompress and process at a time. Specify a small number"
                 "if disk memory is in short supply, otherwise it will uncompress all matching"
                 "files before parsing them.",
            default=None,
            type=int
        )

    def handle(self, *args, **options):

        parser_settings = {
            'data_dir': Path(options['data_dir']),
            'work_dir': Path(options['work_dir']),
            'scanners': options['scanners'],
            'new_exams': options.get('new_exams_only', False),
            'tgz_cores': options['tgz_cores'],
            'batch_size': options['batch_size'],
            'version': PARSER_VERSION,
        }

        if not parser_settings['work_dir'].is_dir():
            parser_settings['work_dir'].mkdir(parents=True, exist_ok=True)

        log_fpath = parser_settings['work_dir'] / "gold_parsing_{}.log".format(
            datetime.today().strftime("%Y%m%d_%H%M%S")
        )

        if not log_fpath.is_file():
            log_fpath.touch(exist_ok=True)

        fh = logging.FileHandler(filename=str(log_fpath))
        parser_log = logging.getLogger('parser')
        parser_log.setLevel(logging.INFO)
        parser_log.addHandler(fh)

        has_from = True if options['from'] else False
        has_to = True if options['to'] else False

        time_fmt = "%m%d%Y"

        if has_from:

            try:

                if len(options['from']) != 8:
                    raise ValueError

                parser_settings["from"] = datetime.strptime(options['from'], time_fmt).date()

            except ValueError:
                parser_log.error("Correct date format for --from field is MMDDYYYY")
                return

        if has_to:

            try:
                if len(options['to']) != 8:
                    raise ValueError
                parser_settings["to"] = datetime.strptime(options['to'], time_fmt).date()
            except ValueError:
                parser_log.error("Correct date format for --to field is MMDDYYYY")
                return

        else:

            parser_settings['to'] = datetime.today().date()

        parser_log.info("Runtime Settings:")
        parser_log.info(
            json.dumps(parser_settings, indent=4, default=lambda x: str(x))
        )

        parser_log.info("Searching for compressed Gold archives in data directory...")

        compressed_files = []

        for scanner in parser_settings['scanners']:

            if has_from:

                delta = (parser_settings['to'] - parser_settings['from']) if has_from else None

                dates = [(parser_settings['from'] + timedelta(i)) for i in range(delta.days + 1)]

            else:

                scanner_dir = parser_settings['data_dir'] / scanner

                years = sorted(
                    [int(y.name) for y in scanner_dir.iterdir() if (y.is_dir() and (len(y.name) == 4) and y.name.isdigit())]
                )

                if not years:
                    continue

                from_date = date(years[0], 1, 1)

                delta = parser_settings['to'] - from_date

                dates = [
                    (from_date + timedelta(i)) for i in range(delta.days + 1) if
                    (from_date + timedelta(i)).year in years
                ]

            for curr_date in dates:

                curr_year = str(curr_date.year)
                curr_month = str(curr_date.month).rjust(2, "0")
                curr_day = str(curr_date.day).rjust(2, "0")

                search_dir = parser_settings['data_dir'] / scanner / curr_year / curr_month / curr_day

                if search_dir.is_dir():

                    parser_log.info("Searching in {}...".format(search_dir))

                    file_glob = [f for f in search_dir.glob("*/*.tgz") if f.is_file()]

                    for compressed_file in file_glob:

                        # Check that the file has not been added to the DB already. If it has, skip.

                        chksum = get_checksum(compressed_file)

                        if not chksum:
                            parser_log.error("Error computing checksum for file {}. "
                                             "Skipping this file.".format(compressed_file))

                        exam_id = get_exam_id(chksum, compressed_file)

                        if parser_settings['new_exam']:
                            exam = Exam.objects.filter(exam_id=exam_id)
                        else:
                            exam = None

                        if not exam:
                            compressed_files.append((compressed_file, chksum, exam_id))
                        else:
                            parser_log.warning("Exam {} already is already in the database. "
                                               "Skipping.".format(compressed_file))

        if len(compressed_files) < 1:
            parser_log.info("No compressed files found. Exiting...")
            return

        parser_log.info("Found {} compressed files...".format(len(compressed_files)))

        # Begin processing files, in appropriate batch sizes

        batch_size = parser_settings['batch_size'] if parser_settings['batch_size'] else len(compressed_files)

        compressed_files_iter = iter(compressed_files)

        total_files = len(compressed_files)
        curr_iter = 1
        curr_first_file = 1

        while True:

            curr_files = itertools.islice(compressed_files_iter, batch_size)
            curr_files = list(curr_files)

            if not curr_files:
                break

            parser_log.info(
                "Processing files {} - {} out of {}...".format(
                    curr_first_file, curr_iter*batch_size, total_files
                )
            )

            curr_first_file = curr_iter*batch_size + 1
            curr_iter += 1

            extracted_archives, msgs = uncompress_tgz_files(curr_files, parser_settings)

            for msg in msgs:
                if msg.startswith("Extracted"):
                    parser_log.info(msg)
                else:
                    parser_log.error(msg)

            if len(extracted_archives) < 1:
                parser_log.error("Unable to extract any archive. Exiting...")
                return

            parser_log.info("Parsing DICOM metadata...")

            parse_metadata(extracted_archives, parser_version=parser_settings['version'], log=parser_log)
