from django.db.models import Q
from datetime import datetime, timedelta, time
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from fmrif_archive.models import Exam
import pandas as pd
import rapidjson as json


FMRIF_SCANNERS = [
    'fmrif3ta',
    'fmrif3tb',
    'fmrif3tc',
    'fmrif3td',
    'fmrif7t',
]


class Command(BaseCommand):

    def parse_sched_date(self, d, h):
        curr_date = datetime.strptime(d, "%m%d%Y")
        curr_hour = time(int(h), 0, 0)
        return datetime.combine(curr_date, curr_hour)

    def add_arguments(self, parser):

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
            "--scanner",
            help="Scanner to consider during parsing.",
            choices=FMRIF_SCANNERS,
            required=True
        )

        parser.add_argument(
            "--schedule",
            help="Schedule file corresponding to selected scanner",
            required=True,
        )

        parser.add_argument(
            "--debug",
            help="Output debug statements",
            action='store_true',
            default=False
        )

    def handle(self, *args, **options):

        time_fmt = "%m%d%Y"

        from_date = None

        scanner = options['scanner']

        with open(options['schedule'], 'rt') as infile:
            curr_schedule = pd.read_csv(infile, sep=';', parse_dates={'datetime': ['date', 'hour']},
                                        date_parser=self.parse_sched_date,
                                        names=['fmrif_scanner', 'date', 'hour', 'group'], header=0,
                                        keep_default_na=False)

        if options['from']:

            try:

                if len(options['from']) != 8:
                    raise ValueError

                from_date = datetime.strptime(options['from'], time_fmt).date()

            except ValueError:
                self.stdout.write("Error: Correct date format for --from field is MMDDYYYY")
                return

        to_date = None
        if options['to']:

            try:
                if len(options['to']) != 8:
                    raise ValueError
                to_date = datetime.strptime(options['to'], time_fmt).date()
            except ValueError:
                self.stdout.write("Correct date format for --to field is MMDDYYYY")
                return

        if from_date and to_date:

            exams = Exam.objects.filter(Q(station_name=scanner) &
                                        Q(study_date__gte=from_date) &
                                        Q(study_date__lte=to_date)).prefetch_related('mr_scans')

        elif from_date:

            exams = Exam.objects.filter(Q(station_name=scanner) &
                                        Q(study_date__gte=from_date)).prefetch_related('mr_scans')

        elif to_date:

            exams = Exam.objects.filter(Q(station_name=scanner) &
                                        Q(study_date__lte=to_date)).prefetch_related('mr_scans')

        else:

            exams = Exam.objects.filter(station_name=scanner).prefetch_related('mr_scans')

        with open("{}_sched_matching_from_{}_to_{}.txt".format(scanner, from_date, to_date), "wt") as outfile:

            for exam in exams:

                outfile.write("Working on exam pk {}...\n".format(exam.pk))

                if ("QA_" in exam.filepath) or ("QA-" in exam.filepath):

                    outfile.write("Exam pk {} ({}) assigned to deptcode: '{}', "
                                  "overlap: {}\n".format(exam.pk, exam.filepath, "DEV", "N/A"))

                else:

                    mr_scans_date_times = exam.mr_scans.values_list('series_date', 'series_time')

                    exam_datetimes = [datetime.combine(dt[0], dt[1]) for dt in mr_scans_date_times if
                                      (dt[0] is not None and dt[1] is not None)]

                    exam_datetimes.append(datetime.combine(exam.study_date, exam.study_time))

                    min_exam_actual_dt = min(exam_datetimes).replace(second=0, microsecond=0)

                    max_exam_actual_dt = max(exam_datetimes).replace(second=0, microsecond=0) + timedelta(minutes=1)

                    min_exam_hours_dt = min_exam_actual_dt.replace(microsecond=0, second=0, minute=0)

                    max_exam_hours_dt = max_exam_actual_dt.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)

                    exam_hours_range = pd.date_range(start=min_exam_hours_dt, end=max_exam_hours_dt, freq="H")

                    exam_dt_range = pd.date_range(start=min_exam_actual_dt, end=max_exam_actual_dt, freq="min")

                    schedule_mask = ((curr_schedule['datetime'] >= min_exam_hours_dt) &
                                     (curr_schedule['datetime'] <= max_exam_hours_dt) &
                                     (curr_schedule['fmrif_scanner'] == scanner))

                    scheduler_entries = curr_schedule[schedule_mask]

                    blocks = {}
                    curr_group = None
                    block_count = 0

                    for exam_hour in exam_hours_range:

                        hour_dt = datetime.fromtimestamp(exam_hour.timestamp())

                        # Get schedule entry matching this hour
                        curr_entry = scheduler_entries[scheduler_entries['datetime'] == hour_dt]

                        if curr_entry.empty:
                            curr_entry = {"group": "", "datetime": hour_dt}
                        else:
                            curr_entry = {"group": curr_entry['group'].values[0], "datetime": hour_dt}

                        if curr_entry['group'] != curr_group:

                            curr_group = curr_entry['group']

                            block_count += 1

                            blocks["{}".format(block_count)] = {
                                "deptcode": curr_group,
                                "start_dt": hour_dt,
                                "end_dt": hour_dt.replace(minute=59, second=0),
                                "overlap": 0,
                            }

                        else:

                            pos_start_dt = hour_dt

                            pos_end_dt = hour_dt.replace(minute=59, second=0)

                            if pos_start_dt < blocks["{}".format(block_count)]["start_dt"]:
                                blocks["{}".format(block_count)]["start_dt"] = pos_start_dt

                            if pos_end_dt > blocks["{}".format(block_count)]["end_dt"]:
                                blocks["{}".format(block_count)]["end_dt"] = pos_end_dt

                    # Compute the time ranges for each block, based on obtained start/end datetimes
                    for key in blocks.keys():
                        blocks[key]["dt_range"] = pd.date_range(start=blocks[key]['start_dt'], end=blocks[key]['end_dt'],
                                                                freq="min")

                    # Compute the overlaps for each block
                    for key in blocks.keys():

                        overlap_count = 0

                        for dt in exam_dt_range:

                            if dt in blocks[key]["dt_range"]:
                                overlap_count += 1

                        blocks[key]['overlap'] = overlap_count / len(blocks[key]['dt_range'])

                    # Calculate group assignment based on max overlap
                    assignment = []
                    for key in blocks.keys():
                        if not assignment:
                            assignment.append(
                                (
                                    blocks[key]['deptcode'], blocks[key]['start_dt'],
                                    blocks[key]['end_dt'], blocks[key]['overlap']
                                )
                            )
                        else:
                            if blocks[key]['overlap'] > assignment[-1][-1]:
                                assignment[-1] = (
                                    blocks[key]['deptcode'], blocks[key]['start_dt'],
                                    blocks[key]['end_dt'], blocks[key]['overlap']
                                )
                            elif blocks[key]['overlap'] == assignment[-1][-1]:
                                assignment.append(
                                    (
                                        blocks[key]['deptcode'], blocks[key]['start_dt'],
                                        blocks[key]['end_dt'], blocks[key]['overlap']
                                    )
                                )

                    if len(assignment) > 1:

                        research_codes = {}

                        non_research_codes = ["", "DEV", "UNAVAIL", "GE", "SIEM", "test", "training", "post"]

                        for a in assignment:

                            if a[0] not in non_research_codes:

                                if a[0] not in research_codes.keys():
                                    research_codes[a[0]] = {
                                        'count': 1,
                                        'overlap': a[-1]
                                    }
                                else:
                                    research_codes[a[0]]['count'] += 1

                        if research_codes:

                            max_count = max([v['count'] for v in research_codes.values()])

                            final_assignment = [key for (key, value) in research_codes.items() if
                                                value['count'] == max_count]

                            if len(final_assignment) == 1:

                                curr_key = final_assignment[0]

                                outfile.write("Exam pk {} ({}) assigned to deptcode: '{}', overlap: {}\n".format(
                                    exam.pk, exam.filepath, curr_key, research_codes[curr_key]['overlap']))

                                if options['debug']:

                                    for key, block in blocks.items():
                                        block.pop('dt_range', None)

                                    outfile.write("Min Exam Timestamp: {}\n".format(min_exam_actual_dt))
                                    outfile.write("Max Exam Timestamp: {}\n".format(max_exam_actual_dt))

                                    outfile.write("{}\n".format(
                                        json.dumps(blocks, indent=4,
                                                   default=lambda x: x.__str__() if isinstance(x, datetime) else x)))

                                continue

                        outfile.write("WARNING: Unable to assign exam pk {} ({}) to group - several matching "
                                      "times overlap percentages with scheduler: \n".format(exam.pk, exam.filepath))

                        for key, block in blocks.items():
                            block.pop('dt_range', None)

                        outfile.write(json.dumps(blocks, indent=4,
                                      default=lambda x: x.__str__() if isinstance(x, datetime) else x))

                    elif len(assignment) == 1:

                        outfile.write("Exam pk {} ({}) assigned to deptcode: '{}', overlap: {}\n".format(
                            exam.pk, exam.filepath, assignment[0][0], assignment[0][-1]))

                        if options['debug']:

                            for key, block in blocks.items():
                                block.pop('dt_range', None)

                            outfile.write("Min Exam Timestamp: {}\n".format(min_exam_actual_dt))
                            outfile.write("Max Exam Timestamp: {}\n".format(max_exam_actual_dt))

                            outfile.write("{}\n".format(json.dumps(blocks, indent=4,
                                                        default=lambda x: x.__str__() if isinstance(x, datetime) else x)))

                    else:

                        outfile.write("ERROR: No assignments found for exam pk {} ({})\n".format(exam.pk, exam.filepath))

                        for key, block in blocks.items():
                            block.pop('dt_range', None)

                        outfile.write("{}\n".format(
                            json.dumps(blocks, indent=4, default=lambda x: x.__str__() if isinstance(x, datetime) else x)))

                    outfile.flush()
