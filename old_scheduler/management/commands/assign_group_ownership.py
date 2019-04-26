from django.db.models import Q
from datetime import datetime, timedelta, date
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from fmrif_archive.models import Exam
from old_scheduler.models import SchedulerScanner, SchedulerDate
from sshtunnel import SSHTunnelForwarder


FMRIF_SCANNERS = [
    'fmrif3ta',
    'fmrif3tb',
    'fmrif3tc',
    'fmrif3td',
    'fmrif7t',
]


OSMIUM_TO_SCHEDULER_SCANNER_MAP = {
    "fmrif3ta": "FMRIF 3T-A",
    "fmrif3tb": "FMRIF 3T-B",
    "fmrif3tc": "FMRIF 3T-C",
    "fmrif3td": "FMRIF 3T-D",
    "fmrif7t": "FMRIF 7T",
}


class Command(BaseCommand):

    def add_arguments(self, parser):

        parser.add_argument(
            "--data_dir",
            help="Path to the Gold archives directory",
            default=settings.ARCHIVE_BASE_PATH
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
            "--ssh_user",
            required=True
        )

        parser.add_argument(
            "--ssh_pwd",
            required=True
        )

    def handle(self, *args, **options):

        with SSHTunnelForwarder(
            settings.OLD_SCHEDULER['SERVER'],
            ssh_username=options['ssh_user'],
            ssh_password=options['ssh_pwd'],
            remote_bind_address=(
                settings.OLD_SCHEDULER['HOST'],
                settings.OLD_SCHEDULER['PORT']
            ),
            local_bind_address=(
                settings.DATABASES['old_scheduler']['HOST'],
                int(settings.DATABASES['old_scheduler']['PORT'])
            )
        ) as server:

            self.stdout.write("SSH tunnel config: ")
            self.stdout.write("Local bind addresses: {}".format(server.local_bind_addresses))
            self.stdout.write("Local bind hosts: {}".format(server.local_bind_hosts))
            self.stdout.write("Local bind ports: {}".format(server.local_bind_ports))
            self.stdout.write("Tunnel bindings: {}".format(server.tunnel_bindings))
            self.stdout.write("Server tunnel is active: {}\n\n".format(server.is_active))

            time_fmt = "%m%d%Y"

            from_date = None

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

            for scanner in options['scanners']:

                # sched_scanner = SchedulerScanner.objects.using(
                #     'old_scheduler').get(scanner=OSMIUM_TO_SCHEDULER_SCANNER_MAP[scanner])

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

                for exam in exams:

                    mr_scans_date_times = exam.mr_scans.values_list('series_date', 'series_time')

                    exam_datetimes = [datetime.combine(dt[0], dt[1]) for dt in mr_scans_date_times]

                    exam_datetimes.append(datetime.combine(exam.study_date, exam.study_time))

                    min_exam_dt = min(exam_datetimes)

                    max_exam_dt = max(exam_datetimes)

                    exam_delta = max_exam_dt - min_exam_dt

                    exam_dt_range = [(min_exam_dt + timedelta(seconds=i)) for i in range(exam_delta.seconds + 1)]

                    self.stdout.write("min_exam_dt: {}".format(min_exam_dt))

                    self.stdout.write("max_exam_dt: {}".format(max_exam_dt))

                    self.stdout.write("exam_dt_range: {}".format(exam_dt_range))

                    schedule_range_days = [(min_exam_dt.date() + timedelta(days=i)) for i in range(exam_delta.days + 1)]

                    self.stdout.write("sched range days: {}".format(schedule_range_days))

                    # scheduler_entries = list(SchedulerDate.objects.using('old_scheduler').filter(
                    #                             Q(scanner=sched_scanner) &
                    #                             Q(date__gte=min_exam_dt.date()) &
                    #                             Q(date__lte=max_exam_dt.date()))
                    #                          )
                    #
                    # self.stdout.write("Sched entries: {}".format(scheduler_entries))

                    # blocks = {}
                    # curr_deptcode = None
                    # block_count = 0
                    #
                    # for sched_date in schedule_range_days:
                    #
                    #     for curr_hour in range(24):
                    #
                    #         # Get schedule entry for this date and house
                    #         curr_entry = None
                    #
                    #         for entry in scheduler_entries:
                    #
                    #             if entry.date == sched_date and entry.hour == curr_hour:
                    #
                    #                 curr_entry = entry
                    #                 break
                    #
                    #         if not curr_entry:
                    #             curr_entry = {"deptcode": "", "scheddate": sched_date, "schedhour": curr_hour}
                    #
                    #         if curr_entry['deptcode'] != curr_deptcode:
                    #
                    #             curr_deptcode = curr_entry['deptcode']
                    #
                    #             block_count += 1
                    #
                    #             blocks["{}".format(block_count)] = {
                    #                 "deptcode": curr_entry["deptcode"],
                    #                 "start_dt": datetime(sched_date.year,
                    #                                      sched_date.month,
                    #                                      sched_date.day,
                    #                                      curr_hour, 0, 0),
                    #                 "end_dt": datetime(sched_date.year,
                    #                                    sched_date.month,
                    #                                    sched_date.day,
                    #                                    curr_hour, 59, 59),
                    #                 "overlap": 0,
                    #             }
                    #
                    #         else:
                    #
                    #             pos_start_dt = datetime(sched_date.year,
                    #                                     sched_date.month,
                    #                                     sched_date.day,
                    #                                     curr_hour, 0, 0)
                    #
                    #             pos_end_dt = datetime(sched_date.year,
                    #                                   sched_date.month,
                    #                                   sched_date.day,
                    #                                   curr_hour, 59, 59)
                    #
                    #             if pos_start_dt < blocks["{}".format(block_count)]["start_dt"]:
                    #                 blocks["{}".format(block_count)]["start_dt"] = pos_start_dt
                    #
                    #             if pos_end_dt > blocks["{}".format(block_count)]["end_dt"]:
                    #                 blocks["{}".format(block_count)]["end_dt"] = pos_end_dt
                    #
                    # # Compute the time ranges for each block, based on obtained start/end datetimes
                    #
                    # for key in blocks.keys():
                    #
                    #     curr_delta = blocks[key]['end_dt'] - blocks[key]['start_dt']
                    #     blocks[key]["dt_range"] = [(blocks[key]['start_dt'] + timedelta(seconds=i)) for i in
                    #                                range(curr_delta.seconds + 1)]
                    #
                    # # Compute the overlaps for each block
                    # for key in blocks.keys():
                    #
                    #     overlap_count = 0
                    #     for dt in exam_dt_range:
                    #
                    #         if dt in blocks[key]["dt_range"]:
                    #             overlap_count += 1
                    #
                    #     blocks[key]['overlap'] = overlap_count / len(blocks[key]['dt_range'])
                    #
                    # # Calculate group assignment based on max overlap
                    # assignment = []
                    # for key in blocks.keys():
                    #     if not assignment:
                    #         assignment.append(
                    #             (
                    #                 blocks[key]['deptcode'], blocks[key]['start_dt'],
                    #                 blocks[key]['end_dt'], blocks[key]['overlap']
                    #             )
                    #         )
                    #     else:
                    #         if blocks[key]['overlap'] > assignment[-1][-1]:
                    #             assignment[-1] = (
                    #                 blocks[key]['deptcode'], blocks[key]['start_dt'],
                    #                 blocks[key]['end_dt'], blocks[key]['overlap']
                    #             )
                    #         elif blocks[key]['overlap'] == assignment[-1][-1]:
                    #             assignment.append(
                    #                 (
                    #                     blocks[key]['deptcode'], blocks[key]['start_dt'],
                    #                     blocks[key]['end_dt'], blocks[key]['overlap']
                    #                 )
                    #             )
                    #
                    # if len(assignment) > 1:
                    #
                    #     self.stdout.write("WARNING: Unable to assign exam to group - several matching"
                    #                       "time overlap percentages with scheduler: ")
                    #     for a in assignment:
                    #         self.stdout.write("deptcode: '{}', start_dt: {}, "
                    #                           "end_dt: {}, overlap %: {}".format(a[0], a[1], a[2], a[3]))
                    #
                    # elif len(assignment) == 1:
                    #
                    #     self.stdout.write("Exam pk {} assigned to deptcode: '{}', overlap: {}".format(
                    #         exam.pk, assignment[0][0], assignment[0][-1]))
                    #
                    # else:
                    #
                    #     self.stdout.write("ERROR: No assignments found for exam pk {}".format(exam.pk))
