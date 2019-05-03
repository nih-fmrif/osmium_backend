from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from fmrif_base.models import Institute


class Command(BaseCommand):

    def add_arguments(self, parser):

        parser.add_argument("--nih_institutes", type=str, required=True)

    def handle(self, *args, **options):

        institutes_fpath = Path(options['nih_institutes'])

        with open(institutes_fpath, "rt") as infile:

            for line in infile:

                name, short_name, url = line.rstrip("\n").split(";")

                Institute.objects.create(
                    name=name,
                    short_name=short_name,
                    url=url,
                    is_active=True
                )
