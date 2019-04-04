import rapidjson as json
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db import Error as DjangoDBError
from psycopg2 import Error as PgError
from psycopg2 import Warning as PgWarning
from pathlib import Path
from fmrif_archive.models import (
    DICOMValueRepresentation,
    DICOMTag,
)
from datetime import datetime
from fmrif_archive.utils import parse_pn, get_fmrif_scanner


class Command(BaseCommand):

    help = 'Load study and scan metadata obtained from Oxygen/Gold archives'

    def add_arguments(self, parser):

        parser.add_argument("--json_mapping", type=str, required=True)

        parser.add_argument("--json_types", type=str, required=True)

    def handle(self, *args, **options):

        mapping_path = Path(options['json_mapping'])
        types_path = Path(options['json_types'])

        with open(mapping_path, "rt") as mapping_file:
            json_mapping = json.load(mapping_file)

        with open(types_path, "rt") as types_file:
            json_types = json.load(types_file)

        for symbol, attrs in json_types.items():
            new_vr = DICOMValueRepresentation(
                symbol=symbol,
                type=attrs['type'],
                json_type=attrs['json_type']
            )
            new_vr.save()

        for tag, attrs in json_mapping.items():

            if not attrs['vr']:

                new_tag = DICOMTag(
                    tag=tag,
                    name=attrs['name'],
                    keyword=attrs['keyword'],
                    vr=None,
                    is_multival=attrs['multival'],
                    is_retired=attrs['retired'],
                    can_query=attrs['can_query']
                )

                new_tag.save()

            else:

                for symbol in attrs['vr']:

                    vr_instance = DICOMValueRepresentation.objects.get(symbol=symbol)

                    new_tag = DICOMTag(
                        tag=tag,
                        name=attrs['name'],
                        keyword=attrs['keyword'],
                        vr=vr_instance,
                        is_multival=attrs['multival'],
                        is_retired=attrs['retired'],
                        can_query=attrs['can_query']
                    )

                    new_tag.save()
