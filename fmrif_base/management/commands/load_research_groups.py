from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from fmrif_base.models import Institute, ResearchGroup
from ldap3 import Connection, Server, ALL
from ldap3.core.exceptions import LDAPCursorError
from django.contrib.auth import get_user_model
from django.conf import settings


User = get_user_model()


class Command(BaseCommand):

    def get_ldap_credentials(self, cn):
        # Open LDAP connection
        server = Server(settings.LDAP_SERVER, use_ssl=True, get_info=ALL)
        conn = Connection(server, settings.FMRIF_SVC_ACCT, settings.FMRIF_SVC_ACCT_PWD, auto_bind=True)
        conn.start_tls()

        # Get the list of members with access to Gold
        members = None

        # Check the different user group lists to try to find PIs accounts in case they are listed as owning a group
        # but don't have current access through the Gold users list (e.g. NIMH Director Reserve)

        for ad_group in settings.AD_GROUPS.values():

            self.stdout.write("Checking {}...".format(ad_group))

            if conn.search(ad_group, '(member=*)', attributes=['member']):
                members = conn.entries[0].member.value

            # Find the relevant metadata for the employee id of interest
            for member in members:

                if conn.search(member, "(cn=*)", attributes=['employeeID', 'sn', 'givenName',
                                                             'userPrincipalName', 'cn', 'mail']):

                    results = conn.entries[0]

                    if results.cn.value == cn:

                        try:
                            employee_id = results.employeeID.value
                        except LDAPCursorError:
                            employee_id = None

                        try:
                            last_name = results.sn.value
                        except LDAPCursorError:
                            last_name = None
                        try:
                            first_name = results.givenName.value
                        except LDAPCursorError:
                            first_name = None
                        try:
                            user_principal_name = results.userPrincipalName.value
                        except LDAPCursorError:
                            user_principal_name = None
                        try:
                            username = results.cn.value
                        except LDAPCursorError:
                            username = None
                        try:
                            mail = results.mail.value
                        except LDAPCursorError:
                            mail = None

                        member_data = {
                            'employee_id': employee_id,
                            'username': username,
                            'last_name': last_name,
                            'first_name': first_name,
                            'user_principal_name': user_principal_name,
                            'mail': mail,
                        }

                        self.stdout.write("Found user!")

                        return member_data

        self.stdout.write("User not found on any AD list checked...")
        return None

    def add_arguments(self, parser):

        parser.add_argument("--research_groups", type=str, required=True)

    def handle(self, *args, **options):

        groups_fpath = Path(options['research_groups'])

        with open(groups_fpath, "rt", encoding='iso-8859-1') as infile:

            for line_num, line in enumerate(infile, 1):

                if line_num == 1:
                    continue  # Skip header line

                code, old_name, name, short_name, is_active, \
                    parent_institute, pi_cn, \
                    url = line.rstrip("\n").split("\t")

                if pi_cn:

                    try:

                        pi = User.objects.get(username=pi_cn)

                    except User.DoesNotExist:

                        pi_data = self.get_ldap_credentials(pi_cn)

                        if not pi_data:

                            self.stdout.write("Could not find PI info for {} of research group {} ({}). "
                                              "Not assigning a PI to this group.".format(pi_cn,
                                                                                         name if name else old_name,
                                                                                         code))
                            pi = None

                        else:

                            pi = User.objects.create(
                                employee_id=pi_data.get("employee_id", None),
                                last_name=pi_data.get("last_name", None),
                                first_name=pi_data.get("first_name", None),
                                user_principal_name=pi_data.get("user_principal_name", None),
                                username=pi_data.get("username", None),
                                mail=pi_data.get("mail", None),
                                is_active=True
                            )

                else:
                    pi = None

                if parent_institute:

                    try:
                        institute = Institute.objects.get(short_name=parent_institute)
                    except Institute.DoesNotExist:
                        institute = None
                else:
                    institute = None

                ResearchGroup.objects.create(
                    code=code if code else None,
                    name=name.strip('"') if name else old_name,
                    old_name=old_name.strip('"') if name else None,
                    short_name=short_name if short_name else None,
                    principal_investigator=pi,
                    parent_institute=institute,
                    url=url if url else None,
                    is_active=is_active if is_active is True else False
                )
