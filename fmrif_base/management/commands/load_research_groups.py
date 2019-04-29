from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from fmrif_base.models import Institute, ResearchGroup
from ldap3 import Connection, Server, ALL
from ldap3.core.exceptions import LDAPCursorError
from django.contrib.auth import get_user_model
from django.conf import settings


User = get_user_model()


class Command(BaseCommand):

    def get_ldap_credentials(self, cn, institute):
        # Open LDAP connection
        server = Server(settings.LDAP_SERVER, use_ssl=True, get_info=ALL)
        conn = Connection(server, settings.FMRIF_SVC_ACCT, settings.FMRIF_SVC_ACCT_PWD, auto_bind=True)
        conn.start_tls()

        # Check the different user group lists to try to find PIs accounts in case they are listed as owning a group
        # but don't have current access through the Gold users list (e.g. NIMH Director Reserve)

        self.stdout.write("Checking for user {} in {}'s user list...".format(cn, institute))

        if conn.search(settings.AD_GROUPS['nih_users'].format(institute), '(CN={})'.format(cn),
                       attributes=['employeeID', 'sn', 'givenName', 'userPrincipalName', 'cn', 'mail']):

            member = conn.entries[0] if conn.entries else None

            if member:

                try:
                    employee_id = member.employeeID.value
                except LDAPCursorError:
                    employee_id = None

                try:
                    last_name = member.sn.value
                except LDAPCursorError:
                    last_name = None
                try:
                    first_name = member.givenName.value
                except LDAPCursorError:
                    first_name = None
                try:
                    user_principal_name = member.userPrincipalName.value
                except LDAPCursorError:
                    user_principal_name = None
                try:
                    username = member.cn.value
                except LDAPCursorError:
                    username = None
                try:
                    mail = member.mail.value
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

                self.stdout.write("Found user {}!".format(cn))

                return member_data

        self.stdout.write("User {} not found on {}'s user list...".format(cn, institute))
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

                if parent_institute:

                    try:
                        institute = Institute.objects.get(short_name=parent_institute)
                    except Institute.DoesNotExist:
                        institute = None
                else:
                    institute = None

                if pi_cn:

                    try:

                        pi = User.objects.get(username=pi_cn)

                    except User.DoesNotExist:

                        pi_data = None

                        if parent_institute:

                            pi_data = self.get_ldap_credentials(pi_cn, parent_institute)

                        if not pi_data:

                            if not parent_institute:

                                self.stdout.write("User {} does not have a listed institute, "
                                                  "checking on all the institutes...".format(pi_cn,))

                            else:
                                self.stdout.write("Could not find PI info for user {} in their listed institute ({}), "
                                                  "trying in other institutes...".format(pi_cn, parent_institute))

                            institutes = Institute.objects.all().values_list('short_name', flat=True)

                            for curr_institute in institutes:

                                if curr_institute == parent_institute:
                                    continue  # Dont check twice

                                pi_data = self.get_ldap_credentials(pi_cn, parent_institute)

                                if pi_data:
                                    break

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

                ResearchGroup.objects.create(
                    code=code if code else None,
                    name=name.strip('"') if name else old_name.strip('"'),
                    old_name=old_name.strip('"') if name else None,
                    short_name=short_name if short_name else None,
                    principal_investigator=pi,
                    parent_institute=institute,
                    url=url if url else None,
                    is_active=is_active if is_active is True else False
                )
