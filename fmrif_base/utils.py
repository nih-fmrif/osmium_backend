from ldap3 import Connection, Server, ALL
from ldap3.core.exceptions import LDAPCursorError
from django.conf import settings


def get_ldap_credentials(employee_id):
    # Open LDAP connection
    server = Server(settings.LDAP_SERVER, use_ssl=True, get_info=ALL)
    conn = Connection(server, settings.FMRIF_SVC_ACCT, settings.FMRIF_SVC_ACCT_PWD, auto_bind=True)
    conn.start_tls()

    # Get the list of members with access to Gold
    members = None
    if conn.search(settings.AD_GROUPS['gold_users'], '(member=*)', attributes=['member']):
        members = conn.entries[0].member.value

    # Find the relevant metadata for the employee id of interest
    for member in members:

        if conn.search(member, "(employeeid=*)",
                       attributes=['employeeID', 'sn', 'givenName', 'userPrincipalName', 'cn', 'mail']):

            results = conn.entries[0]

            if results.employeeID.value == employee_id:

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

                return member_data

    return {}
