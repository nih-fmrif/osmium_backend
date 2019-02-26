from django.contrib.auth import get_user_model
from django.db import Error
from rest_framework import authentication
from rest_framework import exceptions
from fmrif_base import siteminder_headers
from fmrif_base.utils import get_ldap_credentials

User = get_user_model()


class FMRIFAuthentication(authentication.BaseAuthentication):

    def authenticate(self, request):

        try:
            employee_id = request.META[siteminder_headers.EMPLOYEE_ID]
        except KeyError:
            raise exceptions.AuthenticationFailed("Unable to retrieve Siteminder credentials")

        try:

            user = User.objects.get(employee_id=employee_id)

        except User.DoesNotExist:

            # User not in the Osmium database, check if they have credentials on the LDAP access list
            # for Gold, and if so, create an account
            user_data = get_ldap_credentials(employee_id=employee_id)

            if user_data:

                try:

                    user = User.objects.create_user(
                        employee_id=user_data['employee_id'],
                        last_name=user_data['last_name'],
                        first_name=user_data['first_name'],
                        user_principal_name=user_data['user_principal_name'],
                        username=user_data['username'],
                        mail=user_data['mail']
                    )

                except Error:

                    raise exceptions.AuthenticationFailed("Unable to create new user entry")

            else:

                return None

        return user, None
