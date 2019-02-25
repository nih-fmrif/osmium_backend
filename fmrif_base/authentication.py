from django.contrib.auth import get_user_model
from rest_framework import authentication
from rest_framework import exceptions
from fmrif_base import siteminder_headers

User = get_user_model()


class FMRIFAuthentication(authentication.BaseAuthentication):

    def authenticate(self, request):

        try:
            employee_id = request.META[siteminder_headers.EMPLOYEE_ID]
        except KeyError:
            raise exceptions.AuthenticationFailed("Siteminder error")

        try:
            user = User.objects.get(employee_id=employee_id)
        except User.DoesNotExist:
            return None

        return user, None
