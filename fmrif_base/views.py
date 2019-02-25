from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import exceptions
from django.contrib.auth import get_user_model
from fmrif_base import siteminder_headers
from fmrif_base.models import AccessRequest
from django.views.decorators.csrf import ensure_csrf_cookie
from django.utils.decorators import method_decorator
from django.middleware.csrf import rotate_token


User = get_user_model()


class UserAccount(APIView):

    AccountStatus = {
        'active': 'active',
        'in_progress': 'in_progress',
        'inactive': 'inactive',
        'does_not_exist': None
    }

    @method_decorator(ensure_csrf_cookie)
    def get(self, request):

        user_account = {
            'employee_id': None,
            'username': None,
            'last_name': None,
            'first_name': None,
            'user_principal_name': None,
            'mail': None,
            'account_status': None,
        }

        # There is a non-anonymous, authenticated user
        if request.user and request.user.is_authenticated:

            user_account['employee_id'] = request.user['employee_id']
            user_account['last_name'] = request.user['last_name']
            user_account['first_name'] = request.user['first_name']
            user_account['user_principal_name'] = request.user['user_principal_name']
            user_account['username'] = request.user['username']
            user_account['mail'] = request.user['mail']

            if request.user.is_active:

                user_account['account_status'] = self.AccountStatus['active']

            else:

                try:
                    AccessRequest.objects.get(applicant=request.user, application_status='InProgress')
                    user_account['account_status'] = self.AccountStatus['in_progress']
                except AccessRequest.DoesNotExist:
                    user_account['account_status'] = self.AccountStatus['inactive']

        else:

            # Request by a user that has siteminder credentials (at least employee id)
            # but is not in the site DB

            try:
                employee_id = request.META[siteminder_headers.EMPLOYEE_ID]
                username = request.META[siteminder_headers.USERNAME]
                last_name = request.META[siteminder_headers.LAST_NAME]
                first_name = request.META[siteminder_headers.FIRST_NAME]
                user_principal_name = request.META.get(siteminder_headers.USER_PRINCIPAL_NAME, None)
                mail = request.META.get(siteminder_headers.MAIL, None)
            except KeyError:
                raise exceptions.AuthenticationFailed("Siteminder error")

            user_account['employee_id'] = employee_id
            user_account['username'] = username
            user_account['last_name'] = last_name
            user_account['first_name'] = first_name
            user_account['user_principal_name'] = user_principal_name
            user_account['mail'] = mail
            user_account['account_status'] = self.AccountStatus['does_not_exist']

        if request.GET.get('login', None):
            rotate_token(request)

        return Response(user_account)
