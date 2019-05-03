from rest_framework.views import APIView
from rest_framework.response import Response
from django.views.decorators.csrf import ensure_csrf_cookie
from django.utils.decorators import method_decorator
from django.middleware.csrf import rotate_token
from fmrif_base.permissions import HasActiveAccount
from fmrif_base.serializers import FMRIFUserSerializer


class FMRIFUserView(APIView):

    permission_classes = (HasActiveAccount,)

    @method_decorator(ensure_csrf_cookie)
    def get(self, request):

        serializer = FMRIFUserSerializer(request.user)
        return Response(serializer.data)
