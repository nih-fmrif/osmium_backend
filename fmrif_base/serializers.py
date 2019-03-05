from rest_framework import serializers
from fmrif_base.models import FMRIFUser


class FMRIFUserSerializer(serializers.ModelSerializer):

    class Meta:

        model = FMRIFUser

        fields = (
            "employee_id",
            "username",
            "last_name",
            "first_name",
            "user_principal_name",
            "mail",
            "is_active",
            "is_staff",
            "is_superuser",
        )

        read_only_fields = (
            "employee_id",
            "username",
            "last_name",
            "first_name",
            "user_principal_name",
            "mail",
            "is_active",
            "is_staff",
            "is_superuser",
        )
