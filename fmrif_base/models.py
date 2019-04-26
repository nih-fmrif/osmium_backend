from django.db import models
from django.conf import settings
from django.contrib.auth.models import (
    Group,
    AbstractBaseUser,
    PermissionsMixin,
    BaseUserManager,
)


class FMRIFUserManager(BaseUserManager):

    def create_user(
        self,
        employee_id,
        username,
        last_name,
        first_name,
        user_principal_name=None,
        mail=None,
        password=None,
    ):

        if not employee_id:
            raise ValueError('Users must have a valid employee id')

        if not username:
            raise ValueError('Users must have valid username')

        if not last_name:
            raise ValueError('Users must have a last name')

        if not first_name:
            raise ValueError('Users must have a first name')

        if user_principal_name:
            user_principal_name = self.normalize_email(user_principal_name)

        if mail:
            mail = self.normalize_email(mail)

        user = self.model(
            employee_id=employee_id,
            username=username,
            last_name=last_name,
            first_name=first_name,
            user_principal_name=user_principal_name,
            mail=mail,
        )

        user.set_password(password)

        user.is_active = True
        user.is_staff = False
        user.is_superuser = False

        user.save(using=self._db)

        return user

    def create_superuser(
        self,
        employee_id,
        username,
        last_name,
        first_name,
        user_principal_name=None,
        mail=None,
        password=None,
    ):

        if not employee_id:
            raise ValueError('Users must have a valid employee id')

        if not username:
            raise ValueError('Users must have a username')

        if not first_name:
            raise ValueError('Users must have a first name')

        if not last_name:
            raise ValueError('Users must have a last name')

        if not password:
            raise ValueError("Must provide a valid password")

        user = self.create_user(
            employee_id=employee_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            user_principal_name=user_principal_name,
            mail=mail,
            password=password
        )

        user.is_active = True
        user.is_staff = True
        user.is_superuser = True
        user.save(using=self._db)

        return user


class FMRIFUser(AbstractBaseUser, PermissionsMixin):
    """General user profile generated from NIH AUTH data"""

    # Mandatory fields
    employee_id = models.CharField(max_length=10, primary_key=True)
    last_name = models.CharField("Last Name", max_length=255)
    first_name = models.CharField("First Name", max_length=255)
    user_principal_name = models.CharField("UPN", max_length=255, null=True)
    username = models.CharField("username", max_length=255)
    mail = models.EmailField("email", max_length=255)

    is_active = models.BooleanField(default=False)  # Active account
    is_staff = models.BooleanField(default=False)  # Can access admin site
    is_superuser = models.BooleanField(default=False)  # Has all permissions by default

    objects = FMRIFUserManager()

    USERNAME_FIELD = 'employee_id'
    REQUIRED_FIELDS = [
        'last_name',
        'first_name',
        'username',
    ]

    def __str__(self):
        """Returns the username of the user as the object representation for this class"""
        return "<{}: {}>".format(self.username, self.mail)

    def get_full_name(self):
        """Returns the full name of the user"""
        return ' '.join([self.first_name, self.last_name])

    def get_short_name(self):
        """Returns the first name of the user"""
        return self.first_name


class Protocol(models.Model):

    protocol_id = models.CharField(max_length=10, primary_key=True)
    name = models.CharField(max_length=255)
    principal_investigator = models.ForeignKey(settings.AUTH_USER_MODEL,
                                               related_name='protocols_pi',
                                               on_delete=models.PROTECT,
                                               null=True)
    research_group = models.ForeignKey('ResearchGroup', related_name='protocols', on_delete=models.PROTECT)
    is_active = models.BooleanField(default=True)


class InstituteManager(models.Manager):
    def get_by_natural_key(self, short_name):
        return self.get(short_name=short_name)


class Institute(models.Model):

    objects = InstituteManager()

    short_name = models.CharField(max_length=10, primary_key=True)
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)


class ResearchGroup(Group):

    # 'permissions' ManyToMany field to Permissions model is inherited
    # 'name' CharField (max_length=150) inherited

    code = models.CharField(max_length=15, primary_key=True)
    # Old name if there was a change when moving to new archive
    old_name = models.CharField(max_length=255, blank=True, null=True)
    short_name = models.CharField(max_length=255)
    principal_investigator = models.ForeignKey(settings.AUTH_USER_MODEL,
                                               on_delete=models.PROTECT,
                                               related_name='groups_led',
                                               null=True)
    parent_institute = models.ForeignKey(Institute,
                                         on_delete=models.PROTECT,
                                         related_name='research_groups',
                                         null=True)
    url = models.URLField(max_length=300, blank=True, null=True)
    members = models.ManyToManyField(settings.AUTH_USER_MODEL, related_name='group_memberships')
    is_active = models.BooleanField(default=True)
