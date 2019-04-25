from django.db import models


class SchedulerDate(models.Model):

    schedid = models.PositiveIntegerField(primary_key=True)
    scheddate = models.DateField(null=True)
    schedhour = models.PositiveSmallIntegerField(null=True)
    deptcode = models.ForeignKey('SchedulerGroup', null=True, blank=True, on_delete=models.PROTECT)
    scannercode = models.ForeignKey('SchedulerScanner', null=True, blank=True, on_delete=models.PROTECT)

    class Meta:

        managed = False
        db_table = 'tblsched'


class SchedulerScanner(models.Model):

    scannercode = models.CharField(max_length=5, primary_key=True)
    scanner = models.CharField(max_length=25)

    class Meta:

        managed = False
        db_table = 'tlkpscanner'


class SchedulerGroup(models.Model):

    deptcode = models.CharField(max_length=10, primary_key=True)
    dept = models.CharField(max_length=75, blank=True, null=True)
    dept_short = models.CharField(max_length=20, blank=True, null=True)
    grp = models.CharField(max_length=10, blank=True, null=True)

    class Meta:

        managed = False
        db_table = 'tlkpdept'
