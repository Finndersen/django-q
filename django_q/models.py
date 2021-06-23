# Django
from django import get_version
from django.core.exceptions import ValidationError
from django.db import models
from django.template.defaultfilters import truncatechars
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _

# External
from picklefield import PickledObjectField
from picklefield.fields import dbsafe_decode

# Local
from django_q.conf import croniter
from django_q.signing import SignedPackage
from django_q.choices import Choices


class TaskManager(models.Manager):
    """
    Custom manager for Tasks
    """

    def get_task(self, task_id):
        """
        Get Task by ID or name
        """
        if len(task_id) == 32 and self.get_queryset().filter(id=task_id).exists():
            return self.get_queryset().get(id=task_id)
        elif self.get_queryset().filter(name=task_id).exists():
            return self.get_queryset().get(name=task_id)

    def get_result(self, task_id):
        """
        Get Task result by ID or name
        """
        return self.get_task(task_id).result

    def get_group_results(self, group_id, failures=False):
        qs = self.get_group_tasks(group_id, failures=failures).values_list("result", flat=True)
        return decode_results(qs)

    def get_group_tasks(self, group_id, failures=True):
        qs = self.get_queryset().filter(group=group_id)
        if failures:
            qs = qs.filter(success=False)
        return qs

    def get_group_count(self, group_id, failures=False):
        return self.get_group_tasks(group_id, failures=failures).count()

    def delete_group(self, group_id, tasks=False):
        group_qs = self.get_group_tasks(group_id)
        if tasks:
            group_qs.delete()
        else:
            group_qs.update(group=None)


class Task(models.Model):
    STATUS_CHOICES = Choices(
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('success', 'Success'),
        ('failed', 'Failed')
    )

    id = models.CharField(max_length=32, primary_key=True, editable=False)
    name = models.CharField(max_length=100, editable=False) #TODO Is this needed..?
    func = models.CharField(max_length=256, help_text='Reference to task function')
    hook = models.CharField(max_length=256, null=True,
                            help_text='Function to call after task completes (passed Task instance)')
    args = PickledObjectField(null=True, protocol=-1, help_text='Positional arguments provided to function')
    kwargs = PickledObjectField(null=True, protocol=-1, help_text='Keyword arguments provided to function')
    result = PickledObjectField(null=True, protocol=-1, help_text="Return value of task function")
    group = models.CharField(max_length=100, editable=False, null=True,
                             help_text="Task group, so results for related tasks can be grouped together")
    cluster_type = models.CharField(max_length=80, null=True,
                                    help_text='Allows specifying which clusters this task can run on')
    created_time = models.DateTimeField(editable=False, help_text='Time task was first created')
    start_time = models.DateTimeField(editable=False, help_text='Start time of most recent task execution')
    duration = models.IntegerField(editable=False, help_text='Duration of most recent task execution')
    status = models.CharField(choices=STATUS_CHOICES, default=STATUS_CHOICES.pending, editable=False)
    attempt_count = models.IntegerField(default=0)

    objects = TaskManager()

    def group_result(self, failures=False):
        if self.group:
            return self.objects.get_group_results(self.group, failures)

    def group_count(self, failures=False):
        if self.group:
            return self.objects.get_group_count(self.group, failures)

    def time_taken(self):
        return (self.end_time - self.start_time).total_seconds()

    @property
    def short_result(self):
        return truncatechars(self.result, 100)

    def __str__(self):
        return f"{self.name or self.id}"

    class Meta:
        app_label = "django_q"
        ordering = ["-stopped"]


class SuccessManager(TaskManager):
    def get_queryset(self):
        return super(SuccessManager, self).get_queryset().filter(success=True)


class Success(Task):
    objects = SuccessManager()

    class Meta:
        app_label = "django_q"
        verbose_name = _("Successful task")
        verbose_name_plural = _("Successful tasks")
        ordering = ["-stopped"]
        proxy = True


class FailureManager(TaskManager):
    def get_queryset(self):
        return super(FailureManager, self).get_queryset().filter(success=False)


class Failure(Task):
    objects = FailureManager()

    class Meta:
        app_label = "django_q"
        verbose_name = _("Failed task")
        verbose_name_plural = _("Failed tasks")
        ordering = ["-stopped"]
        proxy = True


# Optional Cron validator
def validate_cron(value):
    if not croniter:
        raise ImportError(_("Please install croniter to enable cron expressions"))
    try:
        croniter.expand(value)
    except ValueError as e:
        raise ValidationError(e)


class Schedule(models.Model):
    name = models.CharField(max_length=100, null=True, blank=True)
    func = models.CharField(max_length=256, help_text="e.g. module.tasks.function")
    hook = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="e.g. module.tasks.result_function",
    )
    args = models.TextField(null=True, blank=True, help_text=_("e.g. 1, 2, 'John'"))
    kwargs = models.TextField(
        null=True, blank=True, help_text=_("e.g. x=1, y=2, name='John'")
    )
    ONCE = "O"
    MINUTES = "I"
    HOURLY = "H"
    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    YEARLY = "Y"
    CRON = "C"
    TYPE = (
        (ONCE, _("Once")),
        (MINUTES, _("Minutes")),
        (HOURLY, _("Hourly")),
        (DAILY, _("Daily")),
        (WEEKLY, _("Weekly")),
        (MONTHLY, _("Monthly")),
        (QUARTERLY, _("Quarterly")),
        (YEARLY, _("Yearly")),
        (CRON, _("Cron")),
    )
    schedule_type = models.CharField(
        max_length=1, choices=TYPE, default=TYPE[0][0], verbose_name=_("Schedule Type")
    )
    minutes = models.PositiveSmallIntegerField(
        null=True, blank=True, help_text=_("Number of minutes for the Minutes type")
    )
    repeats = models.IntegerField(
        default=-1, verbose_name=_("Repeats"), help_text=_("n = n times, -1 = forever")
    )
    next_run = models.DateTimeField(
        verbose_name=_("Next Run"), default=timezone.now, null=True
    )
    cron = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        validators=[validate_cron],
        help_text=_("Cron expression"),
    )
    task = models.CharField(max_length=100, null=True, editable=False)

    def success(self):
        if self.task and Task.objects.filter(id=self.task):
            return Task.objects.get(id=self.task).success

    def last_run(self):
        if self.task and Task.objects.filter(id=self.task):
            task = Task.objects.get(id=self.task)
            if task.success:
                url = reverse("admin:django_q_success_change", args=(task.id,))
            else:
                url = reverse("admin:django_q_failure_change", args=(task.id,))
            return format_html(f'<a href="{url}">[{task.name}]</a>')
        return None

    def __str__(self):
        return self.func

    success.boolean = True
    last_run.allow_tags = True

    class Meta:
        app_label = "django_q"
        verbose_name = _("Scheduled task")
        verbose_name_plural = _("Scheduled tasks")
        ordering = ["next_run"]


class OrmQ(models.Model):
    key = models.CharField(max_length=100)
    payload = models.TextField()
    lock = models.DateTimeField(null=True)

    def task(self):
        return SignedPackage.loads(self.payload)

    def func(self):
        return self.task()["func"]

    def task_id(self):
        return self.task()["id"]

    def name(self):
        return self.task()["name"]

    class Meta:
        app_label = "django_q"
        verbose_name = _("Queued task")
        verbose_name_plural = _("Queued tasks")


# Backwards compatibility for Django 1.7
def decode_results(values):
    if get_version().split(".")[1] == "7":
        # decode values in 1.7
        return [dbsafe_decode(v) for v in values]
    return values


class Cluster(models.Model):
    """
    Model representing an active Django-Q Cluster
    """
    id = models.CharField(max_length=36, primary_key=True, editable=False)
    start_time = models.DateTimeField(default=timezone.now)
    heartbeat_time = models.DateTimeField(default=timezone.now)
    hostname = models.CharField(max_length=200)
    pid = models.IntegerField(verbose_name='Process ID')
    cluster_type = models.CharField(max_length=80, null=True)


class Worker(models.Model):
    """
    Model representing an active Django-Q Worker
    """
    id = models.CharField(max_length=36, primary_key=True, editable=False)
    cluster = models.ForeignKey(Cluster, on_delete=models.CASCADE)
    pid = models.IntegerField(verbose_name='Process ID')
    start_time = models.DateTimeField(default=timezone.now)
    task = models.ForeignKey(Task, on_delete=models.SET_NULL, null=True)