from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0020_warehouseaccountoverview_raw_values"),
    ]

    operations = [
        migrations.AddField(
            model_name="warehouseaccountoverview",
            name="pods",
            field=models.PositiveIntegerField(blank=True, default=0, null=True),
        ),
        migrations.AddField(
            model_name="warehouseaccountoverview",
            name="pods_raw",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
