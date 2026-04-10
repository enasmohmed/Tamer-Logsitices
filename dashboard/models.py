
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone


# Create your models here.


class UploadedFile(models.Model):
    file = models.FileField(upload_to='uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.file.name} ({self.uploaded_at:%Y-%m-%d %H:%M})"




class UploadMonth(models.Model):
    month = models.CharField(max_length=20, unique=True)

    def __str__(self):
        return self.month



class MeetingPoint(models.Model):
    description = models.TextField()  # لازم يكون TextField أو CharField
    is_done = models.BooleanField(default=False)
    created_at = models.DateField(default=timezone.now)
    target_date = models.DateField(null=True, blank=True)
    assigned_to = models.CharField(max_length=255, blank=True, null=True)

    # def save(self, *args, **kwargs):
    #     # لو مفيش تاريخ هدف، حطيه بعد 7 أيام من الإنشاء
    #     if not self.target_date and not self.pk:
    #         from datetime import date
    #         self.target_date = date.today() + timedelta(days=7)
    #     super().save(*args, **kwargs)

    def __str__(self):
        return self.description[:50]


class InboundShipmentRemark(models.Model):
    """ملاحظات/أسباب تُضاف من الأدمن لشحنات Inbound وتظهر في جدول Inbound Shipments Detail"""
    shipment_nbr = models.CharField(max_length=255, db_index=True)
    facility = models.CharField(max_length=255, db_index=True)
    remark = models.TextField(blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Inbound Shipment Remark"
        verbose_name_plural = "Inbound Shipment Remarks"
        unique_together = [["shipment_nbr", "facility"]]

    def __str__(self):
        return f"{self.shipment_nbr} @ {self.facility}"


class ExcelSheetCache(models.Model):
    """كاش بيانات شيت إكسل: يُملأ عند الرفع ويُستخدم لتسريع فتح التابات."""
    sheet_name = models.CharField(max_length=255, unique=True, db_index=True)
    data = models.JSONField(default=list)  # list of dicts (rows)
    source_file_path = models.CharField(max_length=512, blank=True, null=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Excel Sheet Cache"
        verbose_name_plural = "Excel Sheet Caches"

    def __str__(self):
        return f"{self.sheet_name} ({len(self.data)} rows)"


class DashboardDataCache(models.Model):
    """كاش بيانات الداشبورد المُستخرجة من الإكسل: يُملأ عند رفع الملف ويُقرأ من الداتابيز لفتح الداشبورد بسرعة."""
    source_file_path = models.CharField(max_length=512, unique=True, db_index=True)
    data = models.JSONField(default=dict)  # inbound_kpi, pending_shipments, charts, outbound, pods, returns, inventory, warehouse, returns_region
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Dashboard Data Cache"
        verbose_name_plural = "Dashboard Data Caches"

    def __str__(self):
        return f"Dashboard cache ({self.source_file_path})"


class WarehouseAccountOverview(models.Model):
    """
    بيانات تاب Warehouse and Account Overview — مصدرها ملف Da-tamer.xlsx شيت Da-tamer (أو Sheet1).
    الأعمدة: Warehouse, Account, Capacity, Clearance, Inbound, Outbound, Transportation, Occupied Location.
    Utilization % = (Occupied Location / Capacity) * 100 (محسوب عند العرض).
    """
    warehouse = models.CharField(max_length=255, db_index=True)
    account = models.CharField(max_length=255, db_index=True)
    capacity = models.PositiveIntegerField(default=0,  blank=True, null=True)
    capacity_raw = models.CharField(max_length=255, blank=True, null=True)
    clearance = models.PositiveIntegerField(default=0,  blank=True, null=True)
    clearance_raw = models.CharField(max_length=255, blank=True, null=True)
    inbound = models.PositiveIntegerField(default=0,  blank=True, null=True)
    inbound_raw = models.CharField(max_length=255, blank=True, null=True)
    outbound = models.PositiveIntegerField(default=0,  blank=True, null=True)
    outbound_raw = models.CharField(max_length=255, blank=True, null=True)
    transportation = models.PositiveIntegerField(default=0,  blank=True, null=True)
    transportation_raw = models.CharField(max_length=255, blank=True, null=True)
    occupied_location = models.PositiveIntegerField(default=0,  blank=True, null=True)
    occupied_location_raw = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now,  blank=True, null=True)  # قابل للتعديل لإضافة/تعديل تاريخ الداتا
    updated_at = models.DateTimeField(auto_now=True,  blank=True, null=True)

    class Meta:
        verbose_name = "Warehouse & Account Row"
        verbose_name_plural = "Warehouse and Account Overview"
        # نحافظ على ترتيب الصفوف كما تأتي من ملف الإكسل (حسب الإدخال)
        ordering = ("id",)

    def __str__(self):
        return f"{self.warehouse} — {self.account}"


class WarehouseImportLog(models.Model):
    """يسجّل كل استيراد إكسل لشيت Warehouse — الافتراضي في الهوم والأدمن = تاريخ آخر استيراد (effective_date)."""
    effective_date = models.DateField(db_index=True)
    imported_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Warehouse Import Log"
        verbose_name_plural = "Warehouse Import Logs"
        ordering = ("-imported_at",)

    def __str__(self):
        return f"{self.effective_date} @ {self.imported_at}"


class CapacityVolume(models.Model):
    """
    سعة كل مستودع — مصدرها شيت Capacity-volume من ملف Warehouse.xlsx.
    يُجمع Total Capacity من هنا ويُطرح منه مجموع Inbound (Used) لرسم شارت Capacity Used vs Available.
    """
    warehouse = models.CharField(max_length=255, db_index=True)
    capacity = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Capacity Volume (Warehouse)"
        verbose_name_plural = "Capacity Volume (Warehouses)"
        ordering = ("warehouse",)

    def __str__(self):
        return f"{self.warehouse}: {self.capacity}"
