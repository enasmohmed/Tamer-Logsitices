import pandas as pd
from datetime import datetime
from django.contrib import admin
from django.shortcuts import render, redirect
from django.urls import path
from django.contrib import messages
from django.utils import timezone
from .models import MeetingPoint, InboundShipmentRemark, WarehouseAccountOverview, CapacityVolume


class WarehouseDayFilter(admin.SimpleListFilter):
    title = "Day"
    parameter_name = "day"

    def lookups(self, request, model_admin):
        return [
            ("today", "Today"),
            ("yesterday", "Yesterday"),
            ("day_before_yesterday", "Day Before Yesterday"),
        ]

    def queryset(self, request, queryset):
        value = self.value()
        tz_today = timezone.now().date()
        if value == "today" or value is None:
            # الافتراضي: اليوم
            return queryset.filter(created_at__date=tz_today)
        if value == "yesterday":
            return queryset.filter(created_at__date=tz_today - timezone.timedelta(days=1))
        if value == "day_before_yesterday":
            return queryset.filter(created_at__date=tz_today - timezone.timedelta(days=2))
        return queryset


@admin.register(WarehouseAccountOverview)
class WarehouseAccountOverviewAdmin(admin.ModelAdmin):
    list_display = (
        "warehouse",
        "account",
        "capacity",
        "clearance",
        "inbound",
        "outbound",
        "transportation",
        "occupied_location",
        "updated_at",
    )
    list_editable = ("capacity", "inbound", "outbound", "clearance", "occupied_location", "transportation")
    list_filter = ("warehouse", WarehouseDayFilter, "created_at")
    search_fields = ("warehouse", "account")
    ordering = ("warehouse", "account")
    change_list_template = "admin/dashboard/warehouseaccountoverview/change_list.html"

    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path("import-excel/", self.admin_site.admin_view(self.import_excel_view), name="dashboard_warehouseaccountoverview_import"),
        ]
        return custom + urls

    def import_excel_view(self, request):
        from django.http import HttpResponse
        if request.method == "POST" and request.FILES.get("excel_file"):
            f = request.FILES["excel_file"]
            if not f.name.lower().endswith((".xlsx", ".xls")):
                messages.error(request, "يرجى رفع ملف Excel (.xlsx أو .xls) فقط.")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            def safe_int(val, default=0):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return default
                if isinstance(val, str) and val.strip() in ("", "-", "—", "nan", "NaN"):
                    return default
                try:
                    return int(float(val))
                except (ValueError, TypeError):
                    return default

            # تاريخ البيانات (حتى يمكن رفع ملف لليوم أو الأمس أو أي تاريخ قديم)
            effective_date_str = (request.POST.get("effective_date") or "").strip()
            try:
                if effective_date_str:
                    # تحويل للتاريخ من الفورم
                    effective_date = datetime.strptime(effective_date_str, "%Y-%m-%d").date()
                else:
                    effective_date = timezone.now().date()
            except Exception:
                effective_date = timezone.now().date()

            try:
                xl = pd.ExcelFile(f, engine="openpyxl")
                sheet_names = xl.sheet_names
            except Exception as e:
                messages.error(request, f"تعذر قراءة الملف: {e}")
                return redirect("admin:dashboard_warehouseaccountoverview_import")

            # 1) استيراد شيت Da-tamer (أو Sheet1) → WarehouseAccountOverview
            sheet_name = request.POST.get("sheet_name", "").strip() or None
            if not sheet_name:
                # التعرف على اسم الشيت بدون حساسية لحالة الأحرف (Da-tamer, Da-Tamer, DA-TAMER, Sheet1)
                sheet_lower = {s.lower(): s for s in sheet_names}
                if "da-tamer" in sheet_lower:
                    sheet_name = sheet_lower["da-tamer"]
                elif "sheet1" in sheet_lower:
                    sheet_name = sheet_lower["sheet1"]
                else:
                    sheet_name = sheet_names[0] if sheet_names else None
            if not sheet_name or sheet_name not in sheet_names:
                messages.error(request, f"الشيت «{sheet_name or '(غير محدد)'}» غير موجود. الشيتات المتوفرة: {', '.join(sheet_names)}")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            df = pd.read_excel(xl, sheet_name=sheet_name)
            if df.empty or len(df) < 1:
                messages.error(request, "الشفرة المحددة فارغة أو لا تحتوي على بيانات.")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            df.columns = [str(c).strip() for c in df.columns]
            # توحيد أسماء الأعمدة: إزالة مسافات زائدة وأخذ أول تطابق (عمود Transportaion يطابق transportation)
            col_map = {}
            for c in df.columns:
                c_lower = c.lower().strip().replace(" ", "_").replace("-", "_")
                if (not col_map.get("warehouse") and (
                    c_lower == "warehouse" or c_lower == "whs" or "warehouse" in c_lower or c_lower in ("whs", "wh")
                )):
                    col_map["warehouse"] = c
                elif c_lower == "account" or (not col_map.get("account") and "account" in c_lower):
                    col_map["account"] = c
                elif (not col_map.get("inbound") and "inbound" in c_lower):
                    col_map["inbound"] = c
                elif (not col_map.get("outbound") and "outbound" in c_lower):
                    col_map["outbound"] = c
                elif (not col_map.get("clearance") and (
                    "clearance" in c_lower or "clear" in c_lower or c_lower == "cleamce"
                )):
                    col_map["clearance"] = c
                elif (not col_map.get("capacity") and "capacity" in c_lower):
                    col_map["capacity"] = c
                elif (not col_map.get("occupied_location") and (
                    "occupied" in c_lower or "location" in c_lower or "occupled" in c_lower
                )):
                    col_map["occupied_location"] = c
                elif (not col_map.get("transportation") and ("transport" in c_lower or c_lower == "transportaion")):
                    col_map["transportation"] = c
            if "warehouse" not in col_map or "account" not in col_map:
                messages.error(request, "يجب أن يحتوي الشيت على عمودين على الأقل: Warehouse (أو WHs) و Account.")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            wh_col = col_map["warehouse"]
            if wh_col in df.columns:
                df[wh_col] = df[wh_col].replace("", None).ffill().fillna("")
            # في شيت Da-tamer عمود Capacity غالباً مدمج (merged) مع المستودع — نملأ القيمة للأسفل مثل Warehouse
            cap_col = col_map.get("capacity")
            if cap_col and cap_col in df.columns:
                df[cap_col] = df[cap_col].replace("", None).replace("-", None).ffill().fillna(0)
            created = 0
            # نحذف بيانات نفس التاريخ فقط، وليس كل الداتا، حتى نستطيع الاحتفاظ بأيام سابقة
            WarehouseAccountOverview.objects.filter(created_at__date=effective_date).delete()

            # نضبط created_at لكل صف على تاريخ الفورم (مع توقيت بداية اليوم في التايمزون الحالي)
            tz = timezone.get_current_timezone()
            effective_datetime = datetime.combine(effective_date, datetime.min.time()).replace(tzinfo=tz)
            for _, row in df.iterrows():
                w = str(row.get(col_map["warehouse"], "") or "").strip()
                a = str(row.get(col_map["account"], "") or "").strip()
                if not w and not a:
                    continue
                # تخطي صف الهيدر إذا ظهر كصف بيانات (مثلاً عمود Account = "Account")
                if a.lower() == "account" or w.lower() in ("whs", "warehouse", "account"):
                    continue
                WarehouseAccountOverview.objects.create(
                    warehouse=w or "—",
                    account=a or "—",
                    capacity=safe_int(row.get(col_map.get("capacity"))),
                    clearance=safe_int(row.get(col_map.get("clearance"))),
                    inbound=safe_int(row.get(col_map.get("inbound"))),
                    outbound=safe_int(row.get(col_map.get("outbound"))),
                    transportation=safe_int(row.get(col_map.get("transportation"))),
                    occupied_location=safe_int(row.get(col_map.get("occupied_location"))),
                    created_at=effective_datetime,
                )
                created += 1
            msg_parts = [f"تم استيراد {created} صف من الشيت «{sheet_name}» (جدول Warehouse & Account)."]

            # 2) استيراد شيت Capacity-volume → CapacityVolume (إن وُجد)
            cap_sheet = "Capacity-volume"
            if cap_sheet in sheet_names:
                df_cap = pd.read_excel(xl, sheet_name=cap_sheet)
                df_cap.columns = [str(c).strip() for c in df_cap.columns]
                wh_col_cap = None
                cap_col = None
                for c in df_cap.columns:
                    c_lower = c.lower().strip().replace(" ", "_").replace("-", "_")
                    if wh_col_cap is None and (c_lower in ("warehouse", "whs", "wh") or "warehouse" in c_lower):
                        wh_col_cap = c
                    if cap_col is None and "capacity" in c_lower:
                        cap_col = c
                if wh_col_cap is not None and cap_col is not None:
                    CapacityVolume.objects.all().delete()
                    cap_created = 0
                    for _, row in df_cap.iterrows():
                        w = str(row.get(wh_col_cap, "") or "").strip()
                        if not w:
                            continue
                        cap_val = safe_int(row.get(cap_col))
                        CapacityVolume.objects.create(warehouse=w, capacity=cap_val)
                        cap_created += 1
                    msg_parts.append(f"تم استيراد {cap_created} صف من الشيت «{cap_sheet}» (السعة).")
                else:
                    msg_parts.append(f"الشيت «{cap_sheet}» موجود لكن لم يُعثر على أعمدة Warehouse و Capacity.")

            messages.success(request, " ".join(msg_parts))
            return redirect("admin:dashboard_warehouseaccountoverview_changelist")
        context = {
            "title": "استيراد من Excel — Warehouse and Account Overview",
            "opts": self.model._meta,
            "default_effective_date": timezone.now().date().isoformat(),
        }
        return render(request, "admin/dashboard/warehouseaccountoverview/import_excel.html", context)


@admin.register(CapacityVolume)
class CapacityVolumeAdmin(admin.ModelAdmin):
    list_display = ("warehouse", "capacity", "updated_at")
    list_editable = ("capacity",)
    search_fields = ("warehouse",)
    ordering = ("warehouse",)


@admin.register(InboundShipmentRemark)
class InboundShipmentRemarkAdmin(admin.ModelAdmin):
    list_display = ("shipment_nbr", "facility", "remark_short", "updated_at")
    list_editable = ()
    list_filter = ("facility",)
    search_fields = ("shipment_nbr", "facility", "remark")
    ordering = ("-updated_at",)

    def remark_short(self, obj):
        return (obj.remark[:50] + "…") if obj.remark and len(obj.remark) > 50 else (obj.remark or "")

    remark_short.short_description = "Remark"


@admin.register(MeetingPoint)
class MeetingPointAdmin(admin.ModelAdmin):
    list_display = ("description", "is_done", "created_at", "target_date")
    list_editable = ("is_done", "target_date",)
    list_filter = ("is_done", "created_at", "target_date")
    search_fields = ("description",)
    ordering = ("-created_at", "target_date", "assigned_to")

    # ✅ السماح بتعديل created_at من صفحة التفاصيل
    fields = ("description", "is_done", "created_at", "target_date", "assigned_to")
