import pandas as pd
from datetime import datetime, timedelta, timezone as dt_utc
from django import forms
from django.contrib import admin
from django.shortcuts import render, redirect
from django.urls import path
from django.contrib import messages
from django.utils import timezone
from .models import MeetingPoint, InboundShipmentRemark, WarehouseAccountOverview, CapacityVolume, WarehouseImportLog


def _warehouse_available_dates():
    """تواريخ لها داتا (نفس .dates() اللي بيستخدم UTC في الداتابيز)."""
    return list(
        WarehouseAccountOverview.objects.dates("created_at", "day", order="DESC")
    )


def _warehouse_last_import_date():
    """تاريخ آخر ملف إكسل اترفع (شيت Warehouse) — الافتراضي في الأدمن والهوم."""
    last = WarehouseImportLog.objects.order_by("-imported_at").first()
    return last.effective_date if last else None


def _warehouse_day_range(target_date):
    """نطاق اليوم بـ UTC (للاستيراد والحذف)."""
    start = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=dt_utc.utc)
    return start, start + timedelta(days=1)


class WarehouseDayFilter(admin.SimpleListFilter):
    title = "Day"
    parameter_name = "day"

    def lookups(self, request, model_admin):
        dates = _warehouse_available_dates()
        return [(d.strftime("%Y-%m-%d"), d.strftime("%d %b %Y")) for d in dates]

    def queryset(self, request, queryset):
        value = self.value()
        dates = _warehouse_available_dates()
        dates_set = {d for d in dates}
        last_import = _warehouse_last_import_date()
        last_data_date = (last_import if last_import and last_import in dates_set else None) or (dates[0] if dates else timezone.now().date())
        if value is None:
            target_date = last_data_date
        else:
            try:
                target_date = datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return queryset
        # نفس منطق .dates() حتى يطابق قائمة التواريخ ويظهر الداتا (created_at__date)
        return queryset.filter(created_at__date=target_date)


class ZeroAsNoDataInput(forms.NumberInput):
    """يعرض placeholder 'No Data' عندما القيمة 0 مع إبقاء الحقل قابلاً للتعديل."""
    attrs = {"placeholder": "No Data"}

    def get_context(self, name, value, attrs):
        if value is None or value == 0 or (isinstance(value, str) and value.strip() in ("", "0")):
            value = ""
        attrs = {**(self.attrs or {}), **(attrs or {})}
        attrs.setdefault("placeholder", "No Data")
        return super().get_context(name, value, attrs)


def _clean_zero(v):
    if v is None or v == "" or (isinstance(v, str) and v.strip() == ""):
        return 0
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


class WarehouseAccountOverviewChangelistForm(forms.ModelForm):
    """تحويل القيم الفارغة إلى 0 عند التعديل من جدول الـ changelist."""
    class Meta:
        model = WarehouseAccountOverview
        fields = "__all__"

    def clean_capacity(self):
        return _clean_zero(self.cleaned_data.get("capacity"))

    def clean_clearance(self):
        return _clean_zero(self.cleaned_data.get("clearance"))

    def clean_inbound(self):
        return _clean_zero(self.cleaned_data.get("inbound"))

    def clean_outbound(self):
        return _clean_zero(self.cleaned_data.get("outbound"))

    def clean_transportation(self):
        return _clean_zero(self.cleaned_data.get("transportation"))

    def clean_occupied_location(self):
        return _clean_zero(self.cleaned_data.get("occupied_location"))


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
        "created_at",
    )
    list_editable = ("capacity", "inbound", "outbound", "clearance", "occupied_location", "transportation", "created_at")
    list_filter = ("warehouse", WarehouseDayFilter)  # لا نضيف created_at لتفادي تعارضه مع فلتر Day
    search_fields = ("warehouse", "account")
    ordering = ("warehouse", "account")
    change_list_template = "admin/dashboard/warehouseaccountoverview/change_list.html"
    date_hierarchy = None
    form = WarehouseAccountOverviewChangelistForm

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        if db_field.name in ("capacity", "clearance", "inbound", "outbound", "transportation", "occupied_location"):
            kwargs["widget"] = ZeroAsNoDataInput()
        return super().formfield_for_dbfield(db_field, request, **kwargs)

    def get_changelist_form(self, request, **kwargs):
        form = super().get_changelist_form(request, **kwargs)
        for fname in ("capacity", "clearance", "inbound", "outbound", "transportation", "occupied_location"):
            if fname in form.base_fields:
                form.base_fields[fname].required = False
        return form

    def changelist_view(self, request, extra_context=None):
        # لو warehouse__exact فاضي في الرابط، نوجّه بدونه حتى يظهر كل الشيت لليوم (كل المستودعات)
        if request.GET.get("warehouse__exact") == "":
            from django.http import HttpResponseRedirect
            from urllib.parse import urlencode
            q = request.GET.copy()
            q.pop("warehouse__exact", None)
            return HttpResponseRedirect(request.path + ("?" + q.urlencode() if q else ""))
        dates = _warehouse_available_dates()
        dates_set = {d for d in dates}
        last_import = _warehouse_last_import_date()
        last_data_date = (last_import if last_import and last_import in dates_set else None) or (dates[0] if dates else timezone.now().date())
        day_param = (request.GET.get("day") or "").strip()
        try:
            selected_date = datetime.strptime(day_param, "%Y-%m-%d").date() if day_param else last_data_date
        except ValueError:
            selected_date = last_data_date
        # إعادة توجيه عند عدم وجود day لضبط الفلتر على تاريخ آخر رفع إكسل (أو آخر تاريخ فيه داتا)
        if not day_param and (last_import or dates):
            from django.http import HttpResponseRedirect
            from urllib.parse import urlencode
            q = request.GET.copy()
            q["day"] = last_data_date.strftime("%Y-%m-%d")
            return HttpResponseRedirect(request.path + "?" + q.urlencode())
        # لو التاريخ المختار مش في القائمة نوجّه لآخر رفع (أو آخر تاريخ فيه داتا)
        available_set = {d for d in dates}
        if selected_date not in available_set and (last_import or dates):
            from django.http import HttpResponseRedirect
            from urllib.parse import urlencode
            q = request.GET.copy()
            q["day"] = last_data_date.strftime("%Y-%m-%d")
            return HttpResponseRedirect(request.path + "?" + q.urlencode())
        # قائمة المستودعات للتاريخ المحدد (نفس فلتر created_at__date)
        day_filtered = WarehouseAccountOverview.objects.filter(created_at__date=selected_date)
        warehouse_choices = list(day_filtered.values_list("warehouse", flat=True).distinct().order_by("warehouse"))
        extra_context = extra_context or {}
        extra_context["warehouse_available_dates"] = dates
        extra_context["warehouse_selected_date"] = selected_date
        extra_context["warehouse_choices"] = warehouse_choices
        return super().changelist_view(request, extra_context)

    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path("import-excel/", self.admin_site.admin_view(self.import_excel_view), name="dashboard_warehouseaccountoverview_import"),
            path("delete-day/", self.admin_site.admin_view(self.delete_day_view), name="dashboard_warehouseaccountoverview_delete_day"),
        ]
        return custom + urls

    def delete_day_view(self, request):
        """Delete all data for a specific day after confirmation. Requires delete permission."""
        from django.http import HttpResponse
        from django.template.response import TemplateResponse
        if not request.user.has_perm("dashboard.delete_warehouseaccountoverview"):
            messages.error(request, "You don't have permission to delete Warehouse and Account Overview data.")
            return redirect("admin:dashboard_warehouseaccountoverview_changelist")
        day_param = (request.GET.get("day") or request.POST.get("day") or "").strip()
        if not day_param:
            messages.error(request, "Date not specified.")
            return redirect("admin:dashboard_warehouseaccountoverview_changelist")
        try:
            target_date = datetime.strptime(day_param, "%Y-%m-%d").date()
        except ValueError:
            messages.error(request, "Invalid date format. Please use YYYY-MM-DD.")
            return redirect("admin:dashboard_warehouseaccountoverview_changelist")
        qs = WarehouseAccountOverview.objects.filter(created_at__date=target_date)
        count = qs.count()
        if request.method == "POST" and request.POST.get("confirm") == "yes":
            qs.delete()
            messages.success(request, f"Deleted {count} row(s) for {target_date.strftime('%Y-%m-%d')}.")
            return redirect("admin:dashboard_warehouseaccountoverview_changelist")
        context = {
            "title": "Delete Day Data",
            "opts": self.model._meta,
            "target_date": target_date,
            "day_param": day_param,
            "count": count,
        }
        return TemplateResponse(
            request,
            "admin/dashboard/warehouseaccountoverview/delete_day_confirm.html",
            context,
        )

    def import_excel_view(self, request):
        from django.http import HttpResponse
        if request.method == "POST" and request.FILES.get("excel_file"):
            f = request.FILES["excel_file"]
            if not f.name.lower().endswith((".xlsx", ".xls")):
                messages.error(request, "Please upload an Excel file (.xlsx or .xls) only.")
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
                messages.error(request, f"Could not read file: {e}")
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
                messages.error(request, f"Sheet «{sheet_name or '(not set)'}» not found. Available sheets: {', '.join(sheet_names)}")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            df = pd.read_excel(xl, sheet_name=sheet_name)
            if df.empty or len(df) < 1:
                messages.error(request, "The selected sheet is empty or has no data.")
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
                messages.error(request, "The sheet must contain at least two columns: Warehouse (or WHs) and Account.")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            wh_col = col_map["warehouse"]
            if wh_col in df.columns:
                df[wh_col] = df[wh_col].replace("", None).ffill().fillna("")
            # في شيت Da-tamer عمود Capacity غالباً مدمج (merged) مع المستودع — نملأ القيمة للأسفل مثل Warehouse
            cap_col = col_map.get("capacity")
            if cap_col and cap_col in df.columns:
                df[cap_col] = df[cap_col].replace("", None).replace("-", None).ffill().fillna(0)
            created = 0
            # نحذف بيانات نفس اليوم (UTC) ثم ندرج الجديد بنفس التوقيت حتى يطابق فلتر الأدمن
            start, end = _warehouse_day_range(effective_date)
            WarehouseAccountOverview.objects.filter(created_at__gte=start, created_at__lt=end).delete()

            effective_datetime = datetime.combine(effective_date, datetime.min.time()).replace(tzinfo=dt_utc.utc)
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
            msg_parts = [f"Imported {created} row(s) from sheet «{sheet_name}» (Warehouse & Account table)."]

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
                    msg_parts.append(f"Imported {cap_created} row(s) from sheet «{cap_sheet}» (Capacity).")
                else:
                    msg_parts.append(f"Sheet «{cap_sheet}» exists but Warehouse and Capacity columns were not found.")

            WarehouseImportLog.objects.create(effective_date=effective_date)
            messages.success(request, " ".join(msg_parts))
            return redirect("admin:dashboard_warehouseaccountoverview_changelist")
        context = {
            "title": "Import from Excel — Warehouse and Account Overview",
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
