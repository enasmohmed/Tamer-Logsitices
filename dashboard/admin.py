import pandas as pd
from django.contrib import admin
from django.shortcuts import render, redirect
from django.urls import path
from django.contrib import messages
from .models import MeetingPoint, InboundShipmentRemark, WarehouseAccountOverview, CapacityVolume


@admin.register(WarehouseAccountOverview)
class WarehouseAccountOverviewAdmin(admin.ModelAdmin):
    list_display = (
        "warehouse",
        "account",
        "inbound",
        "outbound",
        "clearance",
        "occupied_location",
        "transportation",
        "updated_at",
    )
    list_editable = ("inbound", "outbound", "clearance", "occupied_location", "transportation")
    list_filter = ("warehouse",)
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
                try:
                    return int(float(val))
                except (ValueError, TypeError):
                    return default

            try:
                xl = pd.ExcelFile(f, engine="openpyxl")
                sheet_names = xl.sheet_names
            except Exception as e:
                messages.error(request, f"تعذر قراءة الملف: {e}")
                return redirect("admin:dashboard_warehouseaccountoverview_import")

            # 1) استيراد شيت Da-tamer (أو Sheet1) → WarehouseAccountOverview
            sheet_name = request.POST.get("sheet_name", "").strip() or None
            if not sheet_name:
                sheet_name = "Da-tamer" if "Da-tamer" in sheet_names else "Sheet1"
            if sheet_name not in sheet_names:
                messages.error(request, f"الشيت «{sheet_name}» غير موجود. الشيتات المتوفرة: {', '.join(sheet_names)}")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            df = pd.read_excel(xl, sheet_name=sheet_name)
            if df.empty or len(df) < 1:
                messages.error(request, "الشفرة المحددة فارغة أو لا تحتوي على بيانات.")
                return redirect("admin:dashboard_warehouseaccountoverview_import")
            df.columns = [str(c).strip() for c in df.columns]
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
            created = 0
            WarehouseAccountOverview.objects.all().delete()
            for _, row in df.iterrows():
                w = str(row.get(col_map["warehouse"], "") or "").strip()
                a = str(row.get(col_map["account"], "") or "").strip()
                if not w and not a:
                    continue
                WarehouseAccountOverview.objects.create(
                    warehouse=w or "—",
                    account=a or "—",
                    inbound=safe_int(row.get(col_map.get("inbound"))),
                    outbound=safe_int(row.get(col_map.get("outbound"))),
                    clearance=safe_int(row.get(col_map.get("clearance"))),
                    occupied_location=safe_int(row.get(col_map.get("occupied_location"))),
                    transportation=safe_int(row.get(col_map.get("transportation"))),
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
