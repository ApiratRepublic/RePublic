# =============================================================================
# - รันบน ArcGIS Pro Python environment (ต้องมี arcpy)
# - Script v3 (ปรับปรุง 2025-10-28)
# - ตรวจสอบความถูกต้องของข้อมูล GIS ใน GDB ตามมาตรฐานที่กำหนด
# =============================================================================

import arcpy
import os
import re
import csv
import datetime
import uuid
from collections import defaultdict
import pandas as pd
from openpyxl import load_workbook

###############################################
#----------------- ที่ตั้งไฟล์
###############################################
ROOT_DIR = r"D:\A02-Projects\Clinix\Test_GDB_GPT"  # ที่รวมไฟล์ GDB
REPORT_ROOT = r"D:\A02-Projects\Clinix\Report"  # ที่เก็บรายงานผล
OVERLAP_ROOT = r"D:\A02-Projects\Clinix\Overlaping"  # ที่เก็บไฟล์ผลการตรวจสอบทับซ้อน
SUMMARY_EXCEL_PATH = r"D:\A02-Projects\Clinix\Report\Summary_Report.xlsx"
# --------------------------------------------
#   จัดการค่าต่าง ๆ รวมทั้งฟังก์ชัน ตัวแปร ที่ใช้ร่วมกัน
# --------------------------------------------
NUMERIC_TYPES = {"SmallInteger", "Integer", "Single", "Double", "Float", "DoubleFloat", "SingleFloat", "OID"}


ROAD_LAND_USE_DOMAIN = {
    "พาณิชยกรรม", "อุตสาหกรรม", "พาณิชยกรรมและที่อยู่อาศัย", "ที่อยู่อาศัย",
    "ที่อยู่อาศัยและเกษตรกรรม", "ส่วนราชการ", "เกษตรกรรม", "พื้นที่ป่าสงวน", "พื้นที่อุทยาน"
}
ROAD_STREET_TYPE_DOMAIN = {
    "คอนกรีต", "ลาดยาง", "หินคลุก", "ลูกรัง", "ดิน", "น้ำ", "ไม้", "ทางไม่มีสภาพ"
}
ROAD_REQ_NAME_TD_CODES = {1, 2, 3, 4, 5, 6, 8}
REL_TABLE_NO_DOMAIN = {1, 2, 3, 41, 42, 5, 6, 7}
REL_SUB_TABLE_NO_RANGE = range(0, 7) # 0-6


def write_error_report(error_list, gdb_path, fc_name, check_type, oid, field, value, message):
    """
    รวบรวมข้อผิดพลาดลงใน List
    """
    error_list.append([
        datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        gdb_path,
        fc_name,
        check_type,
        oid,
        field,
        value,
        message
    ])

# ค้นหา GDBs
def find_gdb_paths(root_dir):
    gdb_paths = []
    for root, dirs, _ in os.walk(root_dir):
        
        # 1. ค้นหา GDB ใน Dirs ปัจจุบัน
        found_gdbs = []
        for d in dirs:
            if d.lower().endswith(".gdb"):
                gdb_paths.append(os.path.join(root, d))
                found_gdbs.append(d)

        # 2. (สำคัญ) ลบ GDB ที่พบออกจาก Dirs
        # เพื่อ os.walk จะได้ไม่ค้นหา *ข้างใน* GDB นั้นอีก (เจอบางจังหวัดที่มี GDB ซ้อนกัน)
        # # (ต้องทำใน loop ย้อนกลับ หรือสร้าง list ใหม่ แต่ .remove() ก็ใช้ได้)
        for gdb_dir in found_gdbs:
            try:
                dirs.remove(gdb_dir)
            except ValueError:
                pass # ไม่ควรเกิดขึ้น        
    if not gdb_paths:        
        print(f"คำเตือน: ไม่พบ .gdb ใน {root_dir}")
    else:
        print(f"พบ {len(gdb_paths)} GDB(s) สำหรับดำเนินการต่อ")
    return gdb_paths


# *** ฟังก์ชันแปลง GDB Path ***
def get_short_gdb_path(full_gdb_path):
    """
    แปลง full GDB path เป็นรูปแบบย่อ เพื่อจัดการชื่อจังหวัดและ gdb ให้อยู่ในรูป เช่น 49_มุกดาหาร/GDB_49_2
    """
    try:
        # parent = GDB_49_2
        parent = os.path.basename(os.path.dirname(full_gdb_path))
        # grandparent = 49_มุกดาหาร
        grandparent = os.path.basename(os.path.dirname(os.path.dirname(full_gdb_path)))
        # ใช้ os.path.sep เพื่อให้ทำงานได้ทั้ง Windows (\\) และ Linux (/)
        return f"{grandparent}{os.path.sep}{parent}"
    except Exception:
        return full_gdb_path # ถ้ามีปัญหา ให้คืนค่าเดิม
    
def safe_list_fields(fc_path):
    try:
        return {f.name.upper(): f.type for f in arcpy.ListFields(fc_path)}
    except Exception:
        return {}

def is_numeric_field_type(field_type):
    if not field_type:
        return False
    return field_type in NUMERIC_TYPES or field_type.lower() in {"double","single","float","integer","smallinteger","short","long"}

def can_be_number(val):
    if val is None:
        return False
    if isinstance(val, (int, float)):
        return True
    try:
        float(val)
        return True
    except Exception:
        return False
        
def safe_value_is_int_like(val):
    if val is None: return False
    try:
        if isinstance(val, (int, float)):
            return float(val).is_integer()
        if isinstance(val, str) and val.isdigit():
            return True
        return False
    except Exception:
        return False

########################################
# ฟังก์ชันตรวจสอบทับซ้อน (ทับสนิท)
########################################

def check_for_exact_overlaps(fc_path, error_list, output_dir, output_basename, return_layer_path=False, verbose=True):
    """
    ตรวจสอบโพลีกอนที่ทับกันสนิท (exact overlap) โดยใช้ arcpy.management.FindIdentical
    ทำงานได้ทั้ง ArcGIS Pro และ ArcMap (รองรับกรณีไม่มี FEAT_SEQ หรือ GROUPID)
    
    Parameters
    ----------
    fc_path : str
        Full path ของ feature class ที่ต้องการตรวจสอบ
    error_list : list
        รายการ error ที่จะถูกเขียนเพิ่มผ่าน write_error_report()
    output_dir : str
        โฟลเดอร์สำหรับเก็บ shapefile ที่เป็นผลลัพธ์
    output_basename : str
        ชื่อ prefix สำหรับไฟล์ผลลัพธ์
    return_layer_path : bool
        ถ้า True จะคืน path ของ shapefile ที่สร้างขึ้น
    verbose : bool
        ถ้า True จะแสดง log ระหว่างการทำงาน

    Returns
    -------
    str | None
        คืน path ของ shapefile ที่พบโพลีกอนซ้ำ ถ้ามี
        ถ้าไม่พบซ้ำหรือเกิด error จะคืน None
    """

    gdb_path, fc_name = os.path.split(fc_path)
    uid = uuid.uuid4().hex[:8]
    out_table = os.path.join("in_memory", f"ident_{output_basename}_{uid}")
    temp_layer = f"in_memory_dup_lyr_{uid}"
    output_shp = os.path.join(output_dir, f"{output_basename}_{fc_name}_duplicates.shp")

    # --------------------------
    # ฟังก์ชันช่วยลบข้อมูลอย่างปลอดภัย
    # --------------------------
    def safe_delete(item):
        try:
            if arcpy.Exists(item):
                arcpy.management.Delete(item)
        except Exception:
            pass

    # --------------------------
    # เริ่มตรวจสอบทับซ้อน
    # --------------------------
    if verbose:
        print(f"    ▶ ตรวจสอบการซ้อนทับ (Exact Overlap): {fc_name}")

    safe_delete(out_table)
    safe_delete(temp_layer)

    try:
        # ตรวจสอบว่า FeatureClass มี OID Field หรือไม่
        try:
            oid_field = arcpy.Describe(fc_path).OIDFieldName
        except Exception as e:
            msg = f"ไม่สามารถอ่าน OID Field ของ {fc_name}: {e}"
            if verbose: print(f"      ⚠ {msg}")
            write_error_report(error_list, gdb_path, fc_name, "Geometry Error", -1, "Shape", "", msg)
            return None

        # --------------------------
        # FindIdentical (ตรวจสอบโพลีกอนทับกันสนิท)
        # --------------------------
        arcpy.management.FindIdentical(
            in_dataset=fc_path,
            out_dataset=out_table,
            fields=["Shape"],
            xy_tolerance="0 Meters",
            z_tolerance="0"
        )

        table_fields = [f.name for f in arcpy.ListFields(out_table)]
        upper_fields = [f.upper() for f in table_fields]

        # ตรวจสอบ field สำคัญ
        if "IN_FID" not in upper_fields:
            msg = "FindIdentical ไม่มีฟิลด์ IN_FID"
            if verbose: print(f"      ❌ {msg}")
            write_error_report(error_list, gdb_path, fc_name, "Geometry Error", -1, "Shape", "", msg)
            return None

        # หา field สำหรับ grouping
        seq_field = None
        if "FEAT_SEQ" in upper_fields:
            seq_field = "FEAT_SEQ"
        elif "GROUPID" in upper_fields:
            seq_field = "GROUPID"
        elif "GROUPID" in table_fields:  # fallback (case-sensitive)
            seq_field = "GROUPID"

        # --------------------------
        # Fallback (ถ้าไม่มี FEAT_SEQ/GROUPID)
        # --------------------------
        groups = defaultdict(list)
        if seq_field:
            if verbose: print(f"      • ใช้ฟิลด์สำหรับ grouping: {seq_field}")
            with arcpy.da.SearchCursor(out_table, ["IN_FID", seq_field]) as cur:
                for fid, seq in cur:
                    groups[seq].append(fid)
        else:
            if verbose: print("      ⚠ ไม่มี FEAT_SEQ หรือ GROUPID — ใช้ fallback โดย group จาก FID ซ้ำเอง")
            with arcpy.da.SearchCursor(out_table, ["IN_FID"]) as cur:
                for fid, in cur:
                    groups[fid].append(fid)

        # --------------------------
        # วิเคราะห์ผลลัพธ์
        # --------------------------
        dup_fids = sorted({fid for seq, fids in groups.items() if len(fids) > 1 for fid in fids})
        if not dup_fids:
            if verbose: print("      ✓ ไม่พบ Duplicated Polygon")
            return None

        count = len(dup_fids)
        msg = f"พบโพลีกอนทับกันสนิท {count} รูปแปลง (OIDs: {dup_fids[:20]}{'...' if count > 20 else ''})"
        if verbose: print(f"      ⚠ {msg}")

        write_error_report(
            error_list,
            gdb_path,
            fc_name,
            "Duplicated Polygon",
            str(dup_fids),
            "Shape",
            count,
            msg
        )

        # --------------------------
        # สร้าง Shapefile Output
        # --------------------------
        os.makedirs(output_dir, exist_ok=True)
        where_clause = f"{oid_field} IN ({','.join(map(str, dup_fids))})"
        arcpy.management.MakeFeatureLayer(fc_path, temp_layer, where_clause)
        arcpy.management.CopyFeatures(temp_layer, output_shp)

        if verbose: print(f"      → บันทึก shapefile: {output_shp}")
        return output_shp if return_layer_path else None

    except Exception as e:
        msg = f"เกิดข้อผิดพลาด: {e}"
        if verbose: print(f"      ❌ {msg}")
        write_error_report(error_list, gdb_path, fc_name, "Geometry Error", -1, "Shape", "", msg)
        return None

    finally:
        # Cleanup
        safe_delete(out_table)
        safe_delete(temp_layer)
        if verbose: print("      • Cleanup in_memory เสร็จสิ้น\n")

# ----------------------------------------
# ตรวจสอบประเภทข้อมูลและค่าต่าง ๆ ตามที่กำหนดไว้
# ----------------------------------------

################################################
#--------------------- 1) PARCEL
################################################

def validate_parcel(fc_path, error_list, basename=None):

    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  กำลังตรวจสอบ PARCEL: {fc_name}")
    fields = safe_list_fields(fc_path)

    required = ["UTMMAP1","UTMMAP2","UTMMAP3","UTMMAP4","UTMSCALE","LAND_NO","PARCEL_TYPE","CHANGWAT_CODE","BRANCH_CODE","PARCEL_RN"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบฟิลด์นี้")
# ----------------------------------------    
    # ตรวจสอบประเภทข้อมูล
# ----------------------------------------
    if "UTMMAP1" in fields and fields["UTMMAP1"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP1", fields["UTMMAP1"], "ต้องเป็น String")
    if "UTMMAP2" in fields and not is_numeric_field_type(fields["UTMMAP2"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP2", fields["UTMMAP2"], "ต้องเป็น Number")
    if "UTMMAP3" in fields and fields["UTMMAP3"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP3", fields["UTMMAP3"], "ต้องเป็น String")
    if "UTMMAP4" in fields and fields["UTMMAP4"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP4", fields["UTMMAP4"], "ต้องเป็น String")
    if "UTMSCALE" in fields and not is_numeric_field_type(fields["UTMSCALE"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMSCALE", fields["UTMSCALE"], "ต้องเป็น Number")
    if "LAND_NO" in fields and not is_numeric_field_type(fields["LAND_NO"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "LAND_NO", fields["LAND_NO"], "ต้องเป็น Number") 
    if "PARCEL_TYPE" in fields and not is_numeric_field_type(fields["PARCEL_TYPE"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "PARCEL_TYPE", fields["PARCEL_TYPE"], "ต้องเป็น Number")       
    if "CHANGWAT_CODE" in fields and fields["CHANGWAT_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "CHANGWAT_CODE", fields["CHANGWAT_CODE"], "ต้องเป็น String")
    if "BRANCH_CODE" in fields and fields["BRANCH_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "BRANCH_CODE", fields["BRANCH_CODE"], "ต้องเป็น String")
 
    # ----------------------------------------
    # ตรวจสอบ ความถูกต้องของข้อมูล
    # ----------------------------------------

    utm_key = defaultdict(list)
    branch_parcel_rn = defaultdict(list)

    cursor_fields = [f for f in ["OID@","UTMMAP1","UTMMAP2","UTMMAP3","UTMMAP4","UTMSCALE","LAND_NO","PARCEL_TYPE","CHANGWAT_CODE","BRANCH_CODE","PARCEL_RN"] if f == "OID@" or f.upper() in fields]
    try:
        with arcpy.da.SearchCursor(fc_path, cursor_fields) as cur:
            for row in cur:
                rec = dict(zip([f.upper() for f in cursor_fields], row))
                oid = rec.get("OID@",-1)
                utm1 = rec.get("UTMMAP1"); utm2 = rec.get("UTMMAP2"); utm3=rec.get("UTMMAP3"); utm4=rec.get("UTMMAP4")
                scale = rec.get("UTMSCALE"); land_no=rec.get("LAND_NO"); parcel_type=rec.get("PARCEL_TYPE")
                cwt = rec.get("CHANGWAT_CODE"); branch = rec.get("BRANCH_CODE"); parcel_rn = rec.get("PARCEL_RN")

                # 1.1.1. UTMMAP1 ต้องเป็น String และเป็น 4 หลักเท่านั้น เช่น "5042"
                if not (isinstance(utm1, str) and utm1.isdigit() and len(utm1)==4):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP1", utm1, "UTMMAP1 ต้องเป็น 4 หลัก")
                
                # 1.1.2.UTMMAP2	ต้องเป็น  Number  และต้องเป็น 1 หรือ 2 หรือ 3 หรือ 4 เท่านั้น 
                if not (isinstance(utm2,(int,float)) or (isinstance(utm2,str) and utm2.isdigit())):
                    write_error_report(error_list, gdb_path, fc_name, "Field Type", oid, "UTMMAP2", utm2, "ประเภทข้อมูลต้องเป็น Number และไม่ควรว่าง")
                else:
                    try:
                        if int(float(utm2)) not in (1,2,3,4):
                            write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP2", utm2, "UTMMAP2 ต้องเป็น 1 - 4 ")
                    except:
                        pass
               
                # 1.1.3.UTMMAP3	ต้องเป็น String  และเป็น 4 หลักเท่านั้น เช่น "0016"
                if not (isinstance(utm3,str) and utm3.isdigit() and len(utm3)==4):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP3", utm3, "UTMMAP3 ต้องเป็น 4 หลัก")
                
                # 1.1.4.UTMMAP4	ต้องเป็น String  และเป็น 2 หลักเท่านั้น เช่น "02"
                if not (isinstance(utm4,str) and utm4.isdigit() and len(utm4)==2):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP4", utm4, "UTMMAP4 ของชั้น PARCEL ต้องเป็น 2 หลัก")
                else:

                    # ตรวจสอบความสอดคล้องกับ UTMSCALE
                    try:
                        scale_i = int(float(scale)) if scale is not None else None
                    except:
                        scale_i = None
                    if scale_i == 4000 and utm4 != '00':
                        write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "UTMMAP4", utm4, "UTMMAP4 ต้องเป็น '00' เนื่องจาก UTMSCALE=4000")
                    elif scale_i == 2000:
                        try:
                            if not (1 <= int(utm4) <= 4):
                                write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "UTMMAP4", utm4, "UTMMAP4 ต้องอยู่ระหว่าง '01'-'04' เนื่องจาก UTMSCALE=2000")
                        except:
                            pass
                    elif scale_i == 1000:
                        try:
                            if not (1 <= int(utm4) <= 16):
                                write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "UTMMAP4", utm4, "UTMMAP4 ต้องอยู่ระหว่าง '01'-'16' เนื่องจาก UTMSCALE=1000")
                        except:
                            pass
                    elif scale_i == 500:
                        try:
                            if not (1 <= int(utm4) <= 64):
                                write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "UTMMAP4", utm4, "UTMMAP4 ต้องอยู่ระหว่าง '01'-'64' เนื่องจาก UTMSCALE=500")
                        except:
                            pass

                # 1.1.5.UTMSCALE  ต้องเป็น Number และเป็น  4000 หรือ 2000 หรือ 1000 หรือ 500 เท่านั้น
                if scale is None or int(float(scale)) not in (4000,2000,1000,500):
                    write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "UTMSCALE", scale, "UTMSCALE ของฟีเจอร์คลาส PARCEL จะต้องเป็น 4000,2000,1000 หรือ 500")

                # 1.1.6.LAND_NO  ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)
	            # 1.1.7.PARCEL_TYPE ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)
	            # 1.1.8.CHANGWAT_CODE ต้องเป็น String และเป็น 2 หลัก เช่น "66"
                if not (isinstance(cwt,str) and len(cwt)==2 and cwt.isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "CHANGWAT_CODE", cwt, "CHANGWAT_CODE ต้องเป็น 2 หลัก")

                # 1.1.9.BRANCH_CODE ต้องเป็น String และเป็น 8 หลัก และสองหลักแรก จะต้องตรงกับ CHANGWAT_CODE
                # เช่น CHANGWAT_CODE  เป็น "66" BRANCHCODE จะต้องขึ้นต้นด้วยเลข 66 เช่น "66000000"
                if not (isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "BRANCH_CODE ต้องเป็น 8 หลัก")
                else:
                    if isinstance(cwt,str) and not branch.startswith(cwt):
                        write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "BRANCH_CODE", branch, f"2 หลักแรกของ BRANCH_CODE ไม่ตรงกับ CHANGWAT_CODE {cwt}")
                
                # 1.1.10.PARCEL_RN ต้องเป็น Number  และใน BRANCH_CODE เดียวกัน จะต้องไม่มีค่าซ้ำ
                if parcel_rn is None or not can_be_number(parcel_rn):
                    write_error_report(error_list, gdb_path, fc_name, "Field Type", oid, "PARCEL_RN", parcel_rn, "ต้องเป็น Number และไม่ควรว่าง")
                else:
                    branch_parcel_rn[(branch.strip() if branch else "NULL", int(float(parcel_rn)))].append(oid)

                # 1.2. ถ้า LAND_NO ไม่ใช่ค่าว่าง หรือ 0 
                # ให้ตรวจสอบ BRANCH_CODE, UTMMAP1, UTMMAP2, UTMMAP3, UTMMAP4, UTMSCALE และ LAND_NO จะต้องไม่ซ้ำกัน
                is_land_no_valid = False
                if land_no is not None and can_be_number(land_no):
                    if int(float(land_no)) != 0:
                        is_land_no_valid = True

                if is_land_no_valid:
                    # ถ้า LAND_NO ไม่ใช่ 0 หรือ ว่าง จึงจะเพิ่มเข้า utm_key เพื่อตรวจสอบค่าซ้ำ
                    scale_i = int(float(scale)) if can_be_number(scale) else scale
                    check_key = (branch.strip() if branch else "NULL", utm1, utm2, utm3, utm4, scale_i, land_no)
                    utm_key[check_key].append(oid)
                # (else: ถ้า LAND_NO เป็น 0 หรือ ว่าง, ก็ไม่ต้อง add เข้า utm_key)


            for primery_key, oids in utm_key.items():
                if len(oids) > 1:
                    write_error_report(error_list, gdb_path, fc_name, "Duplicate UTM", str(oids), "PRIMERY_KEY", primery_key, "BRANCH_CODE+UTMMAP1+UTMMAP2+UTMMAP3+UTMMAP4+UTMSCALE+LAND_NO มีค่าซ้ำ")

            for k, oids in branch_parcel_rn.items():
                if len(oids) > 1:
                    write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "PARCEL_RN", k, "PARCEL_RN มีค่าซ้ำภายใน BRANCH_CODE เดียวกัน")
    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))
    
    # 1.3. ตรวจสอบโพลีกอนที่ซ้อนทับกันสนิท
    check_for_exact_overlaps(fc_path, error_list, os.path.join(OVERLAP_ROOT,"PARCEL"), basename or "PARCEL")


################################################
#-------------2) PARCEL_NS3K
################################################

def validate_parcel_ns3k(fc_path, error_list, basename=None):
    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  ตรวจสอบ PARCEL_NS3K: {fc_name}")

    fields = safe_list_fields(fc_path)
    required = ["UTMMAP1","UTMMAP2","UTMMAP3","UTMMAP4","UTMSCALE","LAND_NO","PARCEL_TYPE","CHANGWAT_CODE","BRANCH_CODE","NS3K_RN"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบฟิลด์นี้")
    
    # ตรวจสอบประเภทข้อมูล
    if "UTMMAP1" in fields and fields["UTMMAP1"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP1", fields["UTMMAP1"], "ต้องเป็น String")
    if "UTMMAP2" in fields and not is_numeric_field_type(fields["UTMMAP2"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP2", fields["UTMMAP2"], "ต้องเป็น Number")
    if "UTMMAP3" in fields and fields["UTMMAP3"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP3", fields["UTMMAP3"], "ต้องเป็น String")
    if "UTMMAP4" in fields and fields["UTMMAP4"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMMAP4", fields["UTMMAP4"], "ต้องเป็น String")
    if "UTMSCALE" in fields and not is_numeric_field_type(fields["UTMSCALE"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "UTMSCALE", fields["UTMSCALE"], "ต้องเป็น Number")
    if "LAND_NO" in fields and not is_numeric_field_type(fields["LAND_NO"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "LAND_NO", fields["LAND_NO"], "ต้องเป็น Number")
    if "PARCEL_TYPE" in fields and not is_numeric_field_type(fields["PARCEL_TYPE"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "PARCEL_TYPE", fields["PARCEL_TYPE"], "ต้องเป็น Number")
    if "CHANGWAT_CODE" in fields and fields["CHANGWAT_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "CHANGWAT_CODE", fields["CHANGWAT_CODE"], "ต้องเป็น String")
    if "BRANCH_CODE" in fields and fields["BRANCH_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "BRANCH_CODE", fields["BRANCH_CODE"], "ต้องเป็น String")
    
    # ----------------------------------------
    # ตรวจสอบ ความถูกต้องของข้อมูล
    # ----------------------------------------
    utm_key = defaultdict(list)
    branch_ns3k = defaultdict(list)

    cursor_fields = [f for f in ["OID@","UTMMAP1","UTMMAP2","UTMMAP3","UTMMAP4","UTMSCALE","LAND_NO","PARCEL_TYPE","CHANGWAT_CODE","BRANCH_CODE","NS3K_RN"] if f == "OID@" or f.upper() in fields]
    try:
        with arcpy.da.SearchCursor(fc_path, cursor_fields) as cur:
            for row in cur:
                rec = dict(zip([f.upper() for f in cursor_fields],row))
                oid = rec.get("OID@", -1)
                utm1 = rec.get("UTMMAP1"); utm2=rec.get("UTMMAP2"); utm3=rec.get("UTMMAP3"); utm4=rec.get("UTMMAP4")
                scale = rec.get("UTMSCALE"); land_no = rec.get("LAND_NO"); parcel_type = rec.get("PARCEL_TYPE")
                cwt = rec.get("CHANGWAT_CODE"); branch = rec.get("BRANCH_CODE"); ns3k_rn = rec.get("NS3K_RN")
                
                # 2.1.1. UTMMAP1 ต้องเป็น String และเป็น 4 หลักเท่านั้น เช่น "5042"
                if not (isinstance(utm1,str) and utm1.isdigit() and len(utm1)==4):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP1", utm1, "UTMMAP1 ต้องมี 4 หลัก")
                
                # 2.1.2. UTMMAP2 ต้องเป็น  Number  และต้องเป็น 1 หรือ 2 หรือ 3 หรือ 4 เท่านั้น
                if not (isinstance(utm2,(int,float)) or (isinstance(utm2,str) and utm2.isdigit())):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP2", utm2, "รูปแบบข้อมูลต้องเป็น Number")
                else:
                    try:
                        if int(float(utm2)) not in (1,2,3,4):
                            write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP2", utm2, "UTMMAP2 ต้องอยู่ระหว่าง 1-4")
                    except:
                        pass
                
                # 2.1.3. UTMMAP3 ต้องเป็น String และต้องเป็น '0000' เท่านั้น
                if not (isinstance(utm3,str) and utm3 == "0000"):
                    write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "UTMMAP3", utm3, "UTMMAP3 ของ NS3K ต้องเป็น '0000'")
                
                # 2.1.4. UTMMAP4  ต้องเป็น String และเป็น 3 หลักเท่านั้น เช่น "002"
                if not (isinstance(utm4,str) and utm4.isdigit() and len(utm4)==3):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "UTMMAP4", utm4, "ต้องเป็น 3 หลัก")
                
                # 2.1.5. UTMSCALE  ต้องเป็น Number และต้องเป็น 5000 เท่านั้น
                if scale is None or int(float(scale)) != 5000:
                    write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "UTMSCALE", scale, "UTMSCALE ของ NS3K ต้องเป็น 5000")
                
                # 2.1.6. LAND_NO  ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)
                # 2.1.7. PARCEL_TYPE  ต้องเป็น Number และต้องเป็น 3 เท่านั้น
                if parcel_type != 3:
                    write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "PARCEL_TYPE", parcel_type, "PARCEL_TYPE ของ NS3K ต้องเป็น 3")
                
                # 2.1.8. CHANGWAT_CODE ต้องเป็น String และเป็น 2 หลัก เช่น "66"
                if not (isinstance(cwt,str) and len(cwt)==2 and cwt.isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "CHANGWAT_CODE", cwt, "ต้องเป็น 2 หลัก")
                
                # 2.1.9. BRANCH_CODE  ต้องเป็น String และเป็น 8 หลัก และสองหลักแรก จะต้องตรงกับ CHANGWAT_CODE
                # เช่น CHANGWAT_CODE  เป็น "66" BRANCHCODE จะต้องขึ้นต้นด้วยเลข 66 เช่น "66000000"
                if not (isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "ต้องเป็น 8 หลัก")
                else:
                    if isinstance(cwt,str) and not branch.startswith(cwt):
                        write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "BRANCH_CODE", branch, f"2 หลักแรกของ BRANCH_CODE ไม่ตรงกับ CHANGWAT_CODE {cwt}")
                
                # 2.1.10. NS3K_RN ต้องเป็น Number และใน BRANCH_CODE เดียวกัน จะต้องไม่มีค่าซ้ำ
                if ns3k_rn is None or not can_be_number(ns3k_rn):
                    write_error_report(error_list, gdb_path, fc_name, "Field Type", oid, "NS3K_RN", ns3k_rn, "ต้องเป็น Number")
                else:
                    branch_ns3k[(branch.strip() if branch else "NULL", int(float(ns3k_rn)))].append(oid)
                
                # 2.2. ถ้า LAND_NO ไม่ใช่ค่าว่าง หรือ 0 
                # ให้ตรวจสอบ BRANCH_CODE, UTMMAP1, UTMMAP2, UTMMAP3, UTMMAP4, UTMSCALE และ LAND_NO จะต้องไม่ซ้ำกัน                
                is_land_no_valid = False
                if land_no is not None and can_be_number(land_no):
                    if int(float(land_no)) != 0:
                        is_land_no_valid = True

                if is_land_no_valid:
                    # ถ้า LAND_NO ถูกต้อง, จึงจะเพิ่มเข้า utm_key เพื่อตรวจสอบค่าซ้ำ
                    scale_i = int(float(scale)) if can_be_number(scale) else scale
                    check_key = (branch.strip() if branch else "NULL", utm1, utm2, utm3, utm4, scale_i, land_no)
                    utm_key[check_key].append(oid)
                # (else: ถ้า LAND_NO เป็น 0 หรือ ว่าง, ก็ไม่ต้อง add เข้า utm_key)

            for primery_key,oids in utm_key.items():
                if len(oids)>1:
                    write_error_report(error_list, gdb_path, fc_name, "Duplicate UTM", str(oids), "PRIMERY_KEY", primery_key, "BRANCH_CODE+UTMMAP1+UTMMAP2+UTMMAP3+UTMMAP4+UTMSCALE+LAND_NO not unique")
            for k,oids in branch_ns3k.items():
                if len(oids)>1:
                    write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "NS3K_RN", k, "NS3K_RN ซ้ำภายใน BRANCH_CODE เดียวกัน")
    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))

    # 2.3. ตรวจสอบโพลีกอนที่ซ้อนทับกันสนิท
    check_for_exact_overlaps(fc_path, error_list, os.path.join(OVERLAP_ROOT,"PARCEL"), basename or "PARCEL_NS3K")

################################################
# ---------------3) ROAD
################################################

def validate_road(fc_path, error_list, basename=None):
    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  กำลังตรวจสอบชั้นข้อมูล ROAD: {fc_name}")
    fields = safe_list_fields(fc_path)
    
    VALID_LAND_USE = ROAD_LAND_USE_DOMAIN
    VALID_STREET_TYPE = ROAD_STREET_TYPE_DOMAIN
    VALID_TD_RP3 = ROAD_REQ_NAME_TD_CODES

    required = ["STREET_NAME","STREET_CODE","STREET_DEPTH","LAND_USE","STREET_TYPE","STREET_WIDTH","STREET_AREA","BRANCH_CODE","PARCEL_TYPE","TD_RP3_TYPE_CODE","STREET_RN","CHANGWAT_CODE","STREET_SMG"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบฟิลด์นี้")
    
    # ----------------------------------------
    # ตรวจสอบประเภทข้อมูล
    # ----------------------------------------
    if "STREET_NAME" in fields and fields["STREET_NAME"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_NAME", fields["STREET_NAME"], "ต้องเป็น String")
    if "STREET_CODE" in fields and fields["STREET_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_CODE", fields["STREET_CODE"], "ต้องเป็น String")
    if "STREET_DEPTH" in fields and not is_numeric_field_type(fields["STREET_DEPTH"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_DEPTH", fields["STREET_DEPTH"], "ต้องเป็น Number")
    if "LAND_USE" in fields and fields["LAND_USE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "LAND_USE", fields["LAND_USE"], "ต้องเป็น String")
    if "STREET_TYPE" in fields and fields["STREET_TYPE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_TYPE", fields["STREET_TYPE"], "ต้องเป็น String")
    if "STREET_WIDTH" in fields and not is_numeric_field_type(fields["STREET_WIDTH"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_WIDTH", fields["STREET_WIDTH"], "ต้องเป็น Number")
    if "STREET_AREA" in fields and not is_numeric_field_type(fields["STREET_AREA"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_AREA", fields["STREET_AREA"], "ต้องเป็น Number")
    if "BRANCH_CODE" in fields and fields["BRANCH_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "BRANCH_CODE", fields["BRANCH_CODE"], "ต้องเป็น String")
    if "PARCEL_TYPE" in fields and not is_numeric_field_type(fields["PARCEL_TYPE"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "PARCEL_TYPE", fields["PARCEL_TYPE"], "ต้องเป็น Number")
    if "TD_RP3_TYPE_CODE" in fields and not is_numeric_field_type(fields["TD_RP3_TYPE_CODE"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "TD_RP3_TYPE_CODE", fields["TD_RP3_TYPE_CODE"], "ต้องเป็น Number")
    if "STREET_RN" in fields and not is_numeric_field_type(fields["STREET_RN"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_RN", fields["STREET_RN"], "ต้องเป็น Number")
    if "CHANGWAT_CODE" in fields and fields["CHANGWAT_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "CHANGWAT_CODE", fields["CHANGWAT_CODE"], "ต้องเป็น String")
    if "STREET_SMG" in fields and fields["STREET_SMG"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_SMG", fields["STREET_SMG"], "ต้องเป็น String")

    
    chk_fields = [f for f in ["OID@","BRANCH_CODE","CHANGWAT_CODE","PARCEL_TYPE","TD_RP3_TYPE_CODE","STREET_RN","STREET_NAME","STREET_CODE","STREET_TYPE","STREET_DEPTH","LAND_USE","STREET_WIDTH","STREET_AREA","STREET_SMG"] if f=="OID@" or f.upper() in fields]
    try:
        branch_street_rn_seen = defaultdict(list)
        name_code_pairs = []
        with arcpy.da.SearchCursor(fc_path, chk_fields) as cur:
            for row in cur:
                rec = dict(zip([f.upper() for f in chk_fields],row))
                oid = rec.get("OID@", -1)
                branch = rec.get("BRANCH_CODE"); cwt = rec.get("CHANGWAT_CODE")
                parcel_type = rec.get("PARCEL_TYPE"); td_type = rec.get("TD_RP3_TYPE_CODE")
                street_rn = rec.get("STREET_RN"); name = rec.get("STREET_NAME"); code = rec.get("STREET_CODE")
                street_type = rec.get("STREET_TYPE"); land_use = rec.get("LAND_USE")


                # 3.1.1. STREET_NAME ต้องเป็น String ถ้า TD_RP3_TYPE_CODE เป็น 1 หรือ 2 หรือ 3 หรือ 4 หรือ 5 หรือ 6 หรือ 8 จะต้องไม่ใช่ค่าว่าง
                is_street_name_empty = (name is None or (isinstance(name, str) and name.strip() == ""))

                # 3.1.2. STREET_CODE ต้องเป็น String (เช็คจากประเภทข้อมูลแล้ว)
                # 3.1.3. STREET_DEPTH ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)

                # 3.1.4. LAND_USE ต้องเป็น String มีข้อความในกลุ่มนี้เท่านั้น {VALID_LAND_USE}
                # ตรวจสอบ LAND_USE เฉพาะเมื่อ STREET_NAME ไม่ว่าง                
                if (not is_street_name_empty) and (land_use is None or str(land_use).strip() not in VALID_LAND_USE):
                    write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "LAND_USE", land_use, f"LAND_USE จะต้องมีค่าดังต่อไปนี้ {VALID_LAND_USE} (เมื่อ STREET_NAME มีค่า)")
              
                # 3.1.5. ตรวจสอบ STREET_TYPE เฉพาะเมื่อ STREET_NAME ไม่ว่าง
                # STREET_TYPE ต้องเป็น String มีข้อความในกลุ่ม {VALID_STREET_TYPE} เท่านั้น
                if (not is_street_name_empty) and (street_type is None or str(street_type).strip() not in VALID_STREET_TYPE):
                    write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "STREET_TYPE", street_type, f"STREET_TYPE จะต้องมีค่าดังต่อไปนี้ {VALID_STREET_TYPE} (เมื่อ STREET_NAME มีค่า)")

                # 3.1.6. STREET_WIDTH ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)
                # 3.1.7. STREET_AREA ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)
                # 3.1.8. CHANGWAT_CODE ต้องเป็น String และเป็น 2 หลัก เช่น "66"
                if not (isinstance(cwt,str) and len(cwt)==2 and cwt.isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "CHANGWAT_CODE", cwt, "ต้องเป็น 2 หลัก")
                
                # 3.1.9. BRANCH_CODE ต้องเป็น String และเป็น 8 หลัก และสองหลักแรก จะต้องตรงกับ CHANGWAT_CODE
                if branch and cwt and (not (isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit())):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "BRANCH_CODE ต้องเป็น 8 หลัก")
                if branch and cwt and isinstance(branch,str) and isinstance(cwt,str) and not branch.startswith(cwt):
                    write_error_report(error_list, gdb_path, fc_name, "Conditional Rule", oid, "BRANCH_CODE", branch, f"2 หลักแรกของ BRANCH_CODE ไม่ตรงกับ CHANGWAT_CODE {cwt}")

                # 3.1.10. PARCEL_TYPE ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)

                # 3.1.11. TD_RP3_TYPE_CODE ต้องเป็น Number
                # และจะต้องเป็น 1 หรือ 2 หรือ 3 หรือ 4 หรือ 5 หรือ 6 หรือ 8 เท่านั้น
                
                td_type_int = None
                is_td_type_valid_number = False

                if td_type is None:
                    td_type_int = None
                    is_td_type_valid_number = True # ถือว่า None เป็นค่าที่ถูกต้องสำหรับตรวจสอบ
                elif can_be_number(td_type):
                    td_type_int = int(float(td_type))
                    is_td_type_valid_number = True
                else:
                    # กรณีเป็น non-numeric string (เช่น "abc") ซึ่งผิดเสมอ
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "TD_RP3_TYPE_CODE", td_type, "TD_RP3_TYPE_CODE ต้องเป็นตัวเลขเท่านั้น")
                    is_td_type_valid_number = False

                if is_td_type_valid_number:
                    # ตรวจสอบว่า STREET_NAME ว่างหรือไม่
                    is_street_name_empty = (name is None or (isinstance(name, str) and name.strip() == ""))

                    if not is_street_name_empty:
                        # --- ทางเลือก 1: STREET_NAME มีค่า ---
                        # 3.1.11 TD_RP3_TYPE_CODE ต้องเป็น {1,2,3,4,5,6,8}
                        if td_type_int not in VALID_TD_RP3:
                            write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "TD_RP3_TYPE_CODE", td_type, f"TD_RP3_TYPE_CODE ต้องมีค่าเป็น {sorted(VALID_TD_RP3)} (เนื่องจาก STREET_NAME มีค่า)")
                            # (ข้อ 3.1.1 ถูกต้องโดยอัตโนมัติ เพราะ STREET_NAME ไม่ว่าง)

                    else:
                        # --- ทางเลือก 2: STREET_NAME ว่าง ---
                        # (ข้อ 3.1.1 ไม่ต้องตรวจสอบ)
                        # (ข้อ 3.1.11 ใหม่) TD_RP3_TYPE_CODE ต้องเป็น {0, None} หรือ {1-8}
                        # สร้าง Set ที่อนุญาต (0, None, 1, 2, 3, 4, 5, 6, 8)
                        ALLOWED_VALUES_WHEN_NAME_IS_EMPTY = {0, None} 
                        ALLOWED_VALUES_WHEN_NAME_IS_EMPTY.update(VALID_TD_RP3) # เพิ่ม {1,2,3...}
                        if td_type_int not in ALLOWED_VALUES_WHEN_NAME_IS_EMPTY:
                            # ถ้า td_type เป็นค่าอื่น (เช่น 7, 9, 99) จะไม่ยอมรับ
                            allowed_str = "{0, None} หรือ " + str(sorted(VALID_TD_RP3))
                            write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "TD_RP3_TYPE_CODE", td_type, f"TD_RP3_TYPE_CODE ต้องเป็น {allowed_str} (เนื่องจาก STREET_NAME ว่างเปล่า)")
                
                # ถ้า TD_RP3_TYPE_CODE เป็น 1 หรือ 2 หรือ 3 หรือ 4 หรือ 5 หรือ 6 หรือ 8 STREET_NAME จะต้องไม่ใช่ค่าว่าง
                if (td_type_int in VALID_TD_RP3) and is_street_name_empty:
                    write_error_report(
                        error_list, 
                        gdb_path, 
                        fc_name, 
                        "Data Required",
                        oid, 
                        "STREET_NAME", 
                        name, 
                        f"STREET_NAME ต้องไม่เป็นค่าว่าง เนื่องจาก TD_RP3_TYPE_CODE คือ {td_type_int}"
                    )
                           

                # 3.1.12. STREET_RN ต้องเป็น Number และใน BRANCH_CODE เดียวกัน จะต้องไม่มีค่าซ้ำ
                if street_rn is None or not can_be_number(street_rn):
                     write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "STREET_RN", street_rn, "STREET_RN ต้องเป็น Number")
                else:
                    key = (branch.strip() if isinstance(branch,str) else "NULL", int(float(street_rn)))
                    branch_street_rn_seen[key].append(oid)

                if name and code:
                    name_code_pairs.append((name, code))

          
        
        #  STREET_NAME และ STREET_CODE ต้องจับคู่กันแบบ 1 ต่อ 1
        name_to_code = {}
        code_to_name = {}
        for name, code in name_code_pairs:
            if name in name_to_code and name_to_code[name] != code:
                write_error_report(error_list, gdb_path, fc_name, "OneToOne", "N/A", "STREET_NAME", name, f"{name} ตรวจพบว่าเชื่อมต่อกับ STREET_CODE มากกว่า 1  ({name_to_code[name]} vs {code})")
            if code in code_to_name and code_to_name[code] != name:
                write_error_report(error_list, gdb_path, fc_name, "OneToOne", "N/A", "STREET_CODE", code, f"{code} ตรวจพบว่าเชื่อมต่อกับ STREET_NAME มากกว่า 1 ({code_to_name[code]} vs {name})")
            name_to_code[name] = code
            code_to_name[code] = name
        
        # เขียน Error สำหรับ STREET_RN ที่ซ้ำ
        for key, oids in branch_street_rn_seen.items():
            if len(oids) > 1:
                branch_str, rn_str = key
                write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "STREET_RN", rn_str, f"STREET_RN ซ้ำ ภายใน BRANCH_CODE '{branch_str}'")

    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))
    
    #----- 3.3. ตรวจสอบโพลีกอนที่ซ้อนทับกันสนิท
    check_for_exact_overlaps(fc_path, error_list, os.path.join(OVERLAP_ROOT,"ROAD"), basename or "ROAD")

################################################
# ---------------4) BLOCK_FIX
################################################

def validate_block_fix(fc_path, error_list, basename=None):
    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  กำลังตรวจสอบ BLOCK FIX: {fc_name}")
    fields = safe_list_fields(fc_path)
    
    required = ["STREET_NAME", "STREET_CODE", "BRANCH_CODE", "BLOCK_FIX_RN"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบฟิลด์นี้")
    
    
    # --- (ตรวจสอบประเภท) ---

    if "STREET_NAME" in fields and fields["STREET_NAME"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_NAME", fields["STREET_NAME"], "ต้องเป็น String")
    if "STREET_CODE" in fields and fields["STREET_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_CODE", fields["STREET_CODE"], "ต้องเป็น String")
    if "BRANCH_CODE" in fields and fields["BRANCH_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "BRANCH_CODE", fields["BRANCH_CODE"], "ต้องเป็น String")
    if "BLOCK_FIX_RN" in fields and not is_numeric_field_type(fields["BLOCK_FIX_RN"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "BLOCK_FIX_RN", fields["BLOCK_FIX_RN"], "ต้องเป็น Number")


    chk_fields = [f for f in ["OID@","BRANCH_CODE","BLOCK_FIX_RN","STREET_NAME","STREET_CODE"] if f=="OID@" or f.upper() in fields]
    try:
        branch_rns = defaultdict(list)
        name_code_pairs = []
        with arcpy.da.SearchCursor(fc_path, chk_fields) as cur:
            for row in cur:
                rec = dict(zip([f.upper() for f in chk_fields], row))
                oid = rec.get("OID@", -1)
                branch = rec.get("BRANCH_CODE")
                rn = rec.get("BLOCK_FIX_RN")
                name = rec.get("STREET_NAME")
                code = rec.get("STREET_CODE")

                # 4.1.1. STREET_NAME ต้องเป็น String  และไม่ใช่ค่าว่าง (NULL) หรือ " " หรือขีดกลาง (-)
                if not name or (isinstance(name, str) and (name.strip() == "" or name.strip() == "-")):
                    write_error_report(error_list, gdb_path, fc_name, "Data Required", oid, "STREET_NAME", name, "STREET_NAME ต้องไม่เป็นค่าว่าง, ช่องว่าง หรือ '-'")

                # 4.1.2. STREET_CODE ต้องเป็น String (เช็คจากประเภทข้อมูลแล้ว)
                # 4.1.3: BRANCH_CODE ต้องเป็น String และมี 8 หลักเท่านั้น
                if not (branch and isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "BRANCH_CODE ต้องเป็น 8 หลัก")
                
                # LOCK_FIX_RN ต้องเป็น Number และใน BRANCH_CODE เดียวกัน ต้องไม่ซ้ำ
                if rn is None or not can_be_number(rn):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BLOCK_FIX_RN", rn, "ต้องเป็น Number")
                else:
                    branch_rns[(branch.strip() if branch else "NULL", int(float(rn)))].append(oid)
                
                # 4.2. STREET_NAME กับ STREET_CODE ต้องจับคู่กันแบบ 1 ต่อ 1
                if name and code:
                    name_code_pairs.append((name, code))

        for key, oids in branch_rns.items():
            if len(oids) > 1:
                write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "BLOCK_FIX_RN", key[1], f"BLOCK_FIX_RN ซ้ำใน BRANCH_CODE '{key[0]}'")

        name_to_code = {}; code_to_name={}
        for name,code in name_code_pairs:
            if name in name_to_code and name_to_code[name] != code:
                write_error_report(error_list, gdb_path, fc_name, "OneToOne", "N/A", "STREET_NAME", name, f"{name} มี STREET_CODE มากกว่า 1")
            if code in code_to_name and code_to_name[code] != name:
                write_error_report(error_list, gdb_path, fc_name, "OneToOne", "N/A", "STREET_CODE", code, f"{code} มี STREET_NAME มากกว่า 1")
            name_to_code[name]=code; code_to_name[code]=name

    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))

    # 4.3. ตรวจสอบโพลีกอนที่ซ้อนทับกันสนิท
    check_for_exact_overlaps(fc_path, error_list, os.path.join(OVERLAP_ROOT,"BLOCK"), basename or "BLOCK_FIX")

############################################
###----- 5) BLOCK_PRICE
############################################

def validate_block_price(fc_path, error_list, basename=None):
    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  Validating BLOCK PRICE: {fc_name}")
    fields = safe_list_fields(fc_path)
    
    required = ["STREET_NAME", "STREET_CODE", "BRANCH_CODE", "BLOCK_PRICE_RN"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบฟิลด์นี้")

    #----------------------------------------
    # --- (ตรวจสอบประเภทข้อมูล) ---
    #----------------------------------------
    if "STREET_NAME" in fields and fields["STREET_NAME"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_NAME", fields["STREET_NAME"], "ต้องเป็น String")
    if "STREET_CODE" in fields and fields["STREET_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "STREET_CODE", fields["STREET_CODE"], "ต้องเป็น String")
    if "BRANCH_CODE" in fields and fields["BRANCH_CODE"] != "String":
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "BRANCH_CODE", fields["BRANCH_CODE"], "ต้องเป็น String")
    if "BLOCK_PRICE_RN" in fields and not is_numeric_field_type(fields["BLOCK_PRICE_RN"]):
        write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, "BLOCK_PRICE_RN", fields["BLOCK_PRICE_RN"], "ต้องเป็น Number")

    chk_fields = [f for f in ["OID@","BRANCH_CODE","BLOCK_PRICE_RN","STREET_NAME","STREET_CODE"] if f=="OID@" or f.upper() in fields]
    try:
        branch_rns = defaultdict(list)
        with arcpy.da.SearchCursor(fc_path, chk_fields) as cur:
            for row in cur:
                rec = dict(zip([f.upper() for f in chk_fields], row))
                oid = rec.get("OID@", -1)
                branch = rec.get("BRANCH_CODE")
                rn = rec.get("BLOCK_PRICE_RN")
                name = rec.get("STREET_NAME")

                # 5.1.1. STREET_NAME ต้องเป็น String และไม่ใช่ค่าว่าง (NULL) หรือ " " หรือขีดกลาง (-)E
                if not name or (isinstance(name, str) and (name.strip() == "" or name.strip() == "-")):
                    write_error_report(error_list, gdb_path, fc_name, "Data Required", oid, "STREET_NAME", name, "STREET_NAME ต้องไม่เป็นค่าว่าง, ช่องว่าง หรือ '-'")
                
                # 5.1.2. STREET_CODE ต้องเป็น String (เช็คจากประเภทข้อมูลแล้ว)
                # 5.1.3. BRANCH_CODE ต้องเป็น String  และมี 8 หลักเท่านั้น
                if not (branch and isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "BRANCH_CODE ต้องเป็น 8 หลัก")
                
                # 5.1.4. BLOCK_PRICE_RN ต้องเป็น Number และใน BRANCH_CODE เดียวกัน ต้องไม่ซ้ำกัน
                if rn is None or not can_be_number(rn):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BLOCK_PRICE_RN", rn, "ต้องเป็น Number")
                else:
                    branch_rns[(branch.strip() if branch else "NULL", int(float(rn)))].append(oid)
        
        for key, oids in branch_rns.items():
            if len(oids) > 1:
                write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "BLOCK_PRICE_RN", key[1], f"BLOCK_PRICE_RN ซ้ำใน BRANCH_CODE '{key[0]}'")

    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))
    
    # 5.2. ตรวจสอบโพลีกอนที่ซ้อนทับกันสนิท
    check_for_exact_overlaps(fc_path, error_list, os.path.join(OVERLAP_ROOT,"BLOCK"), basename or "BLOCK_PRICE")

##############################################
#----------------- 6) BLOCK_BLUE
##############################################

def validate_block_blue(fc_path, error_list, basename=None):
    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  กำลังตรวจสอบ BLOCK_BLUE: {fc_name}")
    fields = safe_list_fields(fc_path)
    required = ["BRANCH_CODE","BLOCK_BLUE_RN","BLOCK_TYPE_ID"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบฟิลด์นี้")
    
    # type checks
    try:
        chk_fields = [f for f in ["OID@","BRANCH_CODE","BLOCK_BLUE_RN","BLOCK_TYPE_ID"] if f=="OID@" or f.upper() in fields]
        with arcpy.da.SearchCursor(fc_path, chk_fields) as cur:
            branch_vals = defaultdict(list)
            for row in cur:
                rec = dict(zip([f.upper() for f in chk_fields], row))
                oid = rec.get("OID@", -1); branch = rec.get("BRANCH_CODE"); rn = rec.get("BLOCK_BLUE_RN"); bt = rec.get("BLOCK_TYPE_ID")
                
                # 6.1.1. BRANCH_CODE ต้องเป็น String  และมี 8 หลักเท่านั้น
                if not (branch and isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "BRANCH_CODE ต้องเป็น 8 หลัก")
                
                # BLOCK_BLUE_RN  ต้องเป็น Number และใน BRANCH_CODE เดียวกัน ต้องไม่ซ้ำกัน
                if rn is None or not can_be_number(rn):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BLOCK_BLUE_RN", rn, "ต้องเป็น Number")
                else:
                    branch_vals[(branch.strip() if branch else "NULL", int(float(rn)))].append(oid)
                
                # 6.1.3. BLOCK_TYPE_ID ต้องเป็น Number และต้องเป็น 1 หรือ 2 หรือ 3 เท่านั้น
                if bt not in (1,2,3):
                    write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "BLOCK_TYPE_ID", bt, "BLOCK_TYPE_ID ต้องเป็น 1 หรือ 2 หรือ 3")
            
            for k,oids in branch_vals.items():
                if len(oids)>1:
                    write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "BLOCK_BLUE_RN", k[1], f"พบค่าซ้ำใน BRANCH_CODE '{k[0]}'")
    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))
    
    # 6.2. ตรวจสอบโพลีกอนที่ซ้อนทับกันสนิท
    check_for_exact_overlaps(fc_path, error_list, os.path.join(OVERLAP_ROOT,"BLOCK"), basename or "BLOCK_BLUE")

##############################################
#----------------- 7) PARCEL_REL
##############################################

def validate_parcel_rel(fc_path, error_list, basename=None):
    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  Validating PARCEL_REL: {fc_name}")
    fields = safe_list_fields(fc_path)
    required = ["BRANCH_CODE","REL_RN","PARCEL_RN","STREET_RN","BLOCK_FIX_RN","BLOCK_BLUE_RN","BLOCK_PRICE_RN","TABLE_NO","SUB_TABLE_NO","DEPTH_R","DEPTH_GROUP","START_X","START_Y","END_X","END_Y"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบฟิลด์")

    # ตรงรวจสอบประเภทข้อมูลที่เป็น Number
    numeric_fields = ["REL_RN","PARCEL_RN","STREET_RN","BLOCK_FIX_RN","BLOCK_BLUE_RN","BLOCK_PRICE_RN","TABLE_NO","SUB_TABLE_NO","DEPTH_R","DEPTH_GROUP","START_X","START_Y","END_X","END_Y"]
    for nf in numeric_fields:
        if nf in fields and not is_numeric_field_type(fields[nf]):
            write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, nf, fields[nf], "ต้องเป็น Number")


    chk_fields = [f for f in required if f.upper() in fields]
    VALID_TABLE_NO = REL_TABLE_NO_DOMAIN
    VALID_SUB_TABLE_NO = REL_SUB_TABLE_NO_RANGE

    branch_rel_rn = defaultdict(list)
    try:
        with arcpy.da.SearchCursor(fc_path, ["OID@"]+chk_fields) as cur:
            for row in cur:
                rec = dict(zip(["OID@"]+[f.upper() for f in chk_fields], row))
                oid = rec.get("OID@", -1)
                branch = rec.get("BRANCH_CODE"); rel_rn = rec.get("REL_RN")

                # 7.1.1. BRANCH_CODE ต้องเป็น String  และมี 8 หลักเท่านั้น
                if not (branch and isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "BRANCH_CODE ต้องเป็น 8 หลัก")
                
                # 7.1.2. REL_RN ต้องเป็น Number และใน BRANCH_CODE เดียวกันจะต้องไม่ซ้ำกัน
                if rel_rn is None or not can_be_number(rel_rn):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "REL_RN", rel_rn, "REL_RN ต้องเป็น Number")
                else:
                    branch_key = branch.strip() if branch else "NULL"
                    branch_rel_rn[(branch_key, int(float(rel_rn)))].append(oid)

                # 7.1.3. PARCEL_RN ต้องเป็น Number
                # 7.1.4. STREET_RN ต้องเป็น Number
                # 7.1.5. BLOCK_FIX_RN ต้องเป็น Number
                # 7.1.6. BLOCK_BLUE_RN ต้องเป็น Number
                # 7.1.7. BLOCK_PRICE_RN ต้องเป็น Number
                # 7.1.8. TABLE_NO ต้องเป็น Number
                # และมีค่าเป็น 1 หรือ 2 หรือ 3 หรือ 41 หรือ 42 หรือ 5 หรือ 6 หรือ 7 เท่านั้น
                table_no = rec.get("TABLE_NO"); sub_no = rec.get("SUB_TABLE_NO")
                if not can_be_number(table_no) or int(float(table_no)) not in VALID_TABLE_NO:
                    write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "TABLE_NO", table_no, f"ต้องเป็น {sorted(VALID_TABLE_NO)}")
                
                # 7.1.9. SUB_TABLE_NO ต้องเป็น Number และต้องมีค่าระหว่าง 0 - 6 หรือค่าว่าง เท่านั้น
                # อนุญาตให้เป็นค่าว่าง (None) ได้
                if sub_no is not None:
                    # ถ้าไม่ว่าง ต้องเป็น 0-6
                    if not can_be_number(sub_no) or int(float(sub_no)) not in VALID_SUB_TABLE_NO:
                        write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "SUB_TABLE_NO", sub_no, "ต้องเป็น 0-6 ")
                
                # 	7.1.10. DEPTH_R ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 	7.1.11. DEPTH_GROUP ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)
                # 	7.1.12. START_X ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 	7.1.13. START_Y ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 	7.1.14. END_X ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 	7.1.15. END_Y ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                for fld in ("DEPTH_R","START_X","START_Y","END_X","END_Y"):
                    val = rec.get(fld)
                    if val is None or (can_be_number(val) and float(val)==0.0):
                        write_error_report(error_list, gdb_path, fc_name, "Data Required", oid, fld, val, f"{fld} ต้องไม่ใช่ 0 หรือค่าว่าง")
            
            # 7.1.2. REL_RN ต้องเป็น Number    และใน BRANCH_CODE เดียวกันจะต้องไม่ซ้ำกัน
            for key, oids in branch_rel_rn.items():
                if len(oids) > 1:
                    branch_key, rn_val = key
                    write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "REL_RN", rn_val, f"ซ้ำภายใน BRANCH_CODE '{branch_key}'")

    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))

##############################################  
#---------------- 8) NS3K_REL
##############################################
def validate_ns3k_rel(fc_path, error_list, basename=None):

    gdb_path, fc_name = os.path.split(fc_path)
    print(f"  Validating NS3K_REL: {fc_name}")
    fields = safe_list_fields(fc_path)
    
    #   8.1. ตรวจสอบฟิลด์ที่จำเป็น
    required = ["BRANCH_CODE","REL_RN","NS3K_RN","STREET_RN","BLOCK_FIX_RN","BLOCK_BLUE_RN","BLOCK_PRICE_RN","TABLE_NO","SUB_TABLE_NO","DEPTH_R","DEPTH_GROUP","START_X","START_Y","END_X","END_Y"]
    for f in required:
        if f.upper() not in fields:
            write_error_report(error_list, gdb_path, fc_name, "Field Check", -1, f, "", "ไม่พบ field")
    
    #--- (ตรวจสอบประเภทข้อมูล) ---ที่เป็น Number
    numeric_fields = ["REL_RN","NS3K_RN","STREET_RN","BLOCK_FIX_RN","BLOCK_BLUE_RN","BLOCK_PRICE_RN","TABLE_NO","SUB_TABLE_NO","DEPTH_R","DEPTH_GROUP","START_X","START_Y","END_X","END_Y"]
    for nf in numeric_fields:
        if nf in fields and not is_numeric_field_type(fields[nf]):
            write_error_report(error_list, gdb_path, fc_name, "Field Type", -1, nf, fields[nf], "ต้องเป็น Number")


    chk_fields = [f for f in required if f.upper() in fields]
    VALID_TABLE_NO = REL_TABLE_NO_DOMAIN
    VALID_SUB_TABLE_NO = REL_SUB_TABLE_NO_RANGE
    
    branch_rel_rn = defaultdict(list)
    branch_ns3k_rn = defaultdict(list)
    
    try:
        with arcpy.da.SearchCursor(fc_path, ["OID@"]+chk_fields) as cur:
            for row in cur:
                rec = dict(zip(["OID@"]+[f.upper() for f in chk_fields], row))
                oid = rec.get("OID@", -1)
                branch = rec.get("BRANCH_CODE"); 
                rel_rn = rec.get("REL_RN")
                ns3k_rn = rec.get("NS3K_RN")
                
                # 8.1.1. BRANCH_CODE ต้องเป็น String และมี 8 หลักเท่านั้น
                if not (branch and isinstance(branch,str) and len(branch.strip())==8 and branch.strip().isdigit()):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "BRANCH_CODE", branch, "BRANCH_CODE ต้องเป็น 8 หลัก")

                # 8.1.2. REL_RN ต้องเป็น Number และภายใน BRANCH_CODE เดียวกันจะต้องไม่ซ้ำกัน
                if rel_rn is None or not can_be_number(rel_rn):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "REL_RN",rel_rn , "ต้องเป็น Number")
                else:
                    branch_key = branch.strip() if branch else "NULL"
                    branch_rel_rn[(branch_key, int(float(rel_rn)))].append(oid)
                
                # 8.1.3 NS3K_RN ต้องเป็น Number
                # 8.1.4. STREET_RN ต้องเป็น Number
                # 8.1.5. BLOCK_FIX_RN ต้องเป็น Number
                # 8.1.6. BLOCK_BLUE_RN ต้องเป็น Number
                # 8.1.7. BLOCK_PRICE_RN ต้องเป็น Number
                

                if ns3k_rn is None or not can_be_number(ns3k_rn):
                    write_error_report(error_list, gdb_path, fc_name, "Data Format", oid, "NS3K_RN", ns3k_rn, "NS3K_RN ต้องเป็น Number")
   
                # 8.1.8. TABLE_NO ต้องเป็น Number และมีค่าเป็น 1 หรือ 2 หรือ 3 หรือ 41 หรือ 42 หรือ 5 หรือ 6 หรือ 7 เท่านั้น
                table_no = rec.get("TABLE_NO"); sub_no = rec.get("SUB_TABLE_NO")
                if not can_be_number(table_no) or int(float(table_no)) not in VALID_TABLE_NO:
                    write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "TABLE_NO", table_no, f"ต้องเป็น {sorted(VALID_TABLE_NO)} ")
                
                #  SUB_TABLE_NO ต้องเป็น Number และต้องมีค่าระหว่าง 0 – 6 หรือค่าว่าง เท่านั้น
                if sub_no is not None:
                    # ถ้าไม่ว่าง ต้องเป็น 0 หรือ 1-6

                    if not can_be_number(sub_no) or int(float(sub_no)) not in VALID_SUB_TABLE_NO:
                        write_error_report(error_list, gdb_path, fc_name, "Data Specified", oid, "SUB_TABLE_NO", sub_no, "ต้องเป็น 0 หรือ 1-6 ")
                
                # 8.1.10.  DEPTH_R ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 8.1.11. DEPTH_GROUP ต้องเป็น Number (เช็คจากประเภทข้อมูลแล้ว)
                # 8.1.12. START_X ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 8.1.13. START_Y ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 8.1.14. END_X ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง
                # 8.1.15. END_Y ต้องเป็น Number ต้องไม่ใช่ 0 หรือว่าง

                for fld in ("DEPTH_R","START_X","START_Y","END_X","END_Y"):
                    val = rec.get(fld)
                    if val is None or (can_be_number(val) and float(val)==0.0):
                        write_error_report(error_list, gdb_path, fc_name, "Data Required", oid, fld, val, f"{fld} จะต้องไม่ใช่ 0 หรือค่าว่าง")
            
            # 8.1.2. REL_RN ต้องเป็น Number และภายใน BRANCH_CODE เดียวกันจะต้องไม่ซ้ำกัน
            for key, oids in branch_rel_rn.items():
                if len(oids) > 1:
                    branch_key, rn_val = key
                    write_error_report(error_list, gdb_path, fc_name, "Duplicate Value", str(oids), "REL_RN", rn_val, f"REL_RN ซ้ำภายใน BRANCH_CODE '{branch_key}'")


    except Exception as ex:
        write_error_report(error_list, gdb_path, fc_name, "Cursor Error", -1, "", "", str(ex))

# ------------------------------
# ------------ MAIN ------------
# ------------------------------

def main():
    print("เริ่มต้นกระบวนการตรวจสอบมาตรฐาน...")

    validation_map = {
        "PARCEL": {"pattern": re.compile(r'^PARCEL_\d{2}_\d{2}$', re.IGNORECASE), "func": validate_parcel},
        "PARCEL_NS3K": {"pattern": re.compile(r'^PARCEL_\d{2}_NS3K_\d{2}$', re.IGNORECASE), "func": validate_parcel_ns3k},
        "ROAD": {"pattern": re.compile(r'^ROAD_\d{2}$', re.IGNORECASE), "func": validate_road},
        "BLOCK_FIX": {"pattern": re.compile(r'^BLOCK_FIX_\d{2}$', re.IGNORECASE), "func": validate_block_fix},
        "BLOCK_PRICE": {"pattern": re.compile(r'^BLOCK_PRICE_\d{2}$', re.IGNORECASE), "func": validate_block_price},
        "BLOCK_BLUE": {"pattern": re.compile(r'^BLOCK_BLUE_\d{2}$', re.IGNORECASE), "func": validate_block_blue},
        "PARCEL_REL": {"pattern": re.compile(r'^PARCEL_REL_\d{2}$', re.IGNORECASE), "func": validate_parcel_rel},
        "NS3K_REL": {"pattern": re.compile(r'^NS3K_REL_\d{2}$', re.IGNORECASE), "func": validate_ns3k_rel}
    }

    gdb_paths = find_gdb_paths(ROOT_DIR)
    if not gdb_paths:
        print("ไม่พบ GDBs ยกเลิกการดำเนินการ.")
        return

    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    run_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    gdb_report_dir = os.path.join(REPORT_ROOT, today_str)
    os.makedirs(gdb_report_dir, exist_ok=True)
    
    all_data_records = []
    error_summary_records = []


    for gdb in gdb_paths:
        print(f"\nกำลังดำเนินการ: {gdb}")
        
        gdb_error_list = []
        
        try:
            arcpy.env.workspace = gdb
            parent = os.path.basename(os.path.dirname(gdb))
            grandparent = os.path.basename(os.path.dirname(os.path.dirname(gdb)))
            basename = f"{grandparent}_{parent}"
            basename = re.sub(r'[\\/*?:"<>|]','_',basename)

            fcs_and_tables = (arcpy.ListFeatureClasses() or []) + (arcpy.ListTables() or [])
            if not fcs_and_tables:
                print("  ไม่พบฟิเจอร์คลาสใน GDB.")
                continue
            
            for fc in fcs_and_tables:
                fc_upper = fc.upper() 
                for key,meta in validation_map.items():
                    if meta["pattern"].match(fc_upper): 
                        fc_path = os.path.join(gdb, fc)
                        
                        # (Sheet 1: นับจำนวน - ยังใช้ gdb path เต็ม)
                        try:
                            count = int(arcpy.management.GetCount(fc_path)[0])
                            all_data_records.append([
                                run_timestamp,
                                gdb, 
                                fc,
                                count
                            ])
                        except Exception as e:
                            print(f"  !! ไม่สามารถนับจำนวน {fc} ได้: {e}")
                            all_data_records.append([
                                run_timestamp,
                                gdb, 
                                fc,
                                "Error"
                            ])
                        
                        # รัน Validator 
                        try:
                            meta["func"](fc_path, gdb_error_list, basename)
                        except Exception as e:
                            write_error_report(gdb_error_list, gdb, fc, "Validator Error", -1, "", "", str(e))
                        break
            
            if gdb_error_list:
                # ส่งรายงานเป็น excel แยกต่างหากสำหรับ GDB นี้
                report_path = os.path.join(gdb_report_dir, f"{basename}_error_report.xlsx") 
                try:
                    # 1. กำหนด Headers
                    headers = ['Timestamp','GDB_Path','Featureclass','Check_Type','Object_ID(s)','Field_Name','Invalid_Value','Message']
                    
                    # 2. สร้าง DataFrame
                    error_df_gdb = pd.DataFrame(gdb_error_list, columns=headers)
                    
                    # *** แปลง GDB_Path เป็นแบบย่อ ***
                    error_df_gdb['GDB_Path'] = error_df_gdb['GDB_Path'].apply(get_short_gdb_path)
                    
                    # 3. บันทึกเป็น Excel
                    with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
                        error_df_gdb.to_excel(writer, sheet_name='Errors', index=False)
                    
                    print(f"  -> รายงาน Excel ถูกบันทึก: {report_path} (พบ {len(gdb_error_list)} errors)")
                
                except Exception as e:
                     print(f"  !! ไม่สามารถเขียนรายงาน Excel ได้ {report_path}: {e}")
                
                # สรุป Error สำหรับ Sheet 2
                try:
                    error_df = pd.DataFrame(gdb_error_list, columns=['Timestamp', 'GDB_Path', 'Featureclass', 'Check_Type', 'Object_ID(s)', 'Field_Name', 'Invalid_Value', 'Message'])
                    summary_df = error_df.groupby(['GDB_Path', 'Featureclass', 'Check_Type']).size().reset_index(name='Count of Errors')
                    summary_df['Timestamp'] = run_timestamp
                    summary_df = summary_df[['Timestamp', 'GDB_Path', 'Featureclass', 'Check_Type', 'Count of Errors']]
                    error_summary_records.extend(summary_df.values.tolist())
                    
                except Exception as e:
                    print(f"  !! ไม่สามารถสรุป Error GDB นี้ได้: {e}")

            else:
                print(f"  -> ไม่พบข้อผิดพลาด (ไม่ต้องสร้างไฟล์สำหรับ {basename})")

            try:
                arcpy.management.Delete("in_memory")
            except Exception:
                pass

        except Exception as e:
            print(f"  Failed processing {gdb}: {e}")

    # *** เขียนรายงานสรุป Excel ***
    print(f"\nกำลังเขียนรายงานสรุป Excel ที่: {SUMMARY_EXCEL_PATH}")
    try:
        with pd.ExcelWriter(SUMMARY_EXCEL_PATH, engine='openpyxl') as writer:
            # Sheet 1: All_DATA
            if all_data_records:
                all_data_df = pd.DataFrame(all_data_records, columns=['Timestamp', 'GDB_Path', 'Featureclass', 'Count of Polygon or Polyline'])
                
                # *** แปลง Path ใน Sheet 1 ***
                all_data_df['GDB_Path'] = all_data_df['GDB_Path'].apply(get_short_gdb_path)
                
                all_data_df.to_excel(writer, sheet_name='All_DATA', index=False)
                print(f"  -> เขียน Sheet 'All_DATA' ({len(all_data_df)} แถว)")
            else:
                print("  -> ไม่มีข้อมูลสำหรับ 'All_DATA'")

            # Sheet 2: Error SUM
            if error_summary_records:
                error_sum_df = pd.DataFrame(error_summary_records, columns=['Timestamp', 'GDB_Path', 'Featureclass', 'Check_Type', 'Count of Errors'])
                
                # *** แปลง Path ใน Sheet 2 ***
                error_sum_df['GDB_Path'] = error_sum_df['GDB_Path'].apply(get_short_gdb_path)
                
                error_sum_df.to_excel(writer, sheet_name='Error SUM', index=False)
                print(f"  -> เขียน Sheet 'Error SUM' ({len(error_sum_df)} แถว)")
            else:
                print("  -> ไม่มีข้อมูลสำหรับ 'Error SUM'")
        
        print("  -> บันทึกไฟล์สรุป Excel เรียบร้อยแล้ว")

    except Exception as e:
        print(f"  !! ล้มเหลวในการเขียนไฟล์สรุป Excel: {e}")
        print("  !! (โปรดตรวจสอบว่าไฟล์ Excel ปิดอยู่ และคุณมีสิทธิ์เขียนทับ)")

    print("\nเสร็จแล้วจ้า ดูผลลัพธ์ได้เลยจ้า")

if __name__ == "__main__":
    main()