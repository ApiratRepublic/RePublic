V1 (2025-10-31)
This python script is for my friends at PSVD
(Psychosis Sanctified of Voiceless Despair)

## จุดประสงค์
สำหรับเช็คว่า GDB มี Featureclass ที่กำหนดครบหรือไม่
และมีฟิลด์ที่กำหนดครบหรือไม่
แต่ละฟิลด์ ใส่ค่าตรงตามรูปแบบหรือไม่

โปรดอ่าน logic สำหรับการตรวจมาตรฐาน ประกอบ

## สิ่งที่ต้องมี
ไพธอนสคริปต์นี้ ต้องใช้ Arcpy ในการรัน

## การตั้งค่า
-	gdb ทั้งหมด เก็บไว้ใน root directory เดียวกัน
-	ค่าที่ต้องกำหนดในตอนต้นของไฟล์
	-	ROOT_DIR = ที่รวมไฟล์ GDB
	-	REPORT_ROOT = ที่เก็บรายงานผลเป็นเอ็กเซล
	-	OVERLAP_ROOT = ที่เก็บไฟล์ผลการตรวจสอบทับซ้อน
	-	SUMMARY_SUMMARY_EXCEL_PATH = ใส่พาร์ธไฟล์เอ็กเซลสรุปรายงานรวม