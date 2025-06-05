from data_access import db_handler
from processing import base_processor
from processing import table_01_processor
from processing import table_01_1_processor
from processing import table_01_2_processor
import pandas as pd
import os
import configparser


def get_current_year_from_config():
    """อ่านค่า CURRENT_YEAR จาก config.ini."""
    config = configparser.ConfigParser()
    config_path = os.path.join("config", "config.ini")
    try:
        config.read(config_path, encoding="utf-8")
        current_year = int(config["PROCESSING"]["CURRENT_YEAR"])
        return current_year
    except Exception as e:
        # print(f"!!! เกิดข้อผิดพลาดในการอ่าน config.ini: {e}")
        return None


def write_excel_formatted(df, filename, current_year, table_title_th, table_title_en):
    """บันทึก DataFrame เป็นไฟล์ Excel พร้อมจัดรูปแบบตาม Template."""
    # (โค้ดส่วนนี้เหมือนเดิมกับที่คุณมีอยู่ ไม่ต้องเปลี่ยนแปลง)
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    filepath = os.path.join(output_dir, filename)

    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")
    workbook = writer.book
    worksheet = workbook.add_worksheet("Sheet1")  # แก้ไขเรื่อง KeyError แล้ว

    # --- สร้าง Formats ---
    title_format = workbook.add_format(
        {"bold": True, "font_size": 11, "font_name": "TH SarabunPSK"}
    )
    unit_format = workbook.add_format(
        {"align": "right", "font_name": "TH SarabunPSK", "font_size": 11}
    )
    header_base = {
        "align": "center",
        "valign": "vcenter",
        "border": 1,
        "text_wrap": True,
        "font_name": "TH SarabunPSK",
        "font_size": 11,
    }
    wrap_center_format = workbook.add_format(header_base)
    bold_center_format = workbook.add_format({**header_base, "bold": True})
    number_format = workbook.add_format(
        {
            "num_format": "#,##0.00",
            # "border": 1,
            "font_name": "TH SarabunPSK",
            "font_size": 11,
        }
    )
    text_format = workbook.add_format(
        {
            # "border": 1,
            "font_name": "TH SarabunPSK",
            "font_size": 11,
            "valign": "vcenter",
        }
    )
    zero_format = workbook.add_format(
        {
            "num_format": '"-"',
            # "border": 1,
            "align": "right",
            "font_name": "TH SarabunPSK",
            "font_size": 11,
        }
    )
    col_num_format = workbook.add_format(
        {
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "font_name": "TH SarabunPSK",
            "font_size": 11,
        }
    )

    # เพิ่ม format สำหรับส่วนท้าย
    footer_format = workbook.add_format(
        {"font_name": "TH SarabunPSK", "font_size": 11, "align": "left"}
    )

    # --- ตั้งค่าความกว้างคอลัมน์ ---
    worksheet.set_column("A:A", 25)
    worksheet.set_column("B:B", 12)
    worksheet.set_column("C:C", 15)
    worksheet.set_column("D:K", 20)

    # --- เขียนหัวเรื่อง ---
    worksheet.merge_range("A1:K1", f"{table_title_th} {current_year}", title_format)
    worksheet.merge_range("A2:K2", f"{table_title_en}", title_format)
    # *** อาจจะต้องปรับ Unit ตามประเภทตาราง ***
    worksheet.write("K3", "(พันบาท : In thousand baht)", unit_format)

    # --- เขียนหัวตาราง ---
    # (เหมือนเดิม)
    worksheet.merge_range(
        "A5:A16", "ขนาดของสถานประกอบการ\n(จำนวนคนทำงาน)", wrap_center_format
    )
    worksheet.merge_range("B5:B16", "ไตรมาส / ปี\nQuarter / Year", wrap_center_format)
    worksheet.merge_range("C5:C16", "รวม\nTotal", wrap_center_format)
    worksheet.merge_range("D5:D16", "การขายปลีก\nRetail Trade", wrap_center_format)
    worksheet.merge_range("E5:E16", "ที่พักแรม\nAccommodation", wrap_center_format)
    worksheet.merge_range(
        "F5:F16",
        "การบริการอาหารและเครื่องดื่ม\nFood and Beverage Service",
        wrap_center_format,
    )
    worksheet.merge_range(
        "G5:G16",
        "การผลิตภาพยนตร์ วีดิทัศน์ และรายการโทรทัศน์ การบันทึกเสียงลงบนสื่อ  การจัดผังรายการและการแพร่ภาพกระจายเสียง และกิจกรรมสำนักข่าว\nMotion Picture, Video and Television Programme Production, Sound Recording, Programming and Broadcasting and News Agency Activities",
        wrap_center_format,
    )
    worksheet.merge_range(
        "H5:H16",
        "การให้เช่าของใช้ส่วนบุคคลและของใช้ในครัวเรือน และกิจกรรมการคัดเลือกนักแสดงภาพยนตร์  โทรทัศน์ และการแสดงอื่นๆ\nRenting and Leasing of Personal and Household Goods and Activities of Casting Agencies and Bureaus",
        wrap_center_format,
    )
    worksheet.merge_range(
        "I5:I16",
        "กิจกรรมศิลปะ ความบันเทิงและนันทนาการ\nArts, Entertainment and Recreation Activities",
        wrap_center_format,
    )
    worksheet.merge_range(
        "J5:J16",
        "การซ่อมแซมส่วนบุคคล และของใช้ในครัวเรือน และกิจกรรมการบริการส่วนบุคคลอื่นๆ\nRepair of Personal and Household Goods and Other Personal Service Activities",
        wrap_center_format,
    )
    worksheet.merge_range(
        "K5:K16",
        "Size of Establishment\n(Number of Persons Engaged)",
        wrap_center_format,
    )

    for col_num in range(11):
        worksheet.write(16, col_num, f"({col_num + 1})", col_num_format)

    # --- เขียนข้อมูล ---
    start_row = 17
    for row_num, row_data in df.iterrows():
        worksheet.write(start_row + row_num, 0, row_data["col1"], text_format)
        worksheet.write(start_row + row_num, 1, row_data["col2"], text_format)
        for col_idx in range(2, 10):
            value = row_data[f"col{col_idx+1}"]
            if pd.isna(value) or value == 0:
                worksheet.write(start_row + row_num, col_idx, 0, zero_format)
            else:
                worksheet.write(start_row + row_num, col_idx, value, number_format)
        worksheet.write(start_row + row_num, 10, row_data["col11"], text_format)

    # --- เขียนเชิงอรรถ ---
    last_row = start_row + len(df)
    worksheet.merge_range(
        last_row + 2,
        0,
        last_row + 2,
        10,
        f"ที่มา : การสำรวจยอดขายรายไตรมาส พ.ศ.{current_year} สำนักงานสถิติแห่งชาติ กระทรวงดิจิทัลเพื่อเศรษฐกิจและสังคม",
        footer_format,
    )
    worksheet.merge_range(
        last_row + 3,
        0,
        last_row + 3,
        10,
        f"Source : The {current_year} Quarterly Retail Survey : National Statistical Office, Ministry of Digital Economy and Society",
        footer_format,
    )

    writer.close()
    # print(f"บันทึกไฟล์ {filepath} สำเร็จ")


def main():
    """ฟังก์ชันหลักของโปรแกรม."""
    # print("เริ่มต้นการประมวลผล...")
    current_year = get_current_year_from_config()
    if current_year is None:
        # print("...ยุติการทำงาน...")
        return

    previous_year = current_year - 1
    years = [previous_year, current_year]
    quarters = [1, 2, 3, 4]
    data_frames = {}

    # print(f"กำลังประมวลผลสำหรับปี: {previous_year} และ {current_year}")

    # ดึงข้อมูล (เหมือนเดิม)
    for yr in years:
        for qtr in quarters:
            foxpro_yr = str(yr)[-2:]
            key = f"qtr{qtr}_{foxpro_yr}"
            db_year_to_query = int(foxpro_yr)
            data_frames[key] = db_handler.fetch_data_by_quarter(db_year_to_query, qtr)
            # if data_frames[key] is None:
            # print(f"*** คำเตือน: ไม่สามารถดึงข้อมูลจาก YR={db_year_to_query}, QTR={qtr} ได้ ***")

    try:
        # 1. สร้าง Array a และ b
        # print("กำลัง Aggregate ข้อมูล...")
        a, b = base_processor.aggregate_data(data_frames, current_year, previous_year)

        # 2. สร้าง DataFrame สำหรับแต่ละตาราง
        # print("กำลังสร้าง DataFrame ตาราง 1...")
        df_tab1 = table_01_processor.create_table_01_df(b, a, current_year)
        # print("กำลังสร้าง DataFrame ตาราง 1_1...")
        df_tab1_1 = table_01_1_processor.create_table_01_1_df(b, a, current_year)
        # print("กำลังสร้าง DataFrame ตาราง 1_2...")
        df_tab1_2 = table_01_2_processor.create_table_01_2_df(b, a, current_year)

        # print("ประมวลผลข้อมูลสำเร็จ")

        # 3. บันทึกผลลัพธ์ (เรียกใช้ฟังก์ชันเดิม แต่ด้วย DataFrame ที่แยกกันสร้าง)
        # print("กำลังบันทึก Part1_table01...")
        write_excel_formatted(
            df_tab1,
            f"Part1_table01_{current_year}.xlsx",
            current_year,
            "ตาราง 1 ยอดขายรายไตรมาส จำแนกตามขนาดสถานประกอบการ (จำนวนคนทำงาน) และประเภทอุตสาหกรรม",
            "TABLE 1 QUARTERLY TURNOVER, BY SIZE OF ESTABLISHMENT (NUMBER OF PERSONS ENGAGED) AND DIVISION OF INDUSTRY",
        )

        # print("กำลังบันทึก Part1_table01_1...")
        # write_excel_formatted(df_tab1_1, f'Part1_table01_1_{current_year}.xlsx', current_year,
        #                       'ตาราง 1.1 อัตราการเปลี่ยนแปลงของยอดขายรายไตรมาส (QoQ) ...',
        #                       'TABLE 1.1 QUARTERLY TURNOVER PERCENTAGE CHANGE (QoQ) ...')

        # print("กำลังบันทึก Part1_table01_2...")
        # write_excel_formatted(df_tab1_2, f'Part1_table01_2_{current_year}.xlsx', current_year,
        #                       'ตาราง 1.2 อัตราการเปลี่ยนแปลงของยอดขายรายไตรมาส (YoY) ...',
        #                       'TABLE 1.2 QUARTERLY TURNOVER PERCENTAGE CHANGE (YoY) ...')

    except Exception as e:
        # print(f"เกิดข้อผิดพลาดร้ายแรงระหว่างการประมวลผล: {e}")
        import traceback

        traceback.print_exc()

    # print("การประมวลผลเสร็จสิ้น")


if __name__ == "__main__":
    main()
