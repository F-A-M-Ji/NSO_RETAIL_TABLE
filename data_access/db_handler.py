import pyodbc
import pandas as pd
import configparser
import os

# ฟังก์ชัน get_config() และ get_db_connection() เหมือนเดิม ...

def get_config():
    """อ่านค่า Config ทั้งหมดจาก config.ini."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"ไม่พบไฟล์ Config ที่: {config_path}")
    config.read(config_path, encoding='utf-8')
    return config

def get_db_connection():
    """สร้างและคืนค่า Connection object ไปยัง SQL Server."""
    config = get_config()
    db_config = config['SQL_SERVER']

    server = db_config['SERVER']
    database = db_config['DATABASE']
    username = db_config['USERNAME']
    password = db_config['PASSWORD']
    trusted = db_config['TRUSTED_CONNECTION']
    driver = db_config['DRIVER']

    try:
        if trusted.lower() == 'yes':
            conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        else:
            conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};'

        conn = pyodbc.connect(conn_str)
        # print("เชื่อมต่อ SQL Server สำเร็จ!")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        # print(f"เกิดข้อผิดพลาดในการเชื่อมต่อ SQL Server: {sqlstate}")
        # print(ex)
        return None

def fetch_data_by_quarter(year, quarter):
    """ดึงข้อมูลจากตาราง Report และลบแถวที่มี NULL ในคอลัมน์ที่จำเป็น."""
    conn = get_db_connection()
    if conn:
        try:
            sql_query = """
                SELECT ENU, SIZE_R, [TYPE], TR, WWKNSO
                FROM Report
                WHERE YR = ? AND QTR = ?
            """
            df = pd.read_sql(sql_query, conn, params=(year, quarter))
            # print(f"ดึงข้อมูลจาก Report (YR={year}, QTR={quarter}) สำเร็จ ({len(df)} แถว)")

            # --- ส่วนที่แก้ไข ---
            initial_rows = len(df)
            # กำหนดคอลัมน์ที่ *ต้อง* มีข้อมูล (ห้ามเป็น NULL)
            critical_cols = ['TYPE', 'TR', 'WWKNSO']
            
            # ลบแถวที่มีค่า NULL ในคอลัมน์ที่กำหนด
            df.dropna(subset=critical_cols, inplace=True)
            
            rows_after_drop = len(df)
            # print(f"  -> หลังตัดแถวที่มี NULL ใน {critical_cols} เหลือ {rows_after_drop} แถว (จาก {initial_rows})")

            # ถ้าไม่มีข้อมูลเหลือเลย ก็คืนค่า DataFrame ว่างไป
            if df.empty:
                return df

            # จัดการข้อมูลเบื้องต้น (เฉพาะแถวที่เหลือ ซึ่งไม่มี NULL แล้ว)
            df['TR'] = pd.to_numeric(df['TR'], errors='coerce')
            df['WWKNSO'] = pd.to_numeric(df['WWKNSO'], errors='coerce')
            
            # ลบอีกครั้ง เผื่อ to_numeric สร้าง NaN (ถ้าข้อมูลเดิมไม่ใช่ตัวเลข)
            df.dropna(subset=['TR', 'WWKNSO'], inplace=True)
            # print(f"  -> หลังตัดแถวที่แปลงเป็นตัวเลขไม่ได้ เหลือ {len(df)} แถว")

            if df.empty:
                return df

            df['TYPE'] = df['TYPE'].astype(int).astype(str) # แปลงได้เลยเพราะไม่มี NULL แล้ว
            df['ENU'] = df['ENU'].astype(str).str.strip()
            df['SIZE_R'] = df['SIZE_R'].astype(str).str.strip()
            # --- จบส่วนที่แก้ไข ---

            return df
        except Exception as e:
            # print(f"เกิดข้อผิดพลาดในการดึงข้อมูล (YR={year}, QTR={quarter}): {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            conn.close()
            # print(f"ปิดการเชื่อมต่อ SQL Server (YR={year}, QTR={quarter})")
    else:
        return None