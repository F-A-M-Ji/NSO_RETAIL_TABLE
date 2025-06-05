import numpy as np
import pandas as pd

def process_data_revised(df, array, quarter_offset):
    """ประมวลผล DataFrame และอัปเดต Array ตามไตรมาส."""
    if df is None or df.empty:
        return

    for index, row in df.iterrows():
        try:
            value = float(row['TR']) * float(row['WWKNSO'])
        except (ValueError, TypeError):
            continue

        enu = row['ENU']
        size_r = row['SIZE_R']
        typ = row['TYPE']

        typ_map = {'1': 3, '2': 4, '3': 5, '4': 6, '5': 7, '6': 8, '7': 9}
        col_index = typ_map.get(typ)

        if col_index is None or enu != '01':
            continue

        base_row_index = -1
        if size_r in ('01', '02', '03'): base_row_index = 4
        elif size_r in ('04', '05'):     base_row_index = 8
        elif size_r == '06':             base_row_index = 12
        elif size_r == '07':             base_row_index = 16
        elif size_r in ('08', '09'):     base_row_index = 20
        elif size_r in ('10', '11', '12'): base_row_index = 24

        array[0 + quarter_offset, col_index] += value
        if base_row_index != -1:
            array[base_row_index + quarter_offset, col_index] += value

def get_description(i, current_year):
    """คืนค่า col1, col2, col11 ตาม i (1-based) โดยใช้ปีปัจจุบัน."""
    col1, col2, col11 = '', '', ''
    qtr_map = {
        1: f'1/{current_year}', 2: f'2/{current_year}',
        3: f'3/{current_year}', 4: f'4/{current_year}'
    }
    size_map = {
        5: ('1 - 15 คน', '1 - 15 Persons'), 9: ('16 - 25 คน', '16 - 25 Persons'),
        13: ('26 - 30 คน', '26 - 30 Persons'), 17: ('31 - 50 คน', '31 - 50 Persons'),
        21: ('51 - 200 คน', '51 - 200 Persons'), 25: ('มากกว่า 200 คน', 'More than 200 Persons'),
    }

    if i == 1: col1, col2, col11 = 'รวม', f'1/{current_year}', 'Total'
    elif i in size_map: col1, col11 = size_map[i]; col2 = f'1/{current_year}'
    else:
        group_start_row = max(k for k in [1, 5, 9, 13, 17, 21, 25] if k <= i)
        qtr_index = i - group_start_row + 1
        col2 = qtr_map.get(qtr_index, '')
        if i not in size_map: col1 = ''; col11 = ''

    return col1, col2, col11

def aggregate_data(data_frames, current_year, previous_year):
    """ฟังก์ชันหลักในการสร้าง Array a และ b."""
    a = np.zeros((28, 11))
    b = np.zeros((28, 11))

    prev_yr_key = str(previous_year)[-2:]
    curr_yr_key = str(current_year)[-2:]

    # ประมวลผลปีก่อนหน้า (a)
    for i in range(4):
        df_key = f'qtr{i+1}_{prev_yr_key}'
        process_data_revised(data_frames.get(df_key), a, i)

    # ประมวลผลปีปัจจุบัน (b)
    for i in range(4):
        df_key = f'qtr{i+1}_{curr_yr_key}'
        process_data_revised(data_frames.get(df_key), b, i)

    # คำนวณผลรวม (คอลัมน์ 3 -> index 2)
    a[:, 2] = np.sum(a[:, 3:10], axis=1) # Sum index 3 ถึง 9
    b[:, 2] = np.sum(b[:, 3:10], axis=1)

    return a, b