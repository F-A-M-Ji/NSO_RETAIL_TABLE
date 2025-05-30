import pandas as pd
from .base_processor import get_description # Import ฟังก์ชันที่ใช้ร่วมกัน

def create_table_01_df(b, a, current_year):
    """สร้าง DataFrame สำหรับ Part1_table01."""
    rows = []
    col_names = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10', 'col11']

    for i in range(28): # i คือ 0-based index
        row_data = {}
        col1_val, col2_val, col11_val = get_description(i + 1, current_year)
        row_data['col1'] = col1_val
        row_data['col2'] = col2_val
        row_data['col11'] = col11_val

        # คำนวณ col3 - col10 (index 2-9)
        for j in range(3, 11): # j คือ 3-10
            idx = j - 1 # idx คือ 2-9
            row_data[f'col{j}'] = b[i, idx] / 1000.0

        rows.append(row_data)

    df = pd.DataFrame(rows, columns=col_names)
    return df