import pandas as pd
from .base_processor import get_description # Import ฟังก์ชันที่ใช้ร่วมกัน

def create_table_01_1_df(b, a, current_year):
    """สร้าง DataFrame สำหรับ Part1_table01_1."""
    rows = []
    col_names = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10', 'col11']

    for i in range(28): # i คือ 0-based index
        row_data = {}
        col1_val, col2_val, col11_val = get_description(i + 1, current_year)
        row_data['col1'] = col1_val
        row_data['col2'] = col2_val
        row_data['col11'] = col11_val

        rr_map = {1: 4, 5: 8, 9: 12, 13: 16, 17: 20, 21: 24, 25: 28}
        is_base = (i + 1) in [1, 5, 9, 13, 17, 21, 25]

        for j in range(3, 11): # j คือ 3-10
            idx = j - 1 # idx คือ 2-9
            value = 0.0

            if is_base:
                rr = rr_map[i + 1] - 1 # 0-based index
                divisor = a[rr, idx]
                current_val = b[i, idx]
            else:
                divisor = b[i - 1, idx] # ใช้ b ของแถวก่อนหน้า
                current_val = b[i, idx]

            if divisor != 0:
                value = ((current_val - divisor) / divisor) * 100.0

            row_data[f'col{j}'] = value

        rows.append(row_data)

    df = pd.DataFrame(rows, columns=col_names)
    return df