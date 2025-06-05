import pandas as pd
import numpy as np
import os
from data_access.db_handler import fetch_data, execute_query, execute_many_query
from config.app_config import PARAMS_CONFIG, BIGN_CSV_FILE, OUTPUT_DIR, DB_CONFIG

TARGET_TABLE_NAME = DB_CONFIG["table_name"]
TSIC_LENGTH = PARAMS_CONFIG["TSIC_LENGTH"]
OUTPUT_COLUMN_NAME_5DIGIT = "WWKNESDB"


def substr_py(series, start_index_1, length):
    return series.astype(str).str.slice(start_index_1 - 1, start_index_1 - 1 + length)


def load_bign_csv():
    filepath = BIGN_CSV_FILE
    try:
        dtype_spec = {"tsic": str, "size12": int, "big_n": float}
        df = pd.read_csv(filepath, dtype=dtype_spec)
        return df
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()


def initialize_calculate_dataframe(df_bign_input):
    if df_bign_input.empty:
        return pd.DataFrame()

    unique_tsic_codes = sorted(list(df_bign_input["tsic"].astype(str).unique()))
    expected_unique_tsic = 39 if TSIC_LENGTH == 5 else 56

    columns_def = {
        "tsic": str,
        "big_n1": float,
        "n1": float,
        "np1": float,
        "npp1": float,
        "big_np1": float,
        "w1": float,
        "blank1": str,
        "big_n2": float,
        "n2": float,
        "np2": float,
        "npp2": float,
        "big_np2": float,
        "w2": float,
        "blank2": str,
        "big_n3": float,
        "n3": float,
        "np3": float,
        "npp3": float,
        "big_np3": float,
        "w3": float,
        "blank3": str,
        "big_n4": float,
        "n4": float,
        "np4": float,
        "npp4": float,
        "big_np4": float,
        "w4": float,
        "blank4": str,
        "big_n5": float,
        "n5": float,
        "np5": float,
        "npp5": float,
        "big_np5": float,
        "w5": float,
        "blank5": str,
        "big_n6": float,
        "n6": float,
        "np6": float,
        "npp6": float,
        "big_np6": float,
        "w6": float,
        "blank6": str,
        "big_n7": float,
        "n7": float,
        "np7": float,
        "npp7": float,
        "big_np7": float,
        "w7": float,
        "blank7": str,
        "big_n8": float,
        "n8": float,
        "np8": float,
        "npp8": float,
        "big_np8": float,
        "w8": float,
        "blank8": str,
        "big_n9": float,
        "n9": float,
        "np9": float,
        "npp9": float,
        "big_np9": float,
        "w9": float,
        "blank9": str,
        "big_n10": float,
        "n10": float,
        "np10": float,
        "npp10": float,
        "big_np10": float,
        "w10": float,
        "blank10": str,
        "big_n11": float,
        "n11": float,
        "np11": float,
        "npp11": float,
        "big_np11": float,
        "w11": float,
        "blank11": str,
        "big_n12": float,
        "n12": float,
        "np12": float,
        "npp12": float,
        "big_np12": float,
        "w12": float,
        "blank12": str,
    }
    df_calculate_temp = pd.DataFrame(columns=columns_def.keys())
    df_calculate_temp["tsic"] = unique_tsic_codes

    for col, dtype_val in columns_def.items():
        if col == "tsic":
            continue
        if dtype_val == float:
            df_calculate_temp[col] = 0.0
        elif dtype_val == str:
            df_calculate_temp[col] = ""
    return df_calculate_temp


def perform_main_calculations(conn, df_calculate_temp, df_bign_input):
    yr_val = PARAMS_CONFIG["YR"]
    qtr_val = PARAMS_CONFIG["QTR"]

    if df_calculate_temp.empty:
        return df_calculate_temp

    report_cols_needed = "ENU, SIZE_L, TSIC_L, QTR, YR, SIZE_R, TSIC_R"
    query_report = (
        f"SELECT {report_cols_needed} FROM {TARGET_TABLE_NAME} WHERE YR = ? AND QTR = ?"
    )
    df_report = fetch_data(conn, query_report, params=(yr_val, qtr_val))

    if df_report.empty:
        print(
            f"Warning: No data fetched from {TARGET_TABLE_NAME} for YR={yr_val}, QTR={qtr_val}. Counts will be zero."
        )
    else:
        for col_name in ["ENU", "SIZE_L", "TSIC_L", "QTR", "YR", "SIZE_R", "TSIC_R"]:
            if col_name in df_report.columns:
                df_report[col_name] = df_report[col_name].astype(str)
        df_report[f"TSIC_L_{TSIC_LENGTH}"] = substr_py(
            df_report["TSIC_L"].astype(str), 1, TSIC_LENGTH
        )
        df_report[f"TSIC_R_{TSIC_LENGTH}"] = substr_py(
            df_report["TSIC_R"].astype(str), 1, TSIC_LENGTH
        )

    for index, row in df_calculate_temp.iterrows():
        current_tsic = row["tsic"]
        x_big_n = [0.0] * 13

        for i in range(1, 13):
            filtered_bign = df_bign_input[
                (df_bign_input["tsic"].astype(str) == current_tsic)
                & (df_bign_input["size12"] == i)
            ]
            if not filtered_bign.empty:
                x_big_n[i] = filtered_bign["big_n"].iloc[0]
        for i in range(1, 13):
            df_calculate_temp.loc[index, f"big_n{i}"] = x_big_n[i]

        x_n, x_np = [0.0] * 13, [0.0] * 13
        if not df_report.empty:
            for i in range(1, 13):
                size_l_str = str(i).zfill(2)
                x_n[i] = df_report[
                    (df_report["ENU"] == "01")
                    & (df_report["SIZE_L"] == size_l_str)
                    & (df_report[f"TSIC_L_{TSIC_LENGTH}"] == current_tsic)
                    & (df_report["YR"] == yr_val)
                    & (df_report["QTR"] == qtr_val)
                ].shape[0]

                size_r_str = str(i).zfill(2)
                x_np[i] = df_report[
                    (df_report["ENU"] == "01")
                    & (df_report["SIZE_R"] == size_r_str)
                    & (df_report[f"TSIC_R_{TSIC_LENGTH}"] == current_tsic)
                    & (df_report["YR"] == yr_val)
                    & (df_report["QTR"] == qtr_val)
                ].shape[0]

        for i in range(1, 13):
            df_calculate_temp.loc[index, f"n{i}"] = x_n[i]
            df_calculate_temp.loc[index, f"np{i}"] = x_np[i]
            nppi = x_np[i] - x_n[i]
            df_calculate_temp.loc[index, f"npp{i}"] = nppi
            big_npi = x_big_n[i] + nppi
            df_calculate_temp.loc[index, f"big_np{i}"] = big_npi

            wi = 0.0
            if big_npi == 0 and x_np[i] == 0:
                wi = 0.0
            elif big_npi == 1 and x_np[i] == 1:
                wi = 1.0
            elif big_npi == 1 and x_np[i] == 0:
                wi = 9999.9999
            elif big_npi >= 2 and x_np[i] >= 1:
                wi = big_npi / x_np[i]
            elif x_np[i] == 0:
                wi = 0.0

            if wi != 9999.9999:
                df_calculate_temp.loc[index, f"w{i}"] = round(wi, 4)
            else:
                df_calculate_temp.loc[index, f"w{i}"] = wi

    return df_calculate_temp


def apply_weight_adjustments_step6(df_calculate_temp):
    if df_calculate_temp.empty:
        return df_calculate_temp
    for index, row in df_calculate_temp.iterrows():
        w = [0.0] + [row[f"w{i}"] for i in range(1, 13)]
        big_np = [0.0] + [row[f"big_np{i}"] for i in range(1, 13)]
        np_val = [0.0] + [row[f"np{i}"] for i in range(1, 13)]

        def safe_div_round(num, den, default_val=0.0, round_digits=4):
            if den != 0:
                res = num / den
                return round(res, round_digits) if res != 9999.9999 else res
            return default_val

        temp_w1, temp_w2, temp_w3 = w[1], w[2], w[3]
        if (
            w[1] == 9999.9999
            and (w[2] > 0 and w[2] != 9999.9999)
            and ((w[3] > 0 and w[3] != 9999.9999) or w[3] == 0)
        ):
            new_w = safe_div_round((big_np[1] + big_np[2]), (np_val[1] + np_val[2]))
            temp_w1, temp_w2 = new_w, new_w
        elif (w[1] == 9999.9999 and w[2] == 0 and (w[3] > 0 and w[3] != 9999.9999)) or (
            (w[1] > 0 and w[1] != 9999.9999) and w[2] == 0 and w[3] == 9999.9999
        ):
            new_w = safe_div_round((big_np[1] + big_np[3]), (np_val[1] + np_val[3]))
            temp_w1, temp_w3 = new_w, new_w
        elif (w[1] > 0 and w[1] != 9999.9999) and w[2] == 9999.9999 and w[3] == 0:
            new_w = safe_div_round((big_np[1] + big_np[2]), (np_val[1] + np_val[2]))
            temp_w1, temp_w2 = new_w, new_w
        elif w[1] == 0 and w[2] == 9999.9999 and (w[3] > 0 and w[3] != 9999.9999):
            new_w = safe_div_round((big_np[2] + big_np[3]), (np_val[2] + np_val[3]))
            temp_w2, temp_w3 = new_w, new_w
        elif (
            (w[1] > 0 and w[1] != 9999.9999)
            and (w[2] > 0 and w[2] != 9999.9999)
            and w[3] == 9999.9999
        ) or (w[1] == 0 and (w[2] > 0 and w[2] != 9999.9999) and w[3] == 9999.9999):
            new_w = safe_div_round((big_np[2] + big_np[3]), (np_val[2] + np_val[3]))
            temp_w2, temp_w3 = new_w, new_w
        elif (
            (
                (w[1] > 0 and w[1] != 9999.9999)
                and w[2] == 9999.9999
                and (w[3] > 0 and w[3] != 9999.9999)
            )
            or (
                (w[1] > 0 and w[1] != 9999.9999)
                and w[2] == 9999.9999
                and w[3] == 9999.9999
            )
            or (
                w[1] == 9999.9999
                and (w[2] > 0 and w[2] != 9999.9999)
                and w[3] == 9999.9999
            )
            or (
                w[1] == 9999.9999
                and w[2] == 9999.9999
                and (w[3] > 0 and w[3] != 9999.9999)
            )
        ):
            new_w = safe_div_round(
                (big_np[1] + big_np[2] + big_np[3]), (np_val[1] + np_val[2] + np_val[3])
            )
            temp_w1, temp_w2, temp_w3 = new_w, new_w, new_w
        (
            df_calculate_temp.loc[index, "w1"],
            df_calculate_temp.loc[index, "w2"],
            df_calculate_temp.loc[index, "w3"],
        ) = (temp_w1, temp_w2, temp_w3)

        temp_w4, temp_w5 = w[4], w[5]
        if (w[4] == 9999.9999 and np_val[5] != 0) or (
            w[5] == 9999.9999 and np_val[4] != 0
        ):
            new_w = safe_div_round((big_np[4] + big_np[5]), (np_val[4] + np_val[5]))
            temp_w4, temp_w5 = new_w, new_w
        df_calculate_temp.loc[index, "w4"], df_calculate_temp.loc[index, "w5"] = (
            temp_w4,
            temp_w5,
        )

        temp_w8, temp_w9 = w[8], w[9]
        if (w[8] == 9999.9999 and np_val[8] != 0) or (
            w[9] == 9999.9999 and np_val[9] != 0
        ):
            new_w = safe_div_round((big_np[8] + big_np[9]), (np_val[8] + np_val[9]))
            temp_w8, temp_w9 = new_w, new_w
        df_calculate_temp.loc[index, "w8"], df_calculate_temp.loc[index, "w9"] = (
            temp_w8,
            temp_w9,
        )

        temp_w10, temp_w11, temp_w12 = w[10], w[11], w[12]
        if (
            w[10] == 9999.9999
            and (w[11] > 0 and w[11] != 9999.9999)
            and ((w[12] > 0 and w[12] != 9999.9999) or w[12] == 0)
        ):
            new_w = safe_div_round((big_np[10] + big_np[11]), (np_val[10] + np_val[11]))
            temp_w10, temp_w11 = new_w, new_w
        elif (
            w[10] == 9999.9999 and w[11] == 0 and (w[12] > 0 and w[12] != 9999.9999)
        ) or ((w[10] > 0 and w[10] != 9999.9999) and w[11] == 0 and w[12] == 9999.9999):
            new_w = safe_div_round((big_np[10] + big_np[12]), (np_val[10] + np_val[12]))
            temp_w10, temp_w12 = new_w, new_w
        elif (w[10] > 0 and w[10] != 9999.9999) and w[11] == 9999.9999 and w[12] == 0:
            new_w = safe_div_round((big_np[10] + big_np[11]), (np_val[10] + np_val[11]))
            temp_w10, temp_w11 = new_w, new_w
        elif w[10] == 0 and w[11] == 9999.9999 and (w[12] > 0 and w[12] != 9999.9999):
            new_w = safe_div_round((big_np[11] + big_np[12]), (np_val[11] + np_val[12]))
            temp_w11, temp_w12 = new_w, new_w
        elif (
            (w[10] > 0 and w[10] != 9999.9999)
            and (w[11] > 0 and w[11] != 9999.9999)
            and w[12] == 9999.9999
        ) or (w[10] == 0 and (w[11] > 0 and w[11] != 9999.9999) and w[12] == 9999.9999):
            new_w = safe_div_round((big_np[11] + big_np[12]), (np_val[11] + np_val[12]))
            temp_w11, temp_w12 = new_w, new_w
        elif (
            (
                (w[10] > 0 and w[10] != 9999.9999)
                and w[11] == 9999.9999
                and (w[12] > 0 and w[12] != 9999.9999)
            )
            or (
                (w[10] > 0 and w[10] != 9999.9999)
                and w[11] == 9999.9999
                and w[12] == 9999.9999
            )
            or (
                w[10] == 9999.9999
                and (w[11] > 0 and w[11] != 9999.9999)
                and w[12] == 9999.9999
            )
            or (
                w[10] == 9999.9999
                and w[11] == 9999.9999
                and (w[12] > 0 and w[12] != 9999.9999)
            )
        ):
            new_w = safe_div_round(
                (big_np[10] + big_np[11] + big_np[12]),
                (np_val[10] + np_val[11] + np_val[12]),
            )
            temp_w10, temp_w11, temp_w12 = new_w, new_w, new_w
        (
            df_calculate_temp.loc[index, "w10"],
            df_calculate_temp.loc[index, "w11"],
            df_calculate_temp.loc[index, "w12"],
        ) = (temp_w10, temp_w11, temp_w12)

    return df_calculate_temp


def perform_validation_and_update_report(conn, df_calculate_temp):
    yr_val = PARAMS_CONFIG["YR"]
    qtr_val = PARAMS_CONFIG["QTR"]

    output_column_name = OUTPUT_COLUMN_NAME_5DIGIT if TSIC_LENGTH == 5 else "WWKNSO"

    if df_calculate_temp.empty:
        return False

    validation_error_flag = False
    for index, row in df_calculate_temp.iterrows():
        for i in range(1, 13):
            if row[f"big_n{i}"] < row[f"n{i}"]:
                df_calculate_temp.loc[index, f"blank{i}"] = "*"
                validation_error_flag = True

    output_csv_filename = f"calculate_{TSIC_LENGTH}digit_YR{yr_val}_QTR{qtr_val}.csv"
    output_csv_path = os.path.join(OUTPUT_DIR, output_csv_filename)
    try:
        df_calculate_temp.to_csv(
            output_csv_path, index=False, encoding="utf-8-sig", float_format="%.4f"
        )
    except Exception as e:
        print(f"Error saving df_calculate_temp to CSV {output_csv_path}: {e}")

    update_query_reset_w = f"UPDATE {TARGET_TABLE_NAME} SET {output_column_name} = 0 WHERE YR = ? AND QTR = ?"
    if not execute_query(conn, update_query_reset_w, params=(yr_val, qtr_val)):
        return False

    if validation_error_flag:
        return False
    else:
        updates_for_report = []
        for _, row_calc in df_calculate_temp.iterrows():
            current_tsic = row_calc["tsic"]
            for i in range(1, 13):
                w_value = row_calc[f"w{i}"]
                db_w_value = (
                    w_value if w_value == 9999.9999 else round(float(w_value), 4)
                )
                size_r_str = str(i).zfill(2)
                updates_for_report.append(
                    (db_w_value, current_tsic, size_r_str, "01", yr_val, qtr_val)
                )

        if updates_for_report:
            update_w_column_query = f"""
            UPDATE {TARGET_TABLE_NAME} SET {output_column_name} = ?
            WHERE SUBSTRING(CAST(TSIC_R AS NVARCHAR(20)), 1, {TSIC_LENGTH}) = ? AND SIZE_R = ? AND ENU = ? AND YR = ? AND QTR = ?
            """
            if execute_many_query(conn, update_w_column_query, updates_for_report):
                return True
            else:
                return False
        else:
            return True


def run_processing_pipeline(conn):
    yr = PARAMS_CONFIG["YR"]
    qtr = PARAMS_CONFIG["QTR"]

    df_bign_input = load_bign_csv()
    if df_bign_input.empty:
        return

    df_calculate_temp = initialize_calculate_dataframe(df_bign_input)
    if df_calculate_temp.empty:
        return

    df_calculate_temp = perform_main_calculations(
        conn, df_calculate_temp, df_bign_input
    )

    df_calculate_temp = apply_weight_adjustments_step6(df_calculate_temp)

    update_success = perform_validation_and_update_report(conn, df_calculate_temp)
