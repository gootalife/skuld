# excelファイル読み込み
# -i Missense_variant_on_All.xlsx -x 1
# -i allmu_roc.xlsx  -x 2


import argparse
import math
import xlrd
import xlwt
import numpy as np
#import threading


# import pprint as pp


def main(inputf, index=0):
    # Open excel file
    print("# open ", inputf)
    book = xlrd.open_workbook(inputf)
    print("")
    print("# sheet: ", book.nsheets)
    print("# sheet name:")
    i = 0
    for n in book.sheet_names():
        print("# index=", i, ":name=", n)
        i += 1
    print("#    select index -> ", index)
    sheet = book.sheet_by_index(index)


    # 前処理
    data_array = np.array(sheet._cell_values)
    # pp.pprint(data_array)

    # 本処理
    print("")
    n_ndim = data_array.shape
    n_ndim = n_ndim[1]
    print("# 検体個数: ", n_ndim / 2)
    roc_curve_sum = np.array([])
    for q in range(0, n_ndim, 2):
        print("# dim: ", data_array[0][q])
        n_float = q
        n_int = q + 1
        result = get_roc_element(data_array, n_float=n_float, n_int=n_int)
        roc_curve = np.array(
            [["Tool", data_array[0][q]], ["AUC", 0], ["sensitivity", 0], ["1-specificity", 0]])  # 0 1 2 3
        roc_curve = np.concatenate((roc_curve, [["FP rate", "TP rate"]]), axis=0)  # 4
        for p in result:
            TP = p[1]
            FP = p[2]
            TN = p[3]
            FN = p[4]
            try:
                roc_curve = np.concatenate((roc_curve, [[FP / (FP + FN), TP / (TP + TN)]]), axis=0)
            except:
                # print("# Error: divide by zero.")
                print("TP FP TN FN:", p)
        # pp.pprint(roc_curve)
        auc = 0
        h = 0
        sensitive = 0
        specificity = 0
        for r in range(len(result)):
            try:
                auc += math.fabs(float(roc_curve[r + 5, 0]) - float(roc_curve[r + 6, 0])) * float(roc_curve[r + 5, 1])
                h_tmp = float(roc_curve[r + 5, 1]) - float(roc_curve[r + 5, 0])  # TP - FP
                if h < h_tmp:
                    h = h_tmp
                    sensitive = float(roc_curve[r + 5, 1])
                    specificity = float(roc_curve[r + 5, 0])
            except:
                roc_curve[1, 1] = round(auc, 4)
                roc_curve[2, 1] = round(sensitive, 4)
                roc_curve[3, 1] = round(specificity, 4)
                # print("# AUC ", auc)
                break
        if q == 0:
            roc_curve_sum = roc_curve
        else:
            roc_curve_sum = np.concatenate((roc_curve_sum, roc_curve), axis=1)

    # 書き込み
    write2xls(inputf, roc_curve_sum)
    print("# OK")


def write2xls(fname, data):
    write_name = fname + ".result.xls"
    print("# Write")
    book = xlwt.Workbook(write_name)
    newSheet = book.add_sheet('result')
    for p in range(0, len(data) - 1):
        # p行、q列目にデータを書き込み
        for q in range(0, len(data[p])):
            try:
                data_tmp = float(data[p][q])
            except:
                data_tmp = str(data[p][q])
            newSheet.write(p, q, data_tmp)
    book.save(write_name)


def get_roc_element(data_array, n_range=1000, n_float=0, n_int=1):
    result = []
    # 欠損値を含むとエラーになるのでコメントアウト
    # n_max = np.max(data_array[1:, n_float].astype(np.float64))
    # n_min = np.min(data_array[1:, n_float].astype(np.float64))
    # print("# Max, Min:", n_max, n_min)
    # for sensitive in np.arange(n_min, n_max, (n_max - n_min) / n_range):
    for sensitive in np.arange(0, 1, 1 / n_range):
        TP = 0
        FP = 0
        TN = 0
        FN = 0
        for p in data_array:
            try:
                d_float = float(p[n_float])
                d_label = int(float(p[n_int]))
                if d_float >= sensitive and d_label == 1:  # TP
                    TP += 1
                elif d_float >= sensitive and d_label != 1:  # FP
                    FP += 1
                elif d_float < sensitive and d_label == 1:  # TN
                    TN += 1
                elif d_float < sensitive and d_label != 1:  # FN
                    FN += 1
            except:
                continue
        result.append([sensitive, TP, FP, TN, FN])
    # pp.pprint(result)
    return result


if __name__ == '__main__':
    # コマンドライン引数
    parser = argparse.ArgumentParser(description='python3 ROCcurve.py [option]')
    parser.add_argument('--input', '-i', type=str, default="", help='入力ファイル')
    parser.add_argument('--index', '-x', type=int, default=0, help='sheet index')
    args = parser.parse_args()
    main(args.input, args.index)
