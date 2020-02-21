from tensorflow import keras
from collections import defaultdict
import glob
import os
import numpy as np
import json
import xlrd
import xlwt
import pandas as pd

# ソースコード読み込み
def readSourceCodes(directory, commitHash):
    fileList = sorted(glob.glob('{0}/*.txt'.format(directory)))
    sourceCodes = []
    for fileName in fileList:
        commit = os.path.splitext(os.path.basename(fileName))[0]
        # 目的のコミットハッシュに含まれているなら取得
        if commit in commitHash:
           sourceCodes.append(np.loadtxt(fileName, dtype='int').tolist())
    return sourceCodes

# 集計
def totalize(directory, sourceCodes):
    sourceCodes = keras.preprocessing.sequence.pad_sequences(
        sourceCodes,
        maxlen=2000,
        padding='post',
        truncating='post',
        value=0)
    words = defaultdict(int)
    for code in sourceCodes:
        for word in code:
            words[word] += 1
    return sorted(words.items())

if __name__ == '__main__':
    extensions = []
    projectName = ""
    testDir = ""
    with open('settings.json', 'r', encoding='utf-8') as file:
        settings = json.loads(file.read())
        extension = settings["extensions"]
        projectName = settings["projectName"]
        testDir = settings["testDir"]
    # コーパスのロード
    corpus = pd.read_csv('data/projects/{0}/corpus.csv'.format(projectName), header=None)
    # 高スコアのコミットハッシュリストを取得
    wb = xlrd.open_workbook('data/tests/{0}/high_score_commits.xlsx'.format(testDir))
    # wb = xlrd.open_workbook('data/tests/{0}/low_score_commits.xlsx'.format(testDir))
    sheets = wb.sheets()
    commitHash = []
    for i in range(sheets[0].nrows):
        commitHash.append(sheets[0].cell(i, 0).value)
    sourceCodes = readSourceCodes('data/projects/{0}/logs/converted'.format(projectName), commitHash)
    words = totalize('data/projects/{0}/logs/converted'.format(projectName), sourceCodes)

    # 保存
    wb = xlwt.Workbook()
    sheet = wb.add_sheet('sheet1', cell_overwrite_ok=True)
    i = 0
    sheet.write(0, 0, '単語')
    sheet.write(0, 1, '番号')
    sheet.write(0, 2, '出現回数')
    for i in range(len(corpus)):
        if i == 0:
            continue
        sheet.write(i, 0, str(corpus.iat[i - 1, 0]))
        sheet.write(i, 1, i)
        sheet.write(i, 2, 0)
    for key, val in words:
        if int(key) == 0:
            continue
        sheet.write(int(key), 2, int(val))
    wb.save('data/tests/{0}/high_score_words.xls'.format(testDir))
    # wb.save('data/tests/{0}/low_score_words.xls'.format(testDir))