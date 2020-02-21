import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import load_model
import glob
import os
import multiprocessing as mp
import numpy as np
import random
from tensorflow.keras.utils import to_categorical
import yaml
import csv
import sys
import xlwt
import json
from itertools import chain

def randonUndersampling(dataset, labels, commitHash, wordCount):
    if len(dataset) < 1:
        return dataset, labels, commitHash, wordCount
    # データ数を均一にする
    label0 = labels.count(0)
    label1 = len(labels) - label0
    print(label0, label1, len(labels))
    # データ数の差だけデータの除去を行う
    while label0 > label1:
        index = random.randint(0, len(labels) - 1)
        if labels[index] == 0:
            dataset.pop(index)
            labels.pop(index)
            commitHash.pop(index)
            wordCount.pop(index)
            print(labels.count(0), label1)
            label0 -= 1
    while label1 > label0:
        index = random.randint(0, len(labels) - 1)
        if labels[index] == 1:
            dataset.pop(index)
            labels.pop(index)
            commitHash.pop(index)
            wordCount.pop(index)
            print(label0, labels.count(1))
            label1 -= 1
    return dataset, labels, commitHash, wordCount

# データセット読み込み
def loadDataset(x_directory, y_directory):
    fileList = sorted(glob.glob('{0}/*.txt'.format(x_directory)))
    dataset = [[],[],[],[],[],[],[],[],[],[],[]]
    labels = [[],[],[],[],[],[],[],[],[],[],[]]
    commitHash = [[],[],[],[],[],[],[],[],[],[],[]]
    wordCount = [[],[],[],[],[],[],[],[],[],[],[]]
    index = 1
    for fileName in fileList:
        code = np.loadtxt(fileName, dtype='int')
        if code.size < 2: # 2単語以下の差分ファイルは無視
            continue
        print(index, '/', len(fileList), fileName)
        # 対応するラベルの取得
        root, ext = os.path.splitext(fileName)
        basename = os.path.basename(root)
        label = np.loadtxt(y_directory + '/' + basename + '.txt', dtype='int')
        if code.size <= 199:
            dataset[0].append(code.tolist())
            labels[0].append(label)
            commitHash[0].append(basename)
            wordCount[0].append(code.size)
        elif code.size <= 399:
            dataset[1].append(code.tolist())
            labels[1].append(label)
            commitHash[1].append(basename)
            wordCount[1].append(code.size)
        elif code.size <= 599:
            dataset[2].append(code.tolist())
            labels[2].append(label)
            commitHash[2].append(basename)
            wordCount[2].append(code.size)
        elif code.size <= 799:
            dataset[3].append(code.tolist())
            labels[3].append(label)
            commitHash[3].append(basename)
            wordCount[3].append(code.size)
        elif code.size <= 999:
            dataset[4].append(code.tolist())
            labels[4].append(label)
            commitHash[4].append(basename)
            wordCount[4].append(code.size)
        elif code.size <= 1199:
            dataset[5].append(code.tolist())
            labels[5].append(label)
            commitHash[5].append(basename)
            wordCount[5].append(code.size)
        elif code.size <= 1399:
            dataset[6].append(code.tolist())
            labels[6].append(label)
            commitHash[6].append(basename)
            wordCount[6].append(code.size)
        elif code.size <= 1599:
            dataset[7].append(code.tolist())
            labels[7].append(label)
            commitHash[7].append(basename)
            wordCount[7].append(code.size)
        elif code.size <= 1799:
            dataset[8].append(code.tolist())
            labels[8].append(label)
            commitHash[8].append(basename)
            wordCount[8].append(code.size)
        elif code.size <= 1999:
            dataset[9].append(code.tolist())
            labels[9].append(label)
            commitHash[9].append(basename)
            wordCount[9].append(code.size)
        else:
            dataset[10].append(code.tolist())
            labels[10].append(label)
            commitHash[10].append(basename)
            wordCount[10].append(code.size)
        index += 1
    # データ数を均一にする
    for i in range(11):
        dataset[i], labels[i], commitHash[i], wordCount[i] = randonUndersampling(dataset[i], labels[i], commitHash[i], wordCount[i])
    # 平坦化
    dataset = list(chain.from_iterable(dataset))
    labels = list(chain.from_iterable(labels))
    commitHash = list(chain.from_iterable(commitHash))
    wordCount = list(chain.from_iterable(wordCount))
    return np.array(dataset), np.array(labels), commitHash, wordCount

if __name__ == '__main__':
    physical_devices = tf.config.experimental.list_physical_devices('GPU')
    if len(physical_devices) > 0:
        for k in range(len(physical_devices)):
            tf.config.experimental.set_memory_growth(physical_devices[k], True)
            print('memory growth:', tf.config.experimental.get_memory_growth(physical_devices[k]))
    else:
        print("Not enough GPU hardware devices available")

    extensions = []
    projectName = ""
    testDir = ""
    cv_case = sys.argv[1]
    with open('settings.json', 'r', encoding='utf-8') as file:
        settings = json.loads(file.read())
        extensions = settings["extensions"]
        projectName = settings["projectName"]
        testDir = settings["testDir"]
    for i in range(10):
        os.makedirs('data/tests/{0}/case{1}'.format(projectName, i + 1), exist_ok=True)
    max_len = 599
    x_data, y_data, commitHash, wordCount = loadDataset('data/tests/{0}/case{1}/cv{2}'.format(testDir, cv_case, cv_case),
                                'data/tests/{0}/labels'.format(testDir))
    x_data = keras.preprocessing.sequence.pad_sequences(
        x_data,
        maxlen=max_len,
        dtype='int32',
        padding='post',
        truncating='post',
        value=0)
    a, b = x_data.shape
    x_data = x_data.reshape(a, b, 1)
    x_data = np.array(x_data)
    labels = y_data
    y_data = np.array(y_data)
    y_data = to_categorical(y_data, 2)
    # モデルの読み込み
    model = load_model('data/projects/{0}/logs/model{1}.h5'.format(projectName, cv_case))

    predict = model.predict(x_data)

    index = 0
    tp = 0
    tn = 0
    fp = 0
    fn = 0
    wb = xlwt.Workbook()
    sheet = wb.add_sheet('sheet1')
    sheet.write(0, 0, 'buggy')
    sheet.write(0, 1, 'answer')
    data = [['commit_hash', 'answer', 'result', 'category', 'clean', 'buggy', 'word_count']]
    for pred in predict:
        p = pred.argmax()
        category = 0
        answer = labels[index]
        if answer == 1 and p == 1:
            category = 1
            tp += 1
        elif answer == 0 and p == 1:
            category = 2
            fp += 1
        elif answer == 1 and p == 0:
            category = 3
            fn += 1
        else:
            category = 4
            tn += 1
        data.append([commitHash[index], answer, p, category, pred[0], pred[1], wordCount[index]])
        sheet.write(index + 1, 0, float(pred[1]))
        sheet.write(index + 1, 1, int(answer))
        index += 1
    # print('真陽性率', tp / (tp + fn))
    # print('真陰性率', tn / (fp + tn))
    # print('偽陽性率', fp / (fp + tn))
    # print('偽陰性率', fn / (tp + fn))
    data.append(['tp', 'tn', 'fp', 'fn'])
    data.append([tp, tn, fp, fn])
    data.append(['tp_rate', 'tn_rate', 'fp_rate', 'fn_rate', 'accuracy'])
    data.append([tp / (tp + fn), tn / (fp + tn), fp / (fp + tn), fn / (tp + fn), (tp + tn) / (tp + fp + fn + tn)])
    with open('data/tests/{0}/case{1}/result.csv'.format(testDir, cv_case), 'w', encoding='utf-8') as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerows(data)
    wb.save('data/tests/{0}/case{1}/ROCcurve.xls'.format(testDir, cv_case))