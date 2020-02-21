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
from statistics import mean, median
import json

# データセット読み込み
def loadDataset(x_directory, y_directory):
    fileList = sorted(glob.glob('{0}/*.txt'.format(x_directory)))
    dataset = []
    labels = []
    commitHash = []
    wordCount = []
    index = 1
    for fileName in fileList:
        code = np.loadtxt(fileName, dtype='int')
        if code.size < 2: # 2単語未満の差分ファイルは無視
            continue
        print(index, '/', len(fileList), fileName)
        dataset.append(code.tolist())
        # 対応するラベルの取得
        root, ext = os.path.splitext(fileName)
        basename = os.path.basename(root)
        labels.append(np.loadtxt(y_directory + '/' + basename + '.txt', dtype='int'))
        commitHash.append(basename)
        wordCount.append(code.size)
        index += 1
    # データ数を均一にする
    # label0 = labels.count(0)
    # label1 = len(labels) - label0
    # print(label0, label1, len(labels))
    # while label0 > label1:
    #     index = random.randint(0, len(labels) - 1)
    #     if labels[index] == 0:
    #         dataset.pop(index)
    #         labels.pop(index)
    #         commitHash.pop(index)
    #         wordCount.pop(index)
    #         print(labels.count(0), label1)
    #         label0 -= 1
    # while label1 > label0:
    #     index = random.randint(0, len(labels) - 1)
    #     if labels[index] == 1:
    #         dataset.pop(index)
    #         labels.pop(index)
    #         commitHash.pop(index)
    #         wordCount.pop(index)
    #         print(label0, labels.count(1))
    #         label1 -= 1
    return dataset, labels, commitHash, wordCount

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
    with open('settings.json', 'r', encoding='utf-8') as file:
        settings = json.loads(file.read())
        extensions = settings["extensions"]
        projectName = settings["projectName"]

    dataset, labels, commitHash, wordCount = loadDataset('data/projects/{0}/logs/converted'.format(projectName),
                                'data/projects/{0}/logs/labels'.format(projectName))

    wb = xlwt.Workbook()
    sheet = wb.add_sheet('sheet1')
    sheet.write(0, 0, 'commit_hash')
    sheet.write(0, 1, 'label')
    sheet.write(0, 2, 'wordcount')
    sheet.write(0, 3, 'mean')
    sheet.write(0, 4, 'median')
    sheet.write(0, 5, 'clean')
    sheet.write(0, 6, 'buggy')
    sheet.write(1, 3, mean(wordCount))
    sheet.write(1, 4, median(wordCount))
    sheet.write(1, 5, labels.count(0))
    sheet.write(1, 6, labels.count(1))
    for i in range(len(commitHash)):
        sheet.write(i + 1, 0, commitHash[i])
        sheet.write(i + 1, 1, int(labels[i]))
        sheet.write(i + 1, 2, int(wordCount[i]))
    wb.save('data/projects/{0}/statistics.xls'.format(projectName))
    print('mean', mean(wordCount))
    print('median', median(wordCount))
    print('len', len(commitHash))
    print('clean:', labels.count(0), 'buggy', labels.count(1))




