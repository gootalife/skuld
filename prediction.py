import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import load_model
import glob
import os
import multiprocessing as mp
import numpy as np
import setting
import random
from tensorflow.keras.utils import to_categorical
import yaml
import csv


# データセット読み込み
def loadDataset(x_directory, y_directory, extension):
    fileList = sorted(glob.glob('{0}/*.{1}'.format(x_directory, extension)))
    dataset = []
    labels = []
    commitHash = []
    wordCount = []
    index = 1
    for fileName in fileList:
        print(index, '/', len(fileList))
        code = np.loadtxt(fileName, dtype='int')
        if code.size < 2: # 2単語未満の差分ファイルは無視
            continue
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
    # データ数の差だけデータの除去を行う
    # while label0 > label1:
    #     index = random.randint(0, len(labels) - 1)
    #     if labels[index] == 0:
    #         dataset.pop(index)
    #         labels.pop(index)
    #         print(labels.count(0), label1)
    #         label0 -= 1
    # while label1 > label0:
    #     index = random.randint(0, len(labels) - 1)
    #     if labels[index] == 1:
    #         dataset.pop(index)
    #         labels.pop(index)
    #         print(label0, labels.count(1))
    #         label1 -= 1
    return np.array(dataset), np.array(labels), commitHash, wordCount

if __name__ == '__main__':
    # コア数の取得(CPU)
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
    core_num = mp.cpu_count()
    config = tf.ConfigProto(
        inter_op_parallelism_threads=core_num,
        intra_op_parallelism_threads=core_num)
    sess = tf.Session(config=config)
    tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    data_dir = setting.get('settings.ini', 'Test', 'data_dir')

    max_len = 2000
    x_data, y_data, commitHash, wordCount = loadDataset('data/test/{0}/inputs'.format(data_dir),
                                'data/test/{0}/labels'.format(data_dir), extension)
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
    # load model and weight
    model = load_model('data/projects/{0}/logs/model.h5'.format(projectName))
    predict = model.predict(x_data)
    # print('        0          1         ')
    # print('-----------------------------')
    index = 0
    tp = 0
    tn = 0
    fp = 0
    fn = 0
    data = [['commit_hash', 'answer', 'result', 'category', 'clean', 'buggy', 'word_count']]
    for pred in predict:
        p = pred.argmax()
        category = 0
        answer = labels[index]
        if answer == 1 & p == 1:
            category = 1
            tp += 1
        elif answer == 0 & p == 1:
            category = 2
            fp += 1
        elif answer == 1 & p == 0:
            category = 3
            fn += 1
        else:
            category = 4
            tn += 1
        # s = '{0} {1} {2} : {3} {4}'.format(answer, p, category,
        #                                     str(pred[0]).ljust(10, '0'),
        #                                     str(pred[1]).ljust(10, '0'))
        # print(s)
        data.append([commitHash[index], answer, p, category, pred[0], pred[1], wordCount[index]])
        index += 1
    # print('真陽性率', tp / (tp + fn))
    # print('真陰性率', tn / (fp + tn))
    # print('偽陽性率', fp / (fp + tn))
    # print('偽陰性率', fn / (tp + fn))
    data.append(['tp', 'tn', 'fp', 'fn'])
    data.append([tp, tn, fp, fn])
    data.append(['tp_rate', 'tn_rate', 'fp_rate', 'fn_rate', 'accuracy'])
    data.append([tp / (tp + fn), tn / (fp + tn), fp / (fp + tn), fn / (tp + fn), (tp + tn) / (tp + fp + fn + tn)])
    with open('data/test/{0}/result.csv'.format(data_dir), 'w', encoding='utf-8') as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerows(data)