import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import Input, Dense, Embedding, Conv2D, MaxPooling2D, Reshape, Concatenate, Flatten
from tensorflow.keras.models import Model
from tensorflow.keras.utils import to_categorical
import multiprocessing as mp
import glob
from tensorflow.keras.utils import plot_model
import os
import setting
from sklearn.model_selection import train_test_split
import glob
import pandas as pd
import random
import matplotlib
matplotlib.use('Agg') # -----(1)
import matplotlib.pyplot as plt
import sys
import json
from itertools import chain

def randonUndersampling(dataset, labels):
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
            print(labels.count(0), label1)
            label0 -= 1
    while label1 > label0:
        index = random.randint(0, len(labels) - 1)
        if labels[index] == 1:
            dataset.pop(index)
            labels.pop(index)
            print(label0, labels.count(1))
            label1 -= 1
    return dataset, labels

# データセット読み込み
def loadDataset(x_directory, y_directory, cv_case):
    fileList = sorted(glob.glob('{0}/converted/*.txt'.format(x_directory)))
    cvFilePath = sorted(glob.glob('{0}/cv{1}/*.txt'.format(x_directory, cv_case)))
    cvFileList = []
    for cvfilepath in cvFilePath:
        cvFileList.append(os.path.basename(cvfilepath))
    dataset = [[],[],[],[],[],[],[],[],[],[],[]]
    labels = [[],[],[],[],[],[],[],[],[],[],[]]
    index = 1
    for fileName in fileList:
        # 訓練用データは学習対象から除外
        if os.path.basename(fileName) in cvFileList:
            continue
        print(index, '/', len(fileList))
        code = np.loadtxt(fileName, dtype='int')
        if code.size < 2: # 2単語以下の差分ファイルは無視
            continue
        # 対応するラベルの取得
        root, ext = os.path.splitext(fileName)
        basename = os.path.basename(root)
        label = np.loadtxt(y_directory + '/' + basename + '.txt', dtype='int')
        if code.size <= 199:
            dataset[0].append(code.tolist())
            labels[0].append(label)
        elif code.size <= 399:
            dataset[1].append(code.tolist())
            labels[1].append(label)
        elif code.size <= 599:
            dataset[2].append(code.tolist())
            labels[2].append(label)
        elif code.size <= 799:
            dataset[3].append(code.tolist())
            labels[3].append(label)
        elif code.size <= 999:
            dataset[4].append(code.tolist())
            labels[4].append(label)
        elif code.size <= 1199:
            dataset[5].append(code.tolist())
            labels[5].append(label)
        elif code.size <= 1399:
            dataset[6].append(code.tolist())
            labels[6].append(label)
        elif code.size <= 1599:
            dataset[7].append(code.tolist())
            labels[7].append(label)
        elif code.size <= 1799:
            dataset[8].append(code.tolist())
            labels[8].append(label)
        elif code.size <= 1999:
            dataset[9].append(code.tolist())
            labels[9].append(label)
        else:
            dataset[10].append(code.tolist())
            labels[10].append(label)
        index += 1
    # データ数を均一にする
    for i in range(11):
        dataset[i], labels[i] = randonUndersampling(dataset[i], labels[i])
    # 平坦化
    dataset = list(chain.from_iterable(dataset))
    labels = list(chain.from_iterable(labels))
    return np.array(dataset), np.array(labels)

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
    cv_case = sys.argv[1]
    with open('settings.json', 'r', encoding='utf-8') as file:
        settings = json.loads(file.read())
        extensions = settings["extensions"]
        projectName = settings["projectName"]
    corpus = pd.read_csv('data/projects/{0}/corpus.csv'.format(projectName))
    vocab_size = corpus['1'].max() + 1  # 単語上限
    input_dim = 128  # ベクトルの次元数
    max_len = 599

    x_data, y_data = loadDataset('data/projects/{0}/logs'.format(projectName),
                        'data/projects/{0}/logs/labels'.format(projectName), cv_case)
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
    y_data = np.array(y_data)
    y_data = to_categorical(y_data, 2)
    # 訓練データ80%,テストデータ20%に分割
    # x_train, x_test, y_train, y_test = train_test_split(x_data, y_data, test_size=0.2)
    x_train = x_data
    y_train = y_data

    embedding_table = np.load('data/projects/{0}/logs/embedding_table.npy'.format(projectName))

    input = Input(shape=(max_len, 1)) # 入力は1 * 2000

    x = Embedding(input_dim=vocab_size,
                output_dim=input_dim,
                input_length=max_len,
                weights=[embedding_table],
                trainable=False)(input)
    reshaped = Reshape((max_len, input_dim, 1),
                input_shape=(max_len, input_dim))(x)
    h3_conv = Conv2D(filters=input_dim,
                kernel_size=(3, input_dim),
                strides=(1, 1),
                padding='valid')(reshaped)
    h3_pool = MaxPooling2D((h3_conv.shape[1], 1))(h3_conv)
    h4_conv = Conv2D(filters=input_dim,
                kernel_size=(4, input_dim),
                strides=(1, 1),
                padding='valid')(reshaped)
    h4_pool = MaxPooling2D((h4_conv.shape[1], 1))(h4_conv)
    h5_conv = Conv2D(filters=input_dim,
                kernel_size=(5, input_dim),
                strides=(1, 1),
                padding='valid')(reshaped)
    h5_pool = MaxPooling2D((h5_conv.shape[1], 1))(h5_conv)
    x = Concatenate(axis=1)([h3_pool, h4_pool, h5_pool])
    x = Flatten()(x)
    output = Dense(2, activation='softmax')(x)
    model = Model(inputs=input, outputs=output)
    model.summary()
    model.compile(optimizer='adam',
                loss='binary_crossentropy',
                metrics=['accuracy'])
    plot_model(model, # モデル画像の出力
            to_file='data/models/model.png',
            show_shapes=True)
    history = model.fit(x_train, y_train,
            batch_size=64,
            epochs=15,
            verbose=1)
    # history = model.fit(x_train, y_train,
    #         validation_split=0.2,
    #         batch_size=64,
    #         epochs=15,
    #         verbose=1)
    # score = model.evaluate(x_test, y_test)
    # print(score) # 評価
    model.save('data/projects/{0}/logs/model{1}.h5'.format(projectName, cv_case))  # 学習結果の保存
    #Accuracy
    # plt.plot(history.history['acc'])
    # plt.plot(history.history['val_acc'])
    # plt.title('model accuracy')
    # plt.ylabel('accuracy')
    # plt.xlabel('epoch')
    # plt.legend(['train', 'test'], loc='upper left')
    # save as png
    # plt.savefig('data/projects/{0}/logs/model_accuracy.png'.format(projectName))
    # plt.clf()
    #loss
    # plt.plot(history.history['loss'])
    # plt.plot(history.history['val_loss'])
    # plt.title('model loss')
    # plt.ylabel('loss')
    # plt.xlabel('epoch')
    # plt.legend(['train', 'test'], loc='upper left')
    # save as png
    # plt.savefig('data/projects/{0}/logs/model_loss.png'.format(projectName))