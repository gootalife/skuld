import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Embedding
import multiprocessing as mp
import setting
import glob
import os
import pandas as pd

# ソースコード読み込み
def readAllConvertedCodes(directory, extension):
    fileList = sorted(glob.glob('{0}/*.{1}'.format(directory, extension)))
    sourceCodes = []
    for fileName in fileList:
        code = np.loadtxt(fileName, dtype='int')
        if code.size < 2:
            continue
        sourceCodes.append(code)
    return sourceCodes

def embed(convertedCodes, corpus, projectName):
    input_array = np.array(convertedCodes) # 入力
    max_len = 2000  # 単語上限
    vocab_size = corpus['1'].max() + 1
    dim = 128 # ベクトルの次元数
    # 列長vocab_sizeの配列の末尾を0埋め&切り詰め
    input_array = keras.preprocessing.sequence.pad_sequences(
        input_array,
        maxlen=max_len,
        dtype='int32',
        padding='post',
        truncating='post',
        value=0)

    # print(input_array)
    # np.save('data/projects/{0}/logs/embedding_input.npy'.format(projectName), input_array) # 保存
    # Embeddingレイヤー指定
    model = Sequential()
    model.add(Embedding(input_dim=vocab_size,
                        output_dim=dim,
                        input_length=max_len,
                        mask_zero=True))
    model.compile(optimizer='rmsprop', loss='mse')
    output_array = model.predict(input_array)
    np.save('data/projects/{0}/logs/embedding_matrix.npy'.format(projectName), output_array)  # 保存

def vectorMapping(convertedCodes, corpus, projectName):
    codes_count = 1
    corpus_len = corpus['1'].max() + 1
    dim = 128
    matrix = np.load('data/projects/{0}/logs/embedding_matrix.npy'.format(projectName))
    table = np.zeros((corpus_len, dim))
    codes = convertedCodes

    for i in range(codes_count): # ファイル数回
        for j in range(len(codes[i])): # 各ファイルの要素数回
            table[codes[i][j]] = matrix[i][j] # 数字とベクトルの組を対応させる
    np.save('data/projects/{0}/logs/embedding_table.npy'.format(projectName), table)  # 保存

if __name__ == '__main__':
    # #コア数の取得(CPU)
    # os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
    # core_num = mp.cpu_count()
    # config = tf.ConfigProto(
    #     inter_op_parallelism_threads=core_num,
    #     intra_op_parallelism_threads=core_num)
    # sess = tf.Session(config=config)

    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    corpus = pd.read_csv('data/projects/{0}/corpus.csv'.format(projectName))
    convertedCodes = readAllConvertedCodes('data/projects/{0}/logs/converted'.format(projectName), extension)
    embed(convertedCodes, corpus, projectName)
    vectorMapping(convertedCodes, corpus, projectName)