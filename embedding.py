import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Embedding
import multiprocessing as mp
import setting
import glob
import os

# ソースコード読み込み
def readAllConvertedCodes(directory, extension):
    fileList = sorted(glob.glob('{0}/*.{1}'.format(directory, extension)))
    sourceCodes = []
    for fileName in fileList:
        sourceCodes.append(np.loadtxt(fileName, dtype='int'))
    return sourceCodes

def embed(convertedCodes):
    input_array = np.array(convertedCodes) # 入力
    vocab_size = 2000  # 単語上限
    dim = 128 # ベクトルの次元数
    # 列長vocab_sizeの配列の末尾を0埋め&切り詰め
    input_array = keras.preprocessing.sequence.pad_sequences(
        input_array,
        maxlen=vocab_size,
        dtype='int32',
        padding='post',
        truncating='post',
        value=0)

    # print(input_array)
    np.save('embedding_input.npy', input_array) # 保存
    # Embeddingレイヤー指定
    model = Sequential()
    model.add(Embedding(input_dim=vocab_size,
                        output_dim=dim,
                        input_length=vocab_size,
                        mask_zero=True))
    model.compile(optimizer='rmsprop', loss='mse')
    output_array = model.predict(input_array)
    np.save('embedding_matrix.npy', output_array)  # 保存

def vectorMapping():
    codes_count = 1
    courpus_len = 2000
    matrix = np.load('embedding_matrix.npy')
    table = np.zeros((courpus_len, 128))
    codes = np.load('convertedCodes.npy')

    for i in range(codes_count): # ファイル数回
        for j in range(len(codes[i])): # 各ファイルの要素数回
            table[int(codes[i,j])] = matrix[i, j] # 数字とベクトルの組を対応させる
    np.save('embedding_table.npy', table)  # 保存

if __name__ == '__main__':
    #コア数の取得(CPU)
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
    core_num = mp.cpu_count()
    config = tf.ConfigProto(
        inter_op_parallelism_threads=core_num,
        intra_op_parallelism_threads=core_num)
    sess = tf.Session(config=config)

    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    convertedCodes = readAllConvertedCodes('data/projects/{0}/logs/converted'.format(projectName), extension)
    embed(convertedCodes)
    vectorMapping()