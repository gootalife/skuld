import numpy as np
import tensorflow as tf
from tensorflow import keras
import multiprocessing as mp
import setting
import glob

#ソースコード読み込み
def readAllConvertedCodes(directory, extension):
    fileList = sorted(glob.glob('{0}/*.{1}'.format(directory, extension)))
    print(fileList)
    sourceCodes = []
    for fileName in fileList:
        with open(fileName, 'r') as file:
            # 末尾改行削除+改行区切り
            sourceCodes.append(file.read().rstrip('\n').split('\n'))
    return sourceCodes

def embed(convertedCodes):
    input_array = np.array(convertedCodes) #入力
    vocab_size = 2000  #単語上限
    dim = 128 #ベクトルの次元数
    #列長vocab_sizeの配列の末尾を0埋め&切り詰め
    input_array = keras.preprocessing.sequence.pad_sequences(
        input_array,
        maxlen=vocab_size,
        dtype='int32',
        padding='post',
        truncating='post',
        value=0)

    print(input_array)
    np.save('input.npy', input_array) # 保存
    #Embeddingレイヤー指定
    model = keras.Sequential()
    model.add(keras.layers.Embedding(vocab_size, dim, mask_zero=True))
    model.compile(optimizer='adam', loss='mse')
    output_array = model.predict(input_array)
    # print(output_array)
    np.save('output.npy', output_array)  # 保存
    print(np.load('output.npy'))

if __name__ == '__main__':
    #コア数の取得(CPU)
    core_num = mp.cpu_count()
    config = tf.ConfigProto(
        inter_op_parallelism_threads=core_num,
        intra_op_parallelism_threads=core_num)
    sess = tf.Session(config=config)

    extension = setting.get('settings.ini', 'Info', 'extension')
    convertedCodes = readAllConvertedCodes('data/preprocess/converted', extension)
    embed(convertedCodes)