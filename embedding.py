import numpy as np
import tensorflow as tf
from tensorflow import keras

#コア数の取得(CPU)
import multiprocessing as mp
core_num = mp.cpu_count()
config = tf.ConfigProto(
    inter_op_parallelism_threads=core_num,
    intra_op_parallelism_threads=core_num)
sess = tf.Session(config=config)

input_array = np.array([[0, 1, 2, 3, 4], [5, 1, 2, 3, 6]]) #入力
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

#Embeddingレイヤー指定
model = keras.Sequential()
model.add(keras.layers.Embedding(vocab_size, dim, mask_zero=True))
model.compile(optimizer='adam', loss='mse')
output_array = model.predict(input_array)
print(output_array)
