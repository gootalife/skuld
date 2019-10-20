import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import Input, Dense, Embedding, Conv2D, MaxPooling2D, Reshape, Concatenate, Flatten
from tensorflow.keras.models import Model
from tensorflow.keras.utils import to_categorical
import multiprocessing as mp
import setting
import glob
from tensorflow.keras.utils import plot_model
import os
import setting
from sklearn.model_selection import train_test_split
import glob

# データセット読み込み
def loadDataset(directory, extension):
    fileList = sorted(glob.glob('{0}/*.{1}'.format(directory, extension)))
    dataset = []
    for fileName in fileList:
        # with open(fileName, 'r') as file:
        #     # 末尾改行削除+改行区切り
        #     dataset.append(file.read().rstrip('\n').split('\n'))
        dataset.append(np.loadtxt(fileName, dtype='int').tolist())
    return np.array(dataset)

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

    x_data = loadDataset('data/projects/{0}/logs/converted'.format(projectName), extension)
    x_data = keras.preprocessing.sequence.pad_sequences(
        x_data,
        maxlen=2000,
        dtype='int32',
        padding='post',
        truncating='post',
        value=0)
    a, b = x_data.shape
    x_data = x_data.reshape(a, b, 1)
    y_data = loadDataset('data/projects/{0}/logs/labels'.format(projectName), 'txt')
    x_data = np.array(x_data)
    y_data = np.array(y_data)
    y_data = to_categorical(y_data, 2)
    # 訓練データ80%,テストデータ20%に分割
    x_train, x_test, y_train, y_test = train_test_split(x_data, y_data, test_size=0.2)

    vocab_size = 2000  # 単語上限
    input_dim = 128 # ベクトルの次元数
    embedding_table = np.load('embedding_table.npy')

    input = Input(shape=(vocab_size, 1)) # 入力は1 * n

    x = Embedding(input_dim=vocab_size,
                output_dim=input_dim,
                input_length=vocab_size,
                weights=[embedding_table],
                trainable=False)(input)
    reshaped = Reshape((vocab_size, input_dim, 1),
                input_shape=(vocab_size, input_dim))(x)
    h3_conv = Conv2D(filters=input_dim,
                kernel_size=(3, input_dim),
                strides=(1, 1),
                padding='valid',
                activation='relu')(reshaped)
    h3_pool = MaxPooling2D((h3_conv.shape[1], 1))(h3_conv)
    h4_conv = Conv2D(filters=input_dim,
                kernel_size=(4, input_dim),
                strides=(1, 1),
                padding='valid',
                activation='relu')(reshaped)
    h4_pool = MaxPooling2D((h4_conv.shape[1], 1))(h4_conv)
    h5_conv = Conv2D(filters=input_dim,
                kernel_size=(5, input_dim),
                strides=(1, 1),
                padding='valid',
                activation='relu')(reshaped)
    h5_pool = MaxPooling2D((h5_conv.shape[1], 1))(h5_conv)
    x = Concatenate(axis=1)([h3_pool, h4_pool, h5_pool])
    x = Flatten()(x)
    output = Dense(2, activation='softmax')(x)
    model = Model(inputs=input, outputs=output)
    model.summary()
    model.compile(optimizer='adam',
                loss='binary_crossentropy',
                metrics=['accuracy'])
    plot_model(model, to_file='data/models/model.png') # モデル画像の出力
    history = model.fit(x_train, y_train,
            batch_size=64,
            epochs=15,
            verbose=1)
    score = model.evaluate(x_test, y_test)
    print(score) # 評価
    yaml_string = model.to_yaml()
    with open('data/models/model.yaml', 'w') as file: # モデルの保存
        file.write(yaml_string)
    model.save_weights('data/models/weights.h5') # 学習結果の保存


