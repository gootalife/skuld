import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import Input, Dense, Embedding, Conv1D, Conv2D, MaxPooling2D, MaxPooling1D, Reshape
from tensorflow.keras.models import Model
import multiprocessing as mp
import setting
import glob
from tensorflow.keras.utils import plot_model
import os

if __name__ == '__main__':
    #コア数の取得(CPU)
    os.environ[ 'TF_CPP_MIN_LOG_LEVEL'] = '2'
    core_num = mp.cpu_count()
    config = tf.ConfigProto(
        inter_op_parallelism_threads=core_num,
        intra_op_parallelism_threads=core_num)
    sess = tf.Session(config=config)

    vocab_size = 2000  # 単語上限
    input_dim = 128 # ベクトルの次元数
    embedding_table = np.load('embedding_table.npy')

    input = Input(shape=(vocab_size,1)) # 入力は1 * n

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

    # cnct = concatenate([h3_pool, h4_pool, h5_conv], axis=1)
    output = Dense(2, activation='softmax')(x)
    model = Model(inputs=input, outputs=output)
    model.summary()
    model.compile(optimizer='adam',
                loss='mean_squared_error',
                metrics=['accuracy'])
    plot_model(model, to_file='model.png')
    print('h3_conv.shape:' + str(h3_conv.shape))
    print('h3_pool.shape:' + str(h3_pool.shape))
    print('h4_conv.shape:' + str(h4_conv.shape))
    print('h4_pool.shape:' + str(h4_pool.shape))
    print('h5_conv.shape:' + str(h5_conv.shape))
    print('h5_pool.shape:' + str(h5_pool.shape))
    # model.fit(x_train, y_train,
    #         batch_size=64,
    #         epochs=50,
    #         verbose=1)
    # test_loss = model.evaluate(test_images, test_labels)


