import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import Input, Dense, Embedding
from tensorflow.keras.models import Model
import multiprocessing as mp
import setting
import glob

if __name__ == '__main__':
    #コア数の取得(CPU)
    core_num = mp.cpu_count()
    config = tf.ConfigProto(
        inter_op_parallelism_threads=core_num,
        intra_op_parallelism_threads=core_num)
    sess = tf.Session(config=config)

    vocab_size = 2000  # 単語上限
    input_dim = 128 # ベクトルの次元数
    embedding_table = np.load('embedding_table.npy')

    inputs = Input(shape=(vocab_size, input_dim))

    x = Embedding(vocab_size,
                input_dim,
                weights=[embedding_table],
                trainable=False)(inputs)
    h3_conv = Conv2D(input_dim, kernel_size=(input_dim, 3),
                strides=(vocab_size, 1),
                activation='relu')(x)
    h3_pool = MaxPooling2D((vocab_size, 1))(h3_conv)

    h4_conv = Conv2D(input_dim, kernel_size=(input_dim, 4),
                strides=(vocab_size, 1),
                activation='relu')(x)
    h4_pool = MaxPooling2D((vocab_size, 1))(h4_conv)

    h5_conv = Conv2D(input_dim, kernel_size=(input_dim, 5),
                strides=(vocab_size, 1),
                activation='relu')(x)
    h5_pool = MaxPooling2D((vocab_size, 1))(h5_conv)

    cnct = concatenate([h3_pool, h4_pool, h5_conv], axis=1)
    Flatten()
    outputs = Dense(2, activation='softmax')
    model = Model(inputs=inputs, outputs=outputs)
    model.summary()
    model.compile(optimizer='adam',
                loss='mean_squared_error',
                metrics=['accuracy'])
    model.summary()
    # model.fit(x_train, y_train,
    #         batch_size=64,
    #         epochs=50,
    #         verbose=1)
    # test_loss = model.evaluate(test_images, test_labels)


