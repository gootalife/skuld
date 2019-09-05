import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Input, Dense, Embedding, Conv2D, MaxPooling2D, Flatten
import multiprocessing as mp
import setting
import glob
from tensorflow.keras.utils import plot_model

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
    print(embedding_matrix)
    # model = Sequential([
    #     Embedding(vocab_size,
    #         input_dim,
    #         weights=[embedding_table],
    #         trainable=False),
    #     Conv2D(input_dim,
    #         kernel_size=(input_dim, 3),
    #         strides=(128, 1),
    #         activation='relu'),
    #     # MaxPooling2D(2000, 1),
    #     # Flatten(),
    #     # Dense(2, activation='softmax'),
    # ])

    # 埋め込み層
    # model.add(Embedding(vocab_size,
    #         input_dim,
    #         weights=[embedding_matrix],
    #         trainable=False))
    # 畳み込み層
    # model.add(Conv2D(input_dim, kernel_size=(input_dim, 3),
    #                 strides=(128, 1),
    #                 activation='relu'))
    # model.add(layers.MaxPooling2D((2000, 1)))
#     model.add(Conv2D(input_dim, kernel_size=(input_dim, 4),
#                     strides=(128, 1),
#                     activation='relu'))
#     model.add(layers.MaxPooling2D((2, 2)))
#     model.add(Conv2D(input_dim, kernel_size=(input_dim, 5),
#                     strides=(128, 1),
#                     activation='relu'))
#     model.add(layers.MaxPooling2D((2, 2)))
    # 全結合層
    # Flatten()
    # model.add(Dense(2, activation='softmax'))
    # model.compile(optimizer='adam',
    #             loss='mean_squared_error',
    #             metrics=['accuracy'])
    # model.summary()
    # plot_model(model, to_file='model.png')
#     model.fit(x_train, y_train,
#             batch_size=64,
#             epochs=50,
#             verbose=1)
#     test_loss = model.evaluate(test_images, test_labels)

