import numpy as np
import tensorflow as tf
from tensorflow import keras

file = open('input.txt')
text = file.read()
file.close()

#0埋め
# out = keras.preprocessing.sequence.pad_sequences(
#     input_array,
#     maxlen=8,
#     dtype='int32',
#     padding='post',
#     truncating='post',
#     value=0)
# print(out)

res = [keras.preprocessing.text.text_to_word_sequence(text)]
tokenizer = keras.preprocessing.text.Tokenizer()
tokenizer.fit_on_texts(res)

print("各単語の出現数")
print(tokenizer.word_counts) #各単語の出現回数
print()
print(tokenizer.word_index) #各単語のインデックス
print()
print(tokenizer.document_count)  #与えられた単語数

matrix = tokenizer.texts_to_matrix(res, "count")
print(matrix)