from tensorflow import keras
import collections

#ファイル読み込み
file = open('data/out/example1')
text = file.read()
file.close()

#ベクトル化
res = [keras.preprocessing.text.text_to_word_sequence(text)]
tokenizer = keras.preprocessing.text.Tokenizer()
tokenizer.fit_on_texts(res)

#出現数順にソート
vocabulary = collections.OrderedDict(
    sorted(tokenizer.word_counts.items(), key=lambda x: x[1], reverse=True)
)
print(vocabulary)