from tensorflow import keras
import collections

#ファイル読み込み
file = open('data/lscp/out/example1.c')
text = file.read()
file.close()

#ベクトル化
res = [keras.preprocessing.text.text_to_word_sequence(text)]
tokenizer = keras.preprocessing.text.Tokenizer()
tokenizer.fit_on_texts(res)

#出現数順(降順)にソート
vocabulary = collections.OrderedDict(
    sorted(tokenizer.word_counts.items(), key=lambda x: x[1], reverse=True)
)

#単語と数値の対応表の作成
table = collections.OrderedDict() #対応表
num = 1 #各単語の添字
threshold = 2 #対応する数値を割り当てるか当てないかどうかの閾値
for key,val in vocabulary.items():
    if val >= threshold:
        table[key] = num
    else:
        table[key] = 0
    num += 1
for key,val in vocabulary.items():
    print(table[key],key,val)