from tensorflow import keras
import collections
import glob

#ファイル連続読み込み
fileList = sorted(glob.glob('data/lscp/out/*.c'))
sourceCodes = []
for fileName in fileList:
    with open(fileName, 'r') as input:
        sourceCodes.append(input.read())

#ベクトル化
tokens = []
tokenizer = keras.preprocessing.text.Tokenizer()
for sourceCode in sourceCodes:
    tokens.append(keras.preprocessing.text.text_to_word_sequence(sourceCode))
tokenizer.fit_on_texts(tokens)

#出現数順(降順)にソート
vocabulary = collections.OrderedDict(
    sorted(tokenizer.word_counts.items(), key=lambda x: x[1], reverse=True)
)

#コーパスの作成
corpus = collections.OrderedDict()
num = 1 #各単語の添字
threshold = 1 #対応する数値を割り当てるか当てないかどうかの閾値
for key,val in vocabulary.items():
    if val >= threshold:
        corpus[key] = num
    else:
        corpus[key] = 0
    num += 1

#コーパスの出力
with open('data/pre/corpus.csv', 'w') as file: #出力先
    for key,val in corpus.items():
        print(key,val)
        file.write(key + "," + str(corpus[key]) + "\n")
