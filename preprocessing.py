from tensorflow import keras
import collections
import glob
import setting

#ソースコード読み込み
def readAllSourceCodes(directory):
    fileList = sorted(glob.glob(directory))
    sourceCodes = []
    for fileName in fileList:
        with open(fileName, 'r') as input:
            sourceCofileList = sorted(glob.glob(directory))
    sourceCodes = []
    for fileName in fileList:
        with open(fileName, 'r') as input:
            sourceCodes.append(input.read())
    return sourceCodes

#ベクトル化
def vectorize(sourceCodes):
    tokens = []
    tokenizer = keras.preprocessing.text.Tokenizer()
    for sourceCode in sourceCodes:
        tokens.append(keras.preprocessing.text.text_to_word_sequence(sourceCode))
    tokenizer.fit_on_texts(tokens)
    #出現数順(降順)にソート
    vocabulary = collections.OrderedDict(
        sorted(tokenizer.word_counts.items(), key=lambda x: x[1], reverse=True)
    )
    return vocabulary

#コーパスの作成
def makeCorpus(vocabulary, directory):
    corpus = collections.OrderedDict()
    num = 1 #各単語の添字
    threshold = 1 #対応値割り当ての閾値
    for key,val in vocabulary.items():
        if val >= threshold:
            corpus[key] = num
        else:
            corpus[key] = 0
        num += 1
    #出力
    with open(directory, 'w') as file: #出力先
        for key,val in corpus.items():
            print(key,val)
            file.write(key + "," + str(corpus[key]) + "\n")

if __name__ == '__main__'
    extension = setting.get('settings.ini', 'FileInfo', 'extension')
    sourceCodes = readAllSourceCodes('data/lscp/out/*.{0}'.format(extension))
    vocabulary = vectorize(sourceCodes)
    makeCorpus(vocabulary, 'data/preprocess/corpus.csv')