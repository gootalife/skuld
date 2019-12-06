from tensorflow import keras
import collections
import glob
import setting
import os
import numpy as np

# ソースコード読み込み
def readAllSourceCodes(directory, extension):
    fileList = sorted(glob.glob('{0}/*.{1}'.format(directory, extension)))
    sourceCodes = []
    for fileName in fileList:
        with open(fileName, 'r') as file:
            sourceCodes.append(file.read().rstrip('\n')) # 末尾改行削除
    return sourceCodes

# ベクトル化
def vectorize(sourceCodes):
    tokens = []
    tokenizer = keras.preprocessing.text.Tokenizer()
    for sourceCode in sourceCodes:
        tokens.append(keras.preprocessing.text.text_to_word_sequence(sourceCode))
    tokenizer.fit_on_texts(tokens)
    # 出現数順(降順)にソート
    vocabulary = collections.OrderedDict(
        sorted(tokenizer.word_counts.items(), key=lambda x: x[1], reverse=True)
    )
    return vocabulary

# コーパスの作成
def makeCorpus(vocabulary, directory):
    corpus = collections.OrderedDict()
    num = 1 # 各単語の添字
    threshold = 0 # 対応値割り当ての閾値
    for key,val in vocabulary.items():
        if val >= threshold:
            corpus[key] = num
        else:
            corpus[key] = 0
        num += 1
    # 出力
    with open(directory, 'w') as file:
        for key,val in corpus.items():
            file.write('{0},{1},{2}\n'.format(key, str(corpus[key]), vocabulary[key]))
    return corpus

# ソースコード片の数値変換
def convert(corpus, sourceCodes, inDirectory, outDirectory, extension):
    fileList = sorted(glob.glob('{0}/*.{1}'.format(inDirectory, extension)))
    index = 0
    for sourceCode in sourceCodes:
        convertedCode = []
        for word in sourceCode.split('\n'):
            if word in corpus:
                convertedCode.append(corpus[word])
            else:
                convertedCode.append(0)
        fileName = os.path.basename(fileList[index])
        # 出力
        np.savetxt('{0}/{1}'.format(outDirectory, fileName), convertedCode, fmt='%d')
        # with open('{0}/{1}'.format(outDirectory, fileName), 'w') as file:
        #     for code in convertedCode:
        #         file.write('{0}\n'.format(code))
        index += 1

if __name__ == '__main__':
    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    sourceCodes = readAllSourceCodes('data/projects/{0}/logs/preprocessed'.format(projectName), extension)
    vocabulary = vectorize(sourceCodes)
    corpus = makeCorpus(vocabulary, 'data/projects/{0}/corpus.csv'.format(projectName))
    convert(corpus, sourceCodes, 'data/projects/{0}/logs/preprocessed'.format(projectName), 'data/projects/{0}/logs/converted'.format(projectName), extension)