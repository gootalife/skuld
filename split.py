import setting
import glob
import random
import shutil
import numpy as np
import os

if __name__ == '__main__':
    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    data_dir = setting.get('settings.ini', 'Test', 'data_dir')
    # テスト用データのフォルダを作成
    for i in range(10):
        os.makedirs('data/projects/{0}/logs/cv{1}'.format(projectName, i + 1), exist_ok=True)
    # フォルダ内のファイル一覧を取得
    fileList = sorted(glob.glob('{0}/*.{1}'.format('data/projects/{0}/logs/converted'.format(projectName), extension)))
    validFileList = []
    threshold = 2  # 無視するコミットの単語数の閾値
    index = 1
    for fileName in fileList:
        code = np.loadtxt(fileName, dtype='int')
        if code.size < 2: # 2単語未満のコミットは無視
            continue
        print(index, '/', len(fileList), fileName)
        validFileList.append(fileName)
        index += 1
    test_ratio = 0.1  # データをk分割する k = 10
    test_count = len(validFileList) * test_ratio - 1
    # テストデータの数だけランダムにデータを抽出・移動を10回行う
    for i in range(10):
        ct = 0
        while ct < test_count:
            index = random.randint(0, len(validFileList) - 1)
            file = validFileList[index]
            fileName = os.path.basename(file)
            # テストデータの移動
            shutil.copy(file, 'data/projects/{0}/logs/cv{1}/{2}'.format(projectName, i + 1, fileName))
            validFileList.pop(index)
            ct += 1
    # 余りのデータの振り分け
    for i in range(10):
        if len(validFileList) == 0:
            break
        index = random.randint(0, len(validFileList) - 1)
        file = validFileList[index]
        fileName = os.path.basename(file)
        # テストデータの移動
        shutil.copy(file, 'data/projects/{0}/logs/cv{1}/{2}'.format(projectName, i + 1, fileName))
        validFileList.pop(index)
        ct += 1