import glob
import random
import shutil
import numpy as np
import os
import json

if __name__ == '__main__':
    extensions = []
    projectName = ""
    with open('settings.json', 'r', encoding='utf-8') as file:
        settings = json.loads(file.read())
        extensions = settings["extensions"]
        projectName = settings["projectName"]
    # フォルダ内のファイル一覧を取得
    fileList = sorted(glob.glob('{0}/*.txt'.format('data/projects/{0}/logs/converted'.format(projectName))))
    threshold = 2  # 無視するコミットの単語数の閾値
    index = 1
    for fileName in fileList:
        code = np.loadtxt(fileName, dtype='int')
        baseName = os.path.basename(fileName)
        print(index, '/', len(fileList), fileName)
        if code.size < 2: # 1単語以下4001単語以上の差分ファイルは除去
            shutil.move(fileName, 'data/projects/{0}/logs/trash_data/{1}'.format(projectName, baseName))
        index += 1