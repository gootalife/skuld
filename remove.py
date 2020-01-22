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
    # フォルダ内のファイル一覧を取得
    fileList = sorted(glob.glob('{0}/*.{1}'.format('data/projects/{0}/logs/converted'.format(projectName), extension)))
    threshold = 2  # 無視するコミットの単語数の閾値
    index = 1
    for fileName in fileList:
        code = np.loadtxt(fileName, dtype='int')
        baseName = os.path.basename(fileName)
        print(index, '/', len(fileList), fileName)
        if code.size < 2:  # 2単語未満のコミットは無視
            shutil.move(fileName, 'data/projects/{0}/logs/trash_data/{1}'.format(projectName, baseName))
        index += 1