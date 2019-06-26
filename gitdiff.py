import pandas as pd
import subprocess
import sys
import os
import setting

if __name__ == '__main__':
    argc = len(sys.argv)
    if (argc != 2):
        print('対象プロジェクトのディレクトリを引数に指定してください。')
        quit()
    # commit履歴CSVの読み込み
    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    commitLogs = pd.read_csv('data/commits/{0}.csv'.format(projectName))
    # コマンドを実行し、結果を受け取る。
    projectDirectory = sys.argv[1]
    commitHash1 = commitLogs.commit_hash[0]
    commitHash2 = commitLogs.commit_hash[1]
    os.chdir(projectDirectory)
    # ファイル一覧の取得
    command = 'git diff --name-only {0} {1}'.format(commitHash1, commitHash2)
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    changedFiles = result.stdout.decode().split()

    # 変更の取得
    command = 'git diff {0} {1} {2}'.format(commitHash1, commitHash2, changedFiles[1])
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    changedContent = result.stdout.decode()
    print(changedContent)