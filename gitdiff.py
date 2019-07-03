import pandas as pd
import subprocess
import sys
import os
import setting

# ファイル一覧の取得
def getFilelist(commitHash):
    # コマンドを実行し、結果を受け取る。
    command = 'git diff --name-only --diff-filter=d {0}^ {1}'.format(commitHash, commitHash)
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    fileList = result.stdout.decode().split()
    return fileList

# 変更の取得
def getGitDiff(commitHash, fileName):
    # コマンドを実行し、結果を受け取る。
    command = 'git --no-pager diff -w --ignore-blank-lines {0}^ {1} {2}'.format(commitHash, commitHash, fileName)
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    diff = result.stdout.decode().split('\n')
    code = []
    for index, line in enumerate(diff):
        if index <= 3 or line.startswith('@@') or line.startswith('-'):
            continue;
        if line.startswith('+'):
            code.append(line.replace('+', ' ', 1))
        else:
            code.append(line)
    code.insert(0, fileName)
    codeStr = ''
    for line in code:
        print(line)
        codeStr += '{0}\n'.format(line)
    return codeStr

if __name__ == '__main__':
    argc = len(sys.argv)
    if (argc != 2):
        print('対象プロジェクトのディレクトリを引数に1つ指定してください。')
        quit()
    # commit履歴CSVの読み込み
    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    commitLogs = pd.read_csv('data/commits/{0}.csv'.format(projectName))
    # ディレクトリ情報の保持
    currentDirectory = os.getcwd()
    projectDirectory = sys.argv[1]

    # バグ無しコミットのみのリストを作成
    clearCommitHash = []
    for i in range(2):
        if commitLogs.contains_bug[i] == False:
            clearCommitHash.append(commitLogs.commit_hash[i])
    # git diffの取得
    for commitHash in clearCommitHash:
        index = 1
        # 対象プロジェクトのディレクトリに移動
        os.chdir(projectDirectory)
        fileList = getFilelist(commitHash)
        # 変更の取得
        for fileName in fileList:
            # 拡張子が指定のものだけ
            if fileName.endswith(extension):
                os.chdir(projectDirectory)
                code = getGitDiff(commitHash, fileName)
                os.chdir(currentDirectory)
                with open('data/lscp/in/{0}_{1}.{2}'.format(commitHash, index, extension), 'w') as file:  #出力先
                    file.write(code)
                    index += 1