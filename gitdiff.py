import pandas as pd
import subprocess
import sys
import os
import setting

# ファイル一覧の取得
def getFilelist(commitHash):
    # command = 'git checkout {0}'.format(commitHash)
    # subprocess.run(command.split(), stdout=subprocess.PIPE)
    # コマンドを実行し、結果を受け取る。
    command = 'git diff --name-only --diff-filter=d {0}^ {1}'.format(commitHash, commitHash)
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    fileList = result.stdout.decode().split()
    return fileList

# ファイル変更の取得
def getGitDiff(commitHash, fileName):
    # コマンドを実行し、結果を受け取る。
    command = 'git --no-pager diff -U3 -w --ignore-blank-lines {0}^ {1} {2}'.format(commitHash, commitHash, fileName)
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    diff = result.stdout.decode().split('\n')
    code = []
    # print(result.stdout.decode() + '\n')
    for index, line in enumerate(diff):
        if index <= 3 or line.startswith('@@') or line.startswith('-'):
            continue;
        print(line)
        if line.startswith('+'):
            line = line.replace('+', '', 1)
        else:
            line = line.replace(' ', '', 1)
        aline = line.lstrip()
        if aline.startswith('//') or aline.startswith('/*') or aline.startswith('*') or aline.startswith('*/'):
            continue;
        code.append(line)
    code.insert(0, fileName)
    # リストの文字列化
    codeStr = ''
    for line in code:
        codeStr += '{0}\n'.format(line)
    return codeStr

# 各コミットハッシュの変更の取得
def getGitDiffs(commitLogs, currentDirectory, projectDirectory, extension):
    # git diffの取得
    for commitHash in commitLogs.commit_hash:
        # 対象プロジェクトのディレクトリに移動
        os.chdir(projectDirectory)
        fileList = getFilelist(commitHash)
        # 変更の取得
        code = ''
        for fileName in fileList:
            # 拡張子が指定のものだけ
            os.chdir(projectDirectory)
            if fileName.endswith('.{0}'.format(extension)):
                # print(fileName)
                # カレントディレクトリへ移動
                code += getGitDiff(commitHash, fileName)
            os.chdir(currentDirectory)
        # 出力
        with open('data/lscp/in/{0}.{1}'.format(commitHash, extension), 'w') as file:
            file.write(code)
        # print('\n------ ' + commitHash + ' ------\n')
        # print(code)

if __name__ == '__main__':
    argc = len(sys.argv)
    if (argc != 2):
        print('対象プロジェクトのディレクトリを引数に1つ指定してください。')
        quit()
    # commit履歴csvの読み込み
    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    commitLogs = pd.read_csv('data/projects/{0}.csv'.format(projectName))
    # ディレクトリ情報の保持
    currentDirectory = os.getcwd()
    projectDirectory = sys.argv[1]
    # git diffの複数取得
    getGitDiffs(commitLogs, currentDirectory, projectDirectory, extension)
