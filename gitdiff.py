import pandas as pd
import subprocess
import sys
import os
import setting

# ファイル一覧の取得
def getFilelist(commitHash):
    command = 'git checkout {0}'.format(commitHash)
    subprocess.run(command.split())
    command = 'git checkout .'
    subprocess.run(command.split())
    # コマンドを実行し、結果を受け取る。
    command = 'git diff --name-only --diff-filter=d {0}^ {1}'.format(commitHash, commitHash)
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    fileList = result.stdout.decode().split()
    return fileList

# ファイル変更の取得
def getGitDiff(commitHash, fileName):
    # コマンドを実行し、結果を受け取る。
    command = 'git --no-pager diff -U3 -w --ignore-blank-lines {0}^ {1} {2}'.format(commitHash, commitHash, fileName)
    # result = subprocess.run(command.split(), stdout=subprocess.PIPE, universal_newlines=True, encoding='utf-8')
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    diff = result.stdout.decode('utf-8', 'ignore').split('\n')
    # diff = result.stdout.decode('shift_jis').split('\n')
    # diff = result.stdout.split('\n')
    code = []
    # print(result.stdout.decode() + '\n')
    for index, line in enumerate(diff):
        if index <= 3 or line.startswith('@@') or line.startswith('-'):
            continue;
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
def getGitDiffs(commitLogs, currentDirectory, projectDirectory, extension, projectName):
    for index, item in commitLogs.iterrows():
        # if index < 30785:
        #     continue
        print(str(index + 1) + '/' + str(len(commitLogs)))  # 進行状況
        commitHash = item['commit_hash']
        containsBug = item['contains_bug']
        # 対象プロジェクトのディレクトリに移動
        os.chdir(projectDirectory)
        fileList = getFilelist(commitHash)
        # 変更の取得
        code = ''
        for fileName in fileList:
            os.chdir(projectDirectory)
            if fileName.endswith('.{0}'.format(extension)): # 拡張子が指定のものだけ
                code += getGitDiff(commitHash, fileName)
            os.chdir(currentDirectory)  # カレントディレクトリへ移動
        os.chdir(currentDirectory) # カレントディレクトリへ移動
        # 出力
        with open('data/projects/{0}/logs/commits/{1}.{2}'.format(projectName, commitHash, extension), 'w', encoding='utf-8') as file:
            file.write(code)
        with open('data/projects/{0}/logs/labels/{1}.txt'.format(projectName , commitHash), 'w', encoding='utf-8') as file:
            if containsBug:
                file.write('1')
            else:
                file.write('0')
    # command = 'git stash clear'
    # subprocess.run(command.split())



if __name__ == '__main__':
    argc = len(sys.argv)
    if (argc != 2):
        print('対象プロジェクトのディレクトリを引数に1つ指定してください。')
        quit()
    # commit履歴csvの読み込み
    extension = setting.get('settings.ini', 'Info', 'extension')
    projectName = setting.get('settings.ini', 'Info', 'project')
    # プロジェクトごとに必要なデータ保存用フォルダの作成
    os.makedirs('data/projects/{0}/logs/commits'.format(projectName), exist_ok=True)
    os.makedirs('data/projects/{0}/logs/labels'.format(projectName), exist_ok=True)
    os.makedirs('data/projects/{0}/logs/converted'.format(projectName), exist_ok=True)
    os.makedirs('data/projects/{0}/logs/preprocessed'.format(projectName), exist_ok=True)
    os.makedirs('data/projects/{0}/logs/test_data'.format(projectName), exist_ok=True)
    os.makedirs('data/projects/{0}/logs/trash_data'.format(projectName), exist_ok=True)
    commitLogs = pd.read_csv('data/projects/{0}/{1}.csv'.format(projectName, projectName))
    # ディレクトリ情報の保持
    currentDirectory = os.getcwd()
    projectDirectory = sys.argv[1]
    # git diffの複数取得
    getGitDiffs(commitLogs, currentDirectory, projectDirectory, extension, projectName)
