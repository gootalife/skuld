import numpy as np

if __name__ == '__main__':
    codes_count = 1
    code_len = 2000
    courpus_len = 1000
    matrix = np.load('embedding_matrix.npy')
    table = np.zeros((courpus_len, 128))
    codes = np.load('convertedCodes.npy')

    for i in range(codes_count): # ファイル数回
        for j in range(len(codes[i])): # 各ファイルの要素数回
            table[int(codes[i,j])] = matrix[i, j] # 数字とベクトルの組を対応させる
    np.save('embedding_table.npy', table)  # 保存