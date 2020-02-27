# skuld
卒業研究

## 使い方
設定ファイル settings.json
```json
{
    "projectName": "camel", # 対象プロジェクト名
    "extensions": [".java", ".c", ".h", ".cpp", ".hpp", ".cxx", ".hxx"], # 対象ファイルの拡張子
    "testDir": "camel" # 学習済みモデルへのテスト入力フォルダ(testsフォルダ以下)
}

```
* extension … 取得するファイルの拡張子を指定
* project … 対象とするプロジェクトの名前

コミット毎の差分の取得
```
python gitdiff.py {履歴を取得するプロジェクトのパス(.gitが存在するパス)}
```

差分の単語分割
```
perl extracting.pl
```

差分の数値変換
```
python preprocessing.py
```

使用できない(空白)データの移動
```
python remove.py
```

埋め込み表現の学習
※激重
```
python embedding.py
```

学習用データとテスト用データのk分割 k = 10
```
python split.py
```

学習の実行
```
python network.py {試行番号}
python network.py 1
```

学習済みモデルの使用
```
python prediction.py {試行番号}
python prediction.py 1
```

## 分析
ROC曲線の出力
```
python excel_to_ROCcurve.py -i {ファイル名} -x {シート番号}
python excel_to_ROCcurve.py -i ROCcurve.xls -x 0
```

単語の出現回数の集計
```
python wordTotalizing.py
```

プロジェクトの統計
```
python fileCount.py
```