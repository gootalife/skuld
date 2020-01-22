# skuld
卒業研究

## 使い方
設定ファイル settings.ini
```ini
[Info]
extension = java # 対象ファイルの拡張子
project = hadoop # 対象プロジェクト名
cv_case = 1 # クロスバリデーションのケース番号

[Test]
data_dir = hadoop # 学習済みモデルへのテスト入力フォルダ(testsフォルダ以下)
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

使用できないデータの移動
```
python remove.py
```

埋め込み表現の学習
```
python embedding.py
```

学習用データとテスト用データのk分割 k = 10
```
python split.py
```

学習の実行
```
python network.py
```

学習済みモデルの使用
```
python prediction.py
```

ROC曲線の出力
```
python excel_to_ROCcurve.py -i {ファイル名} -x {シート番号}
python excel_to_ROCcurve.py -i ROCcurve.xlsx -x 0
```