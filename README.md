# skuld
卒業研究

## 使い方
設定ファイル settings.ini
```ini
[Info]
extension = java # 対象ファイルの拡張子
project = hadoop # 対象プロジェクト名
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

埋め込み表現の学習
```
python embedding.py
```

学習の実行
```
python network.py
```