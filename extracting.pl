use lscp;
use Config::Tiny;

$preprocessor = lscp->new;
# 設定ファイルの読み込み
$settings = Config::Tiny->read('settings.ini');
# 値へのアクセス
$projectName = $settings->{Info}->{project};

$preprocessor->setOption("logLevel", "error");
$preprocessor->setOption("inPath", "data/projects/" . $projectName . "/logs/commits");
$preprocessor->setOption("outPath", "data/projects/" . $projectName . "/logs/preprocessed");

$preprocessor->setOption("isCode", 1); #コードを対象とする
$preprocessor->setOption("doComments", 0); #コメントを除去
$preprocessor->setOption("doRemoveDigits", 1); #数値を除去
$preprocessor->setOption("doLowerCase", 1); #小文字化
$preprocessor->setOption("doTokenize", 1); #識別子名分割
$preprocessor->setOption("doRemovePunctuation", 1); #記号除去
$preprocessor->setOption("doRemoveSmallWords", 1); #短い単語を除去
$preprocessor->setOption("smallWordSize", 1); #単語の最小サイズ

# And any other options you wish to set

$preprocessor->preprocess();