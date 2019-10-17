# create_csv_for_textmining
総研レポートなどで使用する富士通のテキストマイニングツールに必要なCSVファイルを生成するスクリプト

## 稼働環境

backup-storage01.tracking.private.okwave.jp
```
/var/lib/file/tracking-data/create_csv_for_textmining
```

## 環境構築とアプリケーションの実行

dotenv.sample をコピーして環境に合わせて編集する
```
$ cp dotenv.sample .env
```

question_ids.csv.sample をコピーして環境に合わせて編集する
```
$ cp question_ids.csv.sample question_ids.csv
```

question_ids.csv に解析したい質問IDを記述する。
※接続対象のDBによっては負荷が掛かることでアラートが飛ぶ場合があるので、実行する際は運用チームに一声掛けること

questions.py を実行する
```
$ python questions.py
```
