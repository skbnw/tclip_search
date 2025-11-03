# tclip JSON Uploader - 共有用ドキュメント

ローカルNAS上の統合JSONファイルと画像ファイルをS3にアップロードするツールです。

## 📋 概要

このツールは、ローカルNAS上のq1.00ファイル（完成版の統合JSONファイル）を読み込み、以下の処理を行います：

- **マスターデータ**: 番組全体のメタデータとフルテキストをS3にアップロード
- **チャンクデータ**: トランスクリプトをセグメント単位でチャンキングしてS3にアップロード
- **画像ファイル**: JSON内のscreenshots配列から画像ファイルを取得してS3にアップロード
- **更新チェック**: ファイルが更新されている場合のみ再アップロード（重複回避）

## 🗂️ プロジェクト構成

```
tclip-Json-uploader/
├── venv/              # Python仮想環境
├── code/              # コードフォルダ
│   └── 01-s3-upload/  # S3アップロード処理
│       └── s3_upload_rag_data_v1.3.py
├── output/            # 出力ファイル（参考資料など）
└── README.md          # このファイル
```

## 🚀 セットアップ

### 1. 仮想環境の作成とパッケージインストール

```powershell
# 仮想環境の作成
python -m venv venv

# 仮想環境のアクティベート
.\venv\Scripts\Activate.ps1

# 必要なパッケージのインストール
pip install boto3 jsonlines
```

### 2. AWS認証情報の設定

#### 方法1: 環境変数で設定（推奨）

```powershell
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="ap-northeast-1"
```

**注意**: `YOUR_ACCESS_KEY_ID` と `YOUR_SECRET_ACCESS_KEY` は、管理者から提供された実際の認証情報に置き換えてください。

#### 方法2: AWS CLIで設定

```powershell
aws configure
# Access Key ID: （管理者から提供されたID）
# Secret Access Key: （管理者から提供されたキー）
# Default region: ap-northeast-1
```

### 3. 認証情報の確認

```powershell
aws sts get-caller-identity
```

## 📝 使用方法

### 基本的な実行方法

```powershell
# 仮想環境をアクティベート
.\venv\Scripts\Activate.ps1

# AWS認証情報を設定（環境変数の場合）
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="ap-northeast-1"

# スクリプトの実行
python code\01-s3-upload\s3_upload_rag_data_v1.3.py
```

### 処理内容

1. **ファイル探索**: NAS配下を再帰的に探索
2. **フィルタリング**: ファイル名に `q1.00` が含まれるJSONファイルのみを処理
3. **更新チェック**: S3のオブジェクト更新日時とNASファイルの更新日時を比較
4. **アップロード**: 更新されているファイルのみ再アップロード
5. **画像処理**: JSON内のscreenshots配列から画像ファイルを取得してS3にアップロード

## 🗄️ S3バケット情報

- **バケット名**: `tclip-raw-data-2025`
- **リージョン**: `ap-northeast-1` (アジアパシフィック 東京)
- **ARN**: `arn:aws:s3:::tclip-raw-data-2025`

### S3パス構造

```
tclip-raw-data-2025/
└── rag/
    ├── master_text/        # マスターデータ
    │   └── {event_id}.jsonl
    ├── vector_chunks/      # チャンクデータ
    │   └── {event_id}_segments.jsonl
    └── images/             # 画像ファイル
        └── {event_id}/
            └── {filename}.jpeg
```

## 🔐 アクセス権限の取得方法

### 管理者への依頼手順

1. **アクセス権限の依頼**
   - 管理者に「tclip-raw-data-2025 バケットへの読み取り・書き込みアクセス権限」を依頼
   - 使用目的と必要な権限範囲を明確に伝える

2. **IAMユーザーの作成依頼**
   ```
   必要な権限:
   - s3:GetObject
   - s3:PutObject
   - s3:ListBucket
   
   対象リソース:
   - arn:aws:s3:::tclip-raw-data-2025
   - arn:aws:s3:::tclip-raw-data-2025/*
   ```

3. **アクセスキーの受け取り**
   - アクセスキーID
   - シークレットアクセスキー
   - リージョン情報

### アクセス権限の設定方法（管理者向け）

#### 方法1: IAMユーザーを作成（推奨）

1. **IAMコンソールで新しいユーザーを作成**
   ```
   AWSコンソール > IAM > ユーザー > ユーザーを追加
   ```

2. **プログラムアクセス用のアクセスキーを作成**
   - アクセスの種類: 「プログラムによるアクセス」を選択
   - アクセスキーIDとシークレットアクセスキーを生成

3. **S3へのアクセス権限を付与**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::tclip-raw-data-2025",
           "arn:aws:s3:::tclip-raw-data-2025/*"
         ]
       }
     ]
   }
   ```

4. **アクセスキーを安全に共有**
   - メールやチャットではなく、安全な方法で共有
   - パスワード管理ツールなどを使用

## 🔧 機能詳細

### v1.3の主な機能

1. **ファイル更新チェック**
   - S3オブジェクトの存在確認と最終更新日時の取得
   - NASファイルの更新日時と比較
   - 5秒以上の差分がある場合のみ再アップロード

2. **重複回避**
   - 既に最新のファイルは自動的にスキップ
   - 処理時間とS3リクエスト数を削減

3. **画像ファイルアップロード**
   - JSON内のscreenshots配列から画像パスを取得
   - Linuxパス形式からWindows NASパスに変換
   - 各画像を `rag/images/{event_id}/{filename}.jpeg` としてアップロード

4. **エラーハンドリング**
   - JSON解析エラーの適切な処理
   - ファイルが見つからない場合の警告
   - エラー時も処理を継続（他のファイルへの影響を最小化）

## 📊 実行例

### 実行ログの例

```
[INFO] ファイル探索を開始: \\NAS-TKY-2504\database\program-integration
[INFO] q1.00ファイルを発見: \\NAS-TKY-2504\database\program-integration\20251015\NHKG-TKY\...
...
[INFO] 合計 1332 個のq1.00ファイルが見つかりました
================================================================================

[1/1332] 処理中...
[INFO] ファイル処理を開始: \\NAS-TKY-2504\database\program-integration\...
[INFO] doc_id: AkxAQAELAAM
[INFO] 51個のチャンクを生成
[INFO] 26個の画像ファイルを処理中...
[OK] 画像アップロード完了: s3://tclip-raw-data-2025/rag/images/...
[OK] S3にアップロード完了: s3://tclip-raw-data-2025/rag/master_text/AkxAQAELAAM.jsonl
[OK] S3にアップロード完了: s3://tclip-raw-data-2025/rag/vector_chunks/AkxAQAELAAM_segments.jsonl
[OK] ファイル処理完了

[2/1332] 処理中...
[SKIP] ファイルは最新のためスキップ: ...
  理由: ファイルは最新です (スキップ)
```

### 処理結果サマリー

```
================================================================================
[SUMMARY] 処理完了
  成功: 1332 ファイル
  失敗: 0 ファイル
  合計: 1332 ファイル
```

## 🔍 トラブルシューティング

### エラー: `InvalidAccessKeyId`

**原因**: AWS認証情報が正しく設定されていない

**解決方法**:
```powershell
# 環境変数が正しく設定されているか確認
$env:AWS_ACCESS_KEY_ID
$env:AWS_SECRET_ACCESS_KEY
$env:AWS_DEFAULT_REGION

# 認証情報を再設定（管理者から提供された正しい値を使用）
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="ap-northeast-1"
```

### エラー: `AccessDenied`

**原因**: S3バケットへのアクセス権限がない

**解決方法**:
- 管理者にアクセス権限の付与を依頼
- IAMユーザーに適切な権限が設定されているか確認

### エラー: ファイルが見つからない

**原因**: NASパスがアクセスできない、またはファイルが存在しない

**解決方法**:
- NASへのネットワーク接続を確認
- ファイルパスが正しいか確認
- ファイル名に `q1.00` が含まれているか確認

## 📦 依存パッケージ

- **boto3**: AWS S3操作
- **jsonlines**: JSON Lines形式の処理

## 📝 バージョン履歴

### v1.3 (2025/11/02)
- ファイル更新チェック機能を追加
- 更新されているファイルのみ再アップロード（重複回避）

### v1.2 (2025/11/02)
- 画像ファイル（screenshots）のS3アップロード機能を追加

### v1.1 (2025/11/02)
- program-integration配下全体を探索
- q1.00ファイルのバッチ処理に対応

## 📞 サポート

問題が発生した場合は、以下の情報を確認してください：

1. AWS認証情報が正しく設定されているか
2. NASへのネットワーク接続が確立されているか
3. ファイル名に `q1.00` が含まれているか
4. S3バケットへのアクセス権限があるか

管理者への問い合わせが必要な場合は、上記の情報を添えて連絡してください。

## 🔒 セキュリティ注意事項

- **アクセスキーは絶対に公開しない**: コードやコミットメッセージに含めない
- **環境変数での管理を推奨**: コード内に直接記述しない
- **定期的なキーローテーション**: セキュリティのため定期的に変更
- **最小権限の原則**: 必要最小限の権限のみを使用

---

**バージョン**: 1.3  
**最終更新**: 2025/11/02

