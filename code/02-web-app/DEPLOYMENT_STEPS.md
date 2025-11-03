# Streamlit Community Cloud デプロイ手順

## 📋 事前準備

### 1. GitHubリポジトリの作成

1. GitHubにログイン
2. 新しいリポジトリを作成（例: `tclip-json-uploader`）
3. リポジトリを公開（Public）に設定（無料プランの場合）
   - または、Privateリポジトリの場合はStreamlit Teamプランが必要

### 2. 必要なファイルの確認

以下のファイルがプロジェクトルートにあることを確認：

```
tclip-Json-uploader/
├── requirements.txt          # ← プロジェクトルートに必要
├── .streamlit/
│   └── config.toml          # ← Streamlit設定ファイル
└── code/
    └── 02-web-app/
        ├── search_display_app.py
        └── requirements.txt  # ← アプリ固有の依存関係（オプション）
```

### 3. 環境変数の準備

Streamlit Cloudで設定する環境変数：

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=ap-northeast-1
```

⚠️ **重要**: AWS認証情報はコードに含めず、Streamlit Cloudの環境変数で管理

---

## 🚀 デプロイ手順

### ステップ1: GitHubにコードをプッシュ

```powershell
# Gitリポジトリを初期化（まだの場合）
git init

# リモートリポジトリを追加
git remote add origin https://github.com/yourusername/tclip-json-uploader.git

# ファイルをステージング
git add .

# コミット
git commit -m "Initial commit: Streamlit search app"

# GitHubにプッシュ
git branch -M main
git push -u origin main
```

### ステップ2: Streamlit Cloudでアプリを作成

1. **Streamlit Cloudにアクセス**
   - https://share.streamlit.io/ にアクセス
   - GitHubアカウントでログイン

2. **新しいアプリを作成**
   - 「New app」ボタンをクリック
   - 以下の情報を入力：
     - **Repository**: `yourusername/tclip-json-uploader`
     - **Branch**: `main`
     - **Main file path**: `code/02-web-app/search_display_app.py`

3. **環境変数を設定**
   - 「Advanced settings」をクリック
   - 「Secrets」タブで以下を追加：
     ```
     AWS_ACCESS_KEY_ID=your_access_key
     AWS_SECRET_ACCESS_KEY=your_secret_key
     AWS_DEFAULT_REGION=ap-northeast-1
     ```

4. **デプロイ**
   - 「Deploy」ボタンをクリック
   - 数分でデプロイが完了

---

## 🔧 トラブルシューティング

### エラー: Module not found

**原因**: `requirements.txt`が正しく読み込まれていない

**解決方法**:
- `requirements.txt`がプロジェクトルートにあることを確認
- Streamlit Cloudのログを確認して、どのパッケージが見つからないか確認

### エラー: AWS認証情報が無効

**原因**: 環境変数が正しく設定されていない

**解決方法**:
- Streamlit Cloudの「Settings」→「Secrets」で環境変数を確認
- 値に余分なスペースがないか確認

### エラー: S3アクセス権限エラー

**原因**: IAMロールにS3の読み取り権限がない

**解決方法**:
- AWS IAMで、使用しているAccess Keyに以下の権限を付与：
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
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

---

## 📝 デプロイ後の確認

### 1. アプリの動作確認

- Streamlit Cloudで提供されるURLにアクセス
- 検索機能が正常に動作するか確認
- S3からのデータ取得が正常に行われるか確認

### 2. パフォーマンスの確認

- 初回データ読み込み時間を確認
- 検索の応答時間を確認
- エラーログを確認（Streamlit Cloudのログビューア）

### 3. セキュリティの確認

- 環境変数に認証情報が含まれているか確認
- S3バケットのアクセス権限が最小限になっているか確認

---

## 🔄 更新方法

コードを更新した場合：

```powershell
# 変更をコミット
git add .
git commit -m "Update: 新機能追加"

# GitHubにプッシュ
git push origin main
```

Streamlit Cloudは自動的に変更を検知して再デプロイします。

---

## 📊 モニタリング

### Streamlit Cloudダッシュボード

- **URL**: https://share.streamlit.io/
- **機能**:
  - アプリのログ確認
  - エラーの監視
  - デプロイ履歴の確認

### AWS CloudWatch（オプション）

S3アクセスの監視：
- S3リクエスト数の確認
- データ転送量の確認
- エラー率の確認

---

## 💡 ベストプラクティス

1. **環境変数の管理**
   - 本番環境と開発環境で異なる認証情報を使用
   - 定期的に認証情報をローテーション

2. **パフォーマンス最適化**
   - Streamlitのキャッシュ機能を活用
   - 大量データの読み込みは段階的に実装

3. **エラーハンドリング**
   - ユーザーフレンドリーなエラーメッセージを表示
   - ログで詳細なエラー情報を記録

4. **セキュリティ**
   - IAMロールで最小権限を設定
   - 定期的にアクセスログを確認

---

## 📞 サポート

問題が発生した場合：

1. Streamlit Cloudのログを確認
2. GitHubのIssuesで質問
3. Streamlit公式ドキュメントを参照: https://docs.streamlit.io/

