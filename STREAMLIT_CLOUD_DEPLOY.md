# Streamlit Community Cloud デプロイ手順

## ✅ 現在の状態

- ✅ GitHubリポジトリ: `https://github.com/skbnw/tclip_search`
- ✅ アプリのメインファイル: `code/02-web-app/search_display_app.py`
- ✅ アプリのURL: `tclipsearch-bav7bfzzkyuz8z6t9j7fof.streamlit.app`

## 📝 デプロイ設定

### 1. GitHub URL（インタラクティブピッカー）

以下のURLを入力してください：

```
https://github.com/skbnw/tclip_search/blob/main/code/02-web-app/search_display_app.py
```

または、インタラクティブピッカーを使用する場合：
1. 「インタラクティブピッカーに切り替える」をクリック
2. リポジトリ: `skbnw/tclip_search`
3. ブランチ: `main`
4. メインファイル: `code/02-web-app/search_display_app.py`

### 2. アプリのURL

```
tclipsearch-bav7bfzzkyuz8z6t9j7fof
```

ドメイン: `.streamlit.app`

### 3. 詳細設定

#### 必須の環境変数（Secrets）

Streamlit Cloudの「Settings」→「Secrets」で以下を設定してください：

```toml
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
AWS_DEFAULT_REGION = "ap-northeast-1"

[groq]
api_key = "YOUR_GROQ_API_KEY"
```

**注意**: `YOUR_AWS_ACCESS_KEY_ID`, `YOUR_AWS_SECRET_ACCESS_KEY`, `YOUR_GROQ_API_KEY` を実際の値に置き換えてください。

#### Pythonバージョン

- デフォルト: Python 3.11
- 推奨: Python 3.11 または 3.12

#### 依存パッケージ

プロジェクトルートの `requirements.txt` が自動的に使用されます：

```txt
streamlit>=1.28.0
boto3>=1.28.0
botocore>=1.31.0
```

---

## 🚀 デプロイ手順

### ステップ1: Secretsの設定

1. Streamlit Cloudのダッシュボードでアプリを選択
2. 「Settings」→「Secrets」をクリック
3. 以下の内容を貼り付け：

```toml
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
AWS_DEFAULT_REGION = "ap-northeast-1"

[groq]
api_key = "YOUR_GROQ_API_KEY"
```

**注意**: `YOUR_AWS_ACCESS_KEY_ID`, `YOUR_AWS_SECRET_ACCESS_KEY`, `YOUR_GROQ_API_KEY` を実際の値に置き換えてください。

4. 「Save」をクリック

### ステップ2: アプリのデプロイ

1. 「Deploy」タブをクリック
2. 以下を確認：
   - **Repository**: `skbnw/tclip_search`
   - **Branch**: `main`
   - **Main file path**: `code/02-web-app/search_display_app.py`
3. 「Deploy」ボタンをクリック

### ステップ3: デプロイの確認

1. デプロイログを確認してエラーがないか確認
2. アプリのURLにアクセス: `https://tclipsearch-bav7bfzzkyuz8z6t9j7fof.streamlit.app`
3. アプリが正常に動作するか確認

---

## ⚠️ 注意事項

### 1. 認証情報のセキュリティ

- **Secrets**に設定した認証情報は暗号化されて保存されます
- GitHubリポジトリに認証情報を直接コミットしないでください
- `.gitignore`で`.env`ファイルが除外されていることを確認してください

### 2. S3バケットのアクセス権限

- AWSアクセスキーには、S3バケット `tclip-raw-data-2025` への読み取り権限が必要です
- 必要に応じて、IAMポリシーで権限を制限してください

### 3. パフォーマンス

- 大量のデータを検索する場合、最初の読み込みに時間がかかる場合があります
- Streamlit Cloudの無料プランには制限があります（CPU、メモリ、リクエスト数など）

---

## 🔧 トラブルシューティング

### エラー: "ModuleNotFoundError"

**原因**: 依存パッケージがインストールされていない

**解決策**:
1. `requirements.txt`を確認
2. 必要に応じてパッケージを追加
3. 再デプロイ

### エラー: "Access Denied"（S3）

**原因**: AWS認証情報が正しく設定されていない

**解決策**:
1. Streamlit Cloudの「Settings」→「Secrets」を確認
2. 認証情報が正しく設定されているか確認
3. AWSアクセスキーの権限を確認

### エラー: "File not found"

**原因**: メインファイルのパスが間違っている

**解決策**:
1. GitHubリポジトリでファイルパスを確認
2. デプロイ設定の「Main file path」を確認
3. 正しいパスに修正（例: `code/02-web-app/search_display_app.py`）

---

## 📚 関連ドキュメント

- [Streamlit Community Cloud Documentation](https://docs.streamlit.io/streamlit-community-cloud)
- [Streamlit Secrets Management](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app/secrets-management)
- [AWS S3 Access Control](https://docs.aws.amazon.com/s3/latest/userguide/access-control.html)

---

## 🔄 アップデート方法

コードを更新したら：

1. ローカルで変更をコミット
2. GitHubにプッシュ: `git push origin main`
3. Streamlit Cloudが自動的に再デプロイ（数分かかる場合があります）

または、Streamlit Cloudのダッシュボードで「Reboot app」をクリックして手動で再起動できます。

