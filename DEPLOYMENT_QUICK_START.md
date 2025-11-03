# 🚀 Streamlit Cloud デプロイ - クイックスタート

## ✅ デプロイ前に確認すること

### 1. 必要なファイルの確認

以下のファイルが存在することを確認：

- ✅ `requirements.txt` (プロジェクトルート)
- ✅ `.streamlit/config.toml`
- ✅ `code/02-web-app/search_display_app.py`

### 2. AWS認証情報の準備

**⚠️ 重要**: 以下の情報を準備してください（コードに含めないこと）

- AWS Access Key ID
- AWS Secret Access Key
- AWS Region (ap-northeast-1)

---

## 📝 デプロイ手順（5分で完了）

### ステップ1: GitHubリポジトリを作成

1. GitHubにアクセス: https://github.com
2. 「New repository」をクリック
3. リポジトリ名を入力（例: `tclip-json-uploader`）
4. **Public**を選択（無料プランの場合）
5. 「Create repository」をクリック

### ステップ2: コードをGitHubにプッシュ

```powershell
# プロジェクトフォルダで実行
git init
git add .
git commit -m "Initial commit: Streamlit search app"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/tclip-json-uploader.git
git push -u origin main
```

**注意**: `YOUR_USERNAME`を自分のGitHubユーザー名に置き換えてください。

### ステップ3: Streamlit Cloudでアプリを作成

1. **Streamlit Cloudにアクセス**
   - https://share.streamlit.io/ にアクセス
   - GitHubアカウントでログイン（初回は認証が必要）

2. **新しいアプリを作成**
   - 「New app」ボタンをクリック
   - 以下を入力：
     ```
     Repository: YOUR_USERNAME/tclip-json-uploader
     Branch: main
     Main file path: code/02-web-app/search_display_app.py
     ```

3. **環境変数を設定**
   - 「Advanced settings」をクリック
   - 「Secrets」タブを選択
   - 以下の形式で環境変数を追加：
     ```toml
     AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
     AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
     AWS_DEFAULT_REGION = "ap-northeast-1"
     ```

4. **デプロイ**
   - 「Deploy」ボタンをクリック
   - デプロイが完了するまで数分待つ（初回は5-10分程度）

### ステップ4: アプリの確認

- Streamlit Cloudが提供するURLにアクセス
- アプリが正常に動作することを確認
- 検索機能をテスト

---

## 🔧 よくある問題と解決方法

### 問題1: デプロイが失敗する

**確認事項**:
- `requirements.txt`がプロジェクトルートにあるか
- Streamlit Cloudのログを確認（エラーメッセージを確認）

### 問題2: AWS認証エラー

**確認事項**:
- 環境変数が正しく設定されているか
- AWS Access Keyが有効か
- IAMロールにS3の読み取り権限があるか

### 問題3: モジュールが見つからない

**確認事項**:
- `requirements.txt`に必要なパッケージが含まれているか
- Streamlit Cloudのログで、どのパッケージが見つからないか確認

---

## 📞 次のステップ

- ✅ アプリが正常に動作していることを確認
- ✅ 検索機能をテスト
- ✅ 複数のユーザーでテスト（1000人規模での動作確認）

詳細な情報は `code/02-web-app/DEPLOYMENT_STEPS.md` を参照してください。

