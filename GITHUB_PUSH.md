# GitHubリポジトリへのプッシュ手順

## ✅ 準備完了

- ✅ Gitリポジトリの初期化済み
- ✅ 初回コミット完了（19ファイル）
- ✅ リモートリポジトリ追加済み: `https://github.com/skbnw/tclip_search.git`
- ✅ ブランチ名: `main`

## 🔐 認証方法：Personal Access Token（PAT）を使用

### ステップ1: GitHubでPersonal Access Tokenを生成

1. **GitHubにログイン**
   - https://github.com にアクセス

2. **Personal Access Tokenを作成**
   - 右上のプロフィール画像をクリック
   - 「Settings」をクリック
   - 左サイドバー下部の「Developer settings」をクリック
   - 「Personal access tokens」→「Tokens (classic)」をクリック
   - 「Generate new token」→「Generate new token (classic)」をクリック

3. **トークン設定**
   - **Note**: `tclip_search_push`（任意の名前）
   - **Expiration**: お好みの期間を選択（例: 90 days）
   - **Scopes**: `repo` にチェック
     - これにより、リポジトリへの読み書きが可能になります
   - 「Generate token」をクリック

4. **⚠️ 重要：トークンをコピー**
   - 表示されるトークンをコピーしてください（例: `ghp_xxxxxxxxxxxxxxxxxxxx`）
   - **この画面を閉じると再表示されません**

### ステップ2: コードをプッシュ

PowerShellで以下を実行してください：

```powershell
git push -u origin main
```

認証が求められたら：
- **Username**: `skbnw`（GitHubユーザー名）
- **Password**: **Personal Access Tokenを貼り付け**（GitHubパスワードではありません）

---

## 🔄 代替方法：Git Credential Managerを使用

### Windows Credential Managerに保存する場合

```powershell
# 一度プッシュすると、認証情報が保存されます
git push -u origin main

# 次回以降は自動的に認証されます
```

---

## 📝 確認

プッシュが成功したら：

1. **GitHubリポジトリを確認**
   - https://github.com/skbnw/tclip_search にアクセス
   - すべてのファイルが表示されているか確認

2. **ファイル構造の確認**
   - `requirements.txt` がプロジェクトルートにあるか
   - `.streamlit/config.toml` が存在するか
   - `code/02-web-app/search_display_app.py` が存在するか

---

## 🚀 次のステップ：Streamlit Cloudでデプロイ

GitHubへのプッシュが完了したら：

1. https://share.streamlit.io/ にアクセス
2. GitHubでログイン
3. 「New app」をクリック
4. 以下を設定：
   - **Repository**: `skbnw/tclip_search`
   - **Branch**: `main`
   - **Main file path**: `code/02-web-app/search_display_app.py`
5. 環境変数を設定（Secrets）
6. 「Deploy」をクリック

詳細は `DEPLOYMENT_QUICK_START.md` を参照してください。

