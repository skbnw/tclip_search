# GitHubへのプッシュ（今すぐ実行）

## ⚠️ 重要：GitHubにまだコードがプッシュされていません

Streamlit Cloudでデプロイするには、まずGitHubリポジトリにコードをプッシュする必要があります。

## 🚀 プッシュ手順

### ステップ1: Personal Access Token（PAT）の準備

1. https://github.com にログイン
2. 右上のプロフィール画像 → 「Settings」
3. 左サイドバー下部の「Developer settings」
4. 「Personal access tokens」→「Tokens (classic)」
5. 「Generate new token」→「Generate new token (classic)」
6. 以下を設定：
   - **Note**: `tclip_search_push`
   - **Expiration**: 90 days（お好み）
   - **Scopes**: `repo` にチェック ✅
7. 「Generate token」をクリック
8. **⚠️ 表示されたトークンをコピー**（例: `ghp_xxxxxxxxxxxxxxxxxxxx`）

### ステップ2: PowerShellでプッシュ

PowerShellで以下のコマンドを実行してください：

```powershell
git push -u origin main
```

認証が求められたら：
- **Username**: `skbnw`
- **Password**: **ステップ1でコピーしたPersonal Access Tokenを貼り付け**

---

## ✅ プッシュ後の確認

プッシュが成功したら：

1. https://github.com/skbnw/tclip_search にアクセス
2. 以下のファイルが表示されているか確認：
   - `code/02-web-app/search_display_app.py`
   - `requirements.txt`
   - `.streamlit/config.toml`
   - その他のファイル

---

## 📝 Streamlit Cloudの設定（プッシュ後）

GitHubにプッシュしたら、Streamlit Cloudで以下を設定してください：

### 1. Repository
```
skbnw/tclip_search
```

### 2. Branch
```
main
```
（`master` ではなく `main` を選択）

### 3. Main file path
```
code/02-web-app/search_display_app.py
```

### 4. App URL（オプション）
```
tclipsearch-bav7bfzzkyuz8z6t9j7fof
```

### 5. Secrets（デプロイ後、必ず設定）
Streamlit Cloudの「Settings」→「Secrets」で以下を設定：

```toml
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
AWS_DEFAULT_REGION = "ap-northeast-1"
```

---

## 🔍 トラブルシューティング

### エラー: "repository not found"
→ GitHubリポジトリが存在するか確認してください

### エラー: "authentication failed"
→ Personal Access Tokenが正しく貼り付けられているか確認してください

### エラー: "permission denied"
→ Personal Access Tokenに `repo` スコープが付与されているか確認してください

