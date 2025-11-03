# GitHubリポジトリ作成手順

## ✅ 完了した作業

- ✅ Gitリポジトリの初期化
- ✅ ファイルのステージング
- ✅ 初回コミット（19ファイル、3948行）

## 📝 次のステップ：GitHubリポジトリの作成

### 方法1: Webインターフェースで作成（推奨）

1. **GitHubにアクセス**
   - https://github.com にアクセス
   - GitHubアカウントでログイン（アカウントがない場合は作成）

2. **新しいリポジトリを作成**
   - 右上の「+」ボタンをクリック
   - 「New repository」を選択

3. **リポジトリ情報を入力**
   - **Repository name**: `tclip-json-uploader`（またはお好みの名前）
   - **Description**: `Streamlit search app for S3 data`
   - **Public** または **Private** を選択
     - 無料プランでStreamlit Cloudを使う場合は **Public** が必要
   - **⚠️ 重要**: 「Add a README file」「Add .gitignore」「Choose a license」は**チェックを外す**（既に作成済みのため）

4. **「Create repository」をクリック**

5. **リモートリポジトリを追加**
   GitHubが表示するコマンドを実行します。通常は以下のようになります：

```powershell
git remote add origin https://github.com/YOUR_USERNAME/tclip-json-uploader.git
git branch -M main
git push -u origin main
```

**注意**: `YOUR_USERNAME`を自分のGitHubユーザー名に置き換えてください。

---

### 方法2: GitHub CLIを使用（オプション）

GitHub CLIをインストールしたい場合：

1. **GitHub CLIのインストール**
   - https://cli.github.com/ からダウンロード
   - または、Wingetを使用：
     ```powershell
     winget install --id GitHub.cli
     ```

2. **GitHub CLIでログイン**
   ```powershell
   gh auth login
   ```

3. **リポジトリを作成**
   ```powershell
   gh repo create tclip-json-uploader --public --source=. --remote=origin --push
   ```

---

## 🔐 GitHub認証について

初回プッシュ時に認証が必要です。

### Personal Access Token（PAT）を使用（推奨）

1. **GitHubでトークンを生成**
   - GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
   - 「Generate new token (classic)」をクリック
   - **Note**: `tclip-json-uploader` など任意の名前
   - **Expiration**: お好みの期間を選択
   - **Scope**: `repo` にチェック
   - 「Generate token」をクリック
   - **⚠️ 重要**: トークンをコピー（再表示されません）

2. **プッシュ時に認証**
   ```powershell
   git push -u origin main
   ```
   - Username: 自分のGitHubユーザー名
   - Password: 生成したPersonal Access Token

---

## ✅ 確認事項

GitHubリポジトリが作成できたら、以下を確認：

- [ ] すべてのファイルがGitHubにプッシュされているか
- [ ] `.gitignore`が機能しているか（機密情報が含まれていないか）
- [ ] `requirements.txt`がプロジェクトルートにあるか
- [ ] `.streamlit/config.toml`が存在するか

---

## 🚀 次のステップ：Streamlit Cloudデプロイ

GitHubリポジトリが作成できたら、`DEPLOYMENT_QUICK_START.md`を参照してStreamlit Cloudでデプロイしてください。

