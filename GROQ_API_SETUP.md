# Groq APIキーの設定方法

## 🚀 Streamlit Cloudでの設定

### ステップ1: Streamlit Cloudダッシュボードにアクセス

1. [Streamlit Cloud](https://share.streamlit.io/) にログイン
2. アプリのダッシュボードを開く
3. 左側のメニューから「⚙️ Settings」をクリック

### ステップ2: Secretsを設定

1. 「Secrets」セクションを開く
2. 以下の内容を追加：

```toml
[groq]
api_key = "YOUR_GROQ_API_KEY"
```

3. 「Save」ボタンをクリック

### ステップ3: アプリを再デプロイ

1. 「☰ Menu」→「Redeploy」をクリック
2. または、GitHubにプッシュすると自動的に再デプロイされます

---

## 💻 ローカル環境での設定

### 方法1: 環境変数を使用

PowerShellで以下を実行：

```powershell
$env:GROQ_API_KEY="YOUR_GROQ_API_KEY"
```

### 方法2: Streamlit Secretsファイルを使用

`.streamlit/secrets.toml` ファイルを作成：

```toml
[groq]
api_key = "YOUR_GROQ_API_KEY"
```

**注意**: `.streamlit/secrets.toml` は `.gitignore` に追加してください（Gitにコミットしないように）

---

## ✅ 動作確認

APIキーを設定後、アプリを再起動して「🤖 AI要約」タブを開いてください。

エラーメッセージが表示されず、AI要約が生成されれば成功です。

---

## 🔒 セキュリティ注意事項

- APIキーは機密情報です。GitHubにコミットしないでください
- Streamlit CloudのSecretsは暗号化されて保存されます
- ローカル環境では `.streamlit/secrets.toml` を `.gitignore` に追加してください

