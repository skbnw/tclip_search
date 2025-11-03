# Webアプリの起動方法

## 🚀 起動手順

### 1. 仮想環境のアクティベート

```powershell
.\venv\Scripts\Activate.ps1
```

### 2. AWS認証情報の設定（必須）

**方法1: 環境変数で設定（推奨）**

```powershell
$env:AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
$env:AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="ap-northeast-1"
```

**方法2: アプリ内で設定**

アプリを起動後、サイドバーの「環境変数を使用」チェックボックスを**オフ**にし、以下を入力：
- Access Key ID: `YOUR_AWS_ACCESS_KEY_ID`
- Secret Access Key: `YOUR_AWS_SECRET_ACCESS_KEY`

### 3. Streamlitアプリの起動

```powershell
streamlit run code\02-web-app\search_display_app.py
```

## 📝 AWS認証情報の設定について

### サイドバーの「環境変数を使用」とは？

- **チェックON（推奨）**: PowerShellで設定した環境変数（`$env:AWS_ACCESS_KEY_ID`など）を自動的に使用します
- **チェックOFF**: アプリ内で直接認証情報を入力できます

### どちらを選ぶべき？

**環境変数を使用（チェックON）が推奨です**：
- 既にPowerShellで環境変数を設定している場合
- 認証情報を毎回入力する必要がない
- よりセキュア

**アプリ内で入力（チェックOFF）**：
- 環境変数を設定していない場合
- 一時的に別の認証情報を使用したい場合

## 🌐 アクセス方法

アプリを起動すると、以下のようなメッセージが表示されます：

```
  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.x.x:8501
```

**ブラウザで `http://localhost:8501` にアクセスしてください。**

## ❌ エラーが発生した場合

### エラー: `ERR_CONNECTION_REFUSED`

**原因**: Streamlitアプリが起動していない

**解決方法**:
1. Streamlitが正常に起動しているか確認
2. 別のターミナルで以下を実行してポートを確認：
   ```powershell
   netstat -ano | findstr :8501
   ```
3. 別のポートが使用されている場合、以下でポートを指定：
   ```powershell
   streamlit run code\02-web-app\search_display_app.py --server.port 8502
   ```

### エラー: S3クライアントの初期化に失敗

**原因**: AWS認証情報が正しく設定されていない

**解決方法**:
1. 環境変数が正しく設定されているか確認：
   ```powershell
   $env:AWS_ACCESS_KEY_ID
   $env:AWS_SECRET_ACCESS_KEY
   $env:AWS_DEFAULT_REGION
   ```
2. 再設定してアプリを再起動

## 💡 使い方

1. **AWS認証情報の設定確認**
   - サイドバーで「環境変数を使用」をチェック（環境変数を設定した場合）

2. **番組IDで検索**
   - 検索ボックスに番組IDを入力（例: `AkxAQAJ3gAM`）
   - 「検索」ボタンをクリック

3. **データの表示**
   - **マスターデータタブ**: メタデータと全文
   - **チャンクデータタブ**: セグメント単位のチャンク（検索機能あり）
   - **画像タブ**: screenshots配列に含まれる画像

## 🔧 トラブルシューティング

### アプリが起動しない

1. 仮想環境がアクティベートされているか確認
2. Streamlitがインストールされているか確認：
   ```powershell
   pip list | findstr streamlit
   ```
3. ポート8501が使用されている場合、別のポートを指定

### データが表示されない

1. AWS認証情報が正しいか確認
2. S3バケットにデータが存在するか確認
3. 正しい番組IDを入力しているか確認

---

**最終更新**: 2025/11/02

