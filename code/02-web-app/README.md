# S3データ検索・表示Webアプリ

Streamlitを使用したS3データ検索・表示アプリケーションです。

## 📋 機能

- **番組ID（doc_id）で検索**: S3バケット内のデータを番組IDで検索
- **マスターデータの表示**: 番組のメタデータと全文を表示
- **チャンクデータの表示**: セグメント単位のチャンクとメタデータを表示
- **画像の表示**: screenshots配列に含まれる画像を表示
- **チャンク検索**: チャンク内のテキストを検索

## 🚀 セットアップ

### 1. 仮想環境の作成とパッケージインストール

```powershell
# 仮想環境の作成（まだ作成していない場合）
python -m venv venv

# 仮想環境のアクティベート
.\venv\Scripts\Activate.ps1

# 必要なパッケージのインストール
pip install -r code/02-web-app/requirements.txt
```

### 2. AWS認証情報の設定

環境変数で設定：

```powershell
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="ap-northeast-1"
```

または、アプリ内で設定することも可能です。

## 📝 実行方法

```powershell
# 仮想環境をアクティベート
.\venv\Scripts\Activate.ps1

# AWS認証情報を設定
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="ap-northeast-1"

# Streamlitアプリを起動
streamlit run code/02-web-app/search_display_app.py
```

ブラウザが自動的に開き、アプリが表示されます。
通常は `http://localhost:8501` でアクセスできます。

## 🎯 使い方

1. **AWS認証情報の設定**
   - サイドバーで「環境変数を使用」をチェック
   - または、Access Key IDとSecret Access Keyを入力

2. **番組IDで検索**
   - 検索ボックスに番組ID（doc_id）を入力
   - 例: `AkxAQAJ3gAM`
   - 「検索」ボタンをクリック

3. **データの表示**
   - **マスターデータタブ**: 番組のメタデータと全文
   - **チャンクデータタブ**: セグメント単位のチャンク（検索機能あり）
   - **画像タブ**: screenshots配列に含まれる画像

## 📊 表示される情報

### マスターデータ
- メタデータ（JSON形式）
- 全文テキスト
- 関連画像URL（ある場合）

### チャンクデータ
- 各チャンクのテキスト
- チャンクID
- 開始・終了時刻
- 元のファイルパス
- チャンク内検索機能

### 画像
- screenshots配列に含まれる画像
- グリッド表示（3列）
- 画像ファイル名

## ⚙️ 設定

### S3バケット情報の変更

`search_display_app.py` の設定セクションを編集：

```python
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"
S3_MASTER_PREFIX = "rag/master_text/"
S3_CHUNK_PREFIX = "rag/vector_chunks/"
S3_IMAGE_PREFIX = "rag/images/"
```

## 🔧 トラブルシューティング

### エラー: S3クライアントの初期化に失敗

**原因**: AWS認証情報が正しく設定されていない

**解決方法**:
- 環境変数を設定しているか確認
- または、アプリ内で認証情報を入力

### エラー: データが見つからない

**原因**: 番組IDが存在しない、またはS3にデータがアップロードされていない

**解決方法**:
- 正しい番組IDを入力しているか確認
- S3バケットにデータが存在するか確認

### エラー: 画像が表示されない

**原因**: プリサインドURLの生成に失敗、または画像が存在しない

**解決方法**:
- AWS認証情報にS3へのアクセス権限があるか確認
- 画像ファイルがS3に存在するか確認

## 📝 注意事項

- データは5分間キャッシュされます
- 画像のプリサインドURLは1時間有効です
- 大量のデータを表示する場合、パフォーマンスが低下する可能性があります

## 🎨 カスタマイズ

### テーマの変更

Streamlitのテーマ設定は `~/.streamlit/config.toml` で変更できます：

```toml
[theme]
primaryColor = "#FF6B6B"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"
font = "sans serif"
```

## 📞 サポート

問題が発生した場合は、以下を確認してください：

1. AWS認証情報が正しく設定されているか
2. S3バケットへのアクセス権限があるか
3. 必要なパッケージがインストールされているか

---

**バージョン**: 1.0  
**最終更新**: 2025/11/02

