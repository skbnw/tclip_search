# Streamlit Webアプリ起動スクリプト

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "S3データ検索・表示Webアプリを起動します" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# AWS認証情報の設定
Write-Host "[INFO] AWS認証情報を設定中..." -ForegroundColor Yellow
Write-Host "[WARNING] 認証情報を設定してください。" -ForegroundColor Yellow
Write-Host "[WARNING] 環境変数に設定されている場合は、アプリ内の「環境変数を使用」をチェックしてください。" -ForegroundColor Yellow
# 環境変数が設定されていない場合は、以下を編集してください：
# $env:AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
# $env:AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="ap-northeast-1"

# 仮想環境のアクティベート
Write-Host "[INFO] 仮想環境をアクティベート中..." -ForegroundColor Yellow
if (Test-Path "venv\Scripts\Activate.ps1") {
    & "venv\Scripts\Activate.ps1"
} else {
    Write-Host "[ERROR] 仮想環境が見つかりません。venvフォルダが存在するか確認してください。" -ForegroundColor Red
    exit 1
}

Write-Host "[INFO] Streamlitアプリを起動中..." -ForegroundColor Yellow
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "アプリが起動したら、ブラウザで以下にアクセスしてください:" -ForegroundColor Green
Write-Host "http://localhost:8501" -ForegroundColor White -BackgroundColor DarkBlue
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "[INFO] サイドバーの「環境変数を使用」にチェックを入れてください" -ForegroundColor Cyan
Write-Host ""
Write-Host "停止する場合は Ctrl+C を押してください" -ForegroundColor Yellow
Write-Host ""

# Streamlitアプリの起動
streamlit run code\02-web-app\search_display_app.py

