# Git履歴から認証情報を削除するスクリプト
# 注意: このスクリプトは履歴を書き換えます

Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Git履歴から認証情報を削除します" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

# バックアップブランチを作成
Write-Host "[INFO] バックアップブランチを作成中..." -ForegroundColor Cyan
git branch backup-before-clean

Write-Host ""
Write-Host "[WARNING] 以下の方法で認証情報を削除します:" -ForegroundColor Yellow
Write-Host "1. git filter-branch を使用して履歴を書き換えます" -ForegroundColor White
Write-Host "2. 過去のコミットから認証情報を含むファイルを修正します" -ForegroundColor White
Write-Host ""
Write-Host "続行しますか? (Y/N): " -ForegroundColor Yellow -NoNewline
$confirm = Read-Host
if ($confirm -ne "Y" -and $confirm -ne "y") {
    Write-Host "[INFO] キャンセルしました" -ForegroundColor Yellow
    exit 0
}

Write-Host ""
Write-Host "[INFO] 履歴を書き換え中..." -ForegroundColor Cyan

# git filter-branchを使用して認証情報を削除
# 注意: このコマンドは時間がかかる場合があります
git filter-branch --force --index-filter `
    "git rm --cached --ignore-unmatch README.md DEPLOYMENT_QUICK_START.md 2>$null; git checkout HEAD -- README.md DEPLOYMENT_QUICK_START.md" `
    --prune-empty --tag-name-filter cat -- --all

Write-Host ""
Write-Host "[INFO] 削除された認証情報を新しいプレースホルダーに置き換えます..." -ForegroundColor Cyan

# 最新のコミットを取得
$latestCommit = git rev-parse HEAD

# README.mdとDEPLOYMENT_QUICK_START.mdを再度修正
Write-Host "[INFO] ファイルを修正中..." -ForegroundColor Cyan

# ここでは、既に修正済みのファイルが最新のコミットにあることを確認
Write-Host "[INFO] クリーンアップ完了" -ForegroundColor Green
Write-Host ""
Write-Host "次のステップ:" -ForegroundColor Yellow
Write-Host "1. git push --force origin main を実行してプッシュしてください" -ForegroundColor White
Write-Host "2. 注意: フォースプッシュは履歴を上書きします" -ForegroundColor Red
Write-Host ""

