"""
検索用インデックス作成スクリプト
Version: 1.0
Date: 2025/11/02

機能:
- S3から全マスターデータを読み込み
- メタデータのみを抽出してインデックスファイルを作成
- インデックスファイルをS3に保存
- 検索アプリで使用可能な軽量なインデックスを生成

使用方法:
python code/01-s3-upload/create_search_index.py
"""

import json
import boto3
import sys
import os
from typing import Dict, List, Any
from datetime import datetime

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# S3設定
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"
S3_MASTER_PREFIX = "rag/master_text/"
S3_INDEX_PREFIX = "rag/search_index/"
S3_INDEX_FILE = "rag/search_index/master_index.jsonl"

# S3クライアント
s3_client = boto3.client('s3', region_name=S3_REGION)

def create_search_index():
    """検索用インデックスを作成"""
    print("=" * 80)
    print("[INFO] 検索用インデックスの作成を開始...")
    print("=" * 80)
    
    try:
        # S3から全マスターデータのリストを取得
        print("[INFO] S3からマスターデータのリストを取得中...")
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_MASTER_PREFIX)
        
        if 'Contents' not in response:
            print("[ERROR] マスターデータが見つかりませんでした")
            return
        
        total_files = len(response['Contents'])
        print(f"[INFO] {total_files} 個のマスターデータファイルを発見")
        
        index_data = []
        processed = 0
        errors = 0
        
        # 各マスターデータからメタデータのみを抽出
        for idx, obj in enumerate(response['Contents']):
            try:
                # 進捗表示
                if (idx + 1) % 100 == 0 or idx == total_files - 1:
                    print(f"[INFO] 処理中: {idx + 1}/{total_files} ファイル ({processed} 件成功, {errors} 件エラー)")
                
                # オブジェクトを取得
                file_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
                content = file_response['Body'].read().decode('utf-8')
                lines = content.strip().split('\n')
                
                if not lines:
                    continue
                
                # マスターデータを読み込み
                master_data = json.loads(lines[0])
                
                # インデックス用データを作成（メタデータ + doc_id + full_textの最初の100文字のみ）
                doc_id = master_data.get('doc_id', '')
                metadata = master_data.get('metadata', {})
                full_text = master_data.get('full_text', '')
                
                # 全文テキストの最初の100文字のみを保持（キーワード検索用）
                full_text_preview = full_text[:100] if full_text else ''
                
                index_entry = {
                    'doc_id': doc_id,
                    'metadata': metadata,
                    'full_text_preview': full_text_preview,  # 全文の最初の100文字
                    'full_text_length': len(full_text)  # 全文の長さ（検索時の参考用）
                }
                
                index_data.append(index_entry)
                processed += 1
                
            except Exception as e:
                errors += 1
                print(f"[WARNING] ファイル '{obj['Key']}' の処理でエラー: {str(e)}")
                continue
        
        print(f"[INFO] インデックス作成完了: {processed} 件成功, {errors} 件エラー")
        
        # インデックスファイルをJSON Lines形式で作成
        print("[INFO] インデックスファイルをS3にアップロード中...")
        index_jsonl = '\n'.join([json.dumps(entry, ensure_ascii=False) for entry in index_data])
        
        # S3にアップロード
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_INDEX_FILE,
            Body=index_jsonl.encode('utf-8'),
            ContentType='application/json'
        )
        
        print("=" * 80)
        print("[SUCCESS] インデックスファイルの作成が完了しました")
        print(f"  ファイル: s3://{S3_BUCKET_NAME}/{S3_INDEX_FILE}")
        print(f"  インデックス件数: {len(index_data)} 件")
        print(f"  ファイルサイズ: {len(index_jsonl.encode('utf-8')) / 1024 / 1024:.2f} MB")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] インデックス作成エラー: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_search_index()

