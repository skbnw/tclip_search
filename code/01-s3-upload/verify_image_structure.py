"""
画像ファイル構造の検証スクリプト

各番組ID（doc_id）フォルダに、そのJSONファイルのscreenshots配列に含まれる画像のみが
格納されているかを確認します。
"""

import json
import os
import sys
import boto3
from typing import Dict, Set
from collections import defaultdict

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# S3設定
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"
S3_IMAGE_PREFIX = "rag/images/"
S3_CLIENT = boto3.client('s3', region_name=S3_REGION)

BASE_NAS_PATH = r"\\NAS-TKY-2504\database\program-integration"

def get_all_images_in_s3() -> Dict[str, Set[str]]:
    """
    S3にアップロードされているすべての画像を取得
    戻り値: {doc_id: {image_filenames}}
    """
    images_by_doc_id = defaultdict(set)
    
    paginator = S3_CLIENT.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=S3_IMAGE_PREFIX)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # rag/images/{doc_id}/{filename}.jpeg の形式
                parts = key.replace(S3_IMAGE_PREFIX, '').split('/')
                if len(parts) == 2:
                    doc_id = parts[0]
                    filename = parts[1]
                    images_by_doc_id[doc_id].add(filename)
    
    return dict(images_by_doc_id)

def get_screenshots_from_json(json_path: str) -> tuple[str, Set[str]]:
    """
    JSONファイルからdoc_idとscreenshots配列の画像ファイル名を取得
    戻り値: (doc_id, {image_filenames})
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        doc_id = data.get('program_metadata', {}).get('event_id', '')
        screenshots = data.get('screenshots', [])
        
        image_filenames = set()
        for screenshot in screenshots:
            filename = screenshot.get('file_name', '')
            if filename:
                image_filenames.add(filename)
        
        return doc_id, image_filenames
    except Exception as e:
        print(f"[ERROR] JSON読み込みエラー: {json_path} - {str(e)}")
        return '', set()

def find_q100_json_files(root_path: str) -> list:
    """q1.00ファイルを探索"""
    json_files = []
    
    for root, dirs, files in os.walk(root_path):
        for filename in files:
            if filename.lower().endswith('.json') and 'q1.00' in filename.lower():
                json_files.append(os.path.join(root, filename))
    
    return json_files

def verify_image_structure():
    """
    画像ファイル構造を検証
    """
    print("[INFO] S3から画像ファイル一覧を取得中...")
    s3_images = get_all_images_in_s3()
    print(f"[INFO] {len(s3_images)}個の番組IDフォルダを発見")
    
    print("[INFO] NASからJSONファイルを探索中...")
    json_files = find_q100_json_files(BASE_NAS_PATH)
    print(f"[INFO] {len(json_files)}個のq1.00ファイルを発見")
    
    # 検証結果
    mismatches = []
    total_checked = 0
    
    print("\n[INFO] 検証を開始...")
    for json_file in json_files:
        doc_id, expected_images = get_screenshots_from_json(json_file)
        
        if not doc_id:
            continue
        
        total_checked += 1
        
        # S3にこのdoc_idのフォルダが存在するか確認
        s3_images_for_doc = s3_images.get(doc_id, set())
        
        # JSONに含まれる画像がS3に存在するか確認
        missing_in_s3 = expected_images - s3_images_for_doc
        extra_in_s3 = s3_images_for_doc - expected_images
        
        if missing_in_s3 or extra_in_s3:
            mismatches.append({
                'doc_id': doc_id,
                'json_file': json_file,
                'expected_count': len(expected_images),
                's3_count': len(s3_images_for_doc),
                'missing_in_s3': missing_in_s3,
                'extra_in_s3': extra_in_s3
            })
        
        if total_checked % 100 == 0:
            print(f"[INFO] {total_checked}/{len(json_files)} ファイルを検証完了...")
    
    # 結果表示
    print("\n" + "=" * 80)
    print(f"[SUMMARY] 検証結果")
    print(f"  検証ファイル数: {total_checked}")
    print(f"  不一致: {len(mismatches)}")
    
    if mismatches:
        print("\n[WARNING] 不一致が見つかりました:")
        for mismatch in mismatches[:10]:  # 最初の10件のみ表示
            print(f"\n  doc_id: {mismatch['doc_id']}")
            print(f"  JSONファイル: {mismatch['json_file']}")
            print(f"  JSON内の画像数: {mismatch['expected_count']}")
            print(f"  S3内の画像数: {mismatch['s3_count']}")
            if mismatch['missing_in_s3']:
                print(f"  S3に存在しない画像: {list(mismatch['missing_in_s3'])[:5]}")
            if mismatch['extra_in_s3']:
                print(f"  S3に余分な画像: {list(mismatch['extra_in_s3'])[:5]}")
        
        if len(mismatches) > 10:
            print(f"\n  ... 他 {len(mismatches) - 10} 件の不一致があります")
    else:
        print("\n[OK] すべてのファイルで構造が正しく一致しています")

if __name__ == "__main__":
    verify_image_structure()

