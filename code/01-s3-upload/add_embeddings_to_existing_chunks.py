"""
既存のチャンクデータにベクトル（埋め込み）を追加するスクリプト
Version: 1.0
Date: 2025/01/XX

機能:
- S3に既にアップロードされているチャンクデータを取得
- 各チャンクのテキストからベクトルを計算
- ベクトルをチャンクデータに追加してS3に再アップロード
- 既にベクトルが含まれているチャンクはスキップ可能

依存パッケージ:
- boto3: S3操作
- sentence-transformers: ベクトル計算
"""

import json
import boto3
import sys
from typing import Dict, List, Optional
from botocore.exceptions import ClientError

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# --- 設定 ---
# S3設定
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"  # アジアパシフィック (東京)
S3_CHUNK_PREFIX = "rag/vector_chunks/"
S3_CLIENT = boto3.client('s3', region_name=S3_REGION)

# ベクトル計算用のライブラリ
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    print("[ERROR] sentence-transformersがインストールされていません。")
    print("  インストール: pip install sentence-transformers")
    sys.exit(1)

# --- 埋め込みモデルの読み込み ---
_embedding_model = None

def get_embedding_model():
    """埋め込みモデルを取得（シングルトン）"""
    global _embedding_model
    if _embedding_model is None:
        try:
            print("[INFO] 埋め込みモデルを読み込み中...")
            _embedding_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            print("[OK] 埋め込みモデルの読み込み完了")
        except Exception as e:
            print(f"[ERROR] 埋め込みモデルの読み込みエラー: {str(e)}")
            return None
    return _embedding_model

# --- S3からチャンクデータを取得 ---
def get_chunk_data_from_s3(doc_id: str) -> Optional[List[Dict]]:
    """S3からチャンクデータを取得"""
    try:
        key = f"{S3_CHUNK_PREFIX}{doc_id}_segments.jsonl"
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        chunks = []
        for line in content.strip().split('\n'):
            if line:
                chunks.append(json.loads(line))
        return chunks
    except S3_CLIENT.exceptions.NoSuchKey:
        print(f"[WARNING] チャンクデータが見つかりません: {doc_id}")
        return None
    except Exception as e:
        print(f"[ERROR] チャンクデータの取得エラー: {doc_id} - {str(e)}")
        return None

# --- チャンクデータにベクトルを追加 ---
def add_embeddings_to_chunks(chunks: List[Dict], model, skip_existing: bool = True) -> List[Dict]:
    """チャンクデータにベクトルを追加"""
    updated_chunks = []
    skipped_count = 0
    error_count = 0
    
    for i, chunk in enumerate(chunks):
        # 既にベクトルが含まれている場合はスキップ
        if skip_existing and ('embedding' in chunk or 'vector' in chunk):
            skipped_count += 1
            updated_chunks.append(chunk)
            continue
        
        chunk_text = chunk.get('text', '')
        if not chunk_text:
            error_count += 1
            updated_chunks.append(chunk)
            continue
        
        try:
            # ベクトルを計算
            embedding = model.encode(chunk_text, convert_to_numpy=True)
            # ベクトルをリスト形式で保存（JSONシリアライズ可能にするため）
            chunk['embedding'] = embedding.tolist()
            updated_chunks.append(chunk)
            
            # 進捗表示（100チャンクごと）
            if (i + 1) % 100 == 0:
                print(f"  進捗: {i + 1}/{len(chunks)} チャンク処理完了")
        except Exception as e:
            print(f"[WARNING] チャンク {chunk.get('chunk_id', 'unknown')} のベクトル計算エラー: {str(e)}")
            error_count += 1
            updated_chunks.append(chunk)  # エラーが発生してもチャンクは追加する（ベクトルなし）
    
    print(f"[INFO] ベクトル追加完了: {len(updated_chunks)} チャンク（スキップ: {skipped_count}, エラー: {error_count}）")
    return updated_chunks

# --- チャンクデータをS3にアップロード ---
def upload_chunks_to_s3(chunks: List[Dict], doc_id: str):
    """チャンクデータをS3にアップロード"""
    try:
        key = f"{S3_CHUNK_PREFIX}{doc_id}_segments.jsonl"
        data_str = ""
        for chunk in chunks:
            data_str += json.dumps(chunk, ensure_ascii=False) + "\n"
        
        S3_CLIENT.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=data_str.encode('utf-8'),
            ContentType='application/jsonl; charset=utf-8'
        )
        print(f"[OK] S3にアップロード完了: s3://{S3_BUCKET_NAME}/{key}")
        return True
    except Exception as e:
        print(f"[ERROR] S3アップロードエラー: {doc_id} - {str(e)}")
        return False

# --- S3からすべてのチャンクファイルのリストを取得 ---
def list_all_chunk_files() -> List[str]:
    """S3からすべてのチャンクファイルのリストを取得"""
    chunk_files = []
    try:
        paginator = S3_CLIENT.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=S3_CHUNK_PREFIX)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # ファイル名からdoc_idを抽出
                    # 例: rag/vector_chunks/AkxAQAI40AM_segments.jsonl -> AkxAQAI40AM
                    if key.endswith('_segments.jsonl'):
                        doc_id = key.replace(S3_CHUNK_PREFIX, '').replace('_segments.jsonl', '')
                        chunk_files.append(doc_id)
        
        print(f"[INFO] {len(chunk_files)}個のチャンクファイルを発見")
        return chunk_files
    except Exception as e:
        print(f"[ERROR] チャンクファイルリストの取得エラー: {str(e)}")
        return []

# --- 単一のdoc_idのチャンクデータを処理 ---
def process_single_doc_id(doc_id: str, skip_existing: bool = True) -> bool:
    """単一のdoc_idのチャンクデータにベクトルを追加"""
    print(f"\n[INFO] 処理開始: {doc_id}")
    
    # チャンクデータを取得
    chunks = get_chunk_data_from_s3(doc_id)
    if not chunks:
        return False
    
    print(f"[INFO] {len(chunks)}個のチャンクを取得")
    
    # 埋め込みモデルを取得
    model = get_embedding_model()
    if model is None:
        print(f"[ERROR] 埋め込みモデルが利用できません: {doc_id}")
        return False
    
    # ベクトルを追加
    updated_chunks = add_embeddings_to_chunks(chunks, model, skip_existing=skip_existing)
    
    # S3にアップロード
    success = upload_chunks_to_s3(updated_chunks, doc_id)
    
    if success:
        print(f"[OK] 処理完了: {doc_id}")
    else:
        print(f"[ERROR] 処理失敗: {doc_id}")
    
    return success

# --- メイン処理 ---
def main():
    """メイン処理"""
    import argparse
    
    parser = argparse.ArgumentParser(description='既存のチャンクデータにベクトルを追加')
    parser.add_argument('--doc-id', type=str, help='処理するdoc_id（指定しない場合はすべて処理）')
    parser.add_argument('--skip-existing', action='store_true', default=True, help='既にベクトルが含まれているチャンクをスキップ（デフォルト: True）')
    parser.add_argument('--force', action='store_true', help='既にベクトルが含まれているチャンクも再計算する（--skip-existingを無効化）')
    
    args = parser.parse_args()
    
    skip_existing = args.skip_existing and not args.force
    
    print("=" * 80)
    print("既存チャンクデータへのベクトル追加スクリプト")
    print("=" * 80)
    
    # 埋め込みモデルを事前に読み込む
    model = get_embedding_model()
    if model is None:
        print("[ERROR] 埋め込みモデルの読み込みに失敗しました")
        sys.exit(1)
    
    if args.doc_id:
        # 単一のdoc_idを処理
        success = process_single_doc_id(args.doc_id, skip_existing=skip_existing)
        sys.exit(0 if success else 1)
    else:
        # すべてのチャンクファイルを処理
        chunk_files = list_all_chunk_files()
        if not chunk_files:
            print("[ERROR] チャンクファイルが見つかりませんでした")
            sys.exit(1)
        
        print(f"[INFO] {len(chunk_files)}個のチャンクファイルを処理します")
        print(f"[INFO] 既存ベクトルのスキップ: {'有効' if skip_existing else '無効'}")
        
        success_count = 0
        error_count = 0
        
        for i, doc_id in enumerate(chunk_files, 1):
            print(f"\n[{i}/{len(chunk_files)}] 処理中: {doc_id}")
            if process_single_doc_id(doc_id, skip_existing=skip_existing):
                success_count += 1
            else:
                error_count += 1
        
        print("\n" + "=" * 80)
        print(f"処理完了: 成功 {success_count}件, 失敗 {error_count}件")
        print("=" * 80)
        
        sys.exit(0 if error_count == 0 else 1)

if __name__ == "__main__":
    main()

