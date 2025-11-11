"""
既存のチャンクデータとマスターデータ（全文テキスト）にベクトル（埋め込み）を追加するスクリプト
Version: 2.0
Date: 2025/01/XX

機能:
- S3に既にアップロードされているチャンクデータを取得
- 各チャンクのテキストからベクトルを計算
- ベクトルをチャンクデータに追加してS3に再アップロード
- S3に既にアップロードされているマスターデータを取得
- マスターデータのfull_textからベクトルを計算
- ベクトルをマスターデータに追加してS3に再アップロード
- 既にベクトルが含まれているデータはスキップ可能

依存パッケージ:
- boto3: S3操作
- sentence-transformers: ベクトル計算
"""

import json
import boto3
import sys
import os
from typing import Dict, List, Optional
from botocore.exceptions import ClientError
from botocore.config import Config

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# --- 設定 ---
# S3設定
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"  # アジアパシフィック (東京)
S3_CHUNK_PREFIX = "rag/vector_chunks/"
S3_MASTER_PREFIX = "rag/master_text/"

# S3クライアントの作成
# 認証情報の優先順位: 環境変数 > ~/.aws/credentials > IAMロール
def create_s3_client():
    """S3クライアントを作成（認証情報を自動的に検出）"""
    try:
        # 1. 環境変数から認証情報を取得
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        # 2. 環境変数がない場合、認証情報ファイルから読み込む
        if not access_key or not secret_key:
            try:
                import configparser
                credentials_path = os.path.expanduser('~/.aws/credentials')
                if os.path.exists(credentials_path):
                    config = configparser.ConfigParser()
                    config.read(credentials_path)
                    if 'default' in config:
                        access_key = config['default'].get('aws_access_key_id') or access_key
                        secret_key = config['default'].get('aws_secret_access_key') or secret_key
            except Exception as e:
                print(f"[WARNING] 認証情報ファイルの読み込みエラー: {str(e)}")
        
        # 3. 認証情報が取得できた場合、明示的に渡す
        if access_key and secret_key:
            client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=S3_REGION
            )
        else:
            # 4. 認証情報がない場合、boto3のデフォルトの検索順序に任せる
            client = boto3.client('s3', region_name=S3_REGION)
        
        # 接続テスト（バケットの存在確認）
        client.head_bucket(Bucket=S3_BUCKET_NAME)
        return client
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            print(f"[ERROR] バケット '{S3_BUCKET_NAME}' が見つかりません")
        elif error_code == '403':
            print(f"[ERROR] バケット '{S3_BUCKET_NAME}' へのアクセスが拒否されました")
            print("[INFO] 認証情報の権限を確認してください")
        else:
            print(f"[ERROR] S3クライアントの作成に失敗しました: {str(e)}")
        print("[INFO] AWS認証情報を確認してください:")
        print("  - 環境変数: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        print(f"  - 認証情報ファイル: {os.path.expanduser('~/.aws/credentials')}")
        return None
    except Exception as e:
        print(f"[ERROR] S3クライアントの作成に失敗しました: {str(e)}")
        print("[INFO] AWS認証情報を確認してください（~/.aws/credentials または環境変数）")
        return None

S3_CLIENT = create_s3_client()
if S3_CLIENT is None:
    sys.exit(1)

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
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchKey':
            print(f"[WARNING] チャンクデータが見つかりません: {doc_id}")
            return None
        else:
            print(f"[ERROR] S3エラー: {doc_id} - {str(e)}")
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

# --- S3からマスターデータを取得 ---
def get_master_data_from_s3(doc_id: str) -> Optional[Dict]:
    """S3からマスターデータを取得"""
    try:
        key = f"{S3_MASTER_PREFIX}{doc_id}.jsonl"
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # JSON Lines形式なので最初の行を読み込む
        lines = content.strip().split('\n')
        if lines and lines[0]:
            return json.loads(lines[0])
        return None
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchKey':
            print(f"[WARNING] マスターデータが見つかりません: {doc_id}")
            return None
        else:
            print(f"[ERROR] S3エラー: {doc_id} - {str(e)}")
            return None
    except Exception as e:
        print(f"[ERROR] マスターデータの取得エラー: {doc_id} - {str(e)}")
        return None

# --- マスターデータにベクトルを追加 ---
def add_embedding_to_master(master_data: Dict, model, skip_existing: bool = True) -> Dict:
    """マスターデータのfull_textにベクトルを追加"""
    # 既にベクトルが含まれている場合はスキップ
    if skip_existing and ('embedding' in master_data or 'full_text_embedding' in master_data):
        print(f"[INFO] マスターデータには既にベクトルが含まれています（スキップ）")
        return master_data
    
    full_text = master_data.get('full_text', '')
    if not full_text:
        print(f"[WARNING] full_textが存在しません")
        return master_data
    
    try:
        # ベクトルを計算
        embedding = model.encode(full_text, convert_to_numpy=True)
        # ベクトルをリスト形式で保存（JSONシリアライズ可能にするため）
        master_data['full_text_embedding'] = embedding.tolist()
        print(f"[OK] マスターデータのベクトル計算完了")
        return master_data
    except Exception as e:
        print(f"[WARNING] マスターデータのベクトル計算エラー: {str(e)}")
        return master_data

# --- マスターデータをS3にアップロード ---
def upload_master_to_s3(master_data: Dict, doc_id: str):
    """マスターデータをS3にアップロード"""
    try:
        key = f"{S3_MASTER_PREFIX}{doc_id}.jsonl"
        data_str = json.dumps(master_data, ensure_ascii=False) + "\n"
        
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

# --- S3からすべてのマスターファイルのリストを取得 ---
def list_all_master_files() -> List[str]:
    """S3からすべてのマスターファイルのリストを取得"""
    master_files = []
    try:
        paginator = S3_CLIENT.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=S3_MASTER_PREFIX)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # ファイル名からdoc_idを抽出
                    # 例: rag/master_text/AkxAQAI40AM.jsonl -> AkxAQAI40AM
                    if key.endswith('.jsonl'):
                        doc_id = key.replace(S3_MASTER_PREFIX, '').replace('.jsonl', '')
                        master_files.append(doc_id)
        
        print(f"[INFO] {len(master_files)}個のマスターファイルを発見")
        return master_files
    except Exception as e:
        print(f"[ERROR] マスターファイルリストの取得エラー: {str(e)}")
        return []

# --- 単一のdoc_idのチャンクデータを処理 ---
def process_single_doc_id_chunks(doc_id: str, skip_existing: bool = True) -> bool:
    """単一のdoc_idのチャンクデータにベクトルを追加"""
    print(f"\n[INFO] チャンクデータ処理開始: {doc_id}")
    
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
        print(f"[OK] チャンクデータ処理完了: {doc_id}")
    else:
        print(f"[ERROR] チャンクデータ処理失敗: {doc_id}")
    
    return success

# --- 単一のdoc_idのマスターデータを処理 ---
def process_single_doc_id_master(doc_id: str, skip_existing: bool = True) -> bool:
    """単一のdoc_idのマスターデータにベクトルを追加"""
    print(f"\n[INFO] マスターデータ処理開始: {doc_id}")
    
    # マスターデータを取得
    master_data = get_master_data_from_s3(doc_id)
    if not master_data:
        return False
    
    # 埋め込みモデルを取得
    model = get_embedding_model()
    if model is None:
        print(f"[ERROR] 埋め込みモデルが利用できません: {doc_id}")
        return False
    
    # ベクトルを追加
    updated_master = add_embedding_to_master(master_data, model, skip_existing=skip_existing)
    
    # S3にアップロード
    success = upload_master_to_s3(updated_master, doc_id)
    
    if success:
        print(f"[OK] マスターデータ処理完了: {doc_id}")
    else:
        print(f"[ERROR] マスターデータ処理失敗: {doc_id}")
    
    return success

# --- 単一のdoc_idのチャンクデータとマスターデータの両方を処理 ---
def process_single_doc_id(doc_id: str, skip_existing: bool = True, process_chunks: bool = True, process_master: bool = True) -> bool:
    """単一のdoc_idのチャンクデータとマスターデータの両方にベクトルを追加"""
    print(f"\n[INFO] 処理開始: {doc_id}")
    
    chunk_success = True
    master_success = True
    
    if process_chunks:
        chunk_success = process_single_doc_id_chunks(doc_id, skip_existing=skip_existing)
    
    if process_master:
        master_success = process_single_doc_id_master(doc_id, skip_existing=skip_existing)
    
    return chunk_success and master_success

# --- メイン処理 ---
def main():
    """メイン処理"""
    import argparse
    
    parser = argparse.ArgumentParser(description='既存のチャンクデータとマスターデータ（全文テキスト）にベクトルを追加')
    parser.add_argument('--doc-id', type=str, help='処理するdoc_id（指定しない場合はすべて処理）')
    parser.add_argument('--skip-existing', action='store_true', default=True, help='既にベクトルが含まれているデータをスキップ（デフォルト: True）')
    parser.add_argument('--force', action='store_true', help='既にベクトルが含まれているデータも再計算する（--skip-existingを無効化）')
    parser.add_argument('--chunks-only', action='store_true', help='チャンクデータのみ処理（マスターデータは処理しない）')
    parser.add_argument('--master-only', action='store_true', help='マスターデータのみ処理（チャンクデータは処理しない）')
    
    args = parser.parse_args()
    
    skip_existing = args.skip_existing and not args.force
    process_chunks = not args.master_only
    process_master = not args.chunks_only
    
    print("=" * 80)
    print("既存チャンクデータ・マスターデータへのベクトル追加スクリプト")
    print("=" * 80)
    print(f"[INFO] 処理対象: チャンクデータ={'有効' if process_chunks else '無効'}, マスターデータ={'有効' if process_master else '無効'}")
    print(f"[INFO] 既存ベクトルのスキップ: {'有効' if skip_existing else '無効'}")
    
    # 埋め込みモデルを事前に読み込む
    model = get_embedding_model()
    if model is None:
        print("[ERROR] 埋め込みモデルの読み込みに失敗しました")
        sys.exit(1)
    
    if args.doc_id:
        # 単一のdoc_idを処理
        success = process_single_doc_id(
            args.doc_id, 
            skip_existing=skip_existing,
            process_chunks=process_chunks,
            process_master=process_master
        )
        sys.exit(0 if success else 1)
    else:
        # すべてのファイルを処理
        # doc_idのリストを取得（チャンクとマスターの両方から）
        chunk_files = list_all_chunk_files() if process_chunks else []
        master_files = list_all_master_files() if process_master else []
        
        # 両方のリストからユニークなdoc_idを取得
        all_doc_ids = set(chunk_files + master_files)
        
        if not all_doc_ids:
            print("[ERROR] 処理対象のファイルが見つかりませんでした")
            sys.exit(1)
        
        print(f"[INFO] {len(all_doc_ids)}個のdoc_idを処理します")
        
        success_count = 0
        error_count = 0
        
        for i, doc_id in enumerate(sorted(all_doc_ids), 1):
            print(f"\n[{i}/{len(all_doc_ids)}] 処理中: {doc_id}")
            if process_single_doc_id(
                doc_id, 
                skip_existing=skip_existing,
                process_chunks=process_chunks and doc_id in chunk_files,
                process_master=process_master and doc_id in master_files
            ):
                success_count += 1
            else:
                error_count += 1
        
        print("\n" + "=" * 80)
        print(f"処理完了: 成功 {success_count}件, 失敗 {error_count}件")
        print("=" * 80)
        
        sys.exit(0 if error_count == 0 else 1)

if __name__ == "__main__":
    main()

