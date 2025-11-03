"""
S3アップロード - RAGデータ処理
Version: 1.1
Date: 2025/11/02

機能:
- ローカルNAS上の統合JSONファイルを読み込み
- トランスクリプトデータをセグメントベースでチャンキング
- マスターデータとチャンクデータをS3にアップロード
- JSON Lines形式でS3に保存
- program-integration配下を再帰的に探索
- ファイル名に "q1.00" が含まれる完成ファイルのみを処理（バッチ処理対応）
  （例: NHKG_TKY_20251015_0035-0125_AkxAQAELAAM_integrated_q1.00.json）

変更履歴:
v1.1: program-integration配下全体を探索、q1.00ファイルのバッチ処理に対応
      - バケット名を「tclip-raw-data-2025」に修正
      - リージョンを「ap-northeast-1」に明示的に設定

依存パッケージ:
- boto3: S3操作
- jsonlines: JSON Lines形式の処理
"""

import json
import jsonlines
import uuid
import boto3
import os
import re
import sys
from typing import Dict, List, Any

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# --- 設定 ---
# S3設定
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"  # アジアパシフィック (東京)
S3_MASTER_PREFIX = "rag/master_text/"
S3_CHUNK_PREFIX = "rag/vector_chunks/"
S3_CLIENT = boto3.client('s3', region_name=S3_REGION)

# ローカルファイル設定
BASE_NAS_PATH = r"\\NAS-TKY-2504\database\program-integration"

# --- チャンキング戦略 (トランスクリプトセグメントベース、変更なし) ---
def segment_based_chunking(transcripts: List[Dict], doc_id: str) -> List[Dict[str, Any]]:
    """
    トランスクリプトの各セグメントを基本チャンクとし、時間メタデータを付与する。
    """
    chunks = []
    
    # トランスクリプト配列の各要素をチャンクとして利用
    for i, segment in enumerate(transcripts):
        if 'content' not in segment or 'file_path' not in segment:
             continue 
        
        # 'content'以外のメタデータは、元の統合JSONからコピー
        # 💡 start_time_msとend_time_msは、統合JSONの'transcripts'要素から直接抽出されると仮定（データ品質の問題により、今回は'content'があるかのみチェック）
        
        # ファイルパスの文字列から開始・終了時間の文字列を抽出（より正確な時間情報があればそちらを使用すべき）
        time_match = re.search(r'(\d{8}-\d{6})-\d+-', segment.get('file_name', segment.get('file_path', '')))
        
        chunk_id = f"{doc_id}-p{i:04d}" # 一意なチャンクID（doc_id + インデックス）
        
        # メタデータとして時間情報や元のファイルパスを格納
        metadata = {
            "source": "transcript",
            # JSONに時間情報が無い場合、ここでは一旦空にするか、より堅牢な抽出ロジックが必要
            "start_time": segment.get('start_time'), # 統合JSONの構造に依存
            "end_time": segment.get('end_time'),     # 統合JSONの構造に依存
            "original_file_path": segment['file_path'] 
        }

        chunks.append({
            "chunk_id": chunk_id,
            "doc_id": doc_id,
            "text": segment['content'], # チャンクテキスト
            "level": "segment",
            "metadata": metadata
        })
        
    return chunks

# --- S3アップロード処理（変更なし） ---
def upload_to_s3(data_list: List[Dict], key: str):
    """
    データをJSON Lines形式でS3にアップロード
    """
    data_str = ""
    for item in data_list:
        data_str += json.dumps(item, ensure_ascii=False) + "\n"
        
    # S3クライアントの操作
    S3_CLIENT.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=data_str.encode('utf-8'),
        ContentType='application/jsonl; charset=utf-8'
    )
    print(f"[OK] S3にアップロード完了: s3://{S3_BUCKET_NAME}/{key}")


# --- ファイル探索関数 ---
def find_q100_json_files(root_path: str) -> List[str]:
    """
    program-integration配下を再帰的に探索し、ファイル名に "q1.00" が含まれるJSONファイルのパスを返す
    （例: NHKG_TKY_20251015_0035-0125_AkxAQAELAAM_integrated_q1.00.json）
    """
    json_files = []
    
    if not os.path.exists(root_path):
        print(f"[ERROR] パスが見つかりません: {root_path}")
        return json_files
    
    print(f"[INFO] ファイル探索を開始: {root_path}")
    
    # 再帰的にファイルを探索
    for root, dirs, files in os.walk(root_path):
        for filename in files:
            # ファイル名に "q1.00" が含まれ、.json で終わるファイルのみ
            if filename.lower().endswith('.json') and 'q1.00' in filename.lower():
                file_path = os.path.join(root, filename)
                json_files.append(file_path)
                print(f"[INFO] q1.00ファイルを発見: {file_path}")
    
    return json_files

# --- 単一ファイル処理関数 ---
def process_single_file(file_path: str) -> bool:
    """
    単一のJSONファイルを処理してS3にアップロードする
    成功した場合はTrue、失敗した場合はFalseを返す
    """
    try:
        print(f"\n[INFO] ファイル処理を開始: {file_path}")
        
        # ファイル読み込み
        with open(file_path, 'r', encoding='utf-8') as f:
            integrated_data = json.load(f)
        
        # event_idの取得（エラーハンドリング追加）
        if 'program_metadata' not in integrated_data or 'event_id' not in integrated_data['program_metadata']:
            print(f"[ERROR] program_metadata.event_id が見つかりません: {file_path}")
            return False
        
        doc_id = integrated_data['program_metadata']['event_id']
        print(f"[INFO] doc_id: {doc_id}")
        
        # transcriptsの存在確認
        if 'transcripts' not in integrated_data:
            print(f"[ERROR] transcripts が見つかりません: {file_path}")
            return False
        
        # 1. マスターデータの準備
        master_data = {
            "doc_id": doc_id,
            "metadata": integrated_data['program_metadata'],
            "full_text": "".join([t['content'] for t in integrated_data['transcripts'] if 'content' in t])
        }
        
        # 2. チャンクデータの準備
        all_chunks = segment_based_chunking(integrated_data['transcripts'], doc_id)
        
        if not all_chunks:
            print(f"[WARNING] チャンクが生成されませんでした: {file_path}")
            return False
        
        print(f"[INFO] {len(all_chunks)}個のチャンクを生成")
        
        # --- S3アップロード実行 ---
        
        # A. マスターデータ (PostgreSQLの入力用)
        master_key = f"{S3_MASTER_PREFIX}{doc_id}.jsonl"
        upload_to_s3([master_data], master_key)
        
        # B. チャンクデータ (Weaviate/OpenSearchの入力用)
        chunk_key = f"{S3_CHUNK_PREFIX}{doc_id}_segments.jsonl"
        upload_to_s3(all_chunks, chunk_key)
        
        print(f"[OK] ファイル処理完了: {file_path}")
        return True
        
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON解析エラー: {file_path} - {str(e)}")
        return False
    except Exception as e:
        print(f"[ERROR] 処理エラー: {file_path} - {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# --- メイン処理 (バッチ処理対応) ---
def process_and_upload_local_rag_data():
    """
    program-integration配下のq1.00ファイルを探索し、バッチ処理でS3にアップロードする
    """
    # q1.00ファイルを探索
    json_files = find_q100_json_files(BASE_NAS_PATH)
    
    if not json_files:
        print(f"[WARNING] q1.00ファイルが見つかりませんでした")
        return
    
    print(f"\n[INFO] 合計 {len(json_files)} 個のq1.00ファイルが見つかりました")
    print("=" * 80)
    
    # 各ファイルを処理
    success_count = 0
    error_count = 0
    
    for i, file_path in enumerate(json_files, 1):
        print(f"\n[{i}/{len(json_files)}] 処理中...")
        if process_single_file(file_path):
            success_count += 1
        else:
            error_count += 1
    
    # 処理結果サマリー
    print("\n" + "=" * 80)
    print(f"[SUMMARY] 処理完了")
    print(f"  成功: {success_count} ファイル")
    print(f"  失敗: {error_count} ファイル")
    print(f"  合計: {len(json_files)} ファイル")
    
# --- 実行例 ---
# 実行する前に、BASE_NAS_PATHがネットワーク経由でPythonからアクセス可能であることを確認してください。
# process_and_upload_local_rag_data(CHANNEL_CODE, TARGET_EVENT_ID)

if __name__ == "__main__":
    # メイン実行（q1.00ファイルのバッチ処理）
    process_and_upload_local_rag_data()

