"""
S3アップロード - RAGデータ処理
Version: 1.3
Date: 2025/11/02

機能:
- ローカルNAS上の統合JSONファイルを読み込み
- トランスクリプトデータをセグメントベースでチャンキング
- マスターデータとチャンクデータをS3にアップロード
- JSON Lines形式でS3に保存
- program-integration配下を再帰的に探索
- ファイル名に "q1.00" が含まれる完成ファイルのみを処理（バッチ処理対応）
- 画像ファイル（screenshots）のS3アップロード対応
- ファイル更新チェック機能（更新されたファイルのみ再アップロード）
  （例: NHKG_TKY_20251015_0035-0125_AkxAQAELAAM_integrated_q1.00.json）

変更履歴:
v1.3: ファイル更新チェック機能を追加
      - S3オブジェクトの存在確認と最終更新日時の比較
      - NASファイルの更新日時とS3オブジェクトの更新日時を比較
      - 更新されている場合のみ再アップロード（重複回避）
      - スキップ機能追加
v1.2: 画像ファイル（screenshots）のS3アップロード機能を追加
      - JSON内のLinuxパスをWindows NASパスに変換
      - screenshots配列から画像ファイルを取得してS3にアップロード
      - 画像URLをメタデータに含める
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
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError

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
S3_IMAGE_PREFIX = "rag/images/"  # 画像ファイル用のプレフィックス
S3_CLIENT = boto3.client('s3', region_name=S3_REGION)

# ローカルファイル設定
BASE_NAS_PATH = r"\\NAS-TKY-2504\database\program-integration"
BASE_PROCESSED_NAS_PATH = r"\\NAS-TKY-2504\processed"

# --- パス変換関数 ---
def convert_linux_path_to_windows_nas(linux_path: str, channel_code: str = None, date_str: str = None) -> Optional[str]:
    r"""
    JSON内のLinuxパス形式をWindows NASパスに変換する
    例: /run/user/1000/gvfs/smb-share:server=nas-tky-2504.local,share=processed/NHKG-TKY/20251015AM/screenshot/xxx.jpeg
    -> \\NAS-TKY-2504\processed\NHKG-TKY\20251015AM\screenshot\xxx.jpeg
    
    または screenshotsフォルダを試行
    """
    # Linuxパスからチャンネルコードと日付、ファイル名を抽出
    # パターン: /run/user/.../share=processed/{CHANNEL}/{DATE}/screenshot(s)/{FILENAME}
    pattern = r'/share=processed/([^/]+)/([^/]+)/(?:screenshot|screenshots)/([^/]+\.jpeg)'
    match = re.search(pattern, linux_path)
    
    if not match:
        # 直接ファイル名のみから構成を試行
        filename = os.path.basename(linux_path)
        if channel_code and date_str:
            # チャンネルコードと日付が既に分かっている場合
            for folder_name in ['screenshot', 'screenshots']:
                windows_path = os.path.join(BASE_PROCESSED_NAS_PATH, channel_code, date_str, folder_name, filename)
                if os.path.exists(windows_path):
                    return windows_path
        return None
    
    channel = match.group(1)
    date = match.group(2)
    filename = match.group(3)
    
    # screenshot と screenshots の両方を試行
    for folder_name in ['screenshot', 'screenshots']:
        windows_path = os.path.join(BASE_PROCESSED_NAS_PATH, channel, date, folder_name, filename)
        if os.path.exists(windows_path):
            return windows_path
    
    # 見つからない場合は最初の候補を返す（ファイルが存在しない可能性があるが、エラーハンドリングは呼び出し側で）
    return os.path.join(BASE_PROCESSED_NAS_PATH, channel, date, 'screenshot', filename)

# --- 画像アップロード関数 ---
def upload_image_to_s3(image_path: str, doc_id: str, image_filename: str) -> Optional[str]:
    """
    画像ファイルをS3にアップロードする
    戻り値: S3のオブジェクトキー（成功時）、None（失敗時）
    """
    try:
        if not os.path.exists(image_path):
            print(f"[WARNING] 画像ファイルが見つかりません: {image_path}")
            return None
        
        # S3のキーを生成: rag/images/{doc_id}/{filename}
        # ファイル名の衝突を避けるため、doc_idでフォルダ分け
        s3_key = f"{S3_IMAGE_PREFIX}{doc_id}/{image_filename}"
        
        # Content-Typeを設定
        content_type = 'image/jpeg'
        
        # ファイルを読み込んでアップロード
        with open(image_path, 'rb') as f:
            S3_CLIENT.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=f.read(),
                ContentType=content_type
            )
        
        s3_url = f"s3://{S3_BUCKET_NAME}/{s3_key}"
        return s3_url
        
    except Exception as e:
        print(f"[ERROR] 画像アップロードエラー: {image_path} - {str(e)}")
        return None

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

# --- S3オブジェクト存在確認と更新日時取得 ---
def get_s3_object_metadata(key: str) -> Optional[Dict]:
    """
    S3オブジェクトのメタデータを取得（存在する場合）
    戻り値: {'exists': True, 'LastModified': datetime} または None
    """
    try:
        response = S3_CLIENT.head_object(Bucket=S3_BUCKET_NAME, Key=key)
        return {
            'exists': True,
            'LastModified': response['LastModified'],
            'ETag': response.get('ETag', ''),
            'Size': response.get('ContentLength', 0)
        }
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            return {'exists': False}
        else:
            print(f"[WARNING] S3オブジェクト確認エラー: {key} - {error_code}")
            return None

# --- ファイル更新チェック ---
def should_upload_file(file_path: str, s3_key: str) -> Tuple[bool, str]:
    """
    ファイルをアップロードすべきかチェック
    戻り値: (アップロードすべきか, 理由)
    """
    try:
        # NASファイルの更新日時を取得
        file_mtime = os.path.getmtime(file_path)
        file_datetime = datetime.fromtimestamp(file_mtime)
        
        # S3オブジェクトのメタデータを取得
        s3_metadata = get_s3_object_metadata(s3_key)
        
        if not s3_metadata or not s3_metadata.get('exists'):
            return (True, "S3オブジェクトが存在しない")
        
        # S3オブジェクトの最終更新日時（UTC）
        s3_last_modified = s3_metadata['LastModified']
        
        # タイムゾーンを考慮して比較
        # S3はUTC、ファイルシステムはローカル時刻なので、timezone-naiveで比較
        # 両方をnaive datetimeに変換して比較
        s3_datetime_naive = s3_last_modified.replace(tzinfo=None) if s3_last_modified.tzinfo else s3_last_modified
        
        # ファイルの更新日時がS3より新しい場合のみアップロード
        # 5秒のマージンを設定（ファイルシステムのタイムゾーン誤差を考慮）
        time_diff = (file_datetime - s3_datetime_naive).total_seconds()
        if time_diff > 5:
            return (True, f"ファイルが更新されています (NAS: {file_datetime}, S3: {s3_datetime_naive}, 差分: {time_diff:.1f}秒)")
        else:
            return (False, f"ファイルは最新です (スキップ)")
    
    except Exception as e:
        print(f"[WARNING] 更新チェックエラー: {file_path} - {str(e)}")
        # エラー時は安全のためアップロード
        return (True, f"エラーにより更新チェックをスキップ: {str(e)}")

# --- S3アップロード処理 ---
def upload_to_s3(data_list: List[Dict], key: str, skip_if_exists: bool = False):
    """
    データをJSON Lines形式でS3にアップロード
    skip_if_exists: Trueの場合、既に存在する場合はスキップ（更新チェックは呼び出し側で実施）
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

# --- 画像処理関数 ---
def process_and_upload_images(integrated_data: Dict, doc_id: str) -> List[str]:
    """
    JSON内のscreenshots配列から画像ファイルを処理してS3にアップロード
    戻り値: アップロードされた画像のS3 URLリスト
    """
    uploaded_image_urls = []
    
    if 'screenshots' not in integrated_data or not integrated_data['screenshots']:
        print(f"[INFO] 画像ファイルが見つかりませんでした（doc_id: {doc_id}）")
        return uploaded_image_urls
    
    # チャンネルコードと日付を取得（メタデータから）
    # ファイル名やパスから抽出されるため、ここではNoneのまま（パス変換関数内で処理）
    channel_code = None
    date_str = None
    
    print(f"[INFO] {len(integrated_data['screenshots'])}個の画像ファイルを処理中...")
    
    for screenshot in integrated_data['screenshots']:
        linux_path = screenshot.get('file_path', '')
        filename = screenshot.get('file_name', '')
        
        if not linux_path or not filename:
            continue
        
        # パス変換
        windows_path = convert_linux_path_to_windows_nas(linux_path, channel_code, date_str)
        
        if not windows_path:
            # パス変換に失敗した場合、ファイル名から直接構築を試行
            # ファイル名から日付とチャンネルコードを抽出
            # 例: NHKG-TKY-20251015-003534-xxx.jpeg
            filename_match = re.search(r'([A-Z]+-[A-Z]+)-(\d{8})', filename)
            if filename_match:
                channel = filename_match.group(1)
                date = filename_match.group(2)
                # 日付形式を変換: 20251015 -> 20251015AM または他の形式
                # ここでは一旦そのまま使用
                for folder_name in ['screenshot', 'screenshots']:
                    test_path = os.path.join(BASE_PROCESSED_NAS_PATH, channel, date, folder_name, filename)
                    if os.path.exists(test_path):
                        windows_path = test_path
                        break
                    # AM/PMなどのサフィックスを試行
                    for suffix in ['AM', 'PM']:
                        test_path = os.path.join(BASE_PROCESSED_NAS_PATH, channel, f"{date}{suffix}", folder_name, filename)
                        if os.path.exists(test_path):
                            windows_path = test_path
                            break
        
        if windows_path and os.path.exists(windows_path):
            s3_url = upload_image_to_s3(windows_path, doc_id, filename)
            if s3_url:
                uploaded_image_urls.append(s3_url)
                print(f"[OK] 画像アップロード完了: {s3_url}")
            else:
                print(f"[WARNING] 画像アップロード失敗: {windows_path}")
        else:
            print(f"[WARNING] 画像ファイルが見つかりません: {linux_path} (変換後: {windows_path})")
    
    return uploaded_image_urls

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
        
        # --- 更新チェック ---
        master_key = f"{S3_MASTER_PREFIX}{doc_id}.jsonl"
        chunk_key = f"{S3_CHUNK_PREFIX}{doc_id}_segments.jsonl"
        
        # マスターファイルの更新チェック
        should_upload_master, reason_master = should_upload_file(file_path, master_key)
        should_upload_chunk, reason_chunk = should_upload_file(file_path, chunk_key)
        
        # どちらかが更新されていない場合はスキップ
        if not should_upload_master and not should_upload_chunk:
            print(f"[SKIP] ファイルは最新のためスキップ: {file_path}")
            print(f"  理由: {reason_master}")
            return True
        
        if should_upload_master:
            print(f"[INFO] マスターファイルを更新: {reason_master}")
        if should_upload_chunk:
            print(f"[INFO] チャンクファイルを更新: {reason_chunk}")
        
        # 3. 画像ファイルの処理とアップロード
        uploaded_image_urls = process_and_upload_images(integrated_data, doc_id)
        
        # 画像URLをメタデータに追加
        if uploaded_image_urls:
            master_data['image_urls'] = uploaded_image_urls
            print(f"[INFO] {len(uploaded_image_urls)}個の画像をアップロードしました")
        
        # --- S3アップロード実行 ---
        
        # A. マスターデータ (PostgreSQLの入力用)
        if should_upload_master:
            upload_to_s3([master_data], master_key)
        else:
            print(f"[SKIP] マスターデータをスキップ: {master_key}")
        
        # B. チャンクデータ (Weaviate/OpenSearchの入力用)
        if should_upload_chunk:
            upload_to_s3(all_chunks, chunk_key)
        else:
            print(f"[SKIP] チャンクデータをスキップ: {chunk_key}")
        
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

if __name__ == "__main__":
    # メイン実行（q1.00ファイルのバッチ処理 + 画像アップロード）
    process_and_upload_local_rag_data()

