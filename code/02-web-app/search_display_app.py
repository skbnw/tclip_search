"""
S3データ検索・表示Webアプリ

Streamlitを使用して、S3バケット内のデータを検索・表示します。
- 日付・時間・放送局・キーワードで検索
- マスターデータの表示
- チャンクデータの表示
- 画像の表示
"""

import streamlit as st
import boto3
import json
import sys
import os
import re
from typing import Dict, List, Optional
from io import BytesIO
from datetime import date, time, datetime, timedelta

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# S3設定
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"
S3_MASTER_PREFIX = "rag/master_text/"
S3_CHUNK_PREFIX = "rag/vector_chunks/"
S3_IMAGE_PREFIX = "rag/images/"

# ページ設定
st.set_page_config(
    page_title="テレビ番組データ検索β",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"  # サイドバーをデフォルトで折りたたむ
)

# タイトル（ロゴとタイトルを横並びに）
col_logo, col_title = st.columns([1, 10])
with col_logo:
    # ロゴファイルを読み込む（複数のパスを試す）
    # Streamlitアプリの実行パスからの相対パスを考慮
    import pathlib
    script_dir = pathlib.Path(__file__).parent.absolute()
    project_root = script_dir.parent.parent  # プロジェクトルート
    
    logo_paths = [
        str(project_root / "image" / "tclipLOGO.png"),  # ユーザー指定のパス（絶対パス）
        "image/tclipLOGO.png",  # 相対パス（プロジェクトルートから）
        str(script_dir / "image" / "tclipLOGO.png"),  # code/02-web-app/image/tclipLOGO.png
        str(script_dir / "logo.png"),
        str(script_dir / "logo.jpg"),
        str(script_dir / "logo.jpeg"),
        str(script_dir / "logo.svg"),
        str(project_root / "logo.png"),
        str(project_root / "logo.jpg"),
        str(project_root / "logo.jpeg"),
        str(project_root / "logo.svg"),
        "code/02-web-app/logo.png",
        "code/02-web-app/logo.jpg",
        "code/02-web-app/logo.jpeg",
        "code/02-web-app/logo.svg",
        "logo.png",
        "logo.jpg",
        "logo.jpeg",
        "logo.svg"
    ]
    logo_found = False
    logo_used_path = None
    for logo_path in logo_paths:
        try:
            if os.path.exists(logo_path):
                # ロゴを表示（サイズを大きく）
                st.image(logo_path, width=120, use_container_width=False)
                logo_found = True
                logo_used_path = logo_path
                break
        except Exception as e:
            continue
    
    if not logo_found:
        # ロゴが見つからない場合は空欄
        st.empty()

with col_title:
    st.title("番組データ検索β")
st.markdown("---")

# AWS認証情報の設定（環境変数、Streamlit Secrets、またはユーザー入力）
def get_aws_credentials():
    """AWS認証情報を取得（優先順位: Secrets > 環境変数 > ユーザー入力）"""
    access_key = None
    secret_key = None
    region = S3_REGION
    
    # 1. Streamlit Secretsから取得（Streamlit Cloudで使用）
    try:
        if 'AWS_ACCESS_KEY_ID' in st.secrets:
            access_key = st.secrets['AWS_ACCESS_KEY_ID']
            secret_key = st.secrets['AWS_SECRET_ACCESS_KEY']
            region = st.secrets.get('AWS_DEFAULT_REGION', S3_REGION)
            return access_key, secret_key, region
    except (AttributeError, KeyError):
        pass
    
    # 2. 環境変数から取得
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if access_key and secret_key:
        return access_key, secret_key, os.getenv('AWS_DEFAULT_REGION', S3_REGION)
    
    # 3. ユーザー入力（サイドバーで設定）
    return None, None, None

# AWS認証情報の取得
access_key, secret_key, region = get_aws_credentials()

# 認証情報を環境変数に設定（boto3が自動的に読み込むように）
if access_key and secret_key:
    os.environ['AWS_ACCESS_KEY_ID'] = access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
    os.environ['AWS_DEFAULT_REGION'] = region or S3_REGION

@st.cache_resource
def get_s3_client():
    """S3クライアントを取得（環境変数から認証情報を自動的に読み込む）"""
    try:
        # 認証情報が環境変数に設定されていることを確認
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region = os.getenv('AWS_DEFAULT_REGION', S3_REGION)
        
        if access_key and secret_key:
            # 明示的に認証情報を渡す
            s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
        else:
            # 環境変数から自動的に読み込む（IAMロールなど）
            s3_client = boto3.client('s3', region_name=region)
        
        return s3_client
    except Exception as e:
        st.error(f"S3クライアントの作成に失敗しました: {str(e)}")
        return None

# サイドバー
with st.sidebar:
    st.header("⚙️ 設定")
    
    # AWS認証情報の入力（オプション）
    st.subheader("AWS認証情報")
    
    # Secretsから取得できたか確認
    has_secrets = False
    try:
        has_secrets = bool('AWS_ACCESS_KEY_ID' in st.secrets and 'AWS_SECRET_ACCESS_KEY' in st.secrets)
    except (AttributeError, KeyError):
        pass
    
    # 環境変数に認証情報が設定されているか確認
    env_has_credentials = bool(os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY'))
    
    # 認証情報の状態を表示
    if has_secrets:
        st.success("✅ Streamlit Secretsから認証情報を読み込みました")
    elif env_has_credentials:
        st.info("ℹ️ 環境変数から認証情報を読み込みました")
    else:
        st.warning("⚠️ 認証情報が見つかりません。以下のいずれかを設定してください：")
        st.markdown("1. Streamlit Cloud: Settings → Secrets")
        st.markdown("2. 環境変数（ローカル開発時）")
        st.markdown("3. 下記の入力フィールド（一時的）")
    
    # ユーザー入力用（Secretsや環境変数がない場合）
    if not has_secrets and not env_has_credentials:
        access_key_id = st.text_input(
            "Access Key ID", 
            value="",
            type="password",
            help="一時的に使用する場合は入力してください"
        )
        secret_access_key = st.text_input(
            "Secret Access Key", 
            value="",
            type="password",
            help="一時的に使用する場合は入力してください"
        )
        
        if access_key_id and secret_access_key:
            # 入力された認証情報を環境変数に設定
            os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
            os.environ['AWS_SECRET_ACCESS_KEY'] = secret_access_key
            os.environ['AWS_DEFAULT_REGION'] = S3_REGION
            access_key = access_key_id
            secret_key = secret_access_key
            region = S3_REGION
    
    st.markdown("---")

# S3クライアントの取得（環境変数から自動的に読み込まれる）
s3_client = get_s3_client()

if s3_client is None:
    st.error("S3クライアントの初期化に失敗しました。AWS認証情報を確認してください。")
    st.stop()

# メインコンテンツ

# インデックスファイルのパス
S3_INDEX_FILE = "rag/search_index/master_index.jsonl"

# データ取得関数（インデックスを使用）
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def load_search_index(_s3_client) -> List[Dict]:
    """検索用インデックスを読み込み（軽量版）"""
    try:
        # インデックスファイルを取得
        response = _s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_INDEX_FILE)
        content = response['Body'].read().decode('utf-8')
        
        index_list = []
        for line in content.strip().split('\n'):
            if line:
                index_list.append(json.loads(line))
        
        return index_list
    except _s3_client.exceptions.NoSuchKey:
        # インデックスファイルが存在しない場合は従来の方法で取得
        st.warning("⚠️ インデックスファイルが見つかりません。従来の方法でデータを読み込みます...")
        return list_all_master_data_fallback(_s3_client)
    except Exception as e:
        st.error(f"インデックス読み込みエラー: {str(e)}")
        return list_all_master_data_fallback(_s3_client)

@st.cache_data(ttl=3600)  # 1時間キャッシュ（フォールバック用）
def list_all_master_data_fallback(_s3_client) -> List[Dict]:
    """全マスターデータのリストを取得（フォールバック、インデックスがない場合）"""
    try:
        response = _s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_MASTER_PREFIX)
        
        master_list = []
        if 'Contents' in response:
            total_files = len(response['Contents'])
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, obj in enumerate(response['Contents']):
                try:
                    # 進捗表示
                    if idx % 10 == 0 or idx == total_files - 1:
                        progress = (idx + 1) / total_files
                        progress_bar.progress(progress)
                        status_text.text(f"データ読み込み中: {idx + 1}/{total_files} ファイル")
                    
                    # オブジェクトを取得
                    file_response = _s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
                    content = file_response['Body'].read().decode('utf-8')
                    lines = content.strip().split('\n')
                    if lines:
                        master_data = json.loads(lines[0])
                        master_list.append(master_data)
                except Exception as e:
                    continue  # エラーが発生したファイルはスキップ
            
            progress_bar.empty()
            status_text.empty()
        
        return master_list
    except Exception as e:
        st.error(f"全マスターデータの取得エラー: {str(e)}")
        return []

# 後方互換性のため、list_all_master_dataをインデックス版に置き換え
def list_all_master_data(_s3_client) -> List[Dict]:
    """全マスターデータのリストを取得（インデックスを使用）"""
    return load_search_index(_s3_client)

# 検索オプションの取得（初回のみ読み込み）
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_search_options(_s3_client) -> Dict[str, List[str]]:
    """検索用のオプション（日付、時間、放送局）を取得"""
    try:
        all_masters = list_all_master_data(_s3_client)
        
        dates = set()
        times = set()
        channels = set()
        
        for master in all_masters:
            metadata = master.get('metadata', {})
            
            # 日付
            if 'date' in metadata:
                date_str = str(metadata['date'])
                if date_str:
                    dates.add(date_str)
            
            # 開始時間
            if 'start_time' in metadata:
                start_time = str(metadata['start_time'])
                if start_time:
                    times.add(start_time)
            
            # 終了時間
            if 'end_time' in metadata:
                end_time = str(metadata['end_time'])
                if end_time:
                    times.add(end_time)
            
            # 放送局
            if 'channel' in metadata:
                channel = str(metadata['channel'])
                if channel:
                    channels.add(channel)
        
        return {
            'dates': sorted(list(dates)),
            'times': sorted(list(times)),
            'channels': sorted(list(channels))
        }
    except Exception as e:
        st.error(f"検索オプションの取得エラー: {str(e)}")
        return {'dates': [], 'times': [], 'channels': []}

# 30分単位の時間リスト生成
def generate_time_options():
    """30分単位の時間オプションを生成"""
    times = []
    for hour in range(24):
        for minute in [0, 30]:
            time_obj = time(hour, minute)
            times.append(time_obj)
    return times

# 時間の近似検索（30分単位で最も近い時間を探す）
def find_nearest_time(target_time: time, time_list: List[str]) -> Optional[str]:
    """30分単位で最も近い時間を探す"""
    if not target_time or not time_list:
        return None
    
    # 時間を分に変換
    target_minutes = target_time.hour * 60 + target_time.minute
    
    nearest_time = None
    min_diff = float('inf')
    
    for time_str in time_list:
        try:
            # 時間文字列を解析（HHMM形式またはHH:MM形式）
            if ':' in time_str:
                parts = time_str.split(':')
                time_minutes = int(parts[0]) * 60 + int(parts[1])
            else:
                if len(time_str) >= 4:
                    time_minutes = int(time_str[:2]) * 60 + int(time_str[2:4])
                else:
                    continue
            
            # 30分単位に丸める
            rounded_minutes = round(time_minutes / 30) * 30
            diff = abs(target_minutes - rounded_minutes)
            
            # ±30分以内かチェック
            if diff <= 30 and diff < min_diff:
                min_diff = diff
                nearest_time = time_str
        except (ValueError, IndexError):
            continue
    
    return nearest_time

# 検索フォーム
with st.form("search_form"):
    st.subheader("検索条件")
    
    # 上部: 放送局、日付、時間
    search_options = get_search_options(_s3_client=s3_client)
    
    # 3列レイアウト（均等配置）
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        # 放送局（選択式）
        channel_options = ["すべて"]
        if search_options['channels']:
            channel_options.extend(search_options['channels'])
        else:
            # チャンネル情報がない場合でも表示
            st.warning("⚠️ 放送局データを読み込み中...")
        
        channel = st.selectbox(
            "放送局",
            options=channel_options,
            help="放送局を選択してください"
        )
    
    with col2:
        # 日付
        selected_date = st.date_input(
            "📆 日付",
            value=None,
            help="カレンダーから日付を選択してください（任意）",
            key="date_input"
        )
        date_str = selected_date.strftime("%Y%m%d") if selected_date else None
    
    with col3:
        # 時間（30分単位）
        time_options = generate_time_options()
        selected_time = st.selectbox(
            "🕐 時間",
            options=[None] + time_options,
            format_func=lambda x: x.strftime("%H:%M") if x else "選択なし",
            help="時間を選択してください（30分単位、任意）",
            key="time_input"
        )
        time_str = selected_time.strftime("%H%M") if selected_time else None
    
    st.markdown("---")
    
    # 下部: 番組名検索、主演者検索、キーワード検索
    col_program, col_performer, col_keyword = st.columns([1, 1, 1])
    
    with col_program:
        program_name_search = st.text_input(
            "番組名検索",
            placeholder="番組名を入力してください（任意）",
            help="番組名で検索します"
        )
    
    with col_performer:
        performer_search = st.text_input(
            "主演者検索",
            placeholder="出演者名を入力してください（任意）",
            help="出演者名で検索します"
        )
    
    with col_keyword:
        keyword = st.text_input(
            "キーワード（全文・チャンクテキスト検索）",
            placeholder="キーワードを入力してください（任意）",
            help="全文テキストとチャンクテキストから検索します"
        )
    
    # 検索ボタン
    search_button = st.form_submit_button("🔍 検索", use_container_width=True)
    
    # program_idは削除（使用しない）
    program_id = ""

# セッションステートの初期化（詳細表示用）
if 'selected_doc_id' not in st.session_state:
    st.session_state.selected_doc_id = None
if 'search_results' not in st.session_state:
    st.session_state.search_results = []

# データ取得関数
@st.cache_data(ttl=300)  # 5分間キャッシュ
def get_master_data(_s3_client, doc_id: str) -> Optional[Dict]:
    """マスターデータを取得"""
    try:
        key = f"{S3_MASTER_PREFIX}{doc_id}.jsonl"
        response = _s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # JSON Lines形式なので、最初の行を読み込む
        lines = content.strip().split('\n')
        if lines:
            return json.loads(lines[0])
        return None
    except _s3_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        st.error(f"マスターデータの取得エラー: {str(e)}")
        return None

@st.cache_data(ttl=300)
def get_chunk_data(_s3_client, doc_id: str) -> List[Dict]:
    """チャンクデータを取得"""
    try:
        key = f"{S3_CHUNK_PREFIX}{doc_id}_segments.jsonl"
        response = _s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        chunks = []
        for line in content.strip().split('\n'):
            if line:
                chunks.append(json.loads(line))
        return chunks
    except _s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        st.error(f"チャンクデータの取得エラー: {str(e)}")
        return []

@st.cache_data(ttl=300)
def list_images(_s3_client, doc_id: str) -> List[Dict]:
    """画像URLとメタデータのリストを取得"""
    try:
        prefix = f"{S3_IMAGE_PREFIX}{doc_id}/"
        response = _s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        
        image_data = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith(('.jpeg', '.jpg', '.png')):
                    # ファイル名を抽出
                    filename = os.path.basename(key)
                    
                    # 署名付きURLを生成（1時間有効）
                    url = _s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
                        ExpiresIn=3600
                    )
                    
                    # ファイル名から撮影時間を抽出
                    # 例: NHKG-TKY-20251003-050042-1759435242150-7.jpeg → 05:00:42
                    timestamp = extract_timestamp_from_filename(filename)
                    
                    image_data.append({
                        'url': url,
                        'filename': filename,
                        'timestamp': timestamp,
                        'key': key
                    })
        return image_data
    except Exception as e:
        st.error(f"画像一覧の取得エラー: {str(e)}")
        return []

def extract_timestamp_from_filename(filename: str) -> str:
    """ファイル名から撮影時間を抽出"""
    # パターン: NHKG-TKY-20251003-050042-1759435242150-7.jpeg
    # または: NHKG-TKY-20251003-050042-1759435242150-7.jpg
    # 時間部分: 050042 → 05:00:42
    try:
        # ファイル名から日付と時間部分を抽出
        # パターン: YYYYMMDD-HHMMSS
        pattern = r'(\d{8})-(\d{6})'
        match = re.search(pattern, filename)
        if match:
            time_str = match.group(2)  # HHMMSS
            if len(time_str) == 6:
                hour = time_str[:2]
                minute = time_str[2:4]
                second = time_str[4:6]
                return f"{hour}:{minute}:{second}"
    except Exception:
        pass
    return filename  # 抽出できない場合はファイル名を返す


def search_master_data_advanced(
    master_list: List[Dict], 
    program_id: str = "",
    date_str: str = "",
    time_str: str = "",
    channel: str = "",
    keyword: str = "",
    program_name: str = "",
    performer: str = "",
    time_tolerance_minutes: int = 30
) -> List[Dict]:
    """マスターデータを詳細条件で検索（時間近似検索対応）"""
    results = []
    
    for master in master_list:
        metadata = master.get('metadata', {})
        doc_id = master.get('doc_id', '')
        
        # 各条件でフィルタリング
        match = True
        
        # 日付でフィルタ
        if date_str:
            master_date = str(metadata.get('date', ''))
            # 日付形式を変換して比較（YYYYMMDD形式）
            if date_str not in master_date:
                match = False
                continue
        
        # 時間でフィルタ（近似検索）
        if time_str:
            # メタデータから時間情報を取得（複数のフィールドをチェック）
            start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', ''))
            end_time = str(metadata.get('end_time', '')) or str(metadata.get('終了時間', ''))
            
            # 目標時間を分に変換
            try:
                target_hour = int(time_str[:2])
                target_minute = int(time_str[2:4])
                target_minutes = target_hour * 60 + target_minute
            except (ValueError, IndexError):
                match = False
                continue
            
            # 開始時間と終了時間をチェック
            time_match = False
            
            # 開始時間と終了時間を分に変換
            start_minutes = None
            end_minutes = None
            
            if start_time and start_time != 'None' and start_time.strip():
                try:
                    # 様々な形式に対応
                    # HH:MM:SS形式
                    if ':' in start_time:
                        parts = start_time.split(':')
                        if len(parts) >= 2:
                            start_minutes = int(parts[0]) * 60 + int(parts[1])
                    # YYYYMMDDHHMM形式（12桁）から時間部分を抽出
                    elif len(start_time) == 12 and start_time.isdigit():
                        hour = int(start_time[8:10])
                        minute = int(start_time[10:12])
                        start_minutes = hour * 60 + minute
                    # HHMM形式（4桁）
                    elif len(start_time) >= 4 and start_time.isdigit():
                        # 12桁形式でない場合のみ4桁形式として処理
                        if len(start_time) == 4:
                            start_minutes = int(start_time[:2]) * 60 + int(start_time[2:4])
                        else:
                            # その他の桁数の場合、最後の4桁を時間として扱う
                            if len(start_time) > 4:
                                time_part = start_time[-4:]
                                start_minutes = int(time_part[:2]) * 60 + int(time_part[2:4])
                except (ValueError, IndexError):
                    pass
            
            if end_time and end_time != 'None' and end_time.strip():
                try:
                    # 様々な形式に対応
                    # HH:MM:SS形式
                    if ':' in end_time:
                        parts = end_time.split(':')
                        if len(parts) >= 2:
                            end_minutes = int(parts[0]) * 60 + int(parts[1])
                    # YYYYMMDDHHMM形式（12桁）から時間部分を抽出
                    elif len(end_time) == 12 and end_time.isdigit():
                        hour = int(end_time[8:10])
                        minute = int(end_time[10:12])
                        end_minutes = hour * 60 + minute
                    # HHMM形式（4桁）
                    elif len(end_time) >= 4 and end_time.isdigit():
                        # 12桁形式でない場合のみ4桁形式として処理
                        if len(end_time) == 4:
                            end_minutes = int(end_time[:2]) * 60 + int(end_time[2:4])
                        else:
                            # その他の桁数の場合、最後の4桁を時間として扱う
                            if len(end_time) > 4:
                                time_part = end_time[-4:]
                                end_minutes = int(time_part[:2]) * 60 + int(time_part[2:4])
                except (ValueError, IndexError):
                    pass
            
            # 時間範囲内に目標時間が含まれるかチェック
            # 指定時間以降、59分を含めて検索（例: 06:00で検索 → 06:00:00 ～ 06:59:59）
            target_hour_start = target_minutes  # 指定時間の開始（分）
            target_hour_end = target_minutes + 59  # 指定時間の終了（59分後まで）
            
            if start_minutes is not None and end_minutes is not None:
                # 番組の時間範囲が指定時間の1時間内（00分～59分）と重なるかチェック
                # 番組の開始時間が指定時間の1時間内、または番組の終了時間が指定時間の1時間内、または番組が指定時間の1時間内を含む
                if (start_minutes <= target_hour_end and end_minutes >= target_hour_start):
                    time_match = True
            elif start_minutes is not None:
                # 開始時間のみの場合、指定時間の1時間内（00分～59分）に含まれるかチェック
                if target_hour_start <= start_minutes <= target_hour_end:
                    time_match = True
            elif end_minutes is not None:
                # 終了時間のみの場合、指定時間の1時間内（00分～59分）に含まれるかチェック
                if target_hour_start <= end_minutes <= target_hour_end:
                    time_match = True
            
            if not time_match:
                match = False
                continue
        
        # 放送局でフィルタ
        if channel and channel != "すべて":
            master_channel = str(metadata.get('channel', ''))
            if channel not in master_channel:
                match = False
                continue
        
        # 番組名でフィルタ
        if program_name and program_name.strip():
            program_name_lower = program_name.strip().lower()
            # 番組名の候補フィールドをチェック
            program_fields = [
                metadata.get('program_name', ''),
                metadata.get('program_title', ''),
                metadata.get('master_title', ''),
                metadata.get('title', '')
            ]
            program_match = False
            for field_value in program_fields:
                if field_value and program_name_lower in str(field_value).lower():
                    program_match = True
                    break
            if not program_match:
                match = False
                continue
        
        # 主演者でフィルタ
        if performer and performer.strip():
            performer_lower = performer.strip().lower()
            # 出演者情報を取得
            talents = metadata.get('talents', [])
            performer_match = False
            
            # 出演者リストをチェック
            if talents:
                for talent in talents:
                    if isinstance(talent, dict):
                        talent_name = talent.get('name', '') or talent.get('talent_name', '')
                    else:
                        talent_name = str(talent)
                    if talent_name and performer_lower in talent_name.lower():
                        performer_match = True
                        break
            
            # 出演者名の文字列フィールドもチェック
            if not performer_match:
                talent_fields = [
                    metadata.get('talent_names', ''),
                    metadata.get('performers', ''),
                    metadata.get('cast', '')
                ]
                for field_value in talent_fields:
                    if field_value and performer_lower in str(field_value).lower():
                        performer_match = True
                        break
            
            if not performer_match:
                match = False
                continue
        
        # キーワードでフィルタ（全文とチャンクテキスト）
        if keyword and keyword.strip():
            keyword_lower = keyword.strip().lower()
            
            # 検索対象テキストを取得（複数のソースから）
            search_texts = []
            
            # 1. 全文テキスト
            full_text = master.get('full_text', '')
            if full_text:
                search_texts.append(str(full_text).lower())
            
            # 2. 全文プレビュー（フォールバック）
            full_text_preview = master.get('full_text_preview', '')
            if full_text_preview and not full_text:
                search_texts.append(str(full_text_preview).lower())
            
            # 3. メタデータ内のテキストフィールド
            metadata = master.get('metadata', {})
            if metadata:
                text_fields = [
                    'program_name', 'program_title', 'master_title',
                    'description', 'description_detail', 'program_detail',
                    'title', 'channel', 'channel_code'
                ]
                for field in text_fields:
                    field_value = metadata.get(field, '')
                    if field_value:
                        search_texts.append(str(field_value).lower())
            
            # すべての検索対象テキストを結合して検索
            combined_text = ' '.join(search_texts)
            
            if keyword_lower not in combined_text:
                match = False
                continue
        
        if match:
            results.append(master)
    
    return results

def search_master_data_with_chunks(
    _s3_client,
    master_list: List[Dict], 
    program_id: str = "",
    date_str: str = "",
    time_str: str = "",
    channel: str = "",
    keyword: str = "",
    program_name: str = "",
    performer: str = "",
    time_tolerance_minutes: int = 30,
    max_results: int = 500  # 検索結果の上限（パフォーマンス向上）
) -> List[Dict]:
    """マスターデータとチャンクテキストを含む詳細検索（最適化版）"""
    # まず基本条件でフィルタ（メタデータのみで高速）
    # キーワードは後で全文検索で処理するため、ここでは空文字列を渡す
    filtered_masters = search_master_data_advanced(
        master_list, program_id, date_str, time_str, channel, "", program_name, performer, time_tolerance_minutes
    )
    
    # デバッグ: 基本フィルタ後の件数を確認（st.debugは存在しないため削除）
    
    # キーワードが指定されている場合、全文テキストでフィルタリング
    if keyword and keyword.strip():
        keyword_lower = keyword.strip().lower()
        results = []
        
        # 進捗表示用
        total = len(filtered_masters)
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 全文テキストでフィルタリング（インデックスに全文が含まれているため高速）
        for idx, master in enumerate(filtered_masters):
            # 検索結果の上限に達したら終了
            if len(results) >= max_results:
                status_text.text(f"検索完了: {len(results)} 件（上限に達しました）")
                break
            
            # 進捗表示（50件ごと、大量データでも高速に）
            if idx % 50 == 0 or idx == total - 1:
                progress = (idx + 1) / total
                progress_bar.progress(progress)
                status_text.text(f"キーワード検索中: {idx + 1}/{total} 件（{len(results)} 件ヒット）")
            
            # 検索対象テキストを取得（複数のソースから）
            search_texts = []
            
            # 1. 全文テキスト（インデックスに含まれている場合）
            full_text = master.get('full_text', '')
            if full_text:
                search_texts.append(str(full_text).lower())
            
            # 2. 全文プレビュー（インデックスに全文がない場合のフォールバック）
            full_text_preview = master.get('full_text_preview', '')
            if full_text_preview and not full_text:
                search_texts.append(str(full_text_preview).lower())
            
            # 3. メタデータ内のテキストフィールドも検索対象に含める
            metadata = master.get('metadata', {})
            if metadata:
                # 番組名、説明、詳細説明など
                text_fields = [
                    'program_name', 'program_title', 'master_title',
                    'description', 'description_detail', 'program_detail',
                    'title', 'channel', 'channel_code'
                ]
                for field in text_fields:
                    field_value = metadata.get(field, '')
                    if field_value:
                        search_texts.append(str(field_value).lower())
            
            # すべての検索対象テキストを結合して検索
            combined_text = ' '.join(search_texts)
            
            if keyword_lower in combined_text:
                results.append(master)
        
        # チャンク検索はスキップ（インデックスに全文が含まれているため不要）
        # 全文テキスト検索で十分高速に検索可能
        
        progress_bar.empty()
        status_text.empty()
        
        # 検索結果が上限に達した場合の警告
        if len(results) >= max_results:
            st.info(f"ℹ️ 検索結果が{max_results}件に達したため、表示を制限しました。検索条件を絞り込んでください。")
        
        return results
    
    return filtered_masters

def display_master_data(master_data, chunks, images, doc_id, target_chunk_filename=None):
    """マスターデータ、チャンク、画像を表示"""
    if not master_data:
        st.warning("データが見つかりませんでした")
        return
    
    # メタデータの表示
    metadata = master_data.get('metadata', {})
    
    # タブで表示（番組メタデータ、AI要約、画像、全文、チャンク）
    # 画像から遷移した場合はチャンクタブを最初に表示
    if target_chunk_filename:
        # チャンクタブを最初に表示（タブの順序を変更）
        tab5, tab1, tab2, tab3, tab4 = st.tabs(["📑 チャンク", "📋 番組メタデータ", "🤖 AI要約", "🖼️ 画面スクショ", "📄 全文"])
    else:
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 番組メタデータ", "🤖 AI要約", "🖼️ 画面スクショ", "📄 全文", "📑 チャンク"])
    
    with tab1:
        st.subheader("番組メタデータ")
        
        # メタ情報をテーブル形式で表示
        if metadata:
            # 主要なメタデータをテーブル形式で表示
            table_data = []
            
            # 基本情報
            if metadata.get('event_id'):
                table_data.append({"項目": "イベントID", "値": metadata.get('event_id')})
            if metadata.get('channel_code'):
                table_data.append({"項目": "チャンネルコード", "値": metadata.get('channel_code')})
            if metadata.get('channel'):
                table_data.append({"項目": "放送局", "値": metadata.get('channel')})
            if metadata.get('region'):
                table_data.append({"項目": "地域", "値": metadata.get('region')})
            
            # 日時情報
            if metadata.get('date') or metadata.get('broadcast_date'):
                date_val = metadata.get('broadcast_date') or metadata.get('date')
                date_str = str(date_val)
                # YYYYMMDD形式の場合
                if len(date_str) >= 8 and date_str.isdigit():
                    date_display = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                # YYYY-MM-DD形式の場合
                elif '-' in date_str:
                    date_display = date_str
                else:
                    date_display = date_str
                table_data.append({"項目": "放送日", "値": date_display})
            if metadata.get('start_time'):
                start_time = format_time_display_detail(metadata.get('start_time', ''))
                table_data.append({"項目": "開始時間", "値": start_time})
            if metadata.get('end_time'):
                end_time = format_time_display_detail(metadata.get('end_time', ''))
                table_data.append({"項目": "終了時間", "値": end_time})
            
            # 番組情報
            if metadata.get('program_name') or metadata.get('program_title') or metadata.get('master_title'):
                program_name = metadata.get('program_name') or metadata.get('program_title') or metadata.get('master_title')
                table_data.append({"項目": "番組名", "値": program_name})
            if metadata.get('program_detail'):
                table_data.append({"項目": "番組詳細", "値": metadata.get('program_detail')})
            if metadata.get('description'):
                table_data.append({"項目": "説明", "値": metadata.get('description')})
            if metadata.get('description_detail'):
                table_data.append({"項目": "詳細説明", "値": metadata.get('description_detail')})
            if metadata.get('genre'):
                table_data.append({"項目": "ジャンル", "値": metadata.get('genre')})
            
            # 出演者情報（リンク付き）
            if metadata.get('talents'):
                talents = metadata.get('talents', [])
                if isinstance(talents, list) and len(talents) > 0:
                    talent_links = []
                    for talent in talents:
                        if isinstance(talent, dict):
                            name = talent.get('name', '')
                            link = talent.get('link', '')
                            if name:
                                if link:
                                    talent_links.append(f"[{name}]({link})")
                                else:
                                    talent_links.append(name)
                        elif isinstance(talent, str):
                            talent_links.append(talent)
                    if talent_links:
                        table_data.append({"項目": "出演者", "値": ", ".join(talent_links)})
            if metadata.get('talent_count'):
                table_data.append({"項目": "出演者数", "値": str(metadata.get('talent_count'))})
            
            # テーブル表示（詰めて表示）
            if table_data:
                # HTMLテーブル風の表示（詰めて）
                for row in table_data:
                    col1, col2 = st.columns([2, 5])
                    with col1:
                        st.markdown(f"**{row['項目']}**")
                    with col2:
                        # マークダウンリンクを処理
                        if isinstance(row['値'], str) and row['値'].startswith('[') and '](' in row['値']:
                            st.markdown(row['値'])
                        else:
                            st.markdown(row['値'])
            else:
                st.info("表示可能なメタデータがありません")
            
            # 全メタデータをJSON形式でダウンロード可能にする
            json_str = json.dumps(metadata, ensure_ascii=False, indent=2)
            
            # ファイル名を生成（放送開始時間_放送終了時間_局名_details.json）
            # 日付と時間を取得
            date_str = metadata.get('date', '') or metadata.get('broadcast_date', '')
            start_time = metadata.get('start_time', '')
            end_time = metadata.get('end_time', '')
            channel = metadata.get('channel', '') or metadata.get('channel_code', '')
            
            # ファイル名用の形式に変換
            filename_date = ""
            filename_start = ""
            filename_end = ""
            filename_channel = ""
            
            if date_str:
                date_str = str(date_str)
                if len(date_str) >= 8 and date_str.isdigit():
                    filename_date = date_str  # YYYYMMDD
                elif '-' in date_str:
                    filename_date = date_str.replace('-', '')[:8]  # YYYY-MM-DD -> YYYYMMDD
            
            if start_time:
                start_time_str = str(start_time)
                if len(start_time_str) >= 4:
                    if ':' in start_time_str:
                        filename_start = start_time_str.replace(':', '')[:4]  # HH:MM -> HHMM
                    else:
                        filename_start = start_time_str[:4]  # HHMM
            
            if end_time:
                end_time_str = str(end_time)
                if len(end_time_str) >= 4:
                    if ':' in end_time_str:
                        filename_end = end_time_str.replace(':', '')[:4]  # HH:MM -> HHMM
                    else:
                        filename_end = end_time_str[:4]  # HHMM
            
            if channel:
                # チャンネル名を英語化（簡易版）
                channel_mapping = {
                    'NHK総合': 'NHK',
                    'NHK Eテレ': 'NHK-ETV',
                    'フジテレビ': 'FUJI-TV',
                    '日本テレビ': 'NTV',
                    'TBS': 'TBS',
                    'テレビ朝日': 'TV-ASAHI',
                    'テレビ東京': 'TV-TOKYO'
                }
                filename_channel = channel_mapping.get(channel, channel.replace(' ', '-').replace('　', '-'))
            
            # ファイル名を生成
            if filename_date and filename_start and filename_end and filename_channel:
                json_filename = f"{filename_date}_{filename_start}_{filename_end}_{filename_channel}_details.json"
            else:
                json_filename = f"metadata_{doc_id}.json"
            
            st.download_button(
                label="📥 全メタデータをダウンロード（JSON形式）",
                data=json_str,
                file_name=json_filename,
                mime="application/json"
            )
        else:
            st.info("メタデータがありません")
    
    with tab2:
        st.subheader("AI要約")
        
        # Groq APIを使用して番組の概要を生成
        if metadata:
            try:
                from groq import Groq
                
                # Groq APIキーを取得（Streamlit Secrets > 環境変数 > デフォルト）
                groq_api_key = None
                try:
                    # Streamlit Secretsから取得
                    if hasattr(st, 'secrets') and 'groq' in st.secrets and 'api_key' in st.secrets.groq:
                        groq_api_key = st.secrets.groq.api_key
                except:
                    pass
                
                if not groq_api_key:
                    # 環境変数から取得（osモジュールは既にインポート済み）
                    import os as os_module
                    groq_api_key = os_module.getenv('GROQ_API_KEY')
                
                if not groq_api_key:
                    st.error("⚠️ Groq APIキーが設定されていません。Streamlit Secretsまたは環境変数 `GROQ_API_KEY` を設定してください。")
                    st.info("💡 Streamlit CloudのSecretsに以下を追加してください：")
                    st.code("""
[groq]
api_key = "YOUR_GROQ_API_KEY"
""", language="toml")
                else:
                    # メタデータをJSON形式で準備
                    metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2)
                    
                    # 全文テキストを取得（時間表示を削除）
                    full_text_for_summary = ""
                    if 'full_text' in master_data and master_data['full_text']:
                        full_text_raw = master_data['full_text']
                        # 時間表示のパターンを削除
                        full_text_for_summary = re.sub(r'\[\d{2}:\d{2}:\d{2}\.\d{3}-\d{2}:\d{2}:\d{2}\.\d{3}\]\s*', '', full_text_raw)
                    
                    # 番組タイプを判定（ニュース番組かどうか）
                    program_name = metadata.get('program_name', '') or metadata.get('program_title', '') or metadata.get('master_title', '') or ''
                    is_news = 'ニュース' in program_name or 'news' in program_name.lower()
                    
                    # プロンプトを作成
                    if is_news:
                        # ニュース番組の場合：ニュースのタイトルと3行メモ形式
                        if full_text_for_summary:
                            prompt = f"""以下のニュース番組のメタデータと全文テキストを基に、報じられているニュースのタイトルと3行メモを作成してください。

メタデータ:
{metadata_json}

全文テキスト:
{full_text_for_summary[:5000]}

注意事項:
- 出演者情報は不要です（タグデータで確認できます）
- 報じられているニュースのタイトルを1つ以上挙げてください
- 各ニュースについて3行程度のメモを記載してください
- 番組名や放送局名は不要です
- A4サイズ程度（約2000文字）の長さで詳細に記述してください
- 全文テキストの内容を優先的に参考にしてください（メタデータよりも実際の放送内容が重要です）

ニュースのタイトルと3行メモ:"""
                        else:
                            prompt = f"""以下のニュース番組のメタデータを基に、報じられているニュースのタイトルと3行メモを作成してください。

メタデータ:
{metadata_json}

注意事項:
- 出演者情報は不要です（タグデータで確認できます）
- 報じられているニュースのタイトルを1つ以上挙げてください
- 各ニュースについて3行程度のメモを記載してください
- 番組名や放送局名は不要です
- A4サイズ程度（約2000文字）の長さで詳細に記述してください

ニュースのタイトルと3行メモ:"""
                    else:
                        # その他の番組の場合：通常の要約
                        if full_text_for_summary:
                            prompt = f"""以下の番組メタデータと全文テキストを基に、番組の概要を詳しくまとめてください。

メタデータ:
{metadata_json}

全文テキスト:
{full_text_for_summary[:5000]}

注意事項:
- 出演者情報は不要です（タグデータで確認できます）
- 番組の内容、テーマ、特集などを詳しく説明してください
- A4サイズ程度（約2000文字）の長さで詳細に記述してください
- 番組の主要なポイント、特集内容、重要な情報を含めてください
- 全文テキストの内容を優先的に参考にしてください（メタデータよりも実際の放送内容が重要です）

番組の概要:"""
                        else:
                            prompt = f"""以下の番組メタデータを基に、番組の概要を詳しくまとめてください。

メタデータ:
{metadata_json}

注意事項:
- 出演者情報は不要です（タグデータで確認できます）
- 番組の内容、テーマ、特集などを詳しく説明してください
- A4サイズ程度（約2000文字）の長さで詳細に記述してください
- 番組の主要なポイント、特集内容、重要な情報を含めてください

番組の概要:"""
                    
                    # AI要約を生成（毎回生成、キャッシュなし）
                    def generate_summary(_prompt: str, _api_key: str) -> str:
                        """Groq APIを使用して要約を生成"""
                        try:
                            client = Groq(api_key=_api_key)
                            chat_completion = client.chat.completions.create(
                                messages=[
                                    {
                                        "role": "user",
                                        "content": _prompt
                                    }
                                ],
                                model="llama-3.3-70b-versatile",  # Groqの高速モデル（llama-3.1-70b-versatileの後継）
                                temperature=0.7,
                                max_tokens=2000  # A4サイズ程度の長さ（約2000文字）
                            )
                            return chat_completion.choices[0].message.content
                        except Exception as e:
                            return f"エラー: {str(e)}"
                    
                    # 要約を生成（毎回生成）
                    with st.spinner("AI要約を生成中..."):
                        summary = generate_summary(prompt, groq_api_key)
                    
                    # 要約を表示
                    st.markdown("### 番組概要")
                    st.markdown(summary)
                
            except ImportError:
                st.error("⚠️ Groqパッケージがインストールされていません。")
                st.code("pip install groq", language="bash")
            except Exception as e:
                st.error(f"⚠️ AI要約の生成でエラーが発生しました: {str(e)}")
        else:
            st.info("メタデータがありません")
    
    with tab3:
        st.subheader("画面スクショ")
        if images:
            st.info(f"画面スクショ数: {len(images)}")
            # グリッド表示（3列）
            cols = st.columns(3)
            for idx, img_data in enumerate(images):
                with cols[idx % 3]:
                    try:
                        # 画像データを取得（辞書形式またはURL文字列）
                        if isinstance(img_data, dict):
                            img_url = img_data.get('url', '')
                            timestamp = img_data.get('timestamp', f"画像 {idx+1}")
                            filename = img_data.get('filename', '')
                        else:
                            img_url = img_data
                            timestamp = f"画像 {idx+1}"
                            filename = ''
                        
                        # 画像を表示（撮影時間をキャプションに）
                        st.image(img_url, caption=timestamp, use_container_width=True)
                        
                        # クリックでチャンクタブに飛ぶボタン
                        if filename:
                            if st.button(f"📑 チャンクを表示", key=f"chunk_link_{doc_id}_{idx}"):
                                # チャンクタブに切り替える（セッションステートを使用）
                                st.session_state[f"show_chunk_for_{doc_id}"] = filename
                                st.rerun()
                    except Exception as e:
                        st.error(f"画面スクショの読み込みエラー: {str(e)}")
        else:
            st.info("画面スクショがありません")
    
    with tab4:
        st.subheader("全文テキスト")
        if 'full_text' in master_data and master_data['full_text']:
            # 時間表示を削除（[HH:MM:SS.mmm-HH:MM:SS.mmm]形式）
            full_text = master_data['full_text']
            # 時間表示のパターンを削除
            cleaned_text = re.sub(r'\[\d{2}:\d{2}:\d{2}\.\d{3}-\d{2}:\d{2}:\d{2}\.\d{3}\]\s*', '', full_text)
            st.text_area("", value=cleaned_text, height=400, key=f"full_text_{doc_id}")
            
            # 全文テキストをtxtファイルとしてダウンロード可能にする
            # ファイル名を生成（放送開始時間_放送終了時間_局名_fulltext.txt）
            metadata = master_data.get('metadata', {})
            date_str = metadata.get('date', '') or metadata.get('broadcast_date', '')
            start_time = metadata.get('start_time', '')
            end_time = metadata.get('end_time', '')
            channel = metadata.get('channel', '') or metadata.get('channel_code', '')
            
            # ファイル名用の形式に変換
            filename_date = ""
            filename_start = ""
            filename_end = ""
            filename_channel = ""
            
            if date_str:
                date_str = str(date_str)
                if len(date_str) >= 8 and date_str.isdigit():
                    filename_date = date_str  # YYYYMMDD
                elif '-' in date_str:
                    filename_date = date_str.replace('-', '')[:8]  # YYYY-MM-DD -> YYYYMMDD
            
            if start_time:
                start_time_str = str(start_time)
                if len(start_time_str) >= 4:
                    if ':' in start_time_str:
                        filename_start = start_time_str.replace(':', '')[:4]  # HH:MM -> HHMM
                    else:
                        filename_start = start_time_str[:4]  # HHMM
            
            if end_time:
                end_time_str = str(end_time)
                if len(end_time_str) >= 4:
                    if ':' in end_time_str:
                        filename_end = end_time_str.replace(':', '')[:4]  # HH:MM -> HHMM
                    else:
                        filename_end = end_time_str[:4]  # HHMM
            
            if channel:
                # チャンネル名を英語化（簡易版）
                channel_mapping = {
                    'NHK総合': 'NHK',
                    'NHK Eテレ': 'NHK-ETV',
                    'フジテレビ': 'FUJI-TV',
                    '日本テレビ': 'NTV',
                    'TBS': 'TBS',
                    'テレビ朝日': 'TV-ASAHI',
                    'テレビ東京': 'TV-TOKYO'
                }
                filename_channel = channel_mapping.get(channel, channel.replace(' ', '-').replace('　', '-'))
            
            # ファイル名を生成
            if filename_date and filename_start and filename_end and filename_channel:
                txt_filename = f"{filename_date}_{filename_start}_{filename_end}_{filename_channel}_fulltext.txt"
            else:
                txt_filename = f"full_text_{doc_id}.txt"
            
            st.download_button(
                label="📥 全文テキストをダウンロード（TXT形式）",
                data=cleaned_text,
                file_name=txt_filename,
                mime="text/plain"
            )
        else:
            st.info("全文テキストがありません")
    
    with tab5:
        st.subheader("チャンク")
        if chunks:
            # チャンク検索
            chunk_keyword = st.text_input(
                "チャンク内検索",
                key=f"chunk_search_{doc_id}",
                placeholder="キーワードを入力してください"
            )
            
            filtered_chunks = chunks
            if chunk_keyword:
                keyword_lower = chunk_keyword.lower()
                filtered_chunks = [chunk for chunk in chunks if keyword_lower in chunk.get('text', '').lower()]
            
            st.info(f"チャンク数: {len(chunks)} (表示: {len(filtered_chunks)})")
            
            # 画像から遷移した場合、該当するチャンクを直接探す（検索を経ずに）
            target_chunk_idx = None
            if target_chunk_filename:
                # 画像ファイル名から対応するチャンクを探す
                # 例: NHKG-TKY-20251003-050042-1759435242150-7.jpeg → NHKG-TKY-20251003-050042-1759435242150-7.txt
                txt_filename = target_chunk_filename.replace('.jpeg', '.txt').replace('.jpg', '.txt')
                
                # ファイル名が一致するチャンクを直接探す（検索を経ずに）
                import os as os_module
                for idx, chunk in enumerate(chunks):
                    chunk_metadata = chunk.get('metadata', {})
                    original_file_path = chunk_metadata.get('original_file_path', '')
                    if original_file_path:
                        # ファイル名を抽出して比較
                        path_filename = os_module.path.basename(original_file_path)
                        if txt_filename == path_filename or txt_filename in original_file_path:
                            # 該当チャンクが見つかった場合、filtered_chunksでのインデックスを取得
                            # まず、filtered_chunksに含まれているか確認
                            found_in_filtered = False
                            for filtered_idx, filtered_chunk in enumerate(filtered_chunks):
                                if filtered_chunk == chunk:
                                    target_chunk_idx = filtered_idx
                                    found_in_filtered = True
                                    break
                            
                            # filtered_chunksに含まれていない場合は、先頭に追加
                            if not found_in_filtered:
                                filtered_chunks.insert(0, chunk)
                                target_chunk_idx = 0
                            
                            st.success(f"✅ 画像に対応するチャンクに移動しました")
                            break
                
                # フラグはクリアしない（チャンクが表示されるまで保持）
            
            # チャンクを表示した後にフラグをクリア
            chunk_displayed = False
            for idx, chunk in enumerate(filtered_chunks):
                # 画像から遷移した場合は該当チャンクを展開
                expanded = (target_chunk_idx is not None and idx == target_chunk_idx)
                if expanded:
                    chunk_displayed = True
                
                # チャンクの表示名をファイル名から時間に変更
                chunk_metadata = chunk.get('metadata', {})
                original_file_path = chunk_metadata.get('original_file_path', '')
                chunk_display_name = f"チャンク {idx+1}"
                
                if original_file_path:
                    # ファイル名から時間を抽出
                    # os.path.basenameを使用（osは既にインポート済み）
                    import os as os_module
                    filename = os_module.path.basename(original_file_path)
                    timestamp = extract_timestamp_from_filename(filename)
                    if timestamp and timestamp != filename:
                        chunk_display_name = f"📹 {timestamp}"
                    else:
                        # original_file_pathから直接時間を抽出
                        # パターン: .../20251003AM/transcript/NHKG-TKY-20251003-050042-...
                        pattern = r'(\d{8})[A-Z]*/transcript/[^/]+-(\d{6})'
                        match = re.search(pattern, original_file_path)
                        if match:
                            time_str = match.group(2)  # HHMMSS
                            if len(time_str) == 6:
                                hour = time_str[:2]
                                minute = time_str[2:4]
                                second = time_str[4:6]
                                chunk_display_name = f"📹 {hour}:{minute}:{second}"
                
                with st.expander(chunk_display_name, expanded=expanded):
                    # チャンクテキストを取得
                    chunk_text = chunk.get('text', '')
                    
                    # タイムスタンプで改行処理
                    # パターン: [HH:MM:SS.mmm-HH:MM:SS.mmm]
                    # タイムスタンプの前に改行を追加
                    formatted_text = re.sub(r'(\[(\d{2}):(\d{2}):(\d{2})\.(\d{3})-(\d{2}):(\d{2}):(\d{2})\.(\d{3})\])', r'\n\n\1 ', chunk_text)
                    # 先頭の改行を削除
                    formatted_text = formatted_text.lstrip('\n')
                    
                    # フォーマット済みテキストを表示
                    st.markdown(formatted_text)
                    
                    # original_file_pathから画像を取得して表示
                    
                    if original_file_path:
                        # original_file_pathから画像パスを生成
                        # 例: /run/user/1000/gvfs/smb-share:server=nas-tky-2504.local,share=processed/NHKG-TKY/20251003AM/transcript/NHKG-TKY-20251003-050042-1759435242150-7.txt
                        # → NHKG-TKY-20251003-050042-1759435242150-7.jpeg
                        try:
                            # ファイル名を抽出
                            import os
                            filename = os.path.basename(original_file_path)
                            # .txtを.jpegに置換
                            image_filename = filename.replace('.txt', '.jpeg')
                            
                            # S3から画像を取得
                            image_key = f"{S3_IMAGE_PREFIX}{doc_id}/{image_filename}"
                            try:
                                # 署名付きURLを生成（s3_clientを使用）
                                image_url = s3_client.generate_presigned_url(
                                    'get_object',
                                    Params={'Bucket': S3_BUCKET_NAME, 'Key': image_key},
                                    ExpiresIn=3600
                                )
                                # 画像サイズを調整（最大幅を指定）
                                st.image(image_url, caption=f"画面スクショ: {image_filename}", width=400)
                            except Exception as e:
                                # 画像が見つからない場合はスキップ
                                pass
                        except Exception as e:
                            pass
            
            # チャンクが表示された後にフラグをクリア
            if target_chunk_filename and chunk_displayed:
                show_chunk_key = f"show_chunk_for_{doc_id}"
                if show_chunk_key in st.session_state:
                    st.session_state[show_chunk_key] = None
        else:
            st.info("チャンクデータがありません")

# 詳細表示用の時間・日付フォーマット関数
def format_time_display_detail(time_str):
    """時間形式を変換（詳細表示用）"""
    if not time_str or str(time_str).strip() == '':
        return ''
    try:
        time_str = str(time_str)
        # YYYYMMDDHHMM形式の場合
        if len(time_str) >= 12:
            hour = time_str[8:10]
            minute = time_str[10:12]
            return f"{hour}:{minute}"
        # HHMM形式の場合
        elif len(time_str) >= 4:
            hour = time_str[:2]
            minute = time_str[2:4]
            return f"{hour}:{minute}"
        # HH:MM形式の場合
        elif ':' in time_str:
            return time_str
        else:
            return time_str
    except Exception:
        return ''

def format_date_display_detail(date_str):
    """日付形式を変換（詳細表示用）"""
    if not date_str or str(date_str).strip() == '':
        return ''
    try:
        date_str = str(date_str)
        # YYYYMMDD形式の場合
        if len(date_str) >= 8 and date_str.isdigit():
            return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
        else:
            return date_str
    except Exception:
        return ''

# 検索実行
if search_button:
        # 全データから検索（キャッシュを活用）
        with st.spinner("データを読み込み中...（初回のみ時間がかかります）"):
            all_masters = list_all_master_data(_s3_client=s3_client)
        
        if not all_masters:
            st.error("❌ データの取得に失敗しました")
        else:
            # 検索条件の表示
            search_conditions = []
            if date_str:
                search_conditions.append(f"日付: {selected_date.strftime('%Y年%m月%d日') if selected_date else date_str}")
            if time_str:
                search_conditions.append(f"時間: {selected_time.strftime('%H:%M') if selected_time else time_str}")
            if channel and channel != "すべて":
                search_conditions.append(f"放送局: {channel}")
            if program_name_search:
                search_conditions.append(f"番組名: {program_name_search}")
            if performer_search:
                search_conditions.append(f"主演者: {performer_search}")
            if keyword:
                search_conditions.append(f"キーワード: {keyword}")
            
            # 検索条件のチェック（番組名検索も追加）
            if not date_str and not time_str and (not channel or channel == "すべて") and not keyword and not program_name_search and not performer_search:
                st.warning("⚠️ 検索条件を1つ以上入力してください")
            else:
                with st.spinner(f"検索中: {', '.join(search_conditions) if search_conditions else '条件なし'}..."):
                    search_results = search_master_data_with_chunks(
                        _s3_client=s3_client,
                        master_list=all_masters,
                        program_id="",  # 番組IDは削除
                        date_str=date_str if date_str else "",
                        time_str=time_str if time_str else "",
                        channel=channel if channel != "すべて" else "",
                        keyword=keyword,
                        program_name=program_name_search if program_name_search else "",
                        performer=performer_search if performer_search else "",
                        time_tolerance_minutes=30  # 30分以内の近似検索
                    )
            
            # 検索結果をセッションステートに保存
            st.session_state.search_results = search_results
            
            if not search_results:
                # デバッグ情報を表示
                debug_info = []
                if date_str:
                    debug_info.append(f"日付: {date_str}")
                if time_str:
                    debug_info.append(f"時間: {time_str}")
                if channel and channel != "すべて":
                    debug_info.append(f"放送局: {channel}")
                if program_name_search:
                    debug_info.append(f"番組名: {program_name_search}")
                if performer_search:
                    debug_info.append(f"主演者: {performer_search}")
                if keyword:
                    debug_info.append(f"キーワード: {keyword}")
                
                st.warning("⚠️ 検索条件に一致するデータが見つかりませんでした")
                if debug_info:
                    with st.expander("🔍 検索条件の詳細とデバッグ情報"):
                        st.text("\n".join(debug_info))
                        st.info(f"💡 全データ数: {len(all_masters)} 件")
                        
                        # サンプルデータの構造を確認（最初の5件）
                        if all_masters:
                            st.markdown("**サンプルデータ（最初の5件）のメタデータ構造:**")
                            for idx, master in enumerate(all_masters[:5]):
                                metadata = master.get('metadata', {})
                                st.markdown(f"**サンプル {idx+1}:**")
                                st.json({
                                    'doc_id': master.get('doc_id', 'N/A'),
                                    'start_time': metadata.get('start_time', 'N/A'),
                                    'end_time': metadata.get('end_time', 'N/A'),
                                    '開始時間': metadata.get('開始時間', 'N/A'),
                                    '終了時間': metadata.get('終了時間', 'N/A'),
                                    'program_name': metadata.get('program_name', 'N/A'),
                                    'program_title': metadata.get('program_title', 'N/A'),
                                    'master_title': metadata.get('master_title', 'N/A'),
                                    'title': metadata.get('title', 'N/A'),
                                    'channel': metadata.get('channel', 'N/A')
                                })
                                st.markdown("---")
                            
                            # 検索条件に一致する可能性のあるデータを探す
                            debug_time_str = time_str if time_str else None
                            debug_program_name = program_name_search if program_name_search else None
                            
                            if debug_time_str or debug_program_name:
                                debug_title = "**検索条件に一致する可能性のあるデータ:**"
                                if debug_time_str:
                                    debug_title += f" 時間: {debug_time_str}"
                                if debug_program_name:
                                    debug_title += f" 番組名: {debug_program_name}"
                                st.markdown(debug_title)
                                
                                matching_samples = []
                                for master in all_masters[:50]:  # 最初の50件をチェック
                                    metadata = master.get('metadata', {})
                                    
                                    # 時間チェック
                                    time_match = False
                                    start_time = ''
                                    end_time = ''
                                    if debug_time_str:
                                        start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', ''))
                                        end_time = str(metadata.get('end_time', '')) or str(metadata.get('終了時間', ''))
                                        
                                        if start_time or end_time:
                                            try:
                                                # 目標時間を分に変換
                                                target_hour = int(debug_time_str[:2])
                                                target_minute = int(debug_time_str[2:4])
                                                target_minutes = target_hour * 60 + target_minute
                                                
                                                # 開始時間をチェック
                                                if start_time and start_time != 'None' and start_time.strip():
                                                    if ':' in start_time:
                                                        parts = start_time.split(':')
                                                        if len(parts) >= 2:
                                                            start_minutes = int(parts[0]) * 60 + int(parts[1])
                                                            if abs(target_minutes - start_minutes) <= 30:
                                                                time_match = True
                                                    elif len(start_time) >= 4 and start_time.isdigit():
                                                        start_minutes = int(start_time[:2]) * 60 + int(start_time[2:4])
                                                        if abs(target_minutes - start_minutes) <= 30:
                                                            time_match = True
                                                
                                                # 終了時間をチェック
                                                if not time_match and end_time and end_time != 'None' and end_time.strip():
                                                    if ':' in end_time:
                                                        parts = end_time.split(':')
                                                        if len(parts) >= 2:
                                                            end_minutes = int(parts[0]) * 60 + int(parts[1])
                                                            if abs(target_minutes - end_minutes) <= 30:
                                                                time_match = True
                                                    elif len(end_time) >= 4 and end_time.isdigit():
                                                        end_minutes = int(end_time[:2]) * 60 + int(end_time[2:4])
                                                        if abs(target_minutes - end_minutes) <= 30:
                                                            time_match = True
                                            except:
                                                pass
                                
                                    # 番組名チェック
                                    program_match = False
                                    if debug_program_name:
                                        program_name_lower = debug_program_name.strip().lower()
                                        program_fields = [
                                            metadata.get('program_name', ''),
                                            metadata.get('program_title', ''),
                                            metadata.get('master_title', ''),
                                            metadata.get('title', '')
                                        ]
                                        for field_value in program_fields:
                                            if field_value and program_name_lower in str(field_value).lower():
                                                program_match = True
                                                break
                                    
                                    # 時間または番組名のいずれかに一致する場合
                                    if (debug_time_str and time_match) or (debug_program_name and program_match):
                                        matching_samples.append({
                                            'doc_id': master.get('doc_id', 'N/A'),
                                            'start_time': start_time if debug_time_str else 'N/A',
                                            'end_time': end_time if debug_time_str else 'N/A',
                                            'program_name': metadata.get('program_name', 'N/A'),
                                            'program_title': metadata.get('program_title', 'N/A'),
                                            'time_match': time_match if debug_time_str else False,
                                            'program_match': program_match if debug_program_name else False
                                        })
                                
                                if matching_samples:
                                    st.info(f"最初の50件の中に、検索条件に一致する可能性のあるデータが {len(matching_samples)} 件見つかりました（最大5件を表示）:")
                                    for sample in matching_samples[:5]:
                                        st.json(sample)
                                else:
                                    st.info("最初の50件の中に、検索条件に一致する可能性のあるデータは見つかりませんでした。")
            else:
                st.success(f"✅ {len(search_results)} 件のデータが見つかりました")
                st.markdown("---")

# 検索結果のリスト表示（詳細表示前に）
if st.session_state.search_results:
    st.subheader("📋 検索結果")
    
    # 詳細表示モード（独立した画面として表示）
    if st.session_state.selected_doc_id:
        # 戻るボタンとタイトル
        col_back, col_title = st.columns([1, 9])
        with col_back:
            if st.button("← 戻る", use_container_width=True):
                st.session_state.selected_doc_id = None
                st.rerun()
        with col_title:
            st.markdown("### 📄 詳細情報")
        st.markdown("---")
        doc_id = st.session_state.selected_doc_id
        
        # チャンクタブに切り替えるフラグをチェック
        show_chunk_key = f"show_chunk_for_{doc_id}"
        target_chunk_filename = None
        if show_chunk_key in st.session_state and st.session_state[show_chunk_key]:
            target_chunk_filename = st.session_state[show_chunk_key]
            # フラグは保持（チャンクが表示された後にクリア）
        
        with st.spinner("データを取得中..."):
            full_master_data = get_master_data(_s3_client=s3_client, doc_id=doc_id)
            chunks = get_chunk_data(_s3_client=s3_client, doc_id=doc_id)
            images = list_images(_s3_client=s3_client, doc_id=doc_id)
        
        display_master_data(full_master_data, chunks, images, doc_id, target_chunk_filename)
    
    # リスト表示モード
    else:
        # 時間形式を変換する関数
        def format_time_display(time_str):
            """時間形式を変換（YYYYMMDDHHMM -> HH:MM）"""
            if not time_str or time_str == 'N/A' or str(time_str).strip() == '':
                return ''
            try:
                time_str = str(time_str)
                # YYYYMMDDHHMM形式の場合
                if len(time_str) >= 12:
                    hour = time_str[8:10]
                    minute = time_str[10:12]
                    return f"{hour}:{minute}"
                # HHMM形式の場合
                elif len(time_str) >= 4:
                    hour = time_str[:2]
                    minute = time_str[2:4]
                    return f"{hour}:{minute}"
                # HH:MM形式の場合
                elif ':' in time_str:
                    return time_str
                else:
                    return time_str
            except Exception:
                return ''
        
        # 日付形式を変換する関数
        def format_date_display(date_str):
            """日付形式を変換（YYYYMMDD -> YYYY/MM/DD）"""
            if not date_str or date_str == 'N/A' or str(date_str).strip() == '':
                return ''
            try:
                date_str = str(date_str)
                # YYYYMMDD形式の場合
                if len(date_str) >= 8 and date_str.isdigit():
                    return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
                else:
                    return date_str
            except Exception:
                return ''
        
        # 結果をテーブル形式で表示
        results_data = []
        for idx, master in enumerate(st.session_state.search_results):
            doc_id = master.get('doc_id', '')
            metadata = master.get('metadata', {})
            
            # 放送日時・時間
            date_str = metadata.get('date', '') or metadata.get('broadcast_date', '')
            start_time = metadata.get('start_time', '')
            end_time = metadata.get('end_time', '')
            
            # 時間形式を変換
            start_time_display = format_time_display(str(start_time)) if start_time else ''
            end_time_display = format_time_display(str(end_time)) if end_time else ''
            
            # 時間範囲の表示
            if start_time_display and end_time_display:
                time_range = f"{start_time_display} - {end_time_display}"
            elif start_time_display:
                time_range = start_time_display
            elif end_time_display:
                time_range = end_time_display
            else:
                time_range = ''
            
            # 日付形式を変換（yyyy-mm-dd形式）
            if date_str:
                date_str = str(date_str)
                # YYYYMMDD形式の場合
                if len(date_str) >= 8 and date_str.isdigit():
                    date_display = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                # YYYY-MM-DD形式の場合
                elif '-' in date_str:
                    date_display = date_str
                else:
                    date_display = date_str
            else:
                date_display = ''
            
            # 放送局
            channel = str(metadata.get('channel', '')) if metadata.get('channel') else ''
            
            # 番組名（program_name, program_title, master_titleの順で取得）
            program_name = (metadata.get('program_name') or 
                          metadata.get('program_title') or 
                          metadata.get('master_title') or 
                          metadata.get('title') or '')
            program_name = str(program_name) if program_name else ''
            if len(program_name) > 30:
                program_name = program_name[:30] + "..."
            
            results_data.append({
                'No.': idx + 1,
                '放送日時': date_display,
                '時間': time_range,
                '放送局': channel,
                '番組名': program_name,
                'doc_id': doc_id
            })
        
        # テーブル表示（クリック可能にするためにカスタム表示）
        for idx, row in enumerate(results_data):
            with st.container():
                # カード形式で表示
                col1, col2, col3, col4, col5, col6 = st.columns([0.3, 1.2, 1.5, 1.5, 2, 0.8])
                
                with col1:
                    st.write(f"**{row['No.']}**")
                
                with col2:
                    st.write(f"📅 {row['放送日時']}")
                
                with col3:
                    st.write(f"🕐 {row['時間']}")
                
                with col4:
                    st.write(f"📺 {row['放送局']}")
                
                with col5:
                    st.write(f"📺 {row['番組名']}")
                
                with col6:
                    # 詳細ボタン（新しいタブで開くリンク風）
                    if st.button(f"詳細", key=f"detail_{row['doc_id']}", use_container_width=True):
                        st.session_state.selected_doc_id = row['doc_id']
                        st.rerun()
                
                st.markdown("---")

else:
    # 初期状態の説明（データ範囲のみ表示）
    st.markdown("""
    ## ⚠️ データ範囲について
    
    **現在格納されているデータ期間**: 2025年10月3日 ～ 2025年10月26日
    
    この期間外の日付で検索した場合、検索結果が表示されない可能性があります。
    """)
