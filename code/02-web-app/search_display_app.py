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
    page_title="Tclipデータ検索beta",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"  # サイドバーをデフォルトで折りたたむ
)

# タイトル
st.title("🔍 Tclipデータ検索beta")
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

# データ取得関数（検索オプション取得で使用するため先に定義）
@st.cache_data(ttl=3600)  # 1時間キャッシュ（全データリストは重いため）
def list_all_master_data(_s3_client) -> List[Dict]:
    """全マスターデータのリストを取得（検索用）"""
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
                        # 全文テキストは検索時にのみ使用するため、ここでは除外してメモリ節約
                        # ただし、メタデータは保持
                        if 'full_text' in master_data:
                            # 全文テキストは保持（キーワード検索で必要）
                            pass
                        master_list.append(master_data)
                except Exception as e:
                    continue  # エラーが発生したファイルはスキップ
            
            progress_bar.empty()
            status_text.empty()
        
        return master_list
    except Exception as e:
        st.error(f"全マスターデータの取得エラー: {str(e)}")
        return []

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
    
    # 下部: キーワード
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
def list_images(_s3_client, doc_id: str) -> List[str]:
    """画像URLのリストを取得"""
    try:
        prefix = f"{S3_IMAGE_PREFIX}{doc_id}/"
        response = _s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        
        image_urls = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith(('.jpeg', '.jpg', '.png')):
                    # 署名付きURLを生成（1時間有効）
                    url = _s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
                        ExpiresIn=3600
                    )
                    image_urls.append(url)
        return image_urls
    except Exception as e:
        st.error(f"画像一覧の取得エラー: {str(e)}")
        return []


def search_master_data_advanced(
    master_list: List[Dict], 
    program_id: str = "",
    date_str: str = "",
    time_str: str = "",
    channel: str = "",
    keyword: str = "",
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
            start_time = str(metadata.get('start_time', ''))
            end_time = str(metadata.get('end_time', ''))
            
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
            
            # 開始時間をチェック
            if start_time:
                try:
                    if ':' in start_time:
                        parts = start_time.split(':')
                        start_minutes = int(parts[0]) * 60 + int(parts[1])
                    else:
                        if len(start_time) >= 4:
                            start_minutes = int(start_time[:2]) * 60 + int(start_time[2:4])
                        else:
                            start_minutes = None
                    
                    if start_minutes is not None:
                        diff = abs(target_minutes - start_minutes)
                        if diff <= time_tolerance_minutes:
                            time_match = True
                except (ValueError, IndexError):
                    pass
            
            # 終了時間をチェック
            if not time_match and end_time:
                try:
                    if ':' in end_time:
                        parts = end_time.split(':')
                        end_minutes = int(parts[0]) * 60 + int(parts[1])
                    else:
                        if len(end_time) >= 4:
                            end_minutes = int(end_time[:2]) * 60 + int(end_time[2:4])
                        else:
                            end_minutes = None
                    
                    if end_minutes is not None:
                        diff = abs(target_minutes - end_minutes)
                        if diff <= time_tolerance_minutes:
                            time_match = True
                except (ValueError, IndexError):
                    pass
            
            if not time_match:
                match = False
                continue
        
        # 放送局でフィルタ
        if channel and channel != "すべて":
            master_channel = str(metadata.get('channel', ''))
            if channel not in master_channel:
                match = False
                continue
        
        # キーワードでフィルタ（全文とチャンクテキスト）
        if keyword and keyword.strip():
            keyword_lower = keyword.strip().lower()
            full_text = master.get('full_text', '').lower()
            if keyword_lower not in full_text:
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
    time_tolerance_minutes: int = 30
) -> List[Dict]:
    """マスターデータとチャンクテキストを含む詳細検索（最適化版）"""
    # まず基本条件でフィルタ（メタデータのみで高速）
    filtered_masters = search_master_data_advanced(
        master_list, program_id, date_str, time_str, channel, "", time_tolerance_minutes
    )
    
    # キーワードが指定されている場合、全文テキストでフィルタリング
    if keyword and keyword.strip():
        keyword_lower = keyword.strip().lower()
        results = []
        
        # 進捗表示用
        total = len(filtered_masters)
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # まず全文テキストでフィルタリング（高速）
        chunk_search_candidates = []
        for idx, master in enumerate(filtered_masters):
            # 進捗表示（10件ごと）
            if idx % 10 == 0 or idx == total - 1:
                progress = (idx + 1) / total
                progress_bar.progress(progress)
                status_text.text(f"キーワード検索中: {idx + 1}/{total} 件")
            
            full_text = master.get('full_text', '').lower()
            if keyword_lower in full_text:
                results.append(master)
            else:
                # 全文テキストにマッチしない場合のみ、チャンク検索の候補に
                chunk_search_candidates.append(master)
        
        # チャンク検索（全文テキストにマッチしなかったもののみ）
        if chunk_search_candidates:
            status_text.text(f"チャンクデータ検索中: {len(chunk_search_candidates)} 件...")
            for idx, master in enumerate(chunk_search_candidates):
                try:
                    doc_id = master.get('doc_id', '')
                    chunks = get_chunk_data(_s3_client, doc_id)
                    for chunk in chunks:
                        chunk_text = chunk.get('text', '').lower()
                        if keyword_lower in chunk_text:
                            results.append(master)
                            break
                except Exception:
                    continue
        
        progress_bar.empty()
        status_text.empty()
        
        return results
    
    return filtered_masters

def display_master_data(master_data, chunks, images, doc_id):
    """マスターデータ、チャンク、画像を表示"""
    if not master_data:
        st.warning("データが見つかりませんでした")
        return
    
    # メタデータの表示
    metadata = master_data.get('metadata', {})
    
    st.subheader("📋 マスターデータ")
    
    # メタ情報をカード形式で表示
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if 'date' in metadata:
            st.metric("放送日", metadata.get('date', 'N/A'))
    with col2:
        if 'start_time' in metadata or 'end_time' in metadata:
            time_range = f"{metadata.get('start_time', 'N/A')} - {metadata.get('end_time', 'N/A')}"
            st.metric("時間", time_range)
    with col3:
        if 'channel' in metadata:
            st.metric("放送局", metadata.get('channel', 'N/A'))
    with col4:
        if 'program_name' in metadata:
            st.metric("番組名", metadata.get('program_name', 'N/A'))
    
    st.markdown("---")
    
    # タブで表示
    tab1, tab2, tab3 = st.tabs(["📄 全文", "📑 チャンク", "🖼️ 画像"])
    
    with tab1:
        st.subheader("全文テキスト")
        if 'full_text' in master_data:
            st.text_area("", value=master_data['full_text'], height=400, key=f"full_text_{doc_id}")
        else:
            st.info("全文テキストがありません")
    
    with tab2:
        st.subheader("チャンクデータ")
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
            
            for idx, chunk in enumerate(filtered_chunks):
                with st.expander(f"チャンク {idx+1}", expanded=False):
                    st.write(chunk.get('text', ''))
                    if 'metadata' in chunk:
                        st.json(chunk['metadata'])
        else:
            st.info("チャンクデータがありません")
    
    with tab3:
        st.subheader("画像")
        if images:
            st.info(f"画像数: {len(images)}")
            # グリッド表示（3列）
            cols = st.columns(3)
            for idx, img_url in enumerate(images):
                with cols[idx % 3]:
                    try:
                        st.image(img_url, caption=f"画像 {idx+1}", use_container_width=True)
                    except Exception as e:
                        st.error(f"画像の読み込みエラー: {str(e)}")
        else:
            st.info("画像がありません")

# 検索実行
if search_button:
    # 検索条件をチェック（キーワードだけでも検索可能）
    if not date_str and not time_str and (not channel or channel == "すべて") and not keyword:
        st.warning("⚠️ 検索条件を1つ以上入力してください")
    else:
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
            if keyword:
                search_conditions.append(f"キーワード: {keyword}")
            
            with st.spinner(f"検索中: {', '.join(search_conditions) if search_conditions else '条件なし'}..."):
                search_results = search_master_data_with_chunks(
                    _s3_client=s3_client,
                    master_list=all_masters,
                    program_id="",  # 番組IDは削除
                    date_str=date_str if date_str else "",
                    time_str=time_str if time_str else "",
                    channel=channel if channel != "すべて" else "",
                    keyword=keyword,
                    time_tolerance_minutes=30  # 30分以内の近似検索
                )
            
            # 検索結果をセッションステートに保存
            st.session_state.search_results = search_results
            
            if not search_results:
                st.warning("⚠️ 検索条件に一致するデータが見つかりませんでした")
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
        with st.spinner("データを取得中..."):
            full_master_data = get_master_data(_s3_client=s3_client, doc_id=doc_id)
            chunks = get_chunk_data(_s3_client=s3_client, doc_id=doc_id)
            images = list_images(_s3_client=s3_client, doc_id=doc_id)
        
        display_master_data(full_master_data, chunks, images, doc_id)
    
    # リスト表示モード
    else:
        # 時間形式を変換する関数
        def format_time_display(time_str):
            """時間形式を変換（YYYYMMDDHHMM -> HH:MM）"""
            if not time_str or time_str == 'N/A':
                return 'N/A'
            try:
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
                return time_str
        
        # 日付形式を変換する関数
        def format_date_display(date_str):
            """日付形式を変換（YYYYMMDD -> YYYY/MM/DD）"""
            if not date_str or date_str == 'N/A':
                return 'N/A'
            try:
                # YYYYMMDD形式の場合
                if len(date_str) >= 8 and date_str.isdigit():
                    return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
                else:
                    return date_str
            except Exception:
                return date_str
        
        # 結果をテーブル形式で表示
        results_data = []
        for idx, master in enumerate(st.session_state.search_results):
            doc_id = master.get('doc_id', 'N/A')
            metadata = master.get('metadata', {})
            
            # 放送日時・時間
            date_str = metadata.get('date', 'N/A')
            start_time = metadata.get('start_time', 'N/A')
            end_time = metadata.get('end_time', 'N/A')
            
            # 時間形式を変換
            start_time_display = format_time_display(str(start_time))
            end_time_display = format_time_display(str(end_time))
            time_range = f"{start_time_display} - {end_time_display}" if start_time_display != 'N/A' and end_time_display != 'N/A' else (start_time_display if start_time_display != 'N/A' else end_time_display)
            
            # 日付形式を変換
            date_display = format_date_display(str(date_str))
            
            # 放送局
            channel = str(metadata.get('channel', 'N/A'))
            
            # 番組名
            program_name = str(metadata.get('program_name', metadata.get('title', 'N/A')))
            if len(program_name) > 20:
                program_name = program_name[:20] + "..."
            
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
    # 初期状態の説明
    st.info("👈 サイドバーでAWS認証情報を設定し、検索条件を入力して検索してください")
    
    st.markdown("---")
    
    st.markdown("""
    ## 📖 使い方
    
    1. **検索条件を入力**
       - 放送局、日付、時間、キーワードから選択
       - すべて任意項目です（1つ以上入力してください）
    
    2. **検索結果を確認**
       - 検索結果がリスト形式で表示されます
       - 「詳細を見る」ボタンをクリックして詳細情報を表示
    
    3. **詳細情報の閲覧**
       - 全文テキスト、チャンクデータ、画像を確認できます
    
    ---
    
    ## ⚠️ データ範囲について
    
    **現在格納されているデータ期間**: 2025年10月3日 ～ 2025年10月26日
    
    この期間外の日付で検索した場合、検索結果が表示されない可能性があります。
    """)
