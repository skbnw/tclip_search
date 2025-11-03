"""
S3データ検索・表示Webアプリ

Streamlitを使用して、S3バケット内のデータを検索・表示します。
- 番組ID（doc_id）で検索
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
from datetime import date, time

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
    page_title="S3データ検索アプリ",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# タイトル
st.title("🔍 S3データ検索・表示アプリ")
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

@st.cache_resource
def get_s3_client():
    """S3クライアントを取得（環境変数から認証情報を自動的に読み込む）"""
    try:
        # boto3は環境変数から自動的に認証情報を読み込む
        s3_client = boto3.client('s3', region_name=S3_REGION)
        return s3_client
    except Exception as e:
        st.error(f"S3クライアントの作成に失敗しました: {str(e)}")
        return None

# AWS認証情報の取得
access_key, secret_key, region = get_aws_credentials()

# 認証情報を環境変数に設定（boto3が自動的に読み込むように）
if access_key and secret_key:
    os.environ['AWS_ACCESS_KEY_ID'] = access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
    os.environ['AWS_DEFAULT_REGION'] = region or S3_REGION

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
    st.info("💡 ヒント: 番組ID（doc_id）で検索できます\n\n例: AkxAQAJ3gAM")

# S3クライアントの取得（環境変数から自動的に読み込まれる）
s3_client = get_s3_client()

if s3_client is None:
    st.error("S3クライアントの初期化に失敗しました。AWS認証情報を確認してください。")
    st.stop()

# メインコンテンツ
st.header("📋 データ検索")

# 検索オプションの取得（初回のみ読み込み）
@st.cache_data(ttl=600)
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
        return {'dates': [], 'times': [], 'channels': []}

# 検索フォーム
with st.form("search_form"):
    st.subheader("検索条件")
    
    # 複数列レイアウト
    col1, col2 = st.columns(2)
    
    with col1:
        # 番組ID
        program_id = st.text_input(
            "番組ID",
            placeholder="例: AkxAQAJ3gAM",
            help="番組ID（doc_id）を入力"
        )
        
        # 放送局（選択式）
        search_options = get_search_options(_s3_client=s3_client)
        channel = st.selectbox(
            "放送局",
            options=["すべて"] + search_options['channels'],
            help="放送局を選択してください"
        )
    
    with col2:
        # 日付と時間を1つのセクションにまとめる
        st.markdown("#### 📅 日付・時間でフィルタ")
        use_datetime_filter = st.checkbox(
            "日付・時間でフィルタを有効にする", 
            key="use_datetime_filter", 
            help="チェックを入れると日付と時間でフィルタリングします"
        )
        
        if use_datetime_filter:
            # 日付と時間を横並びに配置
            datetime_col1, datetime_col2 = st.columns(2)
            
            with datetime_col1:
                st.markdown("**📆 日付**")
                selected_date = st.date_input(
                    "日付を選択",
                    value=date.today(),
                    help="カレンダーから日付を選択してください",
                    key="date_input",
                    label_visibility="collapsed"
                )
                date_str = selected_date.strftime("%Y%m%d") if selected_date else None
            
            with datetime_col2:
                st.markdown("**🕐 時間**")
                default_time = time(0, 0)  # 00:00をデフォルト
                selected_time = st.time_input(
                    "時間を選択",
                    value=default_time,
                    help="時間を選択してください（時:分形式）",
                    key="time_input",
                    label_visibility="collapsed"
                )
                time_str = selected_time.strftime("%H%M") if selected_time else None
            
            # 選択された日付と時間をプレビュー表示
            if selected_date and selected_time:
                preview_date = selected_date.strftime("%Y年%m月%d日")
                preview_time = selected_time.strftime("%H:%M")
                st.info(f"📌 検索条件: {preview_date} {preview_time}")
        else:
            date_str = None
            time_str = None
            selected_date = None
            selected_time = None
    
    # キーワード検索（全文とチャンクテキストを対象）
    keyword = st.text_input(
        "キーワード（全文・チャンクテキスト検索）",
        placeholder="キーワードを入力してください",
        help="全文テキストとチャンクテキストから検索します"
    )
    
    # 検索ボタン
    search_button = st.form_submit_button("🔍 検索", use_container_width=True)

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
    """画像ファイル一覧を取得"""
    try:
        prefix = f"{S3_IMAGE_PREFIX}{doc_id}/"
        response = _s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        
        images = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.jpeg') or obj['Key'].endswith('.jpg'):
                    images.append(obj['Key'])
        return sorted(images)
    except Exception as e:
        st.error(f"画像一覧の取得エラー: {str(e)}")
        return []

@st.cache_data(ttl=600)  # 10分間キャッシュ（全データリストは重いため）
def list_all_master_data(_s3_client) -> List[Dict]:
    """全マスターデータのリストを取得（検索用）"""
    try:
        response = _s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_MASTER_PREFIX)
        
        master_list = []
        if 'Contents' in response:
            for obj in response['Contents']:
                try:
                    # オブジェクトを取得
                    file_response = _s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
                    content = file_response['Body'].read().decode('utf-8')
                    lines = content.strip().split('\n')
                    if lines:
                        master_data = json.loads(lines[0])
                        master_list.append(master_data)
                except Exception as e:
                    continue  # エラーが発生したファイルはスキップ
        
        return master_list
    except Exception as e:
        st.error(f"全マスターデータの取得エラー: {str(e)}")
        return []

def search_master_data_advanced(
    master_list: List[Dict], 
    program_id: str = "",
    date_str: str = "",
    time_str: str = "",
    channel: str = "",
    keyword: str = ""
) -> List[Dict]:
    """マスターデータを詳細条件で検索"""
    results = []
    
    for master in master_list:
        metadata = master.get('metadata', {})
        doc_id = master.get('doc_id', '')
        
        # 各条件でフィルタリング
        match = True
        
        # 番組IDでフィルタ
        if program_id and program_id.strip():
            if program_id.strip().lower() not in doc_id.lower():
                match = False
                continue
        
        # 日付でフィルタ
        if date_str:
            master_date = str(metadata.get('date', ''))
            # 日付形式を変換して比較（YYYYMMDD形式）
            if date_str not in master_date:
                match = False
                continue
        
        # 時間でフィルタ
        if time_str:
            start_time = str(metadata.get('start_time', ''))
            end_time = str(metadata.get('end_time', ''))
            # 時間形式を変換して比較（HHMM形式）
            # 開始時間または終了時間に一致するか確認
            if time_str not in start_time and time_str not in end_time:
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
            keyword_match = False
            
            # 全文テキストで検索
            full_text = master.get('full_text', '').lower()
            if keyword_lower in full_text:
                keyword_match = True
            
            # チャンクテキストで検索（チャンクデータを取得する必要がある）
            # ただし、全チャンクを取得するのは重いので、ここではマスターデータのみチェック
            # チャンク検索は別途実装する
            
            if not keyword_match:
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
    keyword: str = ""
) -> List[Dict]:
    """マスターデータとチャンクテキストを含む詳細検索（最適化版）"""
    # まず基本条件でフィルタ
    filtered_masters = search_master_data_advanced(
        master_list, program_id, date_str, time_str, channel, ""
    )
    
    # キーワードが指定されている場合、全文テキストでフィルタリング
    if keyword and keyword.strip():
        keyword_lower = keyword.strip().lower()
        results = []
        
        # まず全文テキストでフィルタリング（高速）
        full_text_matches = []
        chunk_candidates = []
        
        for master in filtered_masters:
            full_text = master.get('full_text', '').lower()
            if keyword_lower in full_text:
                full_text_matches.append(master)
            else:
                # 全文テキストにマッチしない場合のみ、チャンク検索の候補に
                chunk_candidates.append(master)
        
        results.extend(full_text_matches)
        
        # チャンクテキストで検索（全文テキストにマッチしなかったもののみ）
        # 大量のデータの場合は、全文テキストでのマッチを優先し、チャンク検索を最小化
        if chunk_candidates:
            for master in chunk_candidates:
                doc_id = master.get('doc_id', '')
                keyword_match = False
                
                try:
                    # チャンクデータを取得
                    chunks = get_chunk_data(_s3_client=_s3_client, doc_id=doc_id)
                    
                    # チャンクテキストで検索（最初にマッチしたら即座に終了）
                    for chunk in chunks:
                        chunk_text = chunk.get('text', '').lower()
                        if keyword_lower in chunk_text:
                            keyword_match = True
                            break
                except:
                    pass  # チャンク取得エラーは無視
                
                if keyword_match:
                    results.append(master)
        
        return results
    
    return filtered_masters

def get_image_url(s3_client, key: str, expires_in: int = 3600) -> str:
    """画像のプリサインドURLを生成"""
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
            ExpiresIn=expires_in
        )
        return url
    except Exception as e:
        st.error(f"画像URLの生成エラー: {str(e)}")
        return ""

def display_master_data(master_data, chunks, images, doc_id):
    """マスターデータを表示"""
    if master_data is None and not chunks and not images:
        st.info("データが見つかりませんでした")
        return
    
    # タブで表示
    tab1, tab2, tab3 = st.tabs(["📄 マスターデータ", "📝 チャンクデータ", "🖼️ 画像"])
    
    # タブ1: マスターデータ
    with tab1:
        if master_data:
            st.subheader("メタデータ")
            
            # メタデータの表示
            if 'metadata' in master_data:
                metadata = master_data['metadata']
                col1, col2 = st.columns(2)
                
                with col1:
                    st.json(metadata)
                
                with col2:
                    st.write("### 主要情報")
                    if 'event_id' in metadata:
                        st.write(f"**Event ID**: `{metadata['event_id']}`")
                    if 'channel' in metadata:
                        st.write(f"**Channel**: {metadata['channel']}")
                    if 'date' in metadata:
                        st.write(f"**Date**: {metadata['date']}")
            
            st.markdown("---")
            st.subheader("フルテキスト")
            
            if 'full_text' in master_data:
                full_text = master_data['full_text']
                st.text_area(
                    "全文",
                    full_text,
                    height=300,
                    disabled=True,
                    help="番組全体のテキスト"
                )
                st.caption(f"文字数: {len(full_text):,} 文字")
            else:
                st.info("フルテキストがありません")
            
            # 画像URLがある場合
            if 'image_urls' in master_data and master_data['image_urls']:
                st.markdown("---")
                st.subheader("関連画像")
                st.write(f"画像数: {len(master_data['image_urls'])} 枚")
        else:
            st.info("マスターデータが見つかりませんでした")
    
    # タブ2: チャンクデータ
    with tab2:
        if chunks:
            st.subheader(f"チャンクデータ ({len(chunks)} 個)")
            
            # チャンク検索（ユニークなキーを付与）
            chunk_search = st.text_input(
                "チャンク内を検索", 
                placeholder="キーワードを入力",
                key=f"chunk_search_{doc_id}"
            )
            
            # フィルタリング
            filtered_chunks = chunks
            if chunk_search:
                filtered_chunks = [
                    chunk for chunk in chunks
                    if chunk_search.lower() in chunk.get('text', '').lower()
                ]
                st.caption(f"検索結果: {len(filtered_chunks)} / {len(chunks)} 個のチャンク")
            
            # チャンク表示
            for i, chunk in enumerate(filtered_chunks):
                with st.expander(f"チャンク {i+1}: {chunk.get('chunk_id', 'N/A')}"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write("**テキスト**:")
                        st.write(chunk.get('text', ''))
                    
                    with col2:
                        st.write("**メタデータ**:")
                        if 'metadata' in chunk:
                            metadata = chunk['metadata']
                            if 'start_time' in metadata:
                                st.write(f"開始時刻: {metadata['start_time']}")
                            if 'end_time' in metadata:
                                st.write(f"終了時刻: {metadata['end_time']}")
                            if 'original_file_path' in metadata:
                                st.caption(f"パス: {metadata['original_file_path']}")
                    
                    st.caption(f"文字数: {len(chunk.get('text', ''))} 文字")
        else:
            st.info("チャンクデータが見つかりませんでした")
    
    # タブ3: 画像
    with tab3:
        if images:
            st.subheader(f"画像 ({len(images)} 枚)")
            
            # 画像をグリッド表示
            cols = st.columns(3)
            for idx, image_key in enumerate(images):
                col = cols[idx % 3]
                
                with col:
                    try:
                        image_url = get_image_url(s3_client, image_key)
                        if image_url:
                            st.image(image_url, use_container_width=True)
                            filename = image_key.split('/')[-1]
                            st.caption(filename)
                        else:
                            st.error("画像の読み込みに失敗しました")
                    except Exception as e:
                        st.error(f"エラー: {str(e)}")
        else:
            st.info("画像が見つかりませんでした")
    
    # 統計情報
    st.markdown("---")
    st.subheader("📊 統計情報")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("マスターデータ", "あり" if master_data else "なし")
    with col2:
        st.metric("チャンク数", len(chunks))
    with col3:
        st.metric("画像数", len(images))
    with col4:
        if master_data and 'full_text' in master_data:
            st.metric("文字数", f"{len(master_data['full_text']):,}")

# 検索実行
if search_button:
    # 番組IDのみが指定されている場合は直接取得
    if program_id and program_id.strip() and not date_str and not time_str and not channel and not keyword:
        with st.spinner("データを取得中..."):
            doc_id = program_id.strip()
            master_data = get_master_data(_s3_client=s3_client, doc_id=doc_id)
            chunks = get_chunk_data(_s3_client=s3_client, doc_id=doc_id)
            images = list_images(_s3_client=s3_client, doc_id=doc_id)
        
        if master_data is None and not chunks and not images:
            st.error(f"❌ 番組ID '{doc_id}' のデータが見つかりませんでした")
            st.info("💡 正しい番組IDを入力してください")
        else:
            # データ表示
            st.success(f"✅ 番組ID '{doc_id}' のデータを取得しました")
            st.markdown("---")
            display_master_data(master_data, chunks, images, doc_id)
    else:
        # 複数条件またはキーワード検索
        if not program_id and not date_str and not time_str and not channel and not keyword:
            st.warning("⚠️ 検索条件を1つ以上入力してください")
        else:
            # 全データから検索
            with st.spinner("全データを読み込み中...（初回は時間がかかります）"):
                all_masters = list_all_master_data(_s3_client=s3_client)
            
            if not all_masters:
                st.error("❌ データの取得に失敗しました")
            else:
                # 検索条件の表示
                search_conditions = []
                if program_id:
                    search_conditions.append(f"番組ID: {program_id}")
                if date_str:
                    search_conditions.append(f"日付: {date_str}")
                if time_str:
                    search_conditions.append(f"時間: {time_str}")
                if channel and channel != "すべて":
                    search_conditions.append(f"放送局: {channel}")
                if keyword:
                    search_conditions.append(f"キーワード: {keyword}")
                
                with st.spinner(f"検索中: {', '.join(search_conditions) if search_conditions else '条件なし'}..."):
                    search_results = search_master_data_with_chunks(
                        _s3_client=s3_client,
                        master_list=all_masters,
                        program_id=program_id,
                        date_str=date_str if date_str else "",
                        time_str=time_str if time_str else "",
                        channel=channel if channel != "すべて" else "",
                        keyword=keyword
                    )
                
                if not search_results:
                    st.warning("⚠️ 検索条件に一致するデータが見つかりませんでした")
                else:
                    st.success(f"✅ {len(search_results)} 件のデータが見つかりました")
                    st.markdown("---")
                    
                    # 検索結果の表示
                    for idx, master in enumerate(search_results):
                        doc_id = master.get('doc_id', 'N/A')
                        metadata = master.get('metadata', {})
                        
                        # 結果のヘッダー情報
                        result_header = f"結果 {idx+1}: {doc_id}"
                        if 'channel' in metadata:
                            result_header += f" ({metadata['channel']})"
                        if 'date' in metadata:
                            result_header += f" - {metadata['date']}"
                        
                        with st.expander(result_header, expanded=(idx == 0)):
                            # このデータの詳細を表示
                            with st.spinner("データを取得中..."):
                                full_master_data = get_master_data(_s3_client=s3_client, doc_id=doc_id)
                                chunks = get_chunk_data(_s3_client=s3_client, doc_id=doc_id)
                                images = list_images(_s3_client=s3_client, doc_id=doc_id)
                            
                            display_master_data(full_master_data, chunks, images, doc_id)

else:
    # 初期状態の説明
    st.info("👈 サイドバーでAWS認証情報を設定し、検索条件を入力して検索してください")
    
    st.markdown("---")
    st.subheader("📖 使い方")
    st.markdown("""
    1. **AWS認証情報の設定**
       - サイドバーで「環境変数を使用」にチェック（環境変数を設定した場合）
       - または、Access Key IDとSecret Access Keyを直接入力
    
    2. **検索条件の入力**
       - **番組ID**: 番組ID（doc_id）を直接入力
       - **日付**: ドロップダウンから日付を選択（「すべて」を選択すると全件）
       - **時間**: ドロップダウンから時間を選択（「すべて」を選択すると全件）
       - **放送局**: ドロップダウンから放送局を選択（「すべて」を選択すると全件）
       - **キーワード**: 全文テキストとチャンクテキストから検索
    
    3. **検索の実行**
       - 「🔍 検索」ボタンをクリック
       - 複数の条件を組み合わせて検索可能
    
    4. **データの表示**
       - **マスターデータタブ**: 番組のメタデータと全文テキスト
       - **チャンクデータタブ**: セグメント単位のチャンク（チャンク内検索機能あり）
       - **画像タブ**: screenshots配列に含まれる画像
    """)

# フッター
st.markdown("---")
st.caption(f"バケット: {S3_BUCKET_NAME} | リージョン: {S3_REGION}")

