"""
地上波テレビ番組放送をAI要約するシステムのプロトタイプです。

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
import copy
import numpy as np
from typing import Dict, List, Optional, Tuple
from io import BytesIO
from datetime import date, time, datetime, timedelta
import pytz

# ベクトル検索用のライブラリ（オプション）
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    # 警告は後で表示（st.warningはここでは使用しない）

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
S3_AUDIO_PREFIX = "rag/audio/"  # 音声ファイル用のプレフィックス

# ページ設定
st.set_page_config(
    page_title="テレビ番組データ検索β",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",  # サイドバーをデフォルトで折りたたむ
    menu_items={
        'About': '地上波テレビ番組放送をAI要約するシステムのプロトタイプです。'
    }
)

# ベーシック認証は解除しました

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

# JST（日本標準時）で現在時刻を取得する関数
def get_jst_now() -> datetime:
    """JST（日本標準時）で現在時刻を取得"""
    jst = pytz.timezone('Asia/Tokyo')
    return datetime.now(jst)


# 最新番組データの取得関数
@st.cache_data(ttl=300)  # 5分キャッシュ
def get_latest_programs(_s3_client, limit: int = 5) -> List[Dict]:
    """最新の番組データを取得（放送開始時間の新しい順）"""
    try:
        all_masters = list_all_master_data(_s3_client)
        
        # ソート用のキーを取得
        def get_sort_key(master):
            """ソート用のキーを取得（start_timeから日時を抽出）"""
            metadata = master.get('metadata', {})
            start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', ''))
            
            if start_time and len(start_time) >= 12 and start_time[:12].isdigit():
                # YYYYMMDDHHMM形式（12桁）の場合
                return int(start_time[:12])
            elif start_time and len(start_time) >= 8 and start_time[:8].isdigit():
                # YYYYMMDD形式（8桁）の場合
                return int(start_time[:8]) * 10000  # 時間部分を0として扱う
            else:
                # 日時情報がない場合は最後に表示
                return 0
        
        # 放送開始時間の新しい順（降順）にソート
        sorted_masters = sorted(all_masters, key=get_sort_key, reverse=True)
        
        # 最新のN件を返す
        return sorted_masters[:limit]
    except Exception as e:
        return []

# 管理者チェック関数
def is_admin() -> bool:
    """管理者かどうかをチェック（Streamlit secretsまたは環境変数から取得）"""
    try:
        # Streamlit secretsから取得を試行
        if hasattr(st, 'secrets') and 'ADMIN_MODE' in st.secrets:
            return bool(st.secrets.get('ADMIN_MODE', False))
    except (AttributeError, KeyError):
        pass
    
    # 環境変数から取得を試行
    admin_mode = os.getenv('ADMIN_MODE', '').lower()
    return admin_mode in ('true', '1', 'yes', 'on')

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

# ジャンルの固定順序リスト
GENRE_ORDER = [
    "すべて",
    "ニュース／報道",
    "情報／ワイドショー",
    "ドキュメンタリー／教養",
    "ドラマ",
    "スポーツ",
    "バラエティ",
    "劇場／公演",
    "映画",
    "福祉",
    "趣味／教育",
    "アニメ／特撮",
    "音楽",
    "その他"
]

# 検索オプションの取得（初回のみ読み込み）
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_search_options(_s3_client) -> Dict[str, List[str]]:
    """検索用のオプション（日付、時間、放送局、ジャンル）を取得"""
    try:
        all_masters = list_all_master_data(_s3_client)
        
        dates = set()
        times = set()
        channels = set()
        genres = set()
        
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
            
            # ジャンル
            genre_fields = ['genre', 'ジャンル', 'program_genre', 'category', 'カテゴリ']
            for field in genre_fields:
                if field in metadata:
                    genre_value = str(metadata[field])
                    if genre_value and genre_value.strip() and genre_value != 'None':
                        genres.add(genre_value.strip())
        
        # ジャンルを固定順序でソート（固定順序に含まれるものは順序通り、含まれないものは「その他」の前に追加）
        genres_list = list(genres)
        ordered_genres = []
        # 固定順序に含まれるジャンルを順番に追加（データベースに存在するかどうかに関わらず）
        for genre in GENRE_ORDER[1:]:  # "すべて"を除く
            if genre == "その他":
                # 「その他」の前に、固定順序に含まれないジャンルを追加
                for other_genre in sorted(genres_list):
                    if other_genre not in ordered_genres:
                        ordered_genres.append(other_genre)
            # データベースに存在する場合のみ追加
            if genre in genres_list:
                ordered_genres.append(genre)
        
        return {
            'dates': sorted(list(dates)),
            'times': sorted(list(times)),
            'channels': sorted(list(channels)),
            'genres': ordered_genres
        }
    except Exception as e:
        st.error(f"検索オプションの取得エラー: {str(e)}")
        return {'dates': [], 'times': [], 'channels': [], 'genres': []}

# 出演者名リストの取得（初回のみ読み込み）
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_performer_names(_s3_client) -> List[str]:
    """データベースから出演者名のリストを取得"""
    try:
        all_masters = list_all_master_data(_s3_client)
        
        performer_names = set()
        
        for master in all_masters:
            metadata = master.get('metadata', {})
            
            # talentsフィールドから出演者名を抽出
            talents = metadata.get('talents', [])
            if talents:
                for talent in talents:
                    if isinstance(talent, dict):
                        talent_name = talent.get('name', '') or talent.get('talent_name', '')
                    else:
                        talent_name = str(talent)
                    if talent_name and talent_name.strip():
                        performer_names.add(talent_name.strip())
            
            # その他の出演者名フィールドもチェック
            talent_fields = [
                'talent_names', 'performers', 'performer_names',
                'cast', 'cast_names', '出演者', '出演者名'
            ]
            for field in talent_fields:
                field_value = metadata.get(field, '')
                if field_value:
                    if isinstance(field_value, str):
                        # カンマ区切りの文字列の場合
                        names = [n.strip() for n in field_value.split(',') if n.strip()]
                        for name in names:
                            performer_names.add(name)
                    elif isinstance(field_value, list):
                        # リストの場合
                        for name in field_value:
                            if isinstance(name, dict):
                                name_str = name.get('name', '') or name.get('talent_name', '')
                            else:
                                name_str = str(name)
                            if name_str and name_str.strip():
                                performer_names.add(name_str.strip())
        
        return sorted(list(performer_names))
    except Exception as e:
        st.error(f"出演者名リストの取得エラー: {str(e)}")
        return []

# 番組名リストの取得（初回のみ読み込み、ジャンルとテレビ局でフィルタリング可能）
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_program_names(_s3_client, genre_filter: str = None, channel_filters: List[str] = None) -> List[str]:
    """データベースから番組名のリストを取得（ジャンルとテレビ局でフィルタリング可能、日付の新しい順にソート）"""
    try:
        all_masters = list_all_master_data(_s3_client)
        
        # 番組名と日付情報をペアで保存
        program_name_with_date = []  # [(program_name, sort_key), ...]
        
        for master in all_masters:
            metadata = master.get('metadata', {})
            
            # ジャンルでフィルタリング（指定されている場合）
            if genre_filter and genre_filter != "すべて":
                genre_match = False
                genre_lower = genre_filter.strip().lower()
                genre_fields = ['genre', 'ジャンル', 'program_genre', 'category', 'カテゴリ']
                
                for field in genre_fields:
                    genre_value = metadata.get(field, '')
                    if genre_value:
                        genre_value_str = str(genre_value).strip().lower()
                        # 完全一致を優先
                        if genre_lower == genre_value_str:
                            genre_match = True
                            break
                        # 部分一致（大文字小文字を区別しない）
                        elif genre_lower in genre_value_str or genre_value_str in genre_lower:
                            genre_match = True
                            break
                
                # ジャンルが一致しない場合はスキップ
                if not genre_match:
                    continue
            
            # テレビ局でフィルタリング（指定されている場合）
            if channel_filters and len(channel_filters) > 0 and "すべて" not in channel_filters:
                channel_match = False
                # チャンネル情報を複数のフィールドから取得
                master_channel = str(metadata.get('channel', '')) or str(metadata.get('channel_code', '')) or str(metadata.get('放送局', ''))
                
                if master_channel and master_channel.strip():
                    master_channel_lower = master_channel.strip().lower()
                    # 選択されたチャンネルと比較
                    for selected_channel in channel_filters:
                        selected_channel_lower = selected_channel.strip().lower()
                        # チャンネル名のマッピング
                        channel_mapping = {
                            'nhk総合': ['nhk', 'nhk総合', 'nhkg-tky', 'nhk総合1..', '1 nhk総合1..'],
                            'nhk eテレ': ['nhk eテレ', 'nhk-etv', 'eテレ', 'nhk eテレ'],
                            '日本テレビ': ['日本テレビ', 'ntv', '日テレ', '日本テレビ'],
                            'tbs': ['tbs'],
                            'フジテレビ': ['フジテレビ', 'fuji', 'fuji-tv', 'フジ'],
                            'テレビ朝日': ['テレビ朝日', 'tv-asahi', '朝日', 'テレビ朝日'],
                            'テレビ東京': ['テレビ東京', 'tv-tokyo', 'テレ東', 'テレビ東京']
                        }
                        
                        # マッピングから候補を取得
                        candidates = channel_mapping.get(selected_channel_lower, [selected_channel_lower])
                        
                        # 部分一致でチェック
                        for candidate in candidates:
                            if candidate.lower() in master_channel_lower or master_channel_lower in candidate.lower():
                                channel_match = True
                                break
                        
                        if channel_match:
                            break
                
                # テレビ局が一致しない場合はスキップ
                if not channel_match:
                    continue
            
            # ソート用のキーを取得（start_timeから日時を抽出）
            def get_sort_key_from_metadata(metadata):
                """メタデータからソート用のキーを取得"""
                start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', ''))
                
                if start_time and len(start_time) >= 12 and start_time[:12].isdigit():
                    # YYYYMMDDHHMM形式（12桁）の場合
                    return int(start_time[:12])
                elif start_time and len(start_time) >= 8 and start_time[:8].isdigit():
                    # YYYYMMDD形式（8桁）の場合
                    return int(start_time[:8]) * 10000  # 時間部分を0として扱う
                else:
                    # 日時情報がない場合は最後に表示
                    return 0
            
            sort_key = get_sort_key_from_metadata(metadata)
            
            # 番組名の候補フィールドをチェック
            program_fields = [
                metadata.get('program_name', ''),
                metadata.get('program_title', ''),
                metadata.get('master_title', ''),
                metadata.get('title', ''),
                metadata.get('番組名', ''),
                metadata.get('番組タイトル', '')
            ]
            
            for field_value in program_fields:
                if field_value:
                    program_name = str(field_value).strip()
                    if program_name and program_name != 'None':
                        program_name_with_date.append((program_name, sort_key))
        
        # 日付の新しい順（降順）にソート、同じ日付の場合はテキスト順
        program_name_with_date.sort(key=lambda x: (-x[1], x[0]))
        
        # 重複を除去（最初の出現のみを保持）
        seen = set()
        result = []
        for program_name, _ in program_name_with_date:
            if program_name not in seen:
                seen.add(program_name)
                result.append(program_name)
        
        return result
    except Exception as e:
        st.error(f"番組名リストの取得エラー: {str(e)}")
        return []

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

# 検索フォーム（クリアボタンは検索結果の下に移動）

# タブで検索条件を切り替え（最新データを最初のタブに）
tab_latest, tab_date, tab_detail, tab_performer, tab_program_type = st.tabs(["📺 最新", "📅 日付", "🔍 詳細", "👤 出演", "📺 番組"])

# 検索条件の変数をセッションステートで管理（タブ間で共有）
if 'search_channel' not in st.session_state:
    st.session_state.search_channel = "すべて"
if 'search_date' not in st.session_state:
    st.session_state.search_date = None
if 'search_time' not in st.session_state:
    st.session_state.search_time = None
if 'search_program_name' not in st.session_state:
    st.session_state.search_program_name = ""
if 'search_genre' not in st.session_state:
    st.session_state.search_genre = ""
if 'search_performer' not in st.session_state:
    st.session_state.search_performer = ""
if 'search_keyword' not in st.session_state:
    st.session_state.search_keyword = ""
if 'search_program_names' not in st.session_state:
    st.session_state.search_program_names = []
if 'search_period_type' not in st.session_state:
    st.session_state.search_period_type = "オール"
if 'search_start_date' not in st.session_state:
    st.session_state.search_start_date = None
if 'search_end_date' not in st.session_state:
    st.session_state.search_end_date = None
if 'search_genre_program' not in st.session_state:
    st.session_state.search_genre_program = "すべて"
if 'search_channels_program' not in st.session_state:
    st.session_state.search_channels_program = []

search_button_date = False
search_button_detail = False
search_button_performer = False

with tab_date:
    # 日付タブ: 放送局、日付、時間
    with st.form("search_form_date"):
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
            
            # セッションステートから初期値を取得
            initial_channel_index = 0
            if 'channel_date' in st.session_state and st.session_state.channel_date in channel_options:
                initial_channel_index = channel_options.index(st.session_state.channel_date)
            elif st.session_state.search_channel in channel_options:
                initial_channel_index = channel_options.index(st.session_state.search_channel)
            
            channel = st.selectbox(
                "放送局",
                options=channel_options,
                help="放送局を選択してください",
                key="channel_date",
                index=initial_channel_index
            )
        
        with col2:
            # 日付
            initial_date = st.session_state.search_date if 'search_date' in st.session_state else None
            selected_date = st.date_input(
                "📆 日付",
                value=initial_date,
                help="カレンダーから日付を選択してください（任意）",
                key="date_input"
            )
        
        with col3:
            # 時間（30分単位）
            time_options = generate_time_options()
            # セッションステートから時間を復元
            selected_time_index = 0
            if 'time_input' in st.session_state and st.session_state.time_input is not None:
                # セッションステートに保存されている時間オブジェクトを使用
                if st.session_state.time_input in time_options:
                    selected_time_index = time_options.index(st.session_state.time_input) + 1
            elif st.session_state.search_time:
                try:
                    time_obj = datetime.strptime(st.session_state.search_time, "%H:%M").time()
                    if time_obj in time_options:
                        selected_time_index = time_options.index(time_obj) + 1
                except:
                    pass
            
            selected_time = st.selectbox(
                "🕐 時間",
                options=[None] + time_options,
                format_func=lambda x: x.strftime("%H:%M") if x else "----",
                help="時間を選択してください（30分単位、任意）",
                key="time_input",
                index=selected_time_index
            )
        
        # 検索ボタン
        search_button_date = st.form_submit_button("🔍 検索", use_container_width=True)
        
        # フォーム送信時にセッションステートを更新
        if search_button_date:
            st.session_state.search_channel = channel
            st.session_state.search_date = selected_date
            st.session_state.search_time = selected_time.strftime("%H:%M") if selected_time else None
            # 検索時にページをリセット
            st.session_state.current_page = 1
    
    # 全てクリアボタンを右寄せで表示（検索ボタンの下）
    col_clear_left, col_clear_right = st.columns([7, 3])
    with col_clear_right:
        if st.button("🔄 全てクリア", use_container_width=True, key="clear_all_button_date"):
            # 検索条件をクリア
            st.session_state.search_channel = "すべて"
            st.session_state.search_date = None
            st.session_state.search_time = None
            st.session_state.search_program_name = ""
            st.session_state.search_genre = ""
            st.session_state.search_performer = ""
            st.session_state.search_keyword = ""
            st.session_state.search_results = []
            st.session_state.selected_doc_id = None
            st.session_state.current_page = 1
            # 各タブの入力フィールドもクリア
            if 'channel_date' in st.session_state:
                st.session_state.channel_date = "すべて"
            if 'date_input' in st.session_state:
                st.session_state.date_input = None
            if 'time_input' in st.session_state:
                st.session_state.time_input = None
            st.rerun()

with tab_detail:
    # 詳細検索タブ: 放送局、番組名、ジャンル、キーワード（全文・テキスト検索）
    with st.form("search_form_detail"):
        search_options = get_search_options(_s3_client=s3_client)
        
        # 放送局
        col_channel = st.columns([1])[0]
        with col_channel:
            channel_options = ["すべて"]
            if search_options['channels']:
                channel_options.extend(search_options['channels'])
            
            initial_channel_index = 0
            if 'channel_detail' in st.session_state and st.session_state.channel_detail in channel_options:
                initial_channel_index = channel_options.index(st.session_state.channel_detail)
            elif st.session_state.search_channel in channel_options:
                initial_channel_index = channel_options.index(st.session_state.search_channel)
            
            channel_detail = st.selectbox(
                "放送局",
                options=channel_options,
                help="放送局を選択してください",
                key="channel_detail",
                index=initial_channel_index
            )
        
        # 日付と時間
        col_date, col_time = st.columns([1, 1])
        with col_date:
            initial_date = st.session_state.search_date if 'search_date' in st.session_state else None
            selected_date_detail = st.date_input(
                "📆 日付",
                value=initial_date,
                help="カレンダーから日付を選択してください（任意）",
                key="date_input_detail"
            )
        
        with col_time:
            # 時間（30分単位）
            time_options = generate_time_options()
            selected_time_index_detail = 0
            if 'time_input_detail' in st.session_state and st.session_state.time_input_detail is not None:
                if st.session_state.time_input_detail in time_options:
                    selected_time_index_detail = time_options.index(st.session_state.time_input_detail) + 1
            elif st.session_state.search_time:
                try:
                    time_obj = datetime.strptime(st.session_state.search_time, "%H:%M").time()
                    if time_obj in time_options:
                        selected_time_index_detail = time_options.index(time_obj) + 1
                except:
                    pass
            
            selected_time_detail = st.selectbox(
                "🕐 時間",
                options=[None] + time_options,
                format_func=lambda x: x.strftime("%H:%M") if x else "----",
                help="時間を選択してください（30分単位、任意）",
                key="time_input_detail",
                index=selected_time_index_detail
            )
        
        # 番組名、ジャンル、キーワード
        col_program, col_genre, col_keyword = st.columns([1, 1, 1])
        
        with col_program:
            initial_program_name = st.session_state.search_program_name if 'search_program_name' in st.session_state else ""
            program_name_search = st.text_input(
                "番組名",
                value=initial_program_name,
                placeholder="番組名を入力してください（任意）",
                help="番組名で検索します",
                key="program_name_detail"
            )
        
        with col_genre:
            # ジャンルをプルダウンで選択
            genre_options = ["すべて"]
            if search_options.get('genres'):
                genre_options.extend(search_options['genres'])
            
            initial_genre_index = 0
            if 'genre_detail' in st.session_state and st.session_state.genre_detail in genre_options:
                initial_genre_index = genre_options.index(st.session_state.genre_detail)
            elif st.session_state.search_genre in genre_options:
                initial_genre_index = genre_options.index(st.session_state.search_genre)
            
            genre_search = st.selectbox(
                "ジャンル",
                options=genre_options,
                help="ジャンルを選択してください",
                key="genre_detail",
                index=initial_genre_index
            )
        
        with col_keyword:
            initial_keyword = st.session_state.search_keyword if 'search_keyword' in st.session_state else ""
            keyword = st.text_input(
                "キーワード（全文・テキスト検索）",
                value=initial_keyword,
                placeholder="キーワードを入力してください（任意）",
                help="全文テキストとチャンクテキストから検索します（テキストマッチング検索 + ベクトル検索）",
                key="keyword_detail"
            )
            # ベクトル検索のオプション（sentence-transformersが利用可能な場合のみ表示）
            if SENTENCE_TRANSFORMERS_AVAILABLE:
                use_vector_search = st.checkbox(
                    "ベクトル検索を使用",
                    value=st.session_state.get("use_vector_search", False),
                    help="ベクトル類似度検索を使用します（意味的な類似性を検出）",
                    key="use_vector_search_detail"
                )
                st.session_state.use_vector_search = use_vector_search
        
        # 検索ボタン
        search_button_detail = st.form_submit_button("🔍 検索", use_container_width=True)
        
        # フォーム送信時にセッションステートを更新
        if search_button_detail:
            st.session_state.search_channel = channel_detail
            st.session_state.search_date = selected_date_detail
            st.session_state.search_time = selected_time_detail.strftime("%H:%M") if selected_time_detail else None
            st.session_state.search_program_name = program_name_search
            st.session_state.search_genre = genre_search
            st.session_state.search_keyword = keyword
            # 検索時にページをリセット
            st.session_state.current_page = 1
    
    # 全てクリアボタンを右寄せで表示（検索ボタンの下）
    col_clear_left, col_clear_right = st.columns([7, 3])
    with col_clear_right:
        if st.button("🔄 全てクリア", use_container_width=True, key="clear_all_button_detail"):
            # 検索条件をクリア
            st.session_state.search_channel = "すべて"
            st.session_state.search_date = None
            st.session_state.search_time = None
            st.session_state.search_program_name = ""
            st.session_state.search_genre = ""
            st.session_state.search_performer = ""
            st.session_state.search_keyword = ""
            st.session_state.search_results = []
            st.session_state.selected_doc_id = None
            st.session_state.current_page = 1
            # 各タブの入力フィールドもクリア
            if 'channel_detail' in st.session_state:
                st.session_state.channel_detail = "すべて"
            if 'date_input_detail' in st.session_state:
                st.session_state.date_input_detail = None
            if 'time_input_detail' in st.session_state:
                st.session_state.time_input_detail = None
            if 'program_name_detail' in st.session_state:
                st.session_state.program_name_detail = ""
            if 'genre_detail' in st.session_state:
                st.session_state.genre_detail = "すべて"
            if 'keyword_detail' in st.session_state:
                st.session_state.keyword_detail = ""
            if 'use_vector_search' in st.session_state:
                st.session_state.use_vector_search = False
            st.rerun()

with tab_performer:
    # 出演者タブ: 放送局、出演者名（サジェスト付き）、キーワード
    with st.form("search_form_performer"):
        search_options = get_search_options(_s3_client=s3_client)
        
        # 放送局
        col_channel = st.columns([1])[0]
        with col_channel:
            channel_options = ["すべて"]
            if search_options['channels']:
                channel_options.extend(search_options['channels'])
            
            initial_channel_index = 0
            if 'channel_performer' in st.session_state and st.session_state.channel_performer in channel_options:
                initial_channel_index = channel_options.index(st.session_state.channel_performer)
            elif st.session_state.search_channel in channel_options:
                initial_channel_index = channel_options.index(st.session_state.search_channel)
            
            channel_performer = st.selectbox(
                "放送局",
                options=channel_options,
                help="放送局を選択してください",
                key="channel_performer",
                index=initial_channel_index
            )
        
        # 出演者名（サジェスト付き）
        col_performer = st.columns([1])[0]
        with col_performer:
            # 出演者名リストを取得
            performer_names_list = get_performer_names(_s3_client=s3_client)
            
            # セッションステートから初期値を取得
            initial_performer = st.session_state.get("performer_performer", st.session_state.get("search_performer", ""))
            
            # 出演者名の選択（検索可能なselectbox）
            if performer_names_list:
                # 初期値がリストに含まれているかチェック
                initial_index = 0
                if initial_performer and initial_performer in performer_names_list:
                    initial_index = performer_names_list.index(initial_performer)
                
                performer_performer = st.selectbox(
                    "出演者名（検索可能）",
                    options=[""] + performer_names_list,  # 空文字列を最初に追加（選択なし）
                    help="出演者名を選択または検索してください。入力すると絞り込まれます。",
                    key="performer_performer",
                    index=initial_index + 1 if initial_performer else 0,
                    format_func=lambda x: "出演者名を選択してください" if x == "" else x
                )
            else:
                # 出演者名リストが取得できない場合はテキスト入力
                performer_performer = st.text_input(
                    "出演者名",
                    value=initial_performer,
                    placeholder="出演者名を入力してください（任意）",
                    help="出演者名を入力してください",
                    key="performer_performer"
                )
        
        # キーワード
        col_keyword = st.columns([1])[0]
        with col_keyword:
            initial_keyword = st.session_state.search_keyword if 'search_keyword' in st.session_state else ""
            keyword_performer = st.text_input(
                "キーワード（全文・テキスト検索）",
                value=initial_keyword,
                placeholder="キーワードを入力してください（任意）",
                help="全文テキストとチャンクテキストから検索します（テキストマッチング検索 + ベクトル検索）",
                key="keyword_performer"
            )
            # ベクトル検索のオプション（sentence-transformersが利用可能な場合のみ表示）
            if SENTENCE_TRANSFORMERS_AVAILABLE:
                use_vector_search = st.checkbox(
                    "ベクトル検索を使用",
                    value=st.session_state.get("use_vector_search", False),
                    help="ベクトル類似度検索を使用します（意味的な類似性を検出）",
                    key="use_vector_search_performer"
                )
                st.session_state.use_vector_search = use_vector_search
        
        # 検索ボタン
        search_button_performer = st.form_submit_button("🔍 検索", use_container_width=True)
        
        # フォーム送信時にセッションステートを更新
        if search_button_performer:
            st.session_state.search_channel = channel_performer
            st.session_state.search_keyword = keyword_performer
            st.session_state.search_performer = performer_performer if performer_performer else ""
            # 検索時にページをリセット
            st.session_state.current_page = 1
    
    # 全てクリアボタンを右寄せで表示（検索ボタンの下）
    col_clear_left, col_clear_right = st.columns([7, 3])
    with col_clear_right:
        if st.button("🔄 全てクリア", use_container_width=True, key="clear_all_button_performer"):
            # 検索条件をクリア
            st.session_state.search_channel = "すべて"
            st.session_state.search_date = None
            st.session_state.search_time = None
            st.session_state.search_program_name = ""
            st.session_state.search_genre = ""
            st.session_state.search_performer = ""
            st.session_state.search_keyword = ""
            st.session_state.search_results = []
            st.session_state.selected_doc_id = None
            st.session_state.current_page = 1
            # 各タブの入力フィールドもクリア
            if 'channel_performer' in st.session_state:
                st.session_state.channel_performer = "すべて"
            if 'keyword_performer' in st.session_state:
                st.session_state.keyword_performer = ""
            if 'performer_performer' in st.session_state:
                st.session_state.performer_performer = ""
            if 'use_vector_search' in st.session_state:
                st.session_state.use_vector_search = False
            st.rerun()

with tab_program_type:
    # 番組選択タブ: 期間設定、ジャンル、テレビ局、番組名の順
    search_options = get_search_options(_s3_client=s3_client)
    
    # 期間設定（最初に表示）
    st.markdown("### 📅 期間設定")
    period_options = ["すべて", "今週", "先週", "1カ月内", "曜日", "カスタム"]
    initial_period_index = 0
    if 'period_type' in st.session_state and st.session_state.period_type in period_options:
        initial_period_index = period_options.index(st.session_state.period_type)
    elif st.session_state.get("search_period_type", "すべて") in period_options:
        initial_period_index = period_options.index(st.session_state.get("search_period_type", "すべて"))
    
    period_type = st.selectbox(
        "期間タイプ",
        options=period_options,
        help="検索期間のタイプを選択してください",
        key="period_type",
        index=initial_period_index
    )
    
    # 曜日選択（期間タイプが「曜日」の場合、複数選択可能）
    selected_weekdays = []
    if period_type == "曜日":
        weekday_options = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日"]
        initial_weekdays = st.session_state.get("search_weekdays", [])
        if not initial_weekdays:
            initial_weekdays = []
        # 初期選択状態を取得（存在するもののみ）
        valid_initial_weekdays = [w for w in initial_weekdays if w in weekday_options]
        selected_weekdays = st.multiselect(
            "曜日（複数選択可）",
            options=weekday_options,
            default=valid_initial_weekdays,
            help="検索する曜日を選択してください（複数選択可）",
            key="selected_weekdays"
        )
    
    # カスタム期間の場合のみ日付選択を表示
    start_date_program = None
    end_date_program = None
    if period_type == "カスタム":
        col_start, col_end = st.columns(2)
        with col_start:
            initial_start_date = st.session_state.search_start_date if 'search_start_date' in st.session_state else None
            start_date_program = st.date_input(
                "開始日",
                value=initial_start_date,
                help="検索開始日を選択してください",
                key="start_date_input_program"
            )
        with col_end:
            initial_end_date = st.session_state.search_end_date if 'search_end_date' in st.session_state else None
            end_date_program = st.date_input(
                "終了日",
                value=initial_end_date,
                help="検索終了日を選択してください",
                key="end_date_input_program"
            )
    
    # ジャンル（プルダウン、固定順序で表示）
    genre_options = ["すべて"]
    available_genres = set(search_options.get('genres', []))
    
    # 固定順序のジャンルを順番に追加（データベースに存在するかどうかに関わらず）
    for genre in GENRE_ORDER[1:]:  # "すべて"を除く
        if genre == "その他":
            # 「その他」の前に、固定順序に含まれないジャンルを追加
            for other_genre in sorted(available_genres):
                if other_genre not in genre_options:
                    genre_options.append(other_genre)
        # データベースに存在する場合のみ追加
        if genre in available_genres:
            genre_options.append(genre)
    
    initial_genre_index = 0
    if 'genre_program' in st.session_state and st.session_state.genre_program in genre_options:
        initial_genre_index = genre_options.index(st.session_state.genre_program)
    elif st.session_state.get("search_genre_program", "すべて") in genre_options:
        initial_genre_index = genre_options.index(st.session_state.get("search_genre_program", "すべて"))
    
    # ジャンルが変更されたときに番組名リストをリセットするコールバック
    def on_genre_change():
        if 'program_names_multiselect' in st.session_state:
            st.session_state.program_names_multiselect = []
        st.session_state.last_genre_program = st.session_state.genre_program
    
    genre_program = st.selectbox(
        "ジャンル",
        options=genre_options,
        help="ジャンルを選択してください（選択すると番組名が絞り込まれます）",
        key="genre_program",
        index=initial_genre_index,
        on_change=on_genre_change
    )
    
    # テレビ局選択（チェックボックス）
    st.markdown("### 📺 テレビ局選択")
    channel_options = ["すべて", "NHK総合", "NHK Eテレ", "日本テレビ", "TBS", "フジテレビ", "テレビ朝日", "テレビ東京"]
    
    # 初期選択状態を取得
    initial_channels = st.session_state.get("search_channels_program", [])
    if not initial_channels:
        initial_channels = ["すべて"]
    
    selected_channels = []
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        all_checked = st.checkbox("すべて", value="すべて" in initial_channels, key="channel_all_program")
        if all_checked:
            selected_channels.append("すべて")
        nhk_checked = st.checkbox("NHK総合", value="NHK総合" in initial_channels, key="channel_nhk_program")
        if nhk_checked:
            selected_channels.append("NHK総合")
    
    with col2:
        nhk_etv_checked = st.checkbox("NHK Eテレ", value="NHK Eテレ" in initial_channels, key="channel_nhk_etv_program")
        if nhk_etv_checked:
            selected_channels.append("NHK Eテレ")
        ntv_checked = st.checkbox("日本テレビ", value="日本テレビ" in initial_channels, key="channel_ntv_program")
        if ntv_checked:
            selected_channels.append("日本テレビ")
    
    with col3:
        tbs_checked = st.checkbox("TBS", value="TBS" in initial_channels, key="channel_tbs_program")
        if tbs_checked:
            selected_channels.append("TBS")
        fuji_checked = st.checkbox("フジテレビ", value="フジテレビ" in initial_channels, key="channel_fuji_program")
        if fuji_checked:
            selected_channels.append("フジテレビ")
    
    with col4:
        tv_asahi_checked = st.checkbox("テレビ朝日", value="テレビ朝日" in initial_channels, key="channel_tv_asahi_program")
        if tv_asahi_checked:
            selected_channels.append("テレビ朝日")
        tv_tokyo_checked = st.checkbox("テレビ東京", value="テレビ東京" in initial_channels, key="channel_tv_tokyo_program")
        if tv_tokyo_checked:
            selected_channels.append("テレビ東京")
    
    # 「すべて」が選択されている場合、他の選択をクリア
    if "すべて" in selected_channels:
        selected_channels = ["すべて"]
    
    # テレビ局が変更されたときに番組名リストをリセットするコールバック
    def on_channel_change():
        if 'program_names_multiselect' in st.session_state:
            st.session_state.program_names_multiselect = []
        st.session_state.last_channels_program = selected_channels
    
    # テレビ局が変更されたかチェック
    last_channels = st.session_state.get("last_channels_program", [])
    if last_channels != selected_channels:
        if 'program_names_multiselect' in st.session_state:
            st.session_state.program_names_multiselect = []
        st.session_state.last_channels_program = selected_channels
    
    # ジャンルとテレビ局でフィルタリングした番組名リストを取得
    program_names_list = get_program_names(
        _s3_client=s3_client, 
        genre_filter=genre_program,
        channel_filters=selected_channels if selected_channels and "すべて" not in selected_channels else None
    )
    
    # 番組名（複数選択、multiselectで直感的に選択可能）
    st.markdown("### 📺 番組名（複数選択可）")
    if program_names_list:
        # ジャンルが変更された場合、選択された番組名をリセット
        if 'last_genre_program' not in st.session_state or st.session_state.last_genre_program != genre_program:
            if 'program_names_multiselect' in st.session_state:
                st.session_state.program_names_multiselect = []
            st.session_state.last_genre_program = genre_program
        
        initial_program_names = st.session_state.program_names_multiselect if 'program_names_multiselect' in st.session_state else []
        # 選択された番組名が現在のリストに存在するか確認
        valid_program_names = [name for name in initial_program_names if name in program_names_list]
        
        selected_program_names = st.multiselect(
            "番組名を選択してください（複数選択可）",
            options=program_names_list,
            default=valid_program_names,
            help=f"複数の番組を選択できます。Ctrlキー（Mac: Cmdキー）を押しながらクリックで複数選択（{len(program_names_list)}件）",
            key="program_names_multiselect"
        )
    else:
        st.warning("⚠️ 番組名データを読み込み中...")
        selected_program_names = []
    
    # フォーム内で検索ボタンを表示
    with st.form("search_form_program_type"):
        # 検索ボタン
        search_button_program_type = st.form_submit_button("🔍 検索", use_container_width=True)
        
        # フォーム送信時にセッションステートを更新
        if search_button_program_type:
            st.session_state.search_period_type = period_type
            if period_type == "カスタム":
                st.session_state.search_start_date = start_date_program
                st.session_state.search_end_date = end_date_program
            else:
                st.session_state.search_start_date = None
                st.session_state.search_end_date = None
            if period_type == "曜日":
                st.session_state.search_weekdays = selected_weekdays
            else:
                st.session_state.search_weekdays = []
            st.session_state.search_genre_program = genre_program
            st.session_state.search_channels_program = selected_channels
            st.session_state.search_program_names = selected_program_names
            # 検索時にページをリセット
            st.session_state.current_page = 1
    
    # 全てクリアボタンを右寄せで表示（検索ボタンの下）
    col_clear_left, col_clear_right = st.columns([7, 3])
    with col_clear_right:
        if st.button("🔄 全てクリア", use_container_width=True, key="clear_all_button_program"):
            # 検索条件をクリア
            st.session_state.search_channel = "すべて"
            st.session_state.search_date = None
            st.session_state.search_time = None
            st.session_state.search_program_name = ""
            st.session_state.search_genre = ""
            st.session_state.search_performer = ""
            st.session_state.search_keyword = ""
            st.session_state.search_results = []
            st.session_state.selected_doc_id = None
            st.session_state.current_page = 1
            # 各タブの入力フィールドもクリア
            if 'search_program_names' in st.session_state:
                st.session_state.search_program_names = []
            if 'search_period_type' in st.session_state:
                st.session_state.search_period_type = "すべて"
            if 'search_start_date' in st.session_state:
                st.session_state.search_start_date = None
            if 'search_end_date' in st.session_state:
                st.session_state.search_end_date = None
            if 'search_weekdays' in st.session_state:
                st.session_state.search_weekdays = []
            if 'search_genre_program' in st.session_state:
                st.session_state.search_genre_program = "すべて"
            if 'search_channels_program' in st.session_state:
                st.session_state.search_channels_program = []
            # 番組選択タブの入力フィールドもクリア
            if 'period_type' in st.session_state:
                st.session_state.period_type = "すべて"
            if 'genre_program' in st.session_state:
                st.session_state.genre_program = "すべて"
            if 'program_names_multiselect' in st.session_state:
                st.session_state.program_names_multiselect = []
            if 'start_date_input_program' in st.session_state:
                st.session_state.start_date_input_program = None
            if 'end_date_input_program' in st.session_state:
                st.session_state.end_date_input_program = None
            if 'selected_weekdays' in st.session_state:
                st.session_state.selected_weekdays = []
            # テレビ局選択のチェックボックスをクリア
            if 'channel_all_program' in st.session_state:
                st.session_state.channel_all_program = True  # 「すべて」を選択状態にする
            if 'channel_nhk_program' in st.session_state:
                st.session_state.channel_nhk_program = False
            if 'channel_nhk_etv_program' in st.session_state:
                st.session_state.channel_nhk_etv_program = False
            if 'channel_ntv_program' in st.session_state:
                st.session_state.channel_ntv_program = False
            if 'channel_tbs_program' in st.session_state:
                st.session_state.channel_tbs_program = False
            if 'channel_fuji_program' in st.session_state:
                st.session_state.channel_fuji_program = False
            if 'channel_tv_asahi_program' in st.session_state:
                st.session_state.channel_tv_asahi_program = False
            if 'channel_tv_tokyo_program' in st.session_state:
                st.session_state.channel_tv_tokyo_program = False
            # テレビ局選択の状態をリセット
            if 'last_channels_program' in st.session_state:
                st.session_state.last_channels_program = []
            if 'last_genre_program' in st.session_state:
                st.session_state.last_genre_program = "すべて"
            st.rerun()

# 最新データタブ
with tab_latest:
    try:
        latest_programs = get_latest_programs(_s3_client=s3_client, limit=100)  # より多くのデータを取得
        if latest_programs and len(latest_programs) > 0:
            # テレビ局ごとにグループ化
            channel_groups = {}
            channel_mapping = {
                'NHK総合': ['NHK総合', 'NHK', 'NHKG'],
                'NHK Eテレ': ['NHK Eテレ', 'NHK E', 'NHKE'],
                '日本テレビ': ['日本テレビ', 'NTV', '日テレ'],
                'TBS': ['TBS'],
                'フジテレビ': ['フジテレビ', 'フジ', 'Fuji'],
                'テレビ朝日': ['テレビ朝日', 'テレ朝', 'TV Asahi'],
                'テレビ東京': ['テレビ東京', 'テレ東', 'TV Tokyo']
            }
            
            # 主要6局のリスト（指定された順序）
            main_channels = ['NHK総合', 'NHK Eテレ', '日本テレビ', 'TBS', 'フジテレビ', 'テレビ朝日', 'テレビ東京']
            
            for program in latest_programs:
                metadata = program.get('metadata', {})
                channel = metadata.get('channel', '') or metadata.get('放送局', '') or ''
                
                # チャンネル名を正規化してグループ化
                matched_channel = None
                for main_channel in main_channels:
                    candidates = channel_mapping.get(main_channel, [main_channel])
                    channel_lower = channel.lower()
                    for candidate in candidates:
                        if candidate.lower() in channel_lower or channel_lower in candidate.lower():
                            matched_channel = main_channel
                            break
                    if matched_channel:
                        break
                
                if matched_channel:
                    if matched_channel not in channel_groups:
                        channel_groups[matched_channel] = []
                    channel_groups[matched_channel].append(program)
            
            # 各局最大3件まで
            for channel in channel_groups:
                channel_groups[channel] = channel_groups[channel][:3]
            
            if channel_groups:
                # 内窓方式（スクロール可能な領域）で表示
                st.markdown("""
                <style>
                .latest-data-scroll {
                    max-height: 400px;
                    overflow-y: auto;
                    padding: 10px;
                    border: 1px solid #e0e0e0;
                    border-radius: 5px;
                    background-color: #fafafa;
                }
                </style>
                <div class="latest-data-scroll">
                """, unsafe_allow_html=True)
                
                # 指定された順序で3つの段落に分割
                # 段落1: NHK総合、日本テレビ
                # 段落2: TBS、テレビ朝日
                # 段落3: フジテレビ、テレビ東京
                paragraphs = [
                    ['NHK総合', '日本テレビ'],
                    ['TBS', 'テレビ朝日'],
                    ['フジテレビ', 'テレビ東京']
                ]
                
                for para_idx, paragraph_channels in enumerate(paragraphs):
                    if para_idx > 0:
                        st.markdown("<br>", unsafe_allow_html=True)
                    
                    # 各段落は2列
                    cols = st.columns(2)
                    for col_idx, col in enumerate(cols):
                        if col_idx < len(paragraph_channels):
                            channel = paragraph_channels[col_idx]
                            # チャンネルが存在する場合のみ表示
                            if channel not in channel_groups or len(channel_groups[channel]) == 0:
                                continue
                            programs = channel_groups[channel]
                            
                            with col:
                                st.markdown(f"**📡 {channel}**")
                                
                                for program in programs:
                                    metadata = program.get('metadata', {})
                                    doc_id = program.get('doc_id', '')
                                    program_name = metadata.get('program_name', '') or metadata.get('program_title', '') or metadata.get('title', '') or '番組名不明'
                                    start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', '')) or ''
                                    
                                    # 時間を整形
                                    time_display = ''
                                    if start_time and len(start_time) >= 12 and start_time[:12].isdigit():
                                        # YYYYMMDDHHMM形式
                                        hour = start_time[8:10]
                                        minute = start_time[10:12]
                                        time_display = f"{hour}:{minute}"
                                    elif start_time and len(start_time) >= 8 and start_time[:8].isdigit():
                                        # YYYYMMDD形式（時間なし）
                                        time_display = ""
                                    
                                    # 番組名を12文字まで表示
                                    program_name_short = program_name[:12] + "..." if len(program_name) > 12 else program_name
                                    
                                    # 左寄せで表示（時間と番組名を横並び、コンパクトに）
                                    col_time, col_name = st.columns([1, 5])
                                    with col_time:
                                        st.markdown(f"**{time_display}**", help="放送時間")
                                    with col_name:
                                        button_key = f"latest_{channel}_{doc_id}_{para_idx}_{col_idx}_{program_name_short}"
                                        if st.button(program_name_short, key=button_key, use_container_width=True):
                                            st.session_state.selected_doc_id = doc_id
                                            # 検索結果にプログラムデータを追加（詳細表示のため）
                                            if 'search_results' not in st.session_state:
                                                st.session_state.search_results = []
                                            # プログラムデータを検索結果に追加
                                            if program not in st.session_state.search_results:
                                                st.session_state.search_results = [program]
                                            st.rerun()
                
                # 内窓の終了タグ
                st.markdown("</div>", unsafe_allow_html=True)
    except Exception as e:
        # エラーが発生した場合は表示しない（サイレントに失敗）
        # デバッグ用（管理者のみ表示）
        if is_admin():
            import traceback
            st.error(f"最新データの表示エラー: {str(e)}")
            with st.expander("エラー詳細"):
                st.code(traceback.format_exc())

# 検索ボタンの状態を統合
search_button = search_button_date or search_button_detail or search_button_performer or search_button_program_type

# 検索条件を取得（検索ボタンを押したタブの設定のみを使用）
if search_button_date:
    # 日付タブから検索（このタブの設定のみを使用）
    channel = st.session_state.get("channel_date", "すべて")
    selected_date = st.session_state.get("date_input", None)
    selected_time = st.session_state.get("time_input", None)
    # 他のタブの値は使用しない
    program_name_search = ""
    genre_search = ""
    performer_search = ""
    keyword = ""
    program_names_search = []
    period_type_search = "すべて"
    start_date_search = None
    end_date_search = None
    weekdays_search = []
    genre_program_search = "すべて"
    channels_program_search = []
elif search_button_detail:
    # 詳細検索タブから検索（このタブの設定のみを使用）
    channel = st.session_state.get("channel_detail", "すべて")
    program_name_search = st.session_state.get("program_name_detail", "")
    genre_search = st.session_state.get("genre_detail", "すべて")
    keyword = st.session_state.get("keyword_detail", "")
    # 日付と時間を詳細検索タブから取得
    selected_date = st.session_state.get("date_input_detail", None)
    selected_time = st.session_state.get("time_input_detail", None)
    # 他のタブの値は使用しない
    performer_search = ""
    program_names_search = []
    period_type_search = "すべて"
    start_date_search = None
    end_date_search = None
    weekdays_search = []
    genre_program_search = "すべて"
    channels_program_search = []
elif search_button_performer:
    # 出演者タブから検索（このタブの設定のみを使用）
    channel = st.session_state.get("channel_performer", "すべて")
    keyword = st.session_state.get("keyword_performer", "")
    performer_search = st.session_state.get("performer_performer", "")
    # 他のタブの値は使用しない
    selected_date = None
    selected_time = None
    program_name_search = ""
    genre_search = ""
    program_names_search = []
    period_type_search = "すべて"
    start_date_search = None
    end_date_search = None
    weekdays_search = []
    genre_program_search = "すべて"
    channels_program_search = []
elif search_button_program_type:
    # 番組選択タブから検索（このタブの設定のみを使用）
    period_type_search = st.session_state.get("period_type", "すべて")
    if period_type_search == "カスタム":
        start_date_search = st.session_state.get("search_start_date", None)
        end_date_search = st.session_state.get("search_end_date", None)
    else:
        start_date_search = None
        end_date_search = None
    weekdays_search = st.session_state.get("search_weekdays", []) if period_type_search == "曜日" else []
    genre_program_search = st.session_state.get("genre_program", "すべて")
    channels_program_search = st.session_state.get("search_channels_program", [])
    program_names_search = st.session_state.get("program_names_multiselect", [])
    # 他のタブの値は使用しない
    channel = "すべて"
    selected_date = None
    selected_time = None
    program_name_search = ""
    genre_search = ""
    performer_search = ""
    keyword = ""
else:
    # 検索ボタンが押されていない場合、セッションステートから取得（初期状態）
    channel = st.session_state.get("channel_date", st.session_state.get("channel_detail", st.session_state.get("channel_performer", st.session_state.get("search_channel", "すべて"))))
    selected_date = st.session_state.get("date_input", st.session_state.get("search_date", None))
    selected_time = st.session_state.get("time_input", None)
    if selected_time is None and st.session_state.get("search_time"):
        try:
            selected_time = datetime.strptime(st.session_state.search_time, "%H:%M").time()
        except:
            selected_time = None
    program_name_search = st.session_state.get("program_name_detail", st.session_state.get("search_program_name", ""))
    genre_search = st.session_state.get("genre_detail", st.session_state.get("search_genre", ""))
    performer_search = st.session_state.get("search_performer", "")
    keyword = st.session_state.get("keyword_detail", st.session_state.get("keyword_performer", st.session_state.get("search_keyword", "")))
    program_names_search = st.session_state.get("search_program_names", [])
    period_type_search = st.session_state.get("search_period_type", "すべて")
    start_date_search = st.session_state.get("search_start_date", None)
    end_date_search = st.session_state.get("search_end_date", None)
    weekdays_search = st.session_state.get("search_weekdays", [])
    genre_program_search = st.session_state.get("search_genre_program", "すべて")
    channels_program_search = st.session_state.get("search_channels_program", [])

# 日付と時間の文字列変換
date_str = selected_date.strftime("%Y%m%d") if selected_date else None
time_str = selected_time.strftime("%H%M") if selected_time else None

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
    genre: str = "",
    program_names: List[str] = None,
    period_type: str = "すべて",
    start_date: str = None,
    end_date: str = None,
    weekday: str = None,
    weekdays: List[str] = None,
    genre_program: str = "すべて",
    channels_program: List[str] = None,
    time_tolerance_minutes: int = 30
) -> List[Dict]:
    """マスターデータを詳細条件で検索（時間近似検索対応）"""
    results = []
    
    for master in master_list:
        metadata = master.get('metadata', {})
        doc_id = master.get('doc_id', '')
        
        # 各条件でフィルタリング
        match = True
        
        # 日付でフィルタ（完全一致のみ）
        if date_str:
            # 日付情報を複数のフィールドから取得
            master_date = str(metadata.get('date', '')) or str(metadata.get('放送日', '')) or str(metadata.get('放送日時', ''))
            
            # start_timeやend_timeから日付を抽出（YYYYMMDDHHMM形式の場合）
            if not master_date or master_date == 'None' or master_date.strip() == '':
                start_time = str(metadata.get('start_time', ''))
                if start_time and len(start_time) >= 8:
                    # YYYYMMDDHHMM形式から日付部分を抽出
                    if len(start_time) >= 8 and start_time[:8].isdigit():
                        master_date = start_time[:8]
            
            # 日付形式を変換して比較（YYYYMMDD形式）
            # date_strはYYYYMMDD形式（例: 20251022）
            # master_dateもYYYYMMDD形式またはYYYYMMDDHHMM形式、またはYYYY-MM-DD形式を想定
            master_date_clean = None
            if master_date and master_date != 'None' and master_date.strip():
                # YYYY-MM-DD形式の場合
                if '-' in master_date and len(master_date) >= 10:
                    # YYYY-MM-DD形式をYYYYMMDD形式に変換
                    try:
                        parts = master_date.split('-')
                        if len(parts) >= 3:
                            master_date_clean = f"{parts[0]}{parts[1].zfill(2)}{parts[2].zfill(2)}"
                    except:
                        pass
                # YYYYMMDD形式またはYYYYMMDDHHMM形式の場合
                elif len(master_date) >= 8 and master_date[:8].isdigit():
                    master_date_clean = master_date[:8]
                elif len(master_date) == 8 and master_date.isdigit():
                    master_date_clean = master_date
            
            # 完全一致で比較（部分一致ではなく）
            if master_date_clean:
                if master_date_clean != date_str:
                    match = False
                    continue
            else:
                # 日付情報がない場合はスキップ（日付フィルタは適用しない）
                # ただし、日付フィルタが指定されている場合は除外
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
        
        # テレビ局選択でフィルタ（番組選択タブ用）
        if channels_program and len(channels_program) > 0 and "すべて" not in channels_program:
            channel_match = False
            # チャンネル情報を複数のフィールドから取得
            master_channel = str(metadata.get('channel', '')) or str(metadata.get('channel_code', '')) or str(metadata.get('放送局', ''))
            
            if master_channel and master_channel.strip():
                master_channel_lower = master_channel.strip().lower()
                # 選択されたチャンネルと比較
                for selected_channel in channels_program:
                    selected_channel_lower = selected_channel.strip().lower()
                    # チャンネル名のマッピング
                    channel_mapping = {
                        'nhk総合': ['nhk', 'nhk総合', 'nhkg-tky', 'nhk総合1..', '1 nhk総合1..'],
                        'nhk eテレ': ['nhk eテレ', 'nhk-etv', 'eテレ', 'nhk eテレ'],
                        '日本テレビ': ['日本テレビ', 'ntv', '日テレ', '日本テレビ'],
                        'tbs': ['tbs'],
                        'フジテレビ': ['フジテレビ', 'fuji', 'fuji-tv', 'フジ'],
                        'テレビ朝日': ['テレビ朝日', 'tv-asahi', '朝日', 'テレビ朝日'],
                        'テレビ東京': ['テレビ東京', 'tv-tokyo', 'テレ東', 'テレビ東京']
                    }
                    
                    # マッピングから候補を取得
                    candidates = channel_mapping.get(selected_channel_lower, [selected_channel_lower])
                    
                    # 部分一致でチェック
                    for candidate in candidates:
                        if candidate.lower() in master_channel_lower or master_channel_lower in candidate.lower():
                            channel_match = True
                            break
                    
                    if channel_match:
                        break
                
                if not channel_match:
                    match = False
                    continue
        
        # 放送局でフィルタ（「すべて」の場合はフィルタしない）
        if channel and channel.strip() and channel != "すべて":
            # チャンネル情報を複数のフィールドから取得
            master_channel = str(metadata.get('channel', '')) or str(metadata.get('channel_code', '')) or str(metadata.get('放送局', ''))
            
            if not master_channel or master_channel.strip() == '':
                # 放送局情報がない場合はスキップ
                match = False
                continue
            
            # 選択されたチャンネル値と実際のデータを比較（部分一致でも可）
            # チャンネル名の先頭部分を抽出（例: "1 NHK総合1.." → "NHK"）
            channel_clean = channel.strip()
            # 数字とスペースを除去して比較
            channel_clean = re.sub(r'^\d+\s*', '', channel_clean)  # 先頭の数字とスペースを除去
            channel_clean = re.sub(r'\.+$', '', channel_clean)  # 末尾のドットを除去
            
            # マスターチャンネルも同様にクリーンアップ
            master_channel_clean = re.sub(r'^\d+\s*', '', master_channel)
            master_channel_clean = re.sub(r'\.+$', '', master_channel_clean)
            
            # 部分一致でチェック（大文字小文字を区別しない）
            if channel_clean.lower() not in master_channel_clean.lower() and master_channel_clean.lower() not in channel_clean.lower():
                # 元の値でもチェック（フォールバック）
                if channel.lower() not in master_channel.lower() and master_channel.lower() not in channel.lower():
                    match = False
                    continue
        
        # 番組名でフィルタ
        if program_name and program_name.strip():
            program_name_lower = program_name.strip().lower()
            # 番組名の候補フィールドをチェック（より多くのフィールドを対象に）
            program_fields = [
                metadata.get('program_name', ''),
                metadata.get('program_title', ''),
                metadata.get('master_title', ''),
                metadata.get('title', ''),
                metadata.get('番組名', ''),
                metadata.get('番組タイトル', ''),
                metadata.get('description', ''),
                metadata.get('description_detail', ''),
                metadata.get('program_detail', '')
            ]
            program_match = False
            for field_value in program_fields:
                if field_value:
                    field_value_str = str(field_value).lower()
                    # 部分一致でチェック（大文字小文字を区別しない）
                    if program_name_lower in field_value_str or field_value_str in program_name_lower:
                        program_match = True
                        break
            if not program_match:
                match = False
                continue
        
        # 番組名リストでフィルタ（複数選択対応）
        if program_names and len(program_names) > 0:
            program_name_match = False
            # 番組名の候補フィールドをチェック
            program_fields = [
                metadata.get('program_name', ''),
                metadata.get('program_title', ''),
                metadata.get('master_title', ''),
                metadata.get('title', ''),
                metadata.get('番組名', ''),
                metadata.get('番組タイトル', '')
            ]
            
            for program_name_selected in program_names:
                # 特殊文字を除去して比較（🈑、🅍などの絵文字を除去）
                program_name_selected_clean = re.sub(r'[🈑🅍🈓🈔🈕🈖🈗🈘🈙🈚🈛🈜🈝🈞🈟🈠🈡🈢🈣🈤🈥🈦🈧🈨🈩🈪🈫🈬🈭🈮🈯🈰🈱🈲🈳🈴🈵🈶🈷🈸🈹🈺🈻🈼🈽🈾🈿🉀🉁🉂🉃🉄🉅🉆🉇🉈🉉🉊🉋🉌🉍🉎🉏]', '', str(program_name_selected))
                program_name_selected_lower = program_name_selected_clean.strip().lower()
                
                for field_value in program_fields:
                    if field_value:
                        # 特殊文字を除去して比較
                        field_value_clean = re.sub(r'[🈑🅍🈓🈔🈕🈖🈗🈘🈙🈚🈛🈜🈝🈞🈟🈠🈡🈢🈣🈤🈥🈦🈧🈨🈩🈪🈫🈬🈭🈮🈯🈰🈱🈲🈳🈴🈵🈶🈷🈸🈹🈺🈻🈼🈽🈾🈿🉀🉁🉂🉃🉄🉅🉆🉇🉈🉉🉊🉋🉌🉍🉎🉏]', '', str(field_value))
                        field_value_str = field_value_clean.strip().lower()
                        
                        # 完全一致を優先
                        if program_name_selected_lower == field_value_str:
                            program_name_match = True
                            break
                        # 部分一致（特殊文字を除去した後の文字列で比較）
                        elif program_name_selected_lower in field_value_str or field_value_str in program_name_selected_lower:
                            program_name_match = True
                            break
                        # 元の文字列でもチェック（フォールバック）
                        elif str(program_name_selected).strip().lower() in str(field_value).strip().lower() or str(field_value).strip().lower() in str(program_name_selected).strip().lower():
                            program_name_match = True
                            break
                    if program_name_match:
                        break
                if program_name_match:
                    break
            
            if not program_name_match:
                match = False
                continue
        
        # 期間タイプでフィルタ
        if period_type and period_type != "すべて":
            # 日付情報を複数のフィールドから取得
            master_date = str(metadata.get('date', '')) or str(metadata.get('放送日', '')) or str(metadata.get('放送日時', ''))
            
            # start_timeやend_timeから日付を抽出（YYYYMMDDHHMM形式の場合）
            if not master_date or master_date == 'None' or master_date.strip() == '':
                start_time = str(metadata.get('start_time', ''))
                if start_time and len(start_time) >= 8:
                    if len(start_time) >= 8 and start_time[:8].isdigit():
                        master_date = start_time[:8]
            
            # 日付形式を変換（YYYYMMDD形式）
            master_date_clean = None
            if master_date and master_date != 'None' and master_date.strip():
                # YYYY-MM-DD形式の場合
                if '-' in master_date and len(master_date) >= 10:
                    try:
                        parts = master_date.split('-')
                        if len(parts) >= 3:
                            master_date_clean = f"{parts[0]}{parts[1].zfill(2)}{parts[2].zfill(2)}"
                    except:
                        pass
                # YYYYMMDD形式またはYYYYMMDDHHMM形式の場合
                elif len(master_date) >= 8 and master_date[:8].isdigit():
                    master_date_clean = master_date[:8]
                elif len(master_date) == 8 and master_date.isdigit():
                    master_date_clean = master_date
            
            if master_date_clean:
                master_date_int = int(master_date_clean)
                today = get_jst_now()
                today_str = today.strftime("%Y%m%d")
                today_int = int(today_str)
                
                if period_type == "今週":
                    # 今週（月曜日から日曜日まで）
                    # 今日が何曜日かを取得（0=月曜日、6=日曜日）
                    weekday = today.weekday()
                    # 今週の月曜日を計算
                    monday = today - timedelta(days=weekday)
                    monday_str = monday.strftime("%Y%m%d")
                    monday_int = int(monday_str)
                    # 今週の日曜日を計算
                    sunday = monday + timedelta(days=6)
                    sunday_str = sunday.strftime("%Y%m%d")
                    sunday_int = int(sunday_str)
                    if master_date_int < monday_int or master_date_int > sunday_int:
                        match = False
                        continue
                elif period_type == "先週":
                    # 先週（先週の月曜日から日曜日まで）
                    weekday = today.weekday()
                    # 今週の月曜日を計算
                    this_monday = today - timedelta(days=weekday)
                    # 先週の月曜日を計算
                    last_monday = this_monday - timedelta(days=7)
                    last_monday_str = last_monday.strftime("%Y%m%d")
                    last_monday_int = int(last_monday_str)
                    # 先週の日曜日を計算
                    last_sunday = last_monday + timedelta(days=6)
                    last_sunday_str = last_sunday.strftime("%Y%m%d")
                    last_sunday_int = int(last_sunday_str)
                    if master_date_int < last_monday_int or master_date_int > last_sunday_int:
                        match = False
                        continue
                elif period_type == "1カ月内":
                    # 1ヶ月前から今日まで
                    one_month_ago = today - timedelta(days=30)
                    one_month_ago_str = one_month_ago.strftime("%Y%m%d")
                    one_month_ago_int = int(one_month_ago_str)
                    if master_date_int < one_month_ago_int or master_date_int > today_int:
                        match = False
                        continue
                elif period_type == "曜日" and (weekday or weekdays):
                    # 曜日でフィルタ（指定された曜日のデータのみ、複数選択対応）
                    # 日付から曜日を取得
                    try:
                        from datetime import datetime as dt
                        master_date_obj = dt.strptime(master_date_clean, "%Y%m%d")
                        master_weekday = master_date_obj.weekday()  # 0=月曜日、6=日曜日
                        
                        # 曜日名を数値に変換
                        weekday_map = {
                            "月曜日": 0, "火曜日": 1, "水曜日": 2, "木曜日": 3,
                            "金曜日": 4, "土曜日": 5, "日曜日": 6
                        }
                        
                        # weekdaysがリストの場合は複数選択、weekdayが文字列の場合は単一選択（後方互換性）
                        target_weekdays = []
                        if weekdays and len(weekdays) > 0:
                            target_weekdays = [weekday_map.get(w, None) for w in weekdays if w in weekday_map]
                        elif weekday:
                            target_weekday = weekday_map.get(weekday, None)
                            if target_weekday is not None:
                                target_weekdays = [target_weekday]
                        
                        if target_weekdays and master_weekday not in target_weekdays:
                            match = False
                            continue
                    except:
                        # 日付の解析に失敗した場合はスキップ
                        match = False
                        continue
                elif period_type == "カスタム" and (start_date or end_date):
                    # カスタム期間
                    if start_date:
                        start_date_int = int(start_date.replace('-', ''))
                        if master_date_int < start_date_int:
                            match = False
                            continue
                    if end_date:
                        end_date_int = int(end_date.replace('-', ''))
                        if master_date_int > end_date_int:
                            match = False
                            continue
            else:
                # 日付情報がない場合は除外（期間フィルタが指定されている場合）
                if period_type != "すべて":
                    match = False
                    continue
        
        # ジャンル（番組選択タブ用）でフィルタ
        if genre_program and genre_program != "すべて":
            genre_lower = genre_program.strip().lower()
            # ジャンル情報を複数のフィールドから取得
            genre_fields = ['genre', 'ジャンル', 'program_genre', 'category', 'カテゴリ']
            genre_match = False

            for field in genre_fields:
                genre_value = metadata.get(field, '')
                if genre_value:
                    genre_value_str = str(genre_value).strip().lower()
                    # 完全一致を優先
                    if genre_lower == genre_value_str:
                        genre_match = True
                        break
                    # 部分一致（大文字小文字を区別しない）
                    elif genre_lower in genre_value_str or genre_value_str in genre_lower:
                        genre_match = True
                        break

            if not genre_match:
                match = False
                continue
        
        # 主演者でフィルタ（完全一致を優先、次に部分一致）
        if performer and performer.strip():
            performer_lower = performer.strip().lower()
            # 出演者情報を取得
            talents = metadata.get('talents', [])
            performer_match = False
            
            # 出演者リストをチェック（完全一致を優先、次に部分一致）
            if talents:
                for talent in talents:
                    if isinstance(talent, dict):
                        talent_name = talent.get('name', '') or talent.get('talent_name', '')
                    else:
                        talent_name = str(talent)
                    if talent_name:
                        talent_name_lower = talent_name.lower()
                        # 完全一致を優先
                        if performer_lower == talent_name_lower:
                            performer_match = True
                            break
                        # 部分一致（キーワードが出演者名に含まれる、または出演者名がキーワードに含まれる）
                        elif performer_lower in talent_name_lower or talent_name_lower in performer_lower:
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
                    if field_value:
                        field_value_lower = str(field_value).lower()
                        # 完全一致を優先
                        if performer_lower == field_value_lower:
                            performer_match = True
                            break
                        # 部分一致
                        elif performer_lower in field_value_lower or field_value_lower in performer_lower:
                            performer_match = True
                            break
            
            if not performer_match:
                match = False
                continue
        
        # ジャンルでフィルタ
        if genre and genre.strip() and genre != "すべて":
            genre_lower = genre.strip().lower()
            # ジャンル情報を複数のフィールドから取得
            genre_fields = ['genre', 'ジャンル', 'program_genre', 'category', 'カテゴリ']
            genre_match = False
            
            for field in genre_fields:
                genre_value = metadata.get(field, '')
                if genre_value:
                    genre_value_str = str(genre_value).strip().lower()
                    # 完全一致を優先
                    if genre_lower == genre_value_str:
                        genre_match = True
                        break
                    # 部分一致（大文字小文字を区別しない）
                    elif genre_lower in genre_value_str or genre_value_str in genre_lower:
                        genre_match = True
                        break
            
            if not genre_match:
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
    genre: str = "",
    program_names: List[str] = None,
    period_type: str = "すべて",
    start_date: str = None,
    end_date: str = None,
    weekday: str = None,
    weekdays: List[str] = None,
    genre_program: str = "すべて",
    channels_program: List[str] = None,
    time_tolerance_minutes: int = 30,
    max_results: int = 500  # 検索結果の上限（パフォーマンス向上）
) -> List[Dict]:
    """マスターデータとチャンクテキストを含む詳細検索（最適化版）"""
    # まず基本条件でフィルタ（メタデータのみで高速）
    # キーワードは後で全文検索で処理するため、ここでは空文字列を渡す
    filtered_masters = search_master_data_advanced(
        master_list, program_id, date_str, time_str, channel, "", program_name, performer, genre, program_names, period_type, start_date, end_date, weekday, weekdays, genre_program, channels_program, time_tolerance_minutes
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
        
        # ベクトル検索を試行（チャンクデータにベクトルが含まれている場合、またはベクトル検索が有効な場合）
        # テキスト検索で結果が見つからない場合、またはベクトル検索が有効な場合
        use_vector_search = st.session_state.get("use_vector_search", False)
        
        # デバッグ情報（管理者のみ）
        if is_admin() and keyword:
            with st.expander("🔧 ベクトル検索デバッグ（検索実行前）"):
                st.write(f"use_vector_search: {use_vector_search}")
                st.write(f"SENTENCE_TRANSFORMERS_AVAILABLE: {SENTENCE_TRANSFORMERS_AVAILABLE}")
                st.write(f"keyword: {keyword}")
                st.write(f"len(results): {len(results)}")
                st.write(f"条件: (len(results) == 0 or use_vector_search) and SENTENCE_TRANSFORMERS_AVAILABLE = {(len(results) == 0 or use_vector_search) and SENTENCE_TRANSFORMERS_AVAILABLE}")
        
        if (len(results) == 0 or use_vector_search) and SENTENCE_TRANSFORMERS_AVAILABLE and keyword and keyword.strip():
            # チャンクデータを取得してベクトル検索を実行
            vector_results = search_with_vector_similarity(
                _s3_client, filtered_masters, keyword, max_results=max_results
            )
            
            # デバッグ情報（管理者のみ）
            if is_admin():
                with st.expander("🔧 ベクトル検索デバッグ（検索実行後）"):
                    st.write(f"vector_results count: {len(vector_results) if vector_results else 0}")
                    if vector_results:
                        st.write(f"最初の結果のkeys: {list(vector_results[0].keys()) if vector_results else []}")
                        st.write(f"最初の結果のvector_similarity: {vector_results[0].get('vector_similarity') if vector_results else None}")
                        st.write(f"最初の結果のbest_chunk exists: {vector_results[0].get('best_chunk') is not None if vector_results else False}")
            
            if vector_results:
                # 既存の結果のdoc_idをマッピング
                existing_results_by_doc_id = {r.get('doc_id', ''): r for r in results}
                
                # ベクトル検索の結果を処理
                for vector_result in vector_results:
                    doc_id = vector_result.get('doc_id', '')
                    if not doc_id:
                        continue
                    
                    # 既存の結果に存在する場合は、ベクトル検索の情報を追加
                    if doc_id in existing_results_by_doc_id:
                        existing_result = existing_results_by_doc_id[doc_id]
                        # ベクトル検索の情報を追加
                        existing_result['vector_similarity'] = vector_result.get('vector_similarity')
                        existing_result['best_chunk'] = vector_result.get('best_chunk')
                    else:
                        # 新しい結果として追加
                        results.append(vector_result)
                        if len(results) >= max_results:
                            break
                
                # ベクトル検索の結果を類似度でソート（ベクトル検索情報があるもの優先）
                results.sort(key=lambda x: (
                    x.get('vector_similarity', 0) if x.get('vector_similarity') is not None else -1,
                    x.get('doc_id', '')
                ), reverse=True)
        
        progress_bar.empty()
        status_text.empty()
        
        # 検索結果が上限に達した場合の警告
        if len(results) >= max_results:
            st.info(f"ℹ️ 検索結果が{max_results}件に達したため、表示を制限しました。検索条件を絞り込んでください。")
        
        return results
    
    return filtered_masters

# ベクトル検索用の関数
@st.cache_resource
def load_embedding_model():
    """埋め込みモデルを読み込む（キャッシュ）"""
    if not SENTENCE_TRANSFORMERS_AVAILABLE:
        return None
    try:
        # 日本語対応のモデルを使用（multilingual-MiniLM-L12-v2など）
        model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        return model
    except Exception as e:
        st.error(f"埋め込みモデルの読み込みエラー: {str(e)}")
        return None

def compute_cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """コサイン類似度を計算"""
    try:
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return dot_product / (norm1 * norm2)
    except Exception:
        return 0.0

def get_chunk_embedding(chunk: Dict, model) -> Optional[np.ndarray]:
    """チャンクから埋め込みベクトルを取得（既存のベクトルがある場合はそれを使用、ない場合は生成）"""
    # 既存のベクトルがある場合はそれを使用
    if 'embedding' in chunk:
        embedding = chunk.get('embedding')
        if isinstance(embedding, list):
            return np.array(embedding)
        elif isinstance(embedding, np.ndarray):
            return embedding
    elif 'vector' in chunk:
        vector = chunk.get('vector')
        if isinstance(vector, list):
            return np.array(vector)
        elif isinstance(vector, np.ndarray):
            return vector
    
    # ベクトルがない場合は、テキストから生成
    if model is None:
        return None
    
    chunk_text = chunk.get('text', '')
    if not chunk_text:
        return None
    
    try:
        embedding = model.encode(chunk_text, convert_to_numpy=True)
        return embedding
    except Exception as e:
        st.error(f"埋め込みベクトルの生成エラー: {str(e)}")
        return None

def search_with_vector_similarity(
    _s3_client,
    master_list: List[Dict],
    query: str,
    max_results: int = 500,
    similarity_threshold: float = 0.3
) -> List[Dict]:
    """ベクトル類似度検索を実行"""
    if not query or not query.strip():
        return []
    
    # 埋め込みモデルを読み込む
    model = load_embedding_model()
    if model is None:
        return []  # モデルが利用できない場合は空のリストを返す
    
    try:
        # クエリをベクトル化
        query_embedding = model.encode(query, convert_to_numpy=True)
    except Exception as e:
        st.error(f"クエリのベクトル化エラー: {str(e)}")
        return []
    
    # 各マスターデータのチャンクを検索
    results_with_scores = []
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(master_list)
    
    for idx, master in enumerate(master_list):
        if len(results_with_scores) >= max_results:
            break
        
        # 進捗表示
        if idx % 10 == 0 or idx == total - 1:
            progress = (idx + 1) / total
            progress_bar.progress(progress)
            status_text.text(f"ベクトル検索中: {idx + 1}/{total} 件（{len(results_with_scores)} 件ヒット）")
        
        doc_id = master.get('doc_id', '')
        if not doc_id:
            continue
        
        # チャンクデータを取得
        try:
            chunks = get_chunk_data(_s3_client, doc_id)
        except Exception:
            continue
        
        if not chunks:
            continue
        
        # 各チャンクのベクトルとクエリの類似度を計算
        best_similarity = 0.0
        best_chunk = None
        
        for chunk in chunks:
            chunk_embedding = get_chunk_embedding(chunk, model)
            if chunk_embedding is None:
                continue
            
            # コサイン類似度を計算
            similarity = compute_cosine_similarity(query_embedding, chunk_embedding)
            
            if similarity > best_similarity:
                best_similarity = similarity
                best_chunk = chunk
        
        # 類似度が閾値以上の場合、結果に追加（閾値を下げてより多くの結果を取得）
        # ベクトル検索が有効な場合は、閾値を下げる（0.2に変更）
        if best_similarity >= 0.2:  # 0.3から0.2に下げる
            # マスターデータに類似度スコアを追加（ディープコピーで確実に保存）
            master_with_score = copy.deepcopy(master)
            master_with_score['vector_similarity'] = float(best_similarity)  # 明示的にfloatに変換
            master_with_score['best_chunk'] = copy.deepcopy(best_chunk) if best_chunk else None
            results_with_scores.append((best_similarity, master_with_score))
    
    progress_bar.empty()
    status_text.empty()
    
    # 類似度の高い順にソート
    results_with_scores.sort(key=lambda x: x[0], reverse=True)
    
    # マスターデータのみを返す
    results = [master for _, master in results_with_scores[:max_results]]
    
    return results

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
        tab5, tab1, tab2, tab3, tab4 = st.tabs(["📑 チャンク", "📋 番組メタデータ", "🤖 AI要約", "🖼️ 画面", "📄 全文"])
    else:
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 番組メタデータ", "🤖 AI要約", "🖼️ 画面", "📄 全文", "📑 チャンク"])
    
    with tab1:
        # メタ情報を表組形式で表示
        if metadata:
            # データを準備
            table_data = []
            
            # 放送局
            channel = metadata.get('channel') or metadata.get('channel_code', '')
            if channel:
                table_data.append({"項目": "放送局", "値": channel})
            
            # 放送時間
            date_val = metadata.get('broadcast_date') or metadata.get('date')
            start_time = metadata.get('start_time', '')
            end_time = metadata.get('end_time', '')
            
            if date_val or start_time or end_time:
                # 日付をフォーマット
                date_str = str(date_val) if date_val else ''
                if len(date_str) >= 8 and date_str.isdigit():
                    date_display = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                elif '-' in date_str:
                    date_display = date_str
                else:
                    date_display = date_str
                
                # 時間をフォーマット
                start_time_formatted = format_time_display_detail(start_time) if start_time else ''
                end_time_formatted = format_time_display_detail(end_time) if end_time else ''
                
                # 放送時間を組み立て
                if date_display and (start_time_formatted or end_time_formatted):
                    time_range = ""
                    if start_time_formatted and end_time_formatted:
                        time_range = f"{start_time_formatted} - {end_time_formatted}"
                    elif start_time_formatted:
                        time_range = start_time_formatted
                    elif end_time_formatted:
                        time_range = end_time_formatted
                    
                    table_data.append({"項目": "放送時間", "値": f"{date_display} {{ {time_range} }}"})
                elif date_display:
                    table_data.append({"項目": "放送時間", "値": date_display})
            
            # 番組名
            program_name = metadata.get('program_name') or metadata.get('program_title') or metadata.get('master_title')
            if program_name:
                table_data.append({"項目": "番組名", "値": program_name})
            
            # 番組詳細
            program_detail = metadata.get('program_detail')
            if program_detail:
                table_data.append({"項目": "番組詳細", "値": program_detail})
            
            # 出演者
            performer_names = []
            talents = metadata.get('talents', [])
            if talents:
                for talent in talents:
                    if isinstance(talent, dict):
                        talent_name = talent.get('name', '') or talent.get('talent_name', '')
                    else:
                        talent_name = str(talent)
                    if talent_name and talent_name.strip():
                        performer_names.append(talent_name.strip())
            
            # その他の出演者名フィールドもチェック
            talent_fields = [
                'talent_names', 'performers', 'performer_names',
                'cast', 'cast_names', '出演者', '出演者名'
            ]
            for field in talent_fields:
                field_value = metadata.get(field, '')
                if field_value:
                    if isinstance(field_value, str):
                        # カンマ区切りの文字列の場合
                        names = [n.strip() for n in field_value.split(',') if n.strip()]
                        for name in names:
                            if name not in performer_names:
                                performer_names.append(name)
                    elif isinstance(field_value, list):
                        # リストの場合
                        for name in field_value:
                            if isinstance(name, dict):
                                name_str = name.get('name', '') or name.get('talent_name', '')
                            else:
                                name_str = str(name)
                            if name_str and name_str.strip() and name_str.strip() not in performer_names:
                                performer_names.append(name_str.strip())
            
            if performer_names:
                performer_display = " / ".join(performer_names)
                table_data.append({"項目": "出演者", "値": performer_display})
            
            # 表組形式で表示（ヘッダーなし）
            if table_data:
                for row in table_data:
                    # 値に改行が含まれる場合はHTMLで処理
                    value = str(row['値']).replace('|', '\\|')  # パイプ文字をエスケープ
                    st.markdown(f"| {row['項目']} | {value} |")
            
            # 全メタデータをJSON形式でダウンロード可能にする
            json_str = json.dumps(metadata, ensure_ascii=False, indent=2)
            
            # ファイル名を生成（YYYY-MM-DD_HHMM_details.json）
            # 日付と時間を取得
            date_str = metadata.get('date', '') or metadata.get('broadcast_date', '') or metadata.get('放送日', '')
            start_time = metadata.get('start_time', '') or metadata.get('開始時間', '')
            end_time = metadata.get('end_time', '') or metadata.get('終了時間', '')
            channel = metadata.get('channel', '') or metadata.get('channel_code', '')
            
            # ファイル名用の形式に変換
            filename_date = ""
            filename_start = ""
            filename_channel = ""
            
            # 日付をYYYY-MM-DD形式に変換
            if date_str:
                date_str = str(date_str)
                if len(date_str) >= 8 and date_str.isdigit():
                    # YYYYMMDD形式の場合
                    filename_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                elif '-' in date_str:
                    # YYYY-MM-DD形式の場合
                    filename_date = date_str[:10]  # 最初の10文字（YYYY-MM-DD）
                else:
                    # その他の形式の場合、start_timeから日付を抽出
                    if start_time and len(str(start_time)) >= 8:
                        start_time_str = str(start_time)
                        if len(start_time_str) >= 8 and start_time_str[:8].isdigit():
                            # YYYYMMDDHHMM形式から日付部分を抽出
                            filename_date = f"{start_time_str[:4]}-{start_time_str[4:6]}-{start_time_str[6:8]}"
            
            # 開始時間をHHMM形式（4桁）に変換
            if start_time:
                start_time_str = str(start_time)
                if ':' in start_time_str:
                    # HH:MM形式の場合
                    parts = start_time_str.split(':')
                    if len(parts) >= 2:
                        hour = parts[0].zfill(2)
                        minute = parts[1].zfill(2)
                        filename_start = f"{hour}{minute}"
                elif len(start_time_str) >= 12 and start_time_str[:12].isdigit():
                    # YYYYMMDDHHMM形式（12桁）の場合
                    filename_start = start_time_str[8:12]  # HHMM部分を抽出
                elif len(start_time_str) >= 4:
                    # HHMM形式（4桁以上）の場合
                    filename_start = start_time_str[:4].zfill(4)
            
            # チャンネル名を英語化（簡易版）
            if channel:
                channel_mapping = {
                    'NHK総合': 'NHK',
                    'NHK Eテレ': 'NHK-ETV',
                    'フジテレビ': 'FUJI-TV',
                    '日本テレビ': 'NTV',
                    'TBS': 'TBS',
                    'テレビ朝日': 'TV-ASAHI',
                    'テレビ東京': 'TV-TOKYO',
                    '1 NHK総合1..': 'NHK',
                    'NHKG-TKY': 'NHK'
                }
                # チャンネル名の先頭部分を抽出（例: "1 NHK総合1.." → "NHK"）
                channel_clean = channel.strip()
                import re
                channel_clean = re.sub(r'^\d+\s*', '', channel_clean)  # 先頭の数字とスペースを除去
                channel_clean = re.sub(r'\.+$', '', channel_clean)  # 末尾のドットを除去
                filename_channel = channel_mapping.get(channel_clean, channel_mapping.get(channel, channel.replace(' ', '-').replace('　', '-')))
            
            # ファイル名を生成（YYYY-MM-DD_HHMM_details.json）
            if filename_date and filename_start:
                if filename_channel:
                    json_filename = f"{filename_date}_{filename_start}_{filename_channel}_details.json"
                else:
                    json_filename = f"{filename_date}_{filename_start}_details.json"
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
                            error_str = str(e)
                            # レート制限エラー（429）の場合、分かりやすいメッセージを返す
                            if '429' in error_str or 'rate_limit' in error_str.lower() or 'Rate limit' in error_str:
                                if 'try again in' in error_str.lower():
                                    # 再試行可能時間を含むメッセージを抽出
                                    import re
                                    wait_time_match = re.search(r'try again in ([\d\.]+[smh]+)', error_str, re.IGNORECASE)
                                    if wait_time_match:
                                        wait_time = wait_time_match.group(1)
                                        return f"⚠️ APIの利用制限に達しました。{wait_time}後に再試行してください。\n\n詳細: 1日のトークン使用量の上限に達しています。しばらく時間をおいてから再度お試しください。"
                                    else:
                                        return "⚠️ APIの利用制限に達しました。しばらく時間をおいてから再度お試しください。\n\n詳細: 1日のトークン使用量の上限に達しています。"
                                else:
                                    return "⚠️ APIの利用制限に達しました。しばらく時間をおいてから再度お試しください。\n\n詳細: 1日のトークン使用量の上限に達しています。"
                            else:
                                return f"⚠️ エラーが発生しました: {error_str}"
                    
                    # 要約を生成（毎回生成）
                    with st.spinner("AI要約を生成中..."):
                        summary = generate_summary(prompt, groq_api_key)
                    
                    # 要約を表示
                    st.markdown("### 番組概要")
                    st.markdown(summary)
                    # AI要約の注意書きを追加
                    st.markdown('<div style="color: #666; font-size: 0.85em; margin-top: 1em;">この要約はAIで自動生成しており、表現や事実関係が正確でない場合があります。ご注意ください。</div>', unsafe_allow_html=True)
                
            except ImportError:
                st.error("⚠️ Groqパッケージがインストールされていません。")
                st.code("pip install groq", language="bash")
            except Exception as e:
                st.error(f"⚠️ AI要約の生成でエラーが発生しました: {str(e)}")
        else:
            st.info("メタデータがありません")
    
    with tab3:
        if images:
            st.info(f"画面数: {len(images)}")
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
                        # サイズを固定してレイアウトの揺れを防ぐ
                        st.image(img_url, caption=timestamp, width=300)
                        
                        # クリックでチャンクタブに飛ぶボタン
                        if filename:
                            if st.button(f"📑 チャンクを表示", key=f"chunk_link_{doc_id}_{idx}"):
                                # チャンクタブに切り替える（セッションステートを使用）
                                st.session_state[f"show_chunk_for_{doc_id}"] = filename
                                st.rerun()
                    except Exception as e:
                        st.error(f"画面の読み込みエラー: {str(e)}")
        else:
            st.info("画面がありません")
    
    with tab4:
        if 'full_text' in master_data and master_data['full_text']:
            # 時間表示を削除（[HH:MM:SS.mmm-HH:MM:SS.mmm]形式）
            full_text = master_data['full_text']
            # 時間表示のパターンを削除
            cleaned_text = re.sub(r'\[\d{2}:\d{2}:\d{2}\.\d{3}-\d{2}:\d{2}:\d{2}\.\d{3}\]\s*', '', full_text)
            st.text_area("", value=cleaned_text, height=400, key=f"full_text_{doc_id}")
            
            # 全文テキストをtxtファイルとしてダウンロード可能にする
            # ファイル名を生成（YYYY-MM-DD_HHMM_fulltext.txt）
            metadata = master_data.get('metadata', {})
            date_str = metadata.get('date', '') or metadata.get('broadcast_date', '') or metadata.get('放送日', '')
            start_time = metadata.get('start_time', '') or metadata.get('開始時間', '')
            end_time = metadata.get('end_time', '') or metadata.get('終了時間', '')
            channel = metadata.get('channel', '') or metadata.get('channel_code', '')
            
            # ファイル名用の形式に変換
            filename_date = ""
            filename_start = ""
            filename_channel = ""
            
            # 日付をYYYY-MM-DD形式に変換
            if date_str:
                date_str = str(date_str)
                if len(date_str) >= 8 and date_str.isdigit():
                    # YYYYMMDD形式の場合
                    filename_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                elif '-' in date_str:
                    # YYYY-MM-DD形式の場合
                    filename_date = date_str[:10]  # 最初の10文字（YYYY-MM-DD）
                else:
                    # その他の形式の場合、start_timeから日付を抽出
                    if start_time and len(str(start_time)) >= 8:
                        start_time_str = str(start_time)
                        if len(start_time_str) >= 8 and start_time_str[:8].isdigit():
                            # YYYYMMDDHHMM形式から日付部分を抽出
                            filename_date = f"{start_time_str[:4]}-{start_time_str[4:6]}-{start_time_str[6:8]}"
            
            # 開始時間をHHMM形式（4桁）に変換
            if start_time:
                start_time_str = str(start_time)
                if ':' in start_time_str:
                    # HH:MM形式の場合
                    parts = start_time_str.split(':')
                    if len(parts) >= 2:
                        hour = parts[0].zfill(2)
                        minute = parts[1].zfill(2)
                        filename_start = f"{hour}{minute}"
                elif len(start_time_str) >= 12 and start_time_str[:12].isdigit():
                    # YYYYMMDDHHMM形式（12桁）の場合
                    filename_start = start_time_str[8:12]  # HHMM部分を抽出
                elif len(start_time_str) >= 4:
                    # HHMM形式（4桁以上）の場合
                    filename_start = start_time_str[:4].zfill(4)
            
            # チャンネル名を英語化（簡易版）
            if channel:
                channel_mapping = {
                    'NHK総合': 'NHK',
                    'NHK Eテレ': 'NHK-ETV',
                    'フジテレビ': 'FUJI-TV',
                    '日本テレビ': 'NTV',
                    'TBS': 'TBS',
                    'テレビ朝日': 'TV-ASAHI',
                    'テレビ東京': 'TV-TOKYO',
                    '1 NHK総合1..': 'NHK',
                    'NHKG-TKY': 'NHK'
                }
                # チャンネル名の先頭部分を抽出（例: "1 NHK総合1.." → "NHK"）
                channel_clean = channel.strip()
                import re
                channel_clean = re.sub(r'^\d+\s*', '', channel_clean)  # 先頭の数字とスペースを除去
                channel_clean = re.sub(r'\.+$', '', channel_clean)  # 末尾のドットを除去
                filename_channel = channel_mapping.get(channel_clean, channel_mapping.get(channel, channel.replace(' ', '-').replace('　', '-')))
            
            # ファイル名を生成（YYYY-MM-DD_HHMM_fulltext.txt）
            if filename_date and filename_start:
                if filename_channel:
                    txt_filename = f"{filename_date}_{filename_start}_{filename_channel}_fulltext.txt"
                else:
                    txt_filename = f"{filename_date}_{filename_start}_fulltext.txt"
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
        # audio再生プレーヤーを表示（チャンクセクション全体の上）
        audio_urls = master_data.get('audio_urls', [])
        
        # デバッグ情報（管理者のみ表示）
        if not audio_urls or len(audio_urls) == 0:
            # audio_urlsが存在しない場合の情報を表示
            st.info(f"ℹ️ 音声ファイルが見つかりませんでした（doc_id: {doc_id}）。v1.4でアップロードしたデータか確認してください。")
            # 管理者のみデバッグ情報を表示
            if is_admin():
                with st.expander("デバッグ情報（クリックして展開）"):
                    st.write(f"マスターデータのキー: {list(master_data.keys())}")
                    st.write(f"audio_urlsの値: {audio_urls}")
                    st.write(f"audio_urlsの型: {type(audio_urls)}")
                    st.write(f"doc_id: {doc_id}")
        
        if audio_urls and len(audio_urls) > 0:
            st.markdown("### 🎵 音声ファイル")
            for audio_url in audio_urls:
                # S3 URLからファイル名を抽出
                # 例: s3://tclip-raw-data-2025/rag/audio/{doc_id}/{filename}
                try:
                    # S3 URLからファイル名を抽出
                    if audio_url and isinstance(audio_url, str) and audio_url.startswith('s3://'):
                        # s3://bucket/key 形式からファイル名を抽出
                        parts = audio_url.split('/')
                        if len(parts) >= 2:
                            filename = parts[-1]
                            if filename:
                                # S3キーを生成
                                audio_key = f"{S3_AUDIO_PREFIX}{doc_id}/{filename}"
                                # 署名付きURLを生成
                                try:
                                    audio_download_url = s3_client.generate_presigned_url(
                                        'get_object',
                                        Params={'Bucket': S3_BUCKET_NAME, 'Key': audio_key},
                                        ExpiresIn=3600
                                    )
                                    # 音声プレーヤーを表示
                                    st.markdown(f"**{filename}**")
                                    # ファイル拡張子に応じて形式を指定
                                    ext = os.path.splitext(filename)[1].lower()
                                    format_map = {
                                        '.mp3': 'audio/mpeg',
                                        '.wav': 'audio/wav',
                                        '.m4a': 'audio/mp4',
                                        '.aac': 'audio/aac',
                                        '.ogg': 'audio/ogg',
                                        '.flac': 'audio/flac'
                                    }
                                    audio_format = format_map.get(ext, 'audio/mpeg')
                                    st.audio(audio_download_url, format=audio_format)
                                except Exception as e:
                                    # ファイルが見つからない場合はスキップ
                                    st.warning(f"音声ファイルのURL生成エラー: {filename} - {str(e)}")
                except Exception as e:
                    st.warning(f"音声ファイルの処理エラー: {str(e)}")
            st.markdown("---")
        
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
                                st.image(image_url, caption=f"画面: {image_filename}", width=400)
                            except Exception as e:
                                # 画像が見つからない場合はスキップ
                                pass
                        except Exception as e:
                            pass
                    
                    # チャンクの下に音声再生ボタンを表示
                    audio_urls = master_data.get('audio_urls', [])
                    
                    # デバッグ情報（開発用）
                    if not audio_urls or len(audio_urls) == 0:
                        # audio_urlsが存在しない場合の情報を表示
                        with st.expander("デバッグ情報（音声ファイル）", expanded=False):
                            st.write(f"マスターデータのキー: {list(master_data.keys()) if master_data else 'master_dataがNone'}")
                            st.write(f"audio_urlsの値: {audio_urls}")
                            st.write(f"audio_urlsの型: {type(audio_urls)}")
                            st.write(f"doc_id: {doc_id}")
                    
                    if audio_urls and len(audio_urls) > 0:
                        st.markdown("---")
                        st.markdown("### 🎵 音声再生")
                        for audio_url in audio_urls:
                            # S3 URLからファイル名を抽出
                            # 例: s3://tclip-raw-data-2025/rag/audio/{doc_id}/{filename}
                            try:
                                # S3 URLからファイル名を抽出
                                if audio_url and isinstance(audio_url, str) and audio_url.startswith('s3://'):
                                    # s3://bucket/key 形式からファイル名を抽出
                                    parts = audio_url.split('/')
                                    if len(parts) >= 2:
                                        filename = parts[-1]
                                        if filename:
                                            # S3キーを生成
                                            audio_key = f"{S3_AUDIO_PREFIX}{doc_id}/{filename}"
                                            # 署名付きURLを生成
                                            try:
                                                audio_download_url = s3_client.generate_presigned_url(
                                                    'get_object',
                                                    Params={'Bucket': S3_BUCKET_NAME, 'Key': audio_key},
                                                    ExpiresIn=3600
                                                )
                                                # 音声プレーヤーを表示
                                                st.markdown(f"**{filename}**")
                                                # ファイル拡張子に応じて形式を指定
                                                ext = os.path.splitext(filename)[1].lower()
                                                format_map = {
                                                    '.mp3': 'audio/mpeg',
                                                    '.wav': 'audio/wav',
                                                    '.m4a': 'audio/mp4',
                                                    '.aac': 'audio/aac',
                                                    '.ogg': 'audio/ogg',
                                                    '.flac': 'audio/flac'
                                                }
                                                audio_format = format_map.get(ext, 'audio/mpeg')
                                                st.audio(audio_download_url, format=audio_format)
                                            except Exception as e:
                                                # ファイルが見つからない場合はスキップ
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
        # 検索実行時に前回の検索結果をクリア
        st.session_state.search_results = []
        st.session_state.selected_doc_id = None
        st.session_state.current_page = 1
        
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
            if program_names_search and len(program_names_search) > 0:
                search_conditions.append(f"番組名: {', '.join(program_names_search)}")
            if period_type_search and period_type_search != "すべて":
                period_display = period_type_search
                if period_type_search == "曜日" and weekdays_search and len(weekdays_search) > 0:
                    period_display = f"{period_type_search} ({', '.join(weekdays_search)})"
                elif period_type_search == "カスタム":
                    period_display = period_type_search
                    if start_date_search:
                        period_display = f"{period_type_search} (開始: {start_date_search.strftime('%Y年%m月%d日')})"
                    if end_date_search:
                        period_display = f"{period_type_search} (終了: {end_date_search.strftime('%Y年%m月%d日')})"
                search_conditions.append(f"期間: {period_display}")
            if genre_program_search and genre_program_search != "すべて":
                search_conditions.append(f"ジャンル: {genre_program_search}")
            if channels_program_search and len(channels_program_search) > 0 and "すべて" not in channels_program_search:
                search_conditions.append(f"テレビ局: {', '.join(channels_program_search)}")
            
            # 検索条件のチェック（番組名検索、ジャンル検索も追加）
            # 検索条件が空の場合のみ警告を表示
            has_search_condition = (
                date_str or 
                time_str or 
                (channel and channel != "すべて") or 
                keyword or 
                program_name_search or 
                (genre_search and genre_search != "すべて") or
                performer_search or
                (program_names_search and len(program_names_search) > 0) or
                (period_type_search and period_type_search != "すべて") or
                (genre_program_search and genre_program_search != "すべて") or
                (channels_program_search and len(channels_program_search) > 0 and "すべて" not in channels_program_search)
            )
            if not has_search_condition:
                # 管理者のみエラーメッセージを表示
                if is_admin():
                    st.warning("⚠️ 検索条件を1つ以上入力してください")
                
                # 検索条件がない場合、現在時刻に該当する番組を自動検索
                now = get_jst_now()
                current_date_str = now.strftime("%Y%m%d")
                current_time_str = now.strftime("%H%M")
                
                # 現在時刻で検索を実行
                with st.spinner(f"現在時刻（{now.strftime('%Y年%m月%d日 %H:%M')}）に該当する番組を検索中..."):
                    search_results = search_master_data_with_chunks(
                        _s3_client=s3_client,
                        master_list=all_masters,
                        program_id="",
                        date_str=current_date_str,
                        time_str=current_time_str,
                        channel="",
                        keyword="",
                        program_name="",
                        performer="",
                        genre="",
                        program_names=None,
                        period_type="すべて",
                        start_date=None,
                        end_date=None,
                        weekday=None,
                        weekdays=None,
                        genre_program="すべて",
                        channels_program=None,
                        time_tolerance_minutes=30
                    )
                    
                    # 検索結果を放送開始時間の新しい順にソート
                    def get_sort_key(master):
                        """ソート用のキーを取得（start_timeから日時を抽出）"""
                        metadata = master.get('metadata', {})
                        start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', ''))
                        
                        if start_time and len(start_time) >= 12 and start_time[:12].isdigit():
                            # YYYYMMDDHHMM形式（12桁）の場合
                            return int(start_time[:12])
                        elif start_time and len(start_time) >= 8 and start_time[:8].isdigit():
                            # YYYYMMDD形式（8桁）の場合
                            return int(start_time[:8]) * 10000  # 時間部分を0として扱う
                        else:
                            # 日時情報がない場合は最後に表示
                            return 0
                    
                    # 放送開始時間の新しい順（降順）にソート
                    search_results_sorted = sorted(search_results, key=get_sort_key, reverse=True)
                    
                    # 検索結果をセッションステートに保存
                    st.session_state.search_results = search_results_sorted
                    st.session_state.current_page = 1
                    
                    if search_results_sorted:
                        st.info(f"📺 現在時刻（{now.strftime('%Y年%m月%d日 %H:%M')}）に該当する番組を {len(search_results_sorted)} 件見つけました")
                    else:
                        st.info(f"📺 現在時刻（{now.strftime('%Y年%m月%d日 %H:%M')}）に該当する番組は見つかりませんでした")
            else:
                with st.spinner(f"検索中: {', '.join(search_conditions) if search_conditions else '条件なし'}..."):
                    # 期間指定を文字列に変換
                    start_date_str = start_date_search.strftime("%Y%m%d") if start_date_search else None
                    end_date_str = end_date_search.strftime("%Y%m%d") if end_date_search else None
                    
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
                        genre=genre_search if genre_search and genre_search != "すべて" else "",
                        program_names=program_names_search if program_names_search and len(program_names_search) > 0 else None,
                        period_type=period_type_search if period_type_search else "すべて",
                        start_date=start_date_str,
                        end_date=end_date_str,
                        weekday=None,
                        weekdays=weekdays_search if period_type_search == "曜日" and weekdays_search and len(weekdays_search) > 0 else None,
                        genre_program=genre_program_search if genre_program_search and genre_program_search != "すべて" else "すべて",
                        channels_program=channels_program_search if channels_program_search and len(channels_program_search) > 0 and "すべて" not in channels_program_search else None,
                        time_tolerance_minutes=30  # 30分以内の近似検索
                    )
            
            # 検索結果を放送開始時間の新しい順にソート
            def get_sort_key(master):
                """ソート用のキーを取得（start_timeから日時を抽出）"""
                metadata = master.get('metadata', {})
                start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', ''))
                
                if start_time and len(start_time) >= 12 and start_time[:12].isdigit():
                    # YYYYMMDDHHMM形式（12桁）の場合
                    return int(start_time[:12])
                elif start_time and len(start_time) >= 8 and start_time[:8].isdigit():
                    # YYYYMMDD形式（8桁）の場合
                    return int(start_time[:8]) * 10000  # 時間部分を0として扱う
                else:
                    # 日時情報がない場合は最後に表示
                    return 0
            
            # 放送開始時間の新しい順（降順）にソート
            search_results_sorted = sorted(search_results, key=get_sort_key, reverse=True)
            
            # 検索結果をセッションステートに保存
            st.session_state.search_results = search_results_sorted
            # 検索時にページをリセット
            st.session_state.current_page = 1
            
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
                        
                        # 実際に使用された検索条件を表示
                        st.markdown("**実際に使用された検索条件:**")
                        st.json({
                            'date_str': date_str,
                            'time_str': time_str,
                            'channel': channel,
                            'program_name': program_name_search,
                            'performer': performer_search,
                            'keyword': keyword
                        })
                        
                        # 日付フィルタの動作確認用デバッグ情報
                        if date_str:
                            st.markdown("**日付フィルタのデバッグ情報（最初の10件）:**")
                            debug_date_samples = []
                            for idx, master in enumerate(all_masters[:10]):
                                metadata = master.get('metadata', {})
                                # 検索フィルタと同じロジックで日付を抽出
                                master_date = str(metadata.get('date', '')) or str(metadata.get('放送日', '')) or str(metadata.get('放送日時', ''))
                                if not master_date or master_date == 'None' or master_date.strip() == '':
                                    start_time = str(metadata.get('start_time', ''))
                                    if start_time and len(start_time) >= 8:
                                        if len(start_time) >= 8 and start_time[:8].isdigit():
                                            master_date = start_time[:8]
                                
                                master_date_clean = None
                                if master_date and master_date != 'None' and master_date.strip():
                                    if len(master_date) >= 8 and master_date[:8].isdigit():
                                        master_date_clean = master_date[:8]
                                    elif len(master_date) == 8 and master_date.isdigit():
                                        master_date_clean = master_date
                                
                                debug_date_samples.append({
                                    'doc_id': master.get('doc_id', 'N/A'),
                                    'date_field': metadata.get('date', 'N/A'),
                                    'start_time': metadata.get('start_time', 'N/A'),
                                    'extracted_date': master_date_clean or 'N/A',
                                    'matches': master_date_clean == date_str if master_date_clean else False
                                })
                            st.json(debug_date_samples)
                        
                        # サンプルデータの構造を確認（最初の5件）
                        if all_masters:
                            st.markdown("**サンプルデータ（最初の5件）のメタデータ構造:**")
                            for idx, master in enumerate(all_masters[:5]):
                                metadata = master.get('metadata', {})
                                st.markdown(f"**サンプル {idx+1}:**")
                                st.json({
                                    'doc_id': master.get('doc_id', 'N/A'),
                                    'date': metadata.get('date', 'N/A'),
                                    'start_time': metadata.get('start_time', 'N/A'),
                                    'channel': metadata.get('channel', 'N/A'),
                                    'channel_code': metadata.get('channel_code', 'N/A'),
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
                        # 検索条件をクリア
                        st.session_state.search_channel = "すべて"
                        st.session_state.search_date = None
                        st.session_state.search_time = None
                        st.session_state.search_program_name = ""
                        st.session_state.search_genre = ""
                        st.session_state.search_performer = ""
                        st.session_state.search_keyword = ""
                        st.session_state.search_results = []
                        st.session_state.selected_doc_id = None
                        st.session_state.current_page = 1
                        # 各タブの入力フィールドもクリア
                        if 'channel_date' in st.session_state:
                            st.session_state.channel_date = "すべて"
                        if 'channel_detail' in st.session_state:
                            st.session_state.channel_detail = "すべて"
                        if 'channel_performer' in st.session_state:
                            st.session_state.channel_performer = "すべて"
                        if 'date_input' in st.session_state:
                            st.session_state.date_input = None
                        if 'date_input_detail' in st.session_state:
                            st.session_state.date_input_detail = None
                        if 'time_input' in st.session_state:
                            st.session_state.time_input = None
                        if 'time_input_detail' in st.session_state:
                            st.session_state.time_input_detail = None
                        if 'program_name_detail' in st.session_state:
                            st.session_state.program_name_detail = ""
                        if 'genre_detail' in st.session_state:
                            st.session_state.genre_detail = "すべて"
                        if 'keyword_detail' in st.session_state:
                            st.session_state.keyword_detail = ""
                        if 'keyword_performer' in st.session_state:
                            st.session_state.keyword_performer = ""
                        if 'performer_performer' in st.session_state:
                            st.session_state.performer_performer = ""
                        if 'search_program_names' in st.session_state:
                            st.session_state.search_program_names = []
                        if 'search_period_type' in st.session_state:
                            st.session_state.search_period_type = "すべて"
                        if 'search_start_date' in st.session_state:
                            st.session_state.search_start_date = None
                        if 'search_end_date' in st.session_state:
                            st.session_state.search_end_date = None
                        if 'search_weekdays' in st.session_state:
                            st.session_state.search_weekdays = []
                        if 'search_genre_program' in st.session_state:
                            st.session_state.search_genre_program = "すべて"
                        if 'search_channels_program' in st.session_state:
                            st.session_state.search_channels_program = []
                        if 'search_program_names' in st.session_state:
                            st.session_state.search_program_names = []
                        # 番組選択タブの入力フィールドもクリア
                        if 'period_type' in st.session_state:
                            st.session_state.period_type = "すべて"
                        if 'genre_program' in st.session_state:
                            st.session_state.genre_program = "すべて"
                        if 'program_names_multiselect' in st.session_state:
                            st.session_state.program_names_multiselect = []
                        if 'start_date_input_program' in st.session_state:
                            st.session_state.start_date_input_program = None
                        if 'end_date_input_program' in st.session_state:
                            st.session_state.end_date_input_program = None
                        if 'selected_weekdays' in st.session_state:
                            st.session_state.selected_weekdays = []
                        # テレビ局選択のチェックボックスをクリア
                        if 'channel_all_program' in st.session_state:
                            st.session_state.channel_all_program = True  # 「すべて」を選択状態にする
                        if 'channel_nhk_program' in st.session_state:
                            st.session_state.channel_nhk_program = False
                        if 'channel_nhk_etv_program' in st.session_state:
                            st.session_state.channel_nhk_etv_program = False
                        if 'channel_ntv_program' in st.session_state:
                            st.session_state.channel_ntv_program = False
                        if 'channel_tbs_program' in st.session_state:
                            st.session_state.channel_tbs_program = False
                        if 'channel_fuji_program' in st.session_state:
                            st.session_state.channel_fuji_program = False
                        if 'channel_tv_asahi_program' in st.session_state:
                            st.session_state.channel_tv_asahi_program = False
                        if 'channel_tv_tokyo_program' in st.session_state:
                            st.session_state.channel_tv_tokyo_program = False
                        # テレビ局選択の状態をリセット
                        if 'last_channels_program' in st.session_state:
                            st.session_state.last_channels_program = []
                        if 'last_genre_program' in st.session_state:
                            st.session_state.last_genre_program = "すべて"
                        st.rerun()
                st.markdown("---")

# 初期表示時（検索ボタンが押されていない場合）に現在時刻の番組を表示
if not search_button and 'search_results' not in st.session_state:
    # 全データを取得
    with st.spinner("データを読み込み中...（初回のみ時間がかかります）"):
        all_masters = list_all_master_data(_s3_client=s3_client)
    
    if all_masters:
        # 現在時刻に該当する番組を自動検索
        now = get_jst_now()
        current_date_str = now.strftime("%Y%m%d")
        current_time_str = now.strftime("%H%M")
        
        with st.spinner(f"現在時刻（{now.strftime('%Y年%m月%d日 %H:%M')}）に該当する番組を検索中..."):
            search_results = search_master_data_with_chunks(
                _s3_client=s3_client,
                master_list=all_masters,
                program_id="",
                date_str=current_date_str,
                time_str=current_time_str,
                channel="",
                keyword="",
                program_name="",
                performer="",
                genre="",
                program_names=None,
                period_type="すべて",
                start_date=None,
                end_date=None,
                weekday=None,
                weekdays=None,
                genre_program="すべて",
                channels_program=None,
                time_tolerance_minutes=30
            )
            
            # 検索結果を放送開始時間の新しい順にソート
            def get_sort_key_initial(master):
                """ソート用のキーを取得（start_timeから日時を抽出）"""
                metadata = master.get('metadata', {})
                start_time = str(metadata.get('start_time', '')) or str(metadata.get('開始時間', ''))
                
                if start_time and len(start_time) >= 12 and start_time[:12].isdigit():
                    # YYYYMMDDHHMM形式（12桁）の場合
                    return int(start_time[:12])
                elif start_time and len(start_time) >= 8 and start_time[:8].isdigit():
                    # YYYYMMDD形式（8桁）の場合
                    return int(start_time[:8]) * 10000  # 時間部分を0として扱う
                else:
                    # 日時情報がない場合は最後に表示
                    return 0
            
            # 放送開始時間の新しい順（降順）にソート
            search_results_sorted = sorted(search_results, key=get_sort_key_initial, reverse=True)
            
            # 検索結果をセッションステートに保存
            st.session_state.search_results = search_results_sorted
            st.session_state.current_page = 1
            
            if search_results_sorted:
                st.info(f"📺 現在時刻（{now.strftime('%Y年%m月%d日 %H:%M')}）に該当する番組を {len(search_results_sorted)} 件見つけました")

# 検索結果のリスト表示（詳細表示前に）
# selected_doc_idが設定されている場合は詳細表示を優先
if st.session_state.selected_doc_id:
    st.markdown("---")
    # 詳細表示モード（独立した画面として表示）
    # 戻るボタンとタイトル
    col_title, col_back = st.columns([9, 1])
    with col_title:
        st.markdown("### 詳細情報")
    with col_back:
        if st.button("← 戻る", use_container_width=True):
            st.session_state.selected_doc_id = None
            st.rerun()
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
elif st.session_state.search_results:
    st.markdown("---")
    st.subheader("検索結果")
    
    # 検索結果をスクロール可能な内部ウィンドウに表示
    # 検索条件は上部に固定され、検索結果はスクロール可能
    # スクロール可能な領域のスタイルを設定
    st.markdown("""
    <style>
    .search-results-scroll {
        max-height: 600px;
        overflow-y: auto;
        padding: 10px;
        border: 1px solid #e0e0e0;
        border-radius: 5px;
        background-color: #fafafa;
    }
    </style>
    <div class="search-results-scroll">
    """, unsafe_allow_html=True)
    
    # リスト表示モード
    if not st.session_state.selected_doc_id:
        # ページング機能（20件ごと）
        total_results = len(st.session_state.search_results)
        items_per_page = 20
        total_pages = (total_results + items_per_page - 1) // items_per_page if total_results > 0 else 1
        
        # 現在のページ番号をセッションステートで管理
        if 'current_page' not in st.session_state:
            st.session_state.current_page = 1
        
        # ページ番号の表示と選択
        if total_pages > 1:
            col_page_info, col_page_buttons = st.columns([2, 8])
            with col_page_info:
                st.info(f"📄 {total_results} 件中 {((st.session_state.current_page - 1) * items_per_page) + 1} - {min(st.session_state.current_page * items_per_page, total_results)} 件を表示（ページ {st.session_state.current_page}/{total_pages}）")
            
            with col_page_buttons:
                # ページ番号ボタンを表示（最大10ページまで表示）
                page_cols = st.columns(min(total_pages, 10))
                for idx, col in enumerate(page_cols):
                    page_num = idx + 1
                    if page_num <= total_pages:
                        if col.button(str(page_num), key=f"page_{page_num}", use_container_width=True):
                            st.session_state.current_page = page_num
                            st.rerun()
            
            # 前へ/次へボタン
            col_prev, col_next = st.columns([1, 1])
            with col_prev:
                if st.button("◀ 前へ", disabled=(st.session_state.current_page <= 1), key="prev_page", use_container_width=True):
                    st.session_state.current_page = max(1, st.session_state.current_page - 1)
                    st.rerun()
            with col_next:
                if st.button("次へ ▶", disabled=(st.session_state.current_page >= total_pages), key="next_page", use_container_width=True):
                    st.session_state.current_page = min(total_pages, st.session_state.current_page + 1)
                    st.rerun()
        
        # 現在のページに表示する結果を取得
        start_idx = (st.session_state.current_page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        current_page_results = st.session_state.search_results[start_idx:end_idx]
        
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
        for idx, master in enumerate(current_page_results):
            doc_id = master.get('doc_id', '')
            metadata = master.get('metadata', {})
            
            # 放送日時・時間
            # 日付情報を複数のフィールドから取得（検索フィルタと同じロジック）
            date_str = metadata.get('date', '') or metadata.get('broadcast_date', '') or metadata.get('放送日', '') or metadata.get('放送日時', '')
            start_time = metadata.get('start_time', '')
            end_time = metadata.get('end_time', '')
            
            # date_strが空の場合、start_timeから日付を抽出（検索フィルタと同じロジック）
            if not date_str or date_str == 'None' or str(date_str).strip() == '':
                if start_time and len(str(start_time)) >= 8:
                    start_time_str = str(start_time)
                    # YYYYMMDDHHMM形式から日付部分を抽出
                    if len(start_time_str) >= 8 and start_time_str[:8].isdigit():
                        date_str = start_time_str[:8]
            
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
        # キーワード検索の場合、マッチした箇所を表示するための関数
        def get_keyword_snippet(master, keyword):
            """キーワードがマッチした箇所のスニペットを取得"""
            if not keyword or not keyword.strip():
                return None
            
            keyword_lower = keyword.strip().lower()
            snippets = []
            
            # 全文テキストから検索（「全文:」プレフィックスは削除、文字数も3割減）
            full_text = master.get('full_text', '')
            if full_text:
                full_text_str = str(full_text)
                full_text_lower = full_text_str.lower()
                if keyword_lower in full_text_lower:
                    # キーワードの位置を探す
                    pos = full_text_lower.find(keyword_lower)
                    if pos >= 0:
                        # 前後35文字を取得（50文字から3割減）
                        start = max(0, pos - 35)
                        end = min(len(full_text_str), pos + len(keyword_lower) + 35)
                        snippet = full_text_str[start:end]
                        # キーワードをハイライト（大文字小文字を区別しない）
                        snippet_highlighted = re.sub(
                            re.escape(keyword), 
                            lambda m: f"<mark style='background-color: yellow;'>{m.group()}</mark>",
                            snippet,
                            flags=re.IGNORECASE
                        )
                        snippets.append(f"...{snippet_highlighted}...")
            
            # メタデータから検索
            metadata = master.get('metadata', {})
            if metadata:
                text_fields = [
                    'program_name', 'program_title', 'master_title',
                    'description', 'description_detail', 'program_detail'
                ]
                for field in text_fields:
                    field_value = metadata.get(field, '')
                    if field_value:
                        field_value_str = str(field_value)
                        field_value_lower = field_value_str.lower()
                        if keyword_lower in field_value_lower:
                            # キーワードをハイライト（大文字小文字を区別しない）
                            field_value_highlighted = re.sub(
                                re.escape(keyword),
                                lambda m: f"<mark style='background-color: yellow;'>{m.group()}</mark>",
                                field_value_str,
                                flags=re.IGNORECASE
                            )
                            snippets.append(f"{field}: {field_value_highlighted}")
            
            return snippets if snippets else None
        
        for idx, row in enumerate(results_data):
            with st.container():
                # 元のmasterデータを取得
                master = current_page_results[idx]
                
                # キーワードマッチのスニペットを取得
                keyword = st.session_state.get("search_keyword", "")
                keyword_snippets = get_keyword_snippet(master, keyword) if keyword else None
                
                # 2行形式で表示
                # 1行目: 📅 2025-10-23　🕐 14:50 - 15:00　📺 1 NHK総合1..
                col1_line1, col2_line1, col3_line1 = st.columns([1, 1, 0.3])
                with col1_line1:
                    st.markdown(f"📅 {row['放送日時']}　🕐 {row['時間']}　📺 {row['放送局']}")
                with col2_line1:
                    pass
                with col3_line1:
                    # 詳細ボタン
                    if st.button(f"詳細", key=f"detail_{row['doc_id']}", use_container_width=True):
                        st.session_state.selected_doc_id = row['doc_id']
                        st.rerun()
                
                # 2行目: 📺 時論公論 朝鮮労働党創立80年 北朝鮮の"現在地"🈑🈞
                st.markdown(f"📺 {row['番組名']}")
                
                # キーワードマッチのスニペットを表示
                match_info = []
                
                # テキストマッチ（キーワード検索）
                if keyword_snippets:
                    match_info.append(("テキストマッチ", keyword_snippets))
                
                # ベクトル検索の結果
                vector_similarity = master.get('vector_similarity')
                best_chunk = master.get('best_chunk')
                if vector_similarity is not None and best_chunk:
                    chunk_text = best_chunk.get('text', '')
                    if chunk_text:
                        # チャンクテキストを表示（最大140文字、3割減）
                        chunk_preview = chunk_text[:140] + "..." if len(chunk_text) > 140 else chunk_text
                        similarity_percent = f"{vector_similarity * 100:.1f}%"
                        match_info.append(("ベクトル検索", [f"類似度: {similarity_percent}", f"チャンク: {chunk_preview}"]))
                
                # デバッグ情報（管理者のみ、ベクトル検索が有効な場合のみ）
                if is_admin() and keyword and st.session_state.get('use_vector_search', False):
                    debug_info = []
                    debug_info.append(f"vector_similarity: {vector_similarity}")
                    debug_info.append(f"best_chunk exists: {best_chunk is not None}")
                    if best_chunk:
                        debug_info.append(f"best_chunk keys: {list(best_chunk.keys())}")
                        debug_info.append(f"best_chunk text length: {len(best_chunk.get('text', ''))}")
                    debug_info.append(f"use_vector_search: {st.session_state.get('use_vector_search', False)}")
                    debug_info.append(f"SENTENCE_TRANSFORMERS_AVAILABLE: {SENTENCE_TRANSFORMERS_AVAILABLE}")
                    debug_info.append(f"master keys: {list(master.keys())}")
                    with st.expander(f"🔧 デバッグ情報 (doc_id: {row['doc_id']})"):
                        st.text("\n".join(debug_info))
                
                # マッチ情報を表示
                if match_info:
                    for match_type, snippets in match_info:
                        if match_type == "テキストマッチ":
                            st.markdown(f"<div style='padding: 0.5rem; background-color: #f0f0f0; border-left: 3px solid #4CAF50; margin: 0.5rem 0;'><small><strong>🔍 テキストマッチ:</strong></small><br>", unsafe_allow_html=True)
                            for snippet in snippets[:2]:  # 最大2つまで表示
                                st.markdown(f"<small>{snippet}</small>", unsafe_allow_html=True)
                            st.markdown("</div>", unsafe_allow_html=True)
                        elif match_type == "ベクトル検索":
                            st.markdown(f"<div style='padding: 0.5rem; background-color: #e3f2fd; border-left: 3px solid #2196F3; margin: 0.5rem 0;'><small><strong>🔮 ベクトル検索:</strong></small><br>", unsafe_allow_html=True)
                            for snippet in snippets:
                                st.markdown(f"<small>{snippet}</small>", unsafe_allow_html=True)
                            st.markdown("</div>", unsafe_allow_html=True)
                
                st.markdown("---")
        
        # スクロール可能な領域の終了タグ
        st.markdown("</div>", unsafe_allow_html=True)

else:
    # 初期状態の説明（データ範囲のみ表示）
    st.markdown("""
    ## ⚠️ データ範囲について
    
    **現在格納されているデータ期間**: 2025年10月3日 ～ 2025年10月26日
    
    **格納されている放送局**: NHK、NTV、TBSのみ
    
    この期間外の日付で検索した場合、検索結果が表示されない可能性があります。
    """)
