"""
レポート生成のメインロジック

データ抽出、集計、LLM分析、トーン分析、グラフ生成を行う
"""

import sys
import os
import re
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from collections import Counter, defaultdict
import numpy as np

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False

try:
    import matplotlib
    matplotlib.use('Agg')  # GUI不要のバックエンド
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    from pathlib import Path
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

from report_themes import get_theme_config, get_theme_keywords


def get_japanese_font():
    """日本語フォントを取得"""
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    font_dir = Path('C:/Windows/Fonts')
    font_files = [
        font_dir / 'msgothic.ttc',  # MS Gothic
        font_dir / 'yugothic.ttf',  # Yu Gothic
        font_dir / 'meiryo.ttc',    # Meiryo
    ]
    
    for font_file in font_files:
        if font_file.exists():
            return fm.FontProperties(fname=str(font_file))
    
    return None


def extract_data_by_theme(
    master_list: List[Dict],
    theme_name: str,
    start_date: date,
    end_date: date,
    search_master_data_advanced_func
) -> List[Dict]:
    """
    テーマに基づいてデータを抽出
    
    Args:
        master_list: マスターデータリスト
        theme_name: テーマ名
        start_date: 開始日
        end_date: 終了日
        search_master_data_advanced_func: 検索関数
    
    Returns:
        抽出されたマスターデータリスト
    """
    theme_config = get_theme_config(theme_name)
    keywords = theme_config.get("keywords", [])
    genres = theme_config.get("genres", [])
    channels = theme_config.get("channels", [])
    
    # 期間を文字列に変換
    start_date_str = start_date.strftime("%Y%m%d") if start_date else None
    end_date_str = end_date.strftime("%Y%m%d") if end_date else None
    
    # キーワードを結合（OR検索）
    keyword_str = " ".join(keywords) if keywords else ""
    
    # ジャンルフィルタ
    genre_filter = genres[0] if genres else ""
    
    # チャンネルフィルタ
    channel_filter = channels if channels else []
    
    # 検索実行
    results = search_master_data_advanced_func(
        master_list=master_list,
        date_str="",
        time_str="",
        channel=channel_filter[0] if channel_filter else "すべて",
        keyword=keyword_str,
        program_name="",
        performer="",
        genre=genre_filter,
        program_names=None,
        period_type="期間指定",
        start_date=start_date_str,
        end_date=end_date_str,
        weekday=None,
        weekdays=None,
        genre_program=genre_filter if genre_filter else "すべて",
        channels_program=channel_filter if channel_filter else [],
        time_tolerance_minutes=30
    )
    
    return results


def analyze_keyword_frequency(
    master_data_list: List[Dict],
    theme_keywords: List[str],
    chunks_data: Dict[str, List[Dict]]
) -> Dict[str, int]:
    """
    キーワード頻度を分析
    
    Args:
        master_data_list: マスターデータリスト
        theme_keywords: テーマキーワードリスト
        chunks_data: doc_idをキーとしたチャンクデータの辞書
    
    Returns:
        キーワードと頻度の辞書
    """
    keyword_counts = Counter()
    
    # トランスクリプトからキーワードを抽出（補助的処理）
    for master_data in master_data_list:
        doc_id = master_data.get('doc_id', '')
        full_text = master_data.get('full_text', '')
        
        # チャンクデータからも抽出
        chunks = chunks_data.get(doc_id, [])
        chunk_texts = []
        for chunk in chunks:
            content = chunk.get('content', '') or chunk.get('text', '')
            if content:
                chunk_texts.append(content)
        
        # 全テキストを結合
        all_text = full_text + " " + " ".join(chunk_texts)
        
        # キーワードをカウント（大文字小文字を区別しない）
        all_text_lower = all_text.lower()
        for keyword in theme_keywords:
            # 単語境界を考慮した検索
            pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
            matches = len(re.findall(pattern, all_text_lower))
            keyword_counts[keyword] += matches
    
    return dict(keyword_counts)


def analyze_sentiment(
    master_data_list: List[Dict],
    chunks_data: Dict[str, List[Dict]]
) -> Dict[str, float]:
    """
    トーン分析（ポジティブ/ネガティブ/中立）
    
    Args:
        master_data_list: マスターデータリスト
        chunks_data: doc_idをキーとしたチャンクデータの辞書
    
    Returns:
        トーン比率の辞書 {'positive': 0.0-1.0, 'negative': 0.0-1.0, 'neutral': 0.0-1.0}
    """
    if not VADER_AVAILABLE:
        return {'positive': 0.33, 'negative': 0.33, 'neutral': 0.34}
    
    analyzer = SentimentIntensityAnalyzer()
    sentiments = []
    
    # チャンクデータから感情分析
    for master_data in master_data_list:
        doc_id = master_data.get('doc_id', '')
        chunks = chunks_data.get(doc_id, [])
        
        for chunk in chunks:
            content = chunk.get('content', '') or chunk.get('text', '')
            if content and len(content) > 10:  # 短すぎるテキストは除外
                try:
                    scores = analyzer.polarity_scores(content)
                    # compoundスコアを使用（-1から1の範囲）
                    compound = scores.get('compound', 0.0)
                    sentiments.append(compound)
                except Exception:
                    continue
    
    if not sentiments:
        return {'positive': 0.33, 'negative': 0.33, 'neutral': 0.34}
    
    # compoundスコアを分類
    # positive: > 0.05, negative: < -0.05, neutral: その他
    positive_count = sum(1 for s in sentiments if s > 0.05)
    negative_count = sum(1 for s in sentiments if s < -0.05)
    neutral_count = len(sentiments) - positive_count - negative_count
    
    total = len(sentiments)
    return {
        'positive': positive_count / total if total > 0 else 0.0,
        'negative': negative_count / total if total > 0 else 0.0,
        'neutral': neutral_count / total if total > 0 else 0.0
    }


def generate_summary_with_llm(
    master_data_list: List[Dict],
    theme_name: str,
    period_str: str,
    keyword_frequency: Dict[str, int],
    groq_api_key: str
) -> Dict[str, str]:
    """
    LLM（Groq）を使用してサマリーと分析を生成
    
    Args:
        master_data_list: マスターデータリスト
        theme_name: テーマ名
        period_str: 期間文字列
        keyword_frequency: キーワード頻度
        groq_api_key: Groq APIキー
    
    Returns:
        分析結果の辞書
    """
    if not GROQ_AVAILABLE or not groq_api_key:
        return {
            'summary': "LLM分析が利用できません。",
            'topics': [],
            'key_programs': [],
            'recommendations': "データを確認してください。"
        }
    
    # データを要約
    program_count = len(master_data_list)
    top_keywords = sorted(keyword_frequency.items(), key=lambda x: x[1], reverse=True)[:5]
    top_keywords_str = "、".join([f"{kw}({cnt}回)" for kw, cnt in top_keywords])
    
    # 番組情報を収集
    program_info = []
    for master_data in master_data_list[:20]:  # 最大20件
        metadata = master_data.get('metadata', {})
        program_name = metadata.get('program_name') or metadata.get('program_title', '')
        channel = metadata.get('channel', '')
        date_str = metadata.get('date', '')
        start_time = metadata.get('start_time', '')
        
        if program_name:
            program_info.append(f"- {program_name} ({channel}) {date_str} {start_time}")
    
    program_info_str = "\n".join(program_info[:10])  # 最大10件
    
    # プロンプト作成
    prompt = f"""以下のテレビ番組データを分析して、役員会・広報部向けのレポートを作成してください。

【テーマ】{theme_name}
【期間】{period_str}
【対象番組数】{program_count}件
【キーワード頻出上位】{top_keywords_str}

【番組リスト】
{program_info_str}

以下の形式で分析結果を提供してください：

1. サマリー（3-5行）：
   - 今週の主要な傾向・論点を箇条書きで3つ
   - 特に注目すべき番組・発言の概要

2. 主要トピック（最大5つ）：
   各トピックについて：
   - トピック名
   - 概要（1-2行）
   - 主要な言及内容

3. 影響力の高い番組（最大5つ）：
   - 番組名、放送局、放送日時
   - 注目すべき発言・内容

4. 広報上の示唆：
   - リスク要因
   - ビジネスチャンス
   - 推奨対応

JSON形式で返答してください：
{{
  "summary": "サマリーテキスト",
  "topics": [
    {{"name": "トピック名", "overview": "概要", "details": "詳細"}}
  ],
  "key_programs": [
    {{"program": "番組名", "channel": "放送局", "date": "日付", "time": "時間", "highlight": "注目点"}}
  ],
  "recommendations": "推奨事項テキスト"
}}
"""
    
    try:
        client = Groq(api_key=groq_api_key)
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.7,
            max_tokens=2000
        )
        
        response_text = chat_completion.choices[0].message.content
        
        # JSONを抽出（コードブロックがあれば除去）
        response_text = re.sub(r'```json\s*', '', response_text)
        response_text = re.sub(r'```\s*', '', response_text)
        response_text = response_text.strip()
        
        # JSONをパース
        try:
            result = json.loads(response_text)
            return result
        except json.JSONDecodeError:
            # JSONパースに失敗した場合、テキストから構造を抽出
            return {
                'summary': response_text[:500] if len(response_text) > 500 else response_text,
                'topics': [],
                'key_programs': [],
                'recommendations': response_text[-500:] if len(response_text) > 500 else ""
            }
    
    except Exception as e:
        return {
            'summary': f"LLM分析エラー: {str(e)}",
            'topics': [],
            'key_programs': [],
            'recommendations': "データを確認してください。"
        }


def generate_charts(
    keyword_frequency: Dict[str, int],
    sentiment_ratio: Dict[str, float],
    channel_counts: Dict[str, int],
    output_dir: str
) -> Dict[str, str]:
    """
    グラフを生成してファイルに保存
    
    Args:
        keyword_frequency: キーワード頻度
        sentiment_ratio: トーン比率
        channel_counts: 放送局別件数
        output_dir: 出力ディレクトリ
    
    Returns:
        生成されたグラフファイルのパスの辞書
    """
    if not MATPLOTLIB_AVAILABLE:
        return {}
    
    font_prop = get_japanese_font()
    chart_paths = {}
    
    # 1. キーワード頻度グラフ（棒グラフ）
    if keyword_frequency:
        top_keywords = sorted(keyword_frequency.items(), key=lambda x: x[1], reverse=True)[:10]
        if top_keywords:
            plt.figure(figsize=(8, 5))
            keywords = [kw for kw, _ in top_keywords]
            counts = [cnt for _, cnt in top_keywords]
            
            plt.barh(keywords, counts, color='steelblue')
            plt.xlabel('出現回数', fontproperties=font_prop)
            plt.ylabel('キーワード', fontproperties=font_prop)
            plt.title('キーワード頻度トップ10', fontproperties=font_prop)
            plt.tight_layout()
            
            keyword_chart_path = os.path.join(output_dir, 'keyword_frequency.png')
            plt.savefig(keyword_chart_path, dpi=150, bbox_inches='tight')
            plt.close()
            chart_paths['keyword'] = keyword_chart_path
    
    # 2. 感情トーン比率（円グラフ）
    if sentiment_ratio:
        plt.figure(figsize=(6, 6))
        labels = ['ポジティブ', 'ネガティブ', '中立']
        sizes = [
            sentiment_ratio.get('positive', 0.0),
            sentiment_ratio.get('negative', 0.0),
            sentiment_ratio.get('neutral', 0.0)
        ]
        colors = ['#4CAF50', '#F44336', '#9E9E9E']
        
        plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, textprops={'fontproperties': font_prop})
        plt.title('感情トーン比率', fontproperties=font_prop)
        plt.axis('equal')
        
        sentiment_chart_path = os.path.join(output_dir, 'sentiment_ratio.png')
        plt.savefig(sentiment_chart_path, dpi=150, bbox_inches='tight')
        plt.close()
        chart_paths['sentiment'] = sentiment_chart_path
    
    # 3. 放送局別報道量（円グラフ）
    if channel_counts:
        plt.figure(figsize=(6, 6))
        channels = list(channel_counts.keys())
        counts = list(channel_counts.values())
        colors = plt.cm.Set3(np.linspace(0, 1, len(channels)))
        
        plt.pie(counts, labels=channels, colors=colors, autopct='%1.1f%%', startangle=90, textprops={'fontproperties': font_prop})
        plt.title('放送局別報道量', fontproperties=font_prop)
        plt.axis('equal')
        
        channel_chart_path = os.path.join(output_dir, 'channel_distribution.png')
        plt.savefig(channel_chart_path, dpi=150, bbox_inches='tight')
        plt.close()
        chart_paths['channel'] = channel_chart_path
    
    return chart_paths


def aggregate_metadata(
    master_data_list: List[Dict]
) -> Dict:
    """
    metadataベースで集計
    
    Args:
        master_data_list: マスターデータリスト
    
    Returns:
        集計結果の辞書
    """
    total_count = len(master_data_list)
    channel_counts = Counter()
    genre_counts = Counter()
    program_names = []
    total_duration_minutes = 0
    
    for master_data in master_data_list:
        metadata = master_data.get('metadata', {})
        
        # 放送局
        channel = metadata.get('channel', '') or metadata.get('channel_code', '')
        if channel:
            channel_counts[channel] += 1
        
        # ジャンル
        genre = metadata.get('genre', '') or metadata.get('program_genre', '')
        if genre:
            genre_counts[genre] += 1
        
        # 番組名
        program_name = metadata.get('program_name', '') or metadata.get('program_title', '')
        if program_name:
            program_names.append(program_name)
        
        # 放送時間（分）を計算
        start_time = metadata.get('start_time', '')
        end_time = metadata.get('end_time', '')
        if start_time and end_time:
            try:
                # 時間形式をパースして差分を計算
                # 簡易実装（実際の形式に応じて調整が必要）
                duration = 60  # デフォルト60分
                total_duration_minutes += duration
            except Exception:
                pass
    
    return {
        'total_count': total_count,
        'channel_counts': dict(channel_counts),
        'genre_counts': dict(genre_counts),
        'program_names': list(set(program_names)),
        'total_duration_minutes': total_duration_minutes
    }


def extract_key_quotes(
    master_data_list: List[Dict],
    chunks_data: Dict[str, List[Dict]],
    max_quotes: int = 3
) -> List[Dict]:
    """
    重要な発言を抽出
    
    Args:
        master_data_list: マスターデータリスト
        chunks_data: doc_idをキーとしたチャンクデータの辞書
        max_quotes: 最大引用数
    
    Returns:
        引用のリスト
    """
    quotes = []
    
    for master_data in master_data_list[:10]:  # 最大10件の番組から抽出
        doc_id = master_data.get('doc_id', '')
        metadata = master_data.get('metadata', {})
        program_name = metadata.get('program_name', '') or metadata.get('program_title', '')
        channel = metadata.get('channel', '')
        
        chunks = chunks_data.get(doc_id, [])
        for chunk in chunks[:5]:  # 各番組から最大5チャンク
            content = chunk.get('content', '') or chunk.get('text', '')
            if content and len(content) > 20:  # 短すぎるテキストは除外
                quotes.append({
                    'quote': content[:200] + "..." if len(content) > 200 else content,
                    'program': program_name,
                    'channel': channel
                })
                if len(quotes) >= max_quotes:
                    break
        
        if len(quotes) >= max_quotes:
            break
    
    return quotes[:max_quotes]

