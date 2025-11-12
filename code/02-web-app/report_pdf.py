"""
A4レポートPDF生成

reportlabを使用してA4縦1ページのレポートを生成
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib import colors
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


def register_japanese_font():
    """日本語フォントを登録（Windows/Linux対応）"""
    if not REPORTLAB_AVAILABLE:
        return None
    
    # Windows環境のフォントパス
    windows_font_dirs = [
        Path('C:/Windows/Fonts'),
        Path('C:/WINDOWS/Fonts'),
    ]
    
    # Linux環境のフォントパス（Streamlit Cloud対応）
    linux_font_dirs = [
        Path('/usr/share/fonts/truetype/noto'),
        Path('/usr/share/fonts/truetype/dejavu'),
        Path('/usr/share/fonts/opentype/noto'),
        Path('/System/Library/Fonts'),  # macOS
        Path.home() / '.fonts',
    ]
    
    # フォントファイルの候補
    font_candidates = [
        # Windows
        ('msgothic', 'msgothic.ttc'),
        ('yugothic', 'yugothic.ttf'),
        ('meiryo', 'meiryo.ttc'),
        # Linux/Notoフォント
        ('notosans', 'NotoSansCJK-Regular.ttc'),
        ('notosans', 'NotoSansCJK-Regular.otf'),
        ('notosans', 'NotoSans-Regular.ttf'),
        ('dejavu', 'DejaVuSans.ttf'),
    ]
    
    # Windowsフォントを検索
    for font_dir in windows_font_dirs:
        if font_dir.exists():
            for font_name, font_file in font_candidates:
                font_path = font_dir / font_file
                if font_path.exists():
                    try:
                        if font_path.suffix == '.ttc':
                            # TTCファイルは直接読み込めないためスキップ
                            continue
                        else:
                            pdfmetrics.registerFont(TTFont(font_name, str(font_path)))
                            return font_name
                    except Exception:
                        continue
    
    # Linuxフォントを検索
    for font_dir in linux_font_dirs:
        if font_dir.exists():
            for font_name, font_file in font_candidates:
                font_path = font_dir / font_file
                if font_path.exists():
                    try:
                        pdfmetrics.registerFont(TTFont(font_name, str(font_path)))
                        return font_name
                    except Exception:
                        continue
    
    # フォントが見つからない場合は、reportlabのデフォルト日本語フォントを試す
    try:
        # reportlabのデフォルト日本語フォント（存在する場合）
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        pdfmetrics.registerFont(UnicodeCIDFont('HeiseiKakuGo-W5'))
        print("[DEBUG] UnicodeCIDFont 'HeiseiKakuGo-W5' を登録しました")
        return 'HeiseiKakuGo-W5'
    except Exception as e:
        print(f"[DEBUG] UnicodeCIDFontの登録に失敗: {e}")
        pass
    
    # 最後の手段: システムフォントを検索
    import platform
    if platform.system() == 'Linux':
        # Linux環境で利用可能なフォントを検索
        try:
            import subprocess
            result = subprocess.run(['fc-list', ':lang=ja'], capture_output=True, text=True, timeout=2)
            if result.returncode == 0 and result.stdout:
                # 最初に見つかった日本語フォントを使用
                font_path = result.stdout.split('\n')[0].split(':')[0]
                if font_path and Path(font_path).exists():
                    pdfmetrics.registerFont(TTFont('system_japanese', font_path))
                    print(f"[DEBUG] システムフォントを使用: {font_path}")
                    return 'system_japanese'
        except Exception as e:
            print(f"[DEBUG] システムフォントの検索に失敗: {e}")
            pass
    
    # フォントが見つからない場合はデフォルトを使用（日本語は表示されない可能性あり）
    print("[DEBUG] 日本語フォントが見つかりませんでした。デフォルトフォントを使用します。")
    return None


def format_date_japanese(date_obj) -> str:
    """日付を日本語形式に変換"""
    if isinstance(date_obj, str):
        try:
            # YYYYMMDD形式を想定
            if len(date_obj) >= 8 and date_obj[:8].isdigit():
                year = int(date_obj[:4])
                month = int(date_obj[4:6])
                day = int(date_obj[6:8])
                date_obj = datetime(year, month, day).date()
            else:
                return date_obj
        except Exception:
            return date_obj
    
    if hasattr(date_obj, 'strftime'):
        weekday_names = ['月', '火', '水', '木', '金', '土', '日']
        weekday = weekday_names[date_obj.weekday()]
        return f"{date_obj.strftime('%Y年%m月%d日')}（{weekday}）"
    
    return str(date_obj)


def create_report_pdf(
    output_path: str,
    theme_name: str,
    start_date,
    end_date,
    summary_data: Dict,
    llm_analysis: Dict,
    keyword_frequency: Dict[str, int],
    sentiment_ratio: Dict[str, float],
    channel_counts: Dict[str, int],
    key_quotes: List[Dict],
    chart_paths: Dict[str, str],
    total_count: int,
    total_duration_minutes: int
) -> bool:
    """
    A4レポートPDFを生成
    
    Args:
        output_path: 出力ファイルパス
        theme_name: テーマ名
        start_date: 開始日
        end_date: 終了日
        summary_data: サマリーデータ
        llm_analysis: LLM分析結果
        keyword_frequency: キーワード頻度
        sentiment_ratio: トーン比率
        channel_counts: 放送局別件数
        key_quotes: 重要な引用
        chart_paths: グラフファイルパス
        total_count: 総件数
        total_duration_minutes: 総放送時間（分）
    
    Returns:
        成功した場合True
    """
    if not REPORTLAB_AVAILABLE:
        return False
    
    try:
        # 日本語フォントを登録
        font_name = register_japanese_font()
        print(f"[DEBUG] 登録されたフォント: {font_name}")
        
        if not font_name:
            # フォントが見つからない場合はデフォルトを使用
            font_name = 'Helvetica'
            print(f"[DEBUG] フォントが見つからないため、デフォルトフォントを使用: {font_name}")
        
        # デバッグ: 利用可能なフォントを確認
        print(f"[DEBUG] 利用可能なフォント: {list(pdfmetrics.getRegisteredFontNames())[:10]}")
        
        # PDFドキュメントを作成
        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=20*mm,
            leftMargin=20*mm,
            topMargin=20*mm,
            bottomMargin=20*mm
        )
        
        # スタイルを定義
        styles = getSampleStyleSheet()
        
        # カスタムスタイル
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#1f4788'),
            spaceAfter=6,
            fontName=font_name
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=12,
            textColor=colors.HexColor('#1f4788'),
            spaceAfter=6,
            spaceBefore=12,
            fontName=font_name
        )
        
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontSize=9,
            leading=12,
            fontName=font_name
        )
        
        small_style = ParagraphStyle(
            'CustomSmall',
            parent=styles['Normal'],
            fontSize=8,
            leading=10,
            fontName=font_name
        )
        
        # ストーリー（コンテンツ）を構築
        story = []
        
        # タイトル
        title_text = f"今週のテレビ報道レポート（{theme_name}）"
        story.append(Paragraph(title_text, title_style))
        story.append(Spacer(1, 3*mm))
        
        # 期間・対象カテゴリー・作成日・出典
        period_str = f"{format_date_japanese(start_date)} 〜 {format_date_japanese(end_date)}"
        created_date = datetime.now().strftime("%Y年%m月%d日")
        
        channels_str = "、".join(list(channel_counts.keys())[:6]) if channel_counts else "NHK総合・日本テレビ・TBS・フジテレビ・テレビ朝日・テレビ東京"
        
        info_text = f"""
        <b>期間:</b> {period_str}<br/>
        <b>対象カテゴリー:</b> {theme_name}<br/>
        <b>作成日:</b> {created_date}<br/>
        <b>出典:</b> {channels_str}
        """
        story.append(Paragraph(info_text, normal_style))
        story.append(Spacer(1, 5*mm))
        
        # 1. サマリー
        story.append(Paragraph("<b>【1. サマリー（Executive Summary）】</b>", heading_style))
        
        # キーワード頻出上位
        top_keywords = sorted(keyword_frequency.items(), key=lambda x: x[1], reverse=True)[:3]
        keywords_str = "、".join([f"「{kw}（{cnt}回）」" for kw, cnt in top_keywords])
        
        # 景況感（トーン比率から判定）
        if sentiment_ratio:
            positive_ratio = sentiment_ratio.get('positive', 0.0)
            negative_ratio = sentiment_ratio.get('negative', 0.0)
            if negative_ratio > 0.4:
                sentiment_str = "「慎重・やや悲観的」"
            elif positive_ratio > 0.4:
                sentiment_str = "「楽観的」"
            else:
                sentiment_str = "「中立・慎重」"
        else:
            sentiment_str = "「中立」"
        
        summary_text = f"""
        今週は{theme_name}に関連する報道が全局で{total_count}件。<br/>
        キーワード頻出上位は{keywords_str}。<br/>
        {llm_analysis.get('summary', '分析結果がありません。')}<br/>
        全体的に景況感は{sentiment_str}。
        """
        story.append(Paragraph(summary_text, normal_style))
        story.append(Spacer(1, 5*mm))
        
        # 2. トピック別分析
        story.append(Paragraph("<b>【2. トピック別分析（Topic Highlights）】</b>", heading_style))
        
        topics = llm_analysis.get('topics', [])
        if topics:
            # 表のデータを準備
            table_data = [['トピック', '概要', '出演者／発言', '局・番組名', '放送日時']]
            
            for topic in topics[:5]:  # 最大5トピック
                topic_name = topic.get('name', '')
                overview = topic.get('overview', '')
                details = topic.get('details', '')
                
                # キープログラムから該当するものを探す
                key_programs = llm_analysis.get('key_programs', [])
                program_info = key_programs[0] if key_programs else {}
                
                program_name = program_info.get('program', '')
                channel = program_info.get('channel', '')
                date_str = program_info.get('date', '')
                time_str = program_info.get('time', '')
                
                table_data.append([
                    topic_name[:20] if len(topic_name) > 20 else topic_name,
                    overview[:30] if len(overview) > 30 else overview,
                    details[:30] if len(details) > 30 else details,
                    f"{channel}「{program_name}」"[:25] if program_name else channel[:25],
                    f"{date_str} {time_str}"[:15] if date_str else ""
                ])
            
            # 表を作成
            table = Table(table_data, colWidths=[30*mm, 40*mm, 35*mm, 35*mm, 30*mm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4788')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), font_name),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('FONTNAME', (0, 1), (-1, -1), font_name),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]))
            story.append(table)
        else:
            story.append(Paragraph("トピックデータがありません。", normal_style))
        
        story.append(Spacer(1, 5*mm))
        
        # 3. トレンド可視化
        story.append(Paragraph("<b>【3. トレンド可視化（Graph / AI自動生成）】</b>", heading_style))
        
        # グラフを挿入
        if 'keyword' in chart_paths and os.path.exists(chart_paths['keyword']):
            try:
                img = Image(chart_paths['keyword'], width=80*mm, height=50*mm)
                story.append(img)
                story.append(Spacer(1, 3*mm))
            except Exception:
                pass
        
        if 'sentiment' in chart_paths and os.path.exists(chart_paths['sentiment']):
            try:
                img = Image(chart_paths['sentiment'], width=60*mm, height=60*mm)
                story.append(img)
                story.append(Spacer(1, 3*mm))
            except Exception:
                pass
        
        if 'channel' in chart_paths and os.path.exists(chart_paths['channel']):
            try:
                img = Image(chart_paths['channel'], width=60*mm, height=60*mm)
                story.append(img)
            except Exception:
                pass
        
        story.append(Spacer(1, 5*mm))
        
        # 4. 発言トランスクリプト抜粋
        story.append(Paragraph("<b>【4. 発言トランスクリプト抜粋】</b>", heading_style))
        
        if key_quotes:
            for quote in key_quotes:
                quote_text = f"「{quote.get('quote', '')}」"
                program_info = f"（{quote.get('program', '')} / {quote.get('channel', '')}）"
                story.append(Paragraph(quote_text, normal_style))
                story.append(Paragraph(program_info, small_style))
                story.append(Spacer(1, 2*mm))
        else:
            story.append(Paragraph("発言データがありません。", normal_style))
        
        story.append(Spacer(1, 5*mm))
        
        # 5. 広報部への示唆
        story.append(Paragraph("<b>【5. 広報部への示唆】</b>", heading_style))
        
        recommendations = llm_analysis.get('recommendations', '')
        if recommendations:
            story.append(Paragraph(recommendations, normal_style))
        else:
            story.append(Paragraph("推奨事項データがありません。", normal_style))
        
        # PDFを生成
        doc.build(story)
        return True
    
    except Exception as e:
        print(f"PDF生成エラー: {str(e)}")
        return False

