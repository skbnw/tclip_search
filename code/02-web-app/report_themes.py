"""
レポート生成用のテーマ辞書定義

各テーマは以下の情報を含む：
- keywords: 検索キーワードリスト
- genres: ジャンルフィルタ（オプション）
- channels: 放送局フィルタ（オプション）
- description: テーマの説明
"""

from typing import List, Dict

REPORT_THEMES = {
    "今週・ニュース": {
        "keywords": ["ニュース", "報道", "速報", "緊急", "特報"],
        "genres": ["ニュース／報道"],
        "channels": [],  # すべての放送局
        "description": "ニュース・報道番組に関する分析"
    },
    "今週・ワイドショー": {
        "keywords": ["ワイドショー", "情報番組", "情報", "特集", "話題"],
        "genres": ["情報／ワイドショー"],
        "channels": [],  # すべての放送局
        "description": "ワイドショー・情報番組に関する分析"
    },
    "今週・小売業界": {
        "keywords": ["小売", "スーパー", "コンビニ", "百貨店", "EC", "通販", "ネット通販", "値上げ", "値下げ", "物価", "インフレ", "デフレ", "消費", "購買", "売上", "商戦", "年末商戦", "物流", "配送", "在庫", "在庫管理"],
        "genres": [],  # すべてのジャンル
        "channels": [],  # すべての放送局
        "description": "小売業界に関する報道分析"
    },
    "今週・製造業界": {
        "keywords": ["製造", "工場", "生産", "ものづくり", "製造業", "自動車", "電機", "半導体", "部品", "サプライチェーン"],
        "genres": [],
        "channels": [],
        "description": "製造業界に関する報道分析"
    },
    "今週・金融業界": {
        "keywords": ["金融", "銀行", "証券", "投資", "株価", "為替", "金利", "金融政策", "日銀", "FRB", "経済", "景気", "GDP"],
        "genres": [],
        "channels": [],
        "description": "金融業界に関する報道分析"
    },
    "今週・IT・テクノロジー": {
        "keywords": ["IT", "AI", "人工知能", "機械学習", "デジタル", "DX", "クラウド", "データ", "サイバー", "セキュリティ", "SNS", "アプリ", "スマホ", "5G", "6G"],
        "genres": [],
        "channels": [],
        "description": "IT・テクノロジーに関する報道分析"
    },
    "今週・エネルギー": {
        "keywords": ["エネルギー", "電力", "ガス", "石油", "LNG", "再生可能エネルギー", "太陽光", "風力", "原子力", "原発", "節電", "省エネ"],
        "genres": [],
        "channels": [],
        "description": "エネルギー関連の報道分析"
    },
    "今週・医療・健康": {
        "keywords": ["医療", "健康", "病院", "医師", "看護", "薬", "治療", "診療", "保険", "介護", "高齢者", "認知症"],
        "genres": [],
        "channels": [],
        "description": "医療・健康に関する報道分析"
    },
    "今週・教育": {
        "keywords": ["教育", "学校", "大学", "受験", "入試", "学習", "授業", "教師", "教員", "学生", "子供", "子育て"],
        "genres": [],
        "channels": [],
        "description": "教育に関する報道分析"
    },
    "今週・環境": {
        "keywords": ["環境", "温暖化", "気候変動", "CO2", "カーボン", "脱炭素", "エコ", "リサイクル", "プラスチック", "海洋汚染", "大気汚染"],
        "genres": [],
        "channels": [],
        "description": "環境に関する報道分析"
    }
}


def get_theme_list() -> List[str]:
    """テーマリストを取得"""
    return list(REPORT_THEMES.keys())


def get_theme_config(theme_name: str) -> Dict:
    """テーマ設定を取得"""
    return REPORT_THEMES.get(theme_name, {})


def get_theme_keywords(theme_name: str) -> List[str]:
    """テーマのキーワードリストを取得"""
    theme = REPORT_THEMES.get(theme_name, {})
    return theme.get("keywords", [])


def get_theme_genres(theme_name: str) -> List[str]:
    """テーマのジャンルフィルタを取得"""
    theme = REPORT_THEMES.get(theme_name, {})
    return theme.get("genres", [])


def get_theme_channels(theme_name: str) -> List[str]:
    """テーマの放送局フィルタを取得"""
    theme = REPORT_THEMES.get(theme_name, {})
    return theme.get("channels", [])


def get_theme_description(theme_name: str) -> str:
    """テーマの説明を取得"""
    theme = REPORT_THEMES.get(theme_name, {})
    return theme.get("description", "")

