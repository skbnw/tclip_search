"""
S3データ状況チェックスクリプト

S3バケット内のデータ状況を確認します：
- バケット全体のサイズとオブジェクト数
- プレフィックス別のサイズとオブジェクト数
- コスト計算（S3標準ストレージの料金をベース）
"""

import boto3
import sys
from botocore.exceptions import ClientError
from typing import Dict, Tuple, Optional
from collections import defaultdict

# Windows環境での文字エンコーディング対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# S3設定
S3_BUCKET_NAME = "tclip-raw-data-2025"
S3_REGION = "ap-northeast-1"
S3_CLIENT = boto3.client('s3', region_name=S3_REGION)

# プレフィックス設定
S3_MASTER_PREFIX = "rag/master_text/"
S3_CHUNK_PREFIX = "rag/vector_chunks/"
S3_IMAGE_PREFIX = "rag/images/"

# S3コスト計算用の料金（USD per GB per month）
# ap-northeast-1 (東京) の標準ストレージ料金（2024年11月時点）
S3_STANDARD_STORAGE_COST_PER_GB = 0.025  # $0.025 per GB per month
S3_STANDARD_IA_STORAGE_COST_PER_GB = 0.014  # $0.014 per GB per month (Infrequent Access)

def get_prefix_stats(bucket_name: str, prefix: str) -> Tuple[float, int]:
    """
    特定のプレフィックスのオブジェクトサイズと数を取得
    戻り値: (合計サイズ(GB), オブジェクト数)
    """
    total_bytes = 0
    object_count = 0
    
    try:
        paginator = S3_CLIENT.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_bytes += obj['Size']
                    object_count += 1
        
        total_gb = total_bytes / (1024 ** 3)
        return total_gb, object_count
    
    except ClientError as e:
        print(f"[ERROR] プレフィックス '{prefix}' の取得エラー: {e}")
        return 0.0, 0
    except Exception as e:
        print(f"[ERROR] 予期せぬエラー: {e}")
        return 0.0, 0

def get_bucket_size_estimate(bucket_name: str) -> Tuple[Optional[float], Optional[int], Dict[str, Dict]]:
    """
    S3バケット内の全オブジェクトのサイズを合計して容量を推定する
    戻り値: (合計サイズ(GB), 合計オブジェクト数, プレフィックス別統計)
    """
    total_bytes = 0
    object_count = 0
    prefix_stats = defaultdict(lambda: {'size': 0, 'count': 0})
    
    print(f"[INFO] S3バケット '{bucket_name}' のオブジェクトをリストしています...")
    
    try:
        paginator = S3_CLIENT.get_paginator('list_objects_v2')
        
        # 全オブジェクトをページングで取得
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    size = obj['Size']
                    key = obj['Key']
                    
                    total_bytes += size
                    object_count += 1
                    
                    # プレフィックス別に統計を集計
                    if key.startswith(S3_MASTER_PREFIX):
                        prefix_stats['master_text']['size'] += size
                        prefix_stats['master_text']['count'] += 1
                    elif key.startswith(S3_CHUNK_PREFIX):
                        prefix_stats['vector_chunks']['size'] += size
                        prefix_stats['vector_chunks']['count'] += 1
                    elif key.startswith(S3_IMAGE_PREFIX):
                        prefix_stats['images']['size'] += size
                        prefix_stats['images']['count'] += 1
                    else:
                        prefix_stats['other']['size'] += size
                        prefix_stats['other']['count'] += 1
        
        # バイトをギガバイトに変換
        total_gb = total_bytes / (1024 ** 3)
        
        # プレフィックス別の統計もGBに変換
        for prefix in prefix_stats:
            prefix_stats[prefix]['size'] = prefix_stats[prefix]['size'] / (1024 ** 3)
        
        return total_gb, object_count, dict(prefix_stats)

    except ClientError as e:
        print(f"[ERROR] エラーが発生しました: {e}")
        return None, None, {}
    except Exception as e:
        print(f"[ERROR] 予期せぬエラー: {e}")
        return None, None, {}

def calculate_monthly_cost(size_gb: float, storage_class: str = "standard") -> float:
    """
    S3ストレージの月額コストを計算
    戻り値: 月額コスト（USD）
    """
    if storage_class == "standard":
        cost_per_gb = S3_STANDARD_STORAGE_COST_PER_GB
    elif storage_class == "standard-ia":
        cost_per_gb = S3_STANDARD_IA_STORAGE_COST_PER_GB
    else:
        cost_per_gb = S3_STANDARD_STORAGE_COST_PER_GB
    
    monthly_cost = size_gb * cost_per_gb
    return monthly_cost

def format_size(size_gb: float) -> str:
    """
    サイズを読みやすい形式にフォーマット
    """
    if size_gb >= 1024:
        return f"{size_gb / 1024:.2f} TB"
    elif size_gb >= 1:
        return f"{size_gb:.2f} GB"
    else:
        return f"{size_gb * 1024:.2f} MB"

def check_s3_data_status():
    """
    S3データ状況をチェック
    """
    print("=" * 80)
    print("[INFO] S3データ状況チェックを開始...")
    print("=" * 80)
    
    # バケット全体の統計を取得
    total_gb, total_count, prefix_stats = get_bucket_size_estimate(S3_BUCKET_NAME)
    
    if total_gb is None:
        print("[ERROR] データ取得に失敗しました")
        return
    
    print("\n" + "=" * 80)
    print("[SUMMARY] バケット全体の統計")
    print("=" * 80)
    print(f"  バケット名: {S3_BUCKET_NAME}")
    print(f"  リージョン: {S3_REGION}")
    print(f"  合計オブジェクト数: {total_count:,} 個")
    print(f"  合計サイズ: {format_size(total_gb)} ({total_gb:.2f} GB)")
    
    # プレフィックス別の統計
    print("\n" + "=" * 80)
    print("[SUMMARY] プレフィックス別の統計")
    print("=" * 80)
    
    for prefix_name, stats in prefix_stats.items():
        size_gb = stats['size']
        count = stats['count']
        percentage = (size_gb / total_gb * 100) if total_gb > 0 else 0
        
        print(f"\n  {prefix_name}:")
        print(f"    オブジェクト数: {count:,} 個")
        print(f"    サイズ: {format_size(size_gb)} ({size_gb:.2f} GB)")
        print(f"    全体に占める割合: {percentage:.1f}%")
    
    # コスト計算
    print("\n" + "=" * 80)
    print("[SUMMARY] コスト計算（月額見積もり）")
    print("=" * 80)
    print(f"  ストレージクラス: Standard")
    print(f"  料金: ${S3_STANDARD_STORAGE_COST_PER_GB:.4f} per GB per month")
    
    total_monthly_cost = calculate_monthly_cost(total_gb, "standard")
    print(f"\n  バケット全体の月額コスト: ${total_monthly_cost:.2f} / month")
    print(f"  年間コスト（推定）: ${total_monthly_cost * 12:.2f} / year")
    
    # プレフィックス別のコスト
    print("\n  プレフィックス別のコスト:")
    for prefix_name, stats in prefix_stats.items():
        size_gb = stats['size']
        monthly_cost = calculate_monthly_cost(size_gb, "standard")
        percentage = (monthly_cost / total_monthly_cost * 100) if total_monthly_cost > 0 else 0
        
        print(f"    {prefix_name}: ${monthly_cost:.2f} / month ({percentage:.1f}%)")
    
    # 追加情報
    print("\n" + "=" * 80)
    print("[INFO] 追加情報")
    print("=" * 80)
    print(f"  Standard-IA（低頻度アクセス）に移行した場合:")
    total_ia_cost = calculate_monthly_cost(total_gb, "standard-ia")
    print(f"    月額コスト: ${total_ia_cost:.2f} / month")
    print(f"    年間コスト: ${total_ia_cost * 12:.2f} / year")
    print(f"    節約額: ${(total_monthly_cost - total_ia_cost):.2f} / month")
    print(f"    年間節約額: ${(total_monthly_cost - total_ia_cost) * 12:.2f} / year")
    
    print("\n" + "=" * 80)
    print("[NOTE] コスト計算について")
    print("=" * 80)
    print("  - 料金は ap-northeast-1 (東京) のStandardストレージ料金をベースにしています")
    print("  - 実際のコストは、リクエスト数、データ転送量、ライフサイクルポリシーなどにより異なります")
    print("  - 最新の料金情報は AWS 公式サイトで確認してください:")
    print("    https://aws.amazon.com/s3/pricing/")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    try:
        check_s3_data_status()
    except KeyboardInterrupt:
        print("\n[INFO] 処理が中断されました")
    except Exception as e:
        print(f"\n[ERROR] 予期せぬエラー: {e}")
        import traceback
        traceback.print_exc()

