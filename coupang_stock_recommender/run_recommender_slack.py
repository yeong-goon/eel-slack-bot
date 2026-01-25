import os
import sys
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# 로컬 모듈을 찾을 수 있도록 스크립트 디렉토리를 Python 경로에 추가합니다.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_loader import load_all_data
from data_processor import process_data
from recommender import calculate_coupang_transfer_recommendations

# --- 설정 ---
SLACK_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.environ.get("TARGET_CHANNEL", "#general")

# Google Cloud 자격증명 파일의 경로를 설정합니다.
# 워크플로우가 스크립트 디렉토리에 'credentials.json' 파일을 생성합니다.
script_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(script_dir, "credentials.json")

# 로컬 개발 환경용: 기본 자격증명 파일이 없으면 지정된 경로의 파일을 사용합니다.
if not os.path.exists(creds_path):
    creds_path = os.path.join(
        script_dir, "credentials", "vocal-airline-291707-6cb22418b6f6.json"
    )


def send_slack_notification(text, file_path=None):
    """슬랙 채널에 메시지를 보내고 선택적으로 파일을 업로드합니다."""
    if not SLACK_TOKEN:
        print("경고: SLACK_BOT_TOKEN을 찾을 수 없습니다. 슬랙 알림을 건너뜁니다.")
        print(f"메시지: {text}")
        return

    client = WebClient(token=SLACK_TOKEN)
    try:
        # 텍스트 메시지 보내기
        client.chat_postMessage(channel=SLACK_CHANNEL, text=text)

        # 파일이 존재하면 업로드하기
        if file_path and os.path.exists(file_path):
            client.files_upload_v2(
                channel=SLACK_CHANNEL,
                file=file_path,
                title="쿠팡 발송 추천 목록",
                initial_comment="상세 추천 목록을 Excel 파일로 첨부합니다.",
            )
        print("슬랙 알림을 성공적으로 보냈습니다.")
    except SlackApiError as e:
        print(f"슬랙 알림 전송 중 오류 발생: {e.response['error']}")


def main():
    """재고 추천 프로세스를 실행하는 메인 함수입니다."""
    print("재고 추천 분석을 시작합니다...")

    # 1. 데이터 로드
    try:
        (
            df_inventory,
            df_rocket,
            df_sales,
            df_bom,
            discontinued_skus,
            coupang_only_skus,
        ) = load_all_data(creds_path=creds_path)
    except Exception as e:
        send_slack_notification(f"데이터 로드 중 오류 발생: {e}")
        return

    # 2. 데이터 처리
    try:
        df_final, _ = process_data(df_inventory, df_rocket, df_sales, df_bom)
    except Exception as e:
        send_slack_notification(f"데이터 처리 중 오류 발생: {e}")
        return

    # 3. 추천 목록 생성
    if df_final.empty:
        send_slack_notification("분석할 데이터가 없습니다.")
        return

    try:
        df_reco = calculate_coupang_transfer_recommendations(
            df_final,
            df_bom=df_bom,
            coupang_safety_days=30,
            coupang_only_skus=coupang_only_skus,
            discontinued_skus=discontinued_skus,
        )

        if df_reco.empty:
            send_slack_notification(
                "현재 쿠팡으로 배송할 상품이 없습니다 (재고 충분)."
            )
        else:
            # 쿠팡재고 = 0 (즉시 품절)인 상품 필터링
            stockout_mask = df_reco["쿠팡재고"] == 0
            stockout_products = df_reco[stockout_mask]
            stockout_count = len(stockout_products)

            # 품절 상품 개수와 목록으로 메시지 생성
            msg = f"🚨 *즉시 품절 상품: {stockout_count}개*\n\n"

            if stockout_count > 0:
                for _, row in stockout_products.iterrows():
                    product_name = str(row["상품명"])
                    msg += f"• {product_name}\n"

            send_slack_notification(msg)

    except Exception as e:
        send_slack_notification(f"추천 분석 중 오류 발생: {e}")


if __name__ == "__main__":
    main()