import os
import sys
import pandas as pd

# 로컬 모듈을 찾을 수 있도록 스크립트 디렉토리를 Python 경로에 추가합니다.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_loader import load_all_data
from data_processor import process_data
from recommender import calculate_coupang_transfer_recommendations

# Google Cloud 자격증명 파일의 경로를 설정합니다.
script_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(script_dir, "credentials.json")

# 로컬 개발 환경용: 기본 자격증명 파일이 없으면 지정된 경로의 파일을 사용합니다.
if not os.path.exists(creds_path):
    creds_path = os.path.join(
        script_dir, "credentials", "vocal-airline-291707-6cb22418b6f6.json"
    )

# Excel 출력용 컬럼 (재고 소진 예상일, 쿠팡 재고, 메인 재고는 제외)
OUTPUT_COLUMNS = [
    "상품그룹",
    "sku",
    "상품명",
    "쿠팡재고",
    "쿠팡_재고소진_예상일",
    "입고수량",
]

# 일일 작업 목록의 최대 총 수량 (입고수량의 누적 합계)
DAILY_WORK_QTY_LIMIT = 160


def main():
    """재고 추천 프로세스를 로컬에서 실행하는 메인 함수입니다."""
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
        print(f"데이터 로드 중 오류 발생: {e}")
        return

    # 2. 데이터 처리
    try:
        df_final, _ = process_data(df_inventory, df_rocket, df_sales, df_bom)
    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")
        return

    # 3. 추천 목록 생성
    if df_final.empty:
        print("분석할 데이터가 없습니다.")
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
            print("현재 쿠팡으로 배송할 상품이 없습니다 (재고 충분).")
            return

        # 콘솔 요약
        total_products = len(df_reco)
        total_quantity = int(df_reco["입고수량"].sum())

        print(f"\n{'='*50}")
        print(f"추천 결과 요약")
        print(f"{ '='*50}")
        print(f"총 추천 상품 수: {total_products}개")
        print(f"총 추천 입고 수량: {total_quantity}개")
        print(f"{ '='*50}\n")

        # 그룹 긴급도 순으로 전체 목록 정렬
        # 그룹의 긴급도는 그룹 내 최소 재고 소진 예상일로 결정됩니다.
        df_reco["_group_min_depletion"] = df_reco.groupby("상품그룹")[
            "쿠팡_재고소진_예상일"
        ].transform("min")
        df_reco = df_reco.sort_values(
            by=["_group_min_depletion", "상품그룹", "쿠팡_재고소진_예상일"],
            ascending=[True, True, True],
        ).reset_index(drop=True)

        # Excel 출력용 컬럼 필터링
        available_cols = [col for col in OUTPUT_COLUMNS if col in df_reco.columns]
        df_export = df_reco[available_cols].copy()

        # 1. 전체 추천 목록 저장
        excel_path = os.path.join(script_dir, "recommendation_result_local.xlsx")
        df_export.to_excel(excel_path, index=False)
        print(f"전체 추천 목록 저장 완료: {excel_path}")

        # 2. 일일 작업 목록 저장 (입고수량 누적 합계 160개까지)
        # 정렬 기준: 재고 0개 우선, 그 다음 재고 소진 예상일 (오름차순)
        df_daily = df_reco.copy()
        df_daily["_is_zero_stock"] = (df_daily["쿠팡재고"] == 0).astype(int)
        df_daily = df_daily.sort_values(
            by=["_is_zero_stock", "쿠팡_재고소진_예상일", "쿠팡_일평균_판매량"],
            ascending=[False, True, False],
        ).reset_index(drop=True)
        df_daily["_cumsum"] = df_daily["입고수량"].cumsum()
        df_daily = df_daily[df_daily["_cumsum"] <= DAILY_WORK_QTY_LIMIT].copy()

        # 목록 확정 후 "긴급" 열 추가
        # 조건: 재고 소진 예상일 < 7일 또는 재고 <= 1개
        df_daily["긴급"] = ""
        urgent_condition = (df_daily["쿠팡_재고소진_예상일"] < 7) | (
            df_daily["쿠팡재고"] <= 1
        )
        df_daily.loc[urgent_condition, "긴급"] = "긴급"

        # 160개 리스트업이 완료된 후, 제품군별로 나열
        df_daily = df_daily.sort_values(by=["상품그룹"]).reset_index(drop=True)

        # "긴급" 열을 맨 마지막에 추가하여 일일 작업 목록의 출력 열 정의
        daily_output_cols = available_cols + ["긴급"]
        final_daily_cols = [col for col in daily_output_cols if col in df_daily.columns]
        df_daily = df_daily[final_daily_cols]

        daily_excel_path = os.path.join(script_dir, "daily_work_stocks.xlsx")
        df_daily.to_excel(daily_excel_path, index=False)
        daily_qty = int(df_daily["입고수량"].sum())
        print(
            f"일일 작업 목록 저장 완료: {daily_excel_path} ({len(df_daily)}개 상품, {daily_qty}개 수량)"
        )

    except Exception as e:
        print(f"추천 분석 중 오류 발생: {e}")


if __name__ == "__main__":

    main()
