import pandas as pd
from config import EXCLUDED_SKU_PREFIXES

# --- 컬럼명 상수 ---

# 원본 시트의 컬럼명 (data_loader에서 사용된 이름과 일치)
SRC_INV_SKU = "옵션ID_이이엘"
SRC_INV_PRODUCT_NAME_OLD = "상품명"
SRC_INV_PRODUCT_NAME_NEW = "구분값"
SRC_INV_STOCK_MAIN = "한국창고재고"
SRC_INV_ORDER_QTY = "최근발주 완료수량"
SRC_INV_SHIPPING = "배송중"
SRC_INV_AVG_PRICE = "평균가격 (25.06.09기준)"
SRC_INV_MIN_ORDER_QTY = "최소발주수량"
SRC_INV_COUPANG_OPTION_CODE = "쿠팡로켓_옵션코드"

SRC_ROCKET_OPTION_ID = "Option ID"
SRC_ROCKET_STOCK = "Orderable quantity (real-time)"
SRC_ROCKET_INBOUND = "Pending inbounds (real-time)"
SRC_ROCKET_SALES_7D = "Recent sales quantity Last 7 days"
SRC_ROCKET_SALES_30D = "Recent sales quantity Last 30 days"

SRC_SALES_SKU = "옵션관리코드"
SRC_SALES_QTY = "수량"
SRC_SALES_DATE = "날짜"

# 전처리 후 최종적으로 사용될 컬럼명
COL_SKU = "sku"
COL_PRODUCT_NAME = "상품명"
COL_STOCK_MAIN = "메인창고_재고"
COL_COUPANG_OPTION_CODE = "쿠팡로켓_옵션코드"

COL_STOCK_COUPANG = "쿠팡재고"
COL_INBOUND_COUPANG = "쿠팡입고예정"
COL_SALES_7D_COUPANG = "쿠팡_7일_판매량"
COL_SALES_30D_COUPANG = "쿠팡_30일_판매량"
COL_DIRECT_SALES_30D_COUPANG = (
    "쿠팡_30일_순수판매량"  # [추가] 세트 분배 전 순수 판매량 보존용
)

COL_SALES_7D_OWN = "최근7일_자사몰스토어_판매량"
COL_SALES_30D_OWN = "월간_자사몰스토어_판매량"

COL_SALES_30D_TOTAL = "30일_전체판매량"
COL_SALES_7D_TOTAL = "7일_전체판매량"

# BOM(세트구성품) 관련 컬럼
COL_SET_ID = "세트_ID"
COL_BOM_COMPONENT_SKU = "구성품_sku_추출"

# 계산 과정에서 사용되는 임시 컬럼
COL_TEMP_DIST_SALES_COUPANG = "세트분배_쿠팡판매량"
COL_TEMP_DIST_SALES_OWN = "세트분배_자사몰판매량"


def clean_numeric_column(series):
    """쉼표를 제거하고 숫자형으로 변환하는 도우미 함수"""
    return pd.to_numeric(
        series.astype(str).str.replace(",", ""), errors="coerce"
    ).fillna(0)


def process_data(df_inventory, df_rocket, df_sales, df_bom):
    """
    각 시트의 데이터를 정제하고 'SKU'를 기준으로 통합된 DataFrame을 반환합니다.

    """
    if (
        df_inventory is None or df_rocket is None or df_sales is None
    ):  # df_bom은 없을 수도 있으므로 체크에서 제외
        print("입력 데이터 중 일부가 없어 처리를 중단합니다.")
        return pd.DataFrame(), []

    # --- 1. '재고 시트' 전처리 ---
    # '상품명' 컬럼이 중복될 수 있으므로, 이름 변경 전에 기존 '상품명' 컬럼을 삭제합니다.
    # '구분값'을 '상품명'으로 사용할 것이므로 기존 '상품명'은 필요하지 않습니다.
    if SRC_INV_PRODUCT_NAME_OLD in df_inventory.columns:
        df_inventory.drop(columns=[SRC_INV_PRODUCT_NAME_OLD], inplace=True)

    df_inventory.rename(
        columns={SRC_INV_SKU: COL_SKU, SRC_INV_PRODUCT_NAME_NEW: COL_PRODUCT_NAME},
        inplace=True,
    )

    inv_numeric_cols = [SRC_INV_STOCK_MAIN]
    for col in inv_numeric_cols:
        if col in df_inventory.columns:
            df_inventory[col] = clean_numeric_column(df_inventory[col])

    # 컬럼명 변경
    df_inventory.rename(columns={SRC_INV_STOCK_MAIN: COL_STOCK_MAIN}, inplace=True)

    ## 분석에 필요한 컬럼만 선택합니다.
    inventory_cols_to_keep = [
        COL_SKU,
        COL_PRODUCT_NAME,
        COL_STOCK_MAIN,
        COL_COUPANG_OPTION_CODE,
    ]
    df_inventory_processed = df_inventory[
        [col for col in inventory_cols_to_keep if col in df_inventory.columns]
    ].copy()

    print("'재고 시트' 전처리 완료.")

    # --- 2. '로켓그로스재고' 시트 전처리 ---
    ## '재고 시트'의 매핑 정보를 사용하여 쿠팡 Option ID를 우리 시스템 SKU로 변환합니다.
    if COL_COUPANG_OPTION_CODE in df_inventory_processed.columns:
        # 1. '재고 시트'에서 매핑 테이블(sku <-> 쿠팡_Option_ID) 생성
        mapping_table = (
            df_inventory_processed[[COL_SKU, COL_COUPANG_OPTION_CODE]].dropna().copy()
        )
        mapping_table[COL_COUPANG_OPTION_CODE] = mapping_table[
            COL_COUPANG_OPTION_CODE
        ].astype(str)
        df_rocket[SRC_ROCKET_OPTION_ID] = df_rocket[SRC_ROCKET_OPTION_ID].astype(str)

        # 2. 로켓그로스 데이터에 매핑 테이블을 병합하여 'sku' 컬럼 생성
        df_rocket = pd.merge(
            df_rocket,
            mapping_table,
            left_on=SRC_ROCKET_OPTION_ID,
            right_on=COL_COUPANG_OPTION_CODE,
            how="left",
        )
        print("'재고 시트'의 매핑 정보를 사용하여 'sku'를 연결했습니다.")
    else:
        print(
            f"경고: '재고 시트'에 '{COL_COUPANG_OPTION_CODE}' 컬럼이 없습니다. 쿠팡 데이터가 통합되지 않습니다."
        )
        df_rocket[COL_SKU] = None  # 매핑 시트가 없으면 sku를 비워둡니다.

    rocket_numeric_cols = [
        SRC_ROCKET_STOCK,
        SRC_ROCKET_INBOUND,
        SRC_ROCKET_SALES_7D,
        SRC_ROCKET_SALES_30D,
    ]
    for col in rocket_numeric_cols:
        if col in df_rocket.columns:
            df_rocket[col] = clean_numeric_column(df_rocket[col])
            # [추가] 매출량이 음수면 0으로 처리
            if col in [SRC_ROCKET_SALES_7D, SRC_ROCKET_SALES_30D]:
                df_rocket[col] = df_rocket[col].clip(lower=0)

    ## 분석에 필요한 컬럼만 선택하고, 이름 변경
    rocket_cols_to_keep = {
        COL_SKU: COL_SKU,
        SRC_ROCKET_STOCK: COL_STOCK_COUPANG,
        SRC_ROCKET_INBOUND: COL_INBOUND_COUPANG,
        SRC_ROCKET_SALES_7D: COL_SALES_7D_COUPANG,
        SRC_ROCKET_SALES_30D: COL_SALES_30D_COUPANG,
    }
    df_rocket_processed = df_rocket[
        [col for col in rocket_cols_to_keep if col in df_rocket.columns]
    ].copy()
    df_rocket_processed.rename(columns=rocket_cols_to_keep, inplace=True)
    df_rocket_processed = df_rocket_processed.dropna(
        subset=[COL_SKU]
    )  # sku가 없는 데이터(매핑실패) 제거

    # [수정] 쿠팡 재고와 입고 예정 재고를 합산하여 '쿠팡재고'로 통합
    df_rocket_processed[COL_STOCK_COUPANG] = df_rocket_processed.get(
        COL_STOCK_COUPANG, 0
    ) + df_rocket_processed.get(COL_INBOUND_COUPANG, 0)
    # 더 이상 필요 없는 '쿠팡입고예정' 컬럼은 삭제
    if COL_INBOUND_COUPANG in df_rocket_processed.columns:
        df_rocket_processed.drop(columns=[COL_INBOUND_COUPANG], inplace=True)

    print("'로켓그로스재고' 시트 전처리 완료.")

    # --- 3. '매출시트' 전처리 ---
    # '옵션정보' 컬럼에서 코드를 추출해야 할 수도 있습니다. 우선 '상품번호'로 가정합니다.
    df_sales.rename(columns={SRC_SALES_SKU: COL_SKU}, inplace=True)

    sales_numeric_cols = [SRC_SALES_QTY]  # '수량'을 숫자형으로 변환
    for col in sales_numeric_cols:
        if col in df_sales.columns:
            df_sales[col] = clean_numeric_column(df_sales[col])

    # '날짜' 컬럼이 존재하고, 올바른 형식인지 확인합니다.
    if SRC_SALES_DATE in df_sales.columns:
        try:
            # [수정] 다양한 형식의 날짜를 파싱하고, 시간대를 UTC로 통일합니다.
            # utc=True 옵션은 시간대 정보가 없는 날짜에 UTC 시간대를 부여하고,
            # 시간대 정보가 있는 날짜는 UTC로 변환합니다.
            parsed_dates = pd.to_datetime(
                df_sales[SRC_SALES_DATE], format="mixed", errors="coerce", utc=True
            )

            df_sales[SRC_SALES_DATE] = parsed_dates

            # 파싱 후 NaT 값(변환 불가능한 날짜)이 있는 행은 제거
            df_sales = df_sales.dropna(subset=[SRC_SALES_DATE])

            if df_sales[SRC_SALES_DATE].empty:
                print(
                    "경고: '날짜' 컬럼에서 유효한 날짜를 찾을 수 없습니다. 최근 7일 매출액 계산을 건너뜁니다."
                )
                recent_sales = pd.DataFrame(columns=[COL_SKU, COL_SALES_7D_OWN])
            else:
                # 오늘 날짜를 기준으로 7일 전 날짜를 계산합니다.
                # [수정] 비교를 위해 cutoff_date 또한 UTC 시간대를 갖도록 생성합니다.
                cutoff_date = pd.to_datetime("today", utc=True) - pd.Timedelta(days=7)

                # 최근 7일 동안의 매출 데이터를 필터링합니다.
                df_recent_sales = df_sales[df_sales[SRC_SALES_DATE] >= cutoff_date]
                # 'sku'별로 최근 7일 동안의 판매 '수량'을 합산합니다.
                recent_sales = (
                    df_recent_sales.groupby(COL_SKU)[SRC_SALES_QTY].sum().reset_index()
                )
                recent_sales.rename(
                    columns={SRC_SALES_QTY: COL_SALES_7D_OWN}, inplace=True
                )
                print("최근 7일 매출액 집계 완료.")
        except Exception as e:
            print(f"날짜 처리 중 오류 발생: {e}. 최근 7일 매출액 계산을 건너뜁니다.")
            recent_sales = pd.DataFrame(columns=[COL_SKU, COL_SALES_7D_OWN])
    else:
        print(
            "경고: '매출시트'에 '날짜' 컬럼이 없어 최근 7일 매출액 계산을 건너뜁니다. (rename 실패 가능성)"
        )
        recent_sales = pd.DataFrame(columns=[COL_SKU, COL_SALES_7D_OWN])

    # 최근 한달간의 매출이므로, 'sku'별로 판매 '수량'을 합산합니다.
    if COL_SKU in df_sales.columns and SRC_SALES_QTY in df_sales.columns:
        monthly_sales = df_sales.groupby(COL_SKU)[SRC_SALES_QTY].sum().reset_index()
        monthly_sales.rename(columns={SRC_SALES_QTY: COL_SALES_30D_OWN}, inplace=True)
    else:
        print("'매출시트'에 'sku' 또는 '수량' 컬럼이 없어 집계할 수 없습니다.")
        monthly_sales = pd.DataFrame(columns=[COL_SKU, COL_SALES_30D_OWN])

    # 세트 판매량 분배 결과를 저장할 빈 데이터프레임 초기화
    component_sales_coupang = pd.DataFrame(columns=[COL_SKU, COL_SALES_30D_COUPANG])
    component_sales_ownmall = pd.DataFrame(columns=[COL_SKU, COL_SALES_30D_OWN])

    # --- 3-2. 세트 판매량을 단품 판매량으로 분배 ---
    # 새로운 세트 구성 시트 구조에 맞춰 로직을 변경합니다.
    if df_bom is not None and not df_bom.empty:
        print(
            "세트 구성 정보를 바탕으로 판매량 재계산을 시작합니다 (새로운 시트 구조)."
        )

        # 세트 판매량 데이터를 미리 추출합니다.
        set_sales_coupang = df_rocket_processed[
            df_rocket_processed[COL_SKU].isin(df_bom[COL_SET_ID])
        ].copy()
        set_sales_ownmall = monthly_sales[
            monthly_sales[COL_SKU].isin(df_bom[COL_SET_ID])
        ].copy()

        # 1. 세트 구성 정보를 long format으로 변환 (pd.melt 사용)
        id_vars = ["세트명", "옵션", "세트_ID"]
        # '조합'이 포함된 모든 컬럼을 대상으로 melt 수행
        df_bom_melted = df_bom.melt(
            id_vars=id_vars, var_name="조합_컬럼", value_name="값"
        )
        # 값이 없는 행은 제거
        df_bom_melted = df_bom_melted.dropna(subset=["값"])
        df_bom_melted = df_bom_melted[df_bom_melted["값"] != ""].copy()

        # '조합_컬럼'에서 '조합번호'와 '타입(옵션/개수)' 분리
        df_bom_melted["조합번호"] = (
            df_bom_melted["조합_컬럼"].str.extract(r"(\d+)").astype(int)
        )
        df_bom_melted["타입"] = (
            df_bom_melted["조합_컬럼"]
            .str.contains("옵션")
            .map({True: "조합_옵션", False: "조합_개수"})
        )

        # '옵션'과 '개수'를 별도 컬럼으로 pivot
        df_bom_long = df_bom_melted.pivot_table(
            index=id_vars + ["조합번호"], columns="타입", values="값", aggfunc="first"
        ).reset_index()

        # NaN 또는 빈 문자열인 '조합_옵션' 제거
        df_bom_long = df_bom_long[
            df_bom_long["조합_옵션"].notna() & (df_bom_long["조합_옵션"] != "")
        ].copy()

        # 2. '조합_옵션'에서 SKU 추출 및 '조합_개수' 타입 변환
        df_bom_long[COL_BOM_COMPONENT_SKU] = (
            df_bom_long["조합_옵션"].astype(str).str.split("/").str[0]
        )
        df_bom_long["구성품_개수"] = clean_numeric_column(df_bom_long["조합_개수"])

        # 3. 세트 판매량 -> 단품 판매량 분배 (쿠팡)
        if not set_sales_coupang.empty:
            # 세트 판매 정보와 세트 구성 정보 병합
            bom_coupang_sales = pd.merge(
                df_bom_long,
                set_sales_coupang,
                left_on=COL_SET_ID,
                right_on=COL_SKU,
                how="left",
            )
            # 단품 레벨로 판매량 분배 (NaN은 0으로 처리 후 계산)
            bom_coupang_sales[COL_SALES_30D_COUPANG] = (
                bom_coupang_sales[COL_SALES_30D_COUPANG].fillna(0)
                * bom_coupang_sales["구성품_개수"]
            )
            # '구성품_옵션' 기준으로 판매량 집계
            component_sales_coupang = (
                bom_coupang_sales.groupby(COL_BOM_COMPONENT_SKU)[COL_SALES_30D_COUPANG]
                .sum()
                .reset_index()
            )
            component_sales_coupang.rename(
                columns={COL_BOM_COMPONENT_SKU: COL_SKU}, inplace=True
            )

        # 4. 세트 판매량 -> 단품 판매량 분배 (자사몰/스토어)
        if not set_sales_ownmall.empty:
            # 세트 판매 정보와 세트 구성 정보 병합
            bom_ownmall_sales = pd.merge(
                df_bom_long, set_sales_ownmall, left_on=COL_SET_ID, right_on=COL_SKU
            )

            # 단품 레벨로 판매량 분배 (NaN은 0으로 처리 후 계산)
            bom_ownmall_sales[COL_SALES_30D_OWN] = (
                bom_ownmall_sales[COL_SALES_30D_OWN].fillna(0)
                * bom_ownmall_sales["구성품_개수"]
            )
            # '구성품_옵션' 기준으로 판매량 집계
            component_sales_ownmall = (
                bom_ownmall_sales.groupby(COL_BOM_COMPONENT_SKU)[COL_SALES_30D_OWN]
                .sum()
                .reset_index()
            )
            component_sales_ownmall.rename(
                columns={COL_BOM_COMPONENT_SKU: COL_SKU}, inplace=True
            )
        print("세트 판매량 분배 완료.")
    else:
        print("경고: '세트구성품' 데이터가 없어 판매량 재계산을 건너뜁니다.")

    # --- 4. 데이터 통합 ---
    # 1. 재고 + 로켓그로스 재고 병합
    # 'outer' join을 사용하여 한쪽에만 있는 상품도 포함시킵니다.
    df_merged = pd.merge(
        df_inventory_processed, df_rocket_processed, on=COL_SKU, how="outer"
    )
    print("1차 병합 (재고 + 로켓) 완료.")

    # 2. 위 결과에 매출 데이터 병합
    df_final = pd.merge(df_merged, monthly_sales, on=COL_SKU, how="left")
    print("2차 병합 (매출 추가) 완료.")

    # 3차 병합 (최근 7일 매출액 추가)
    df_final = pd.merge(df_final, recent_sales, on=COL_SKU, how="left")
    print("3차 병합 (최근 7일 매출액 추가) 완료.")

    # --- 4-2. 분배된 세트 판매량 추가 병합 ---
    if not component_sales_coupang.empty:
        df_final = pd.merge(
            df_final,
            component_sales_coupang.rename(
                columns={COL_SALES_30D_COUPANG: COL_TEMP_DIST_SALES_COUPANG}
            ),
            on=COL_SKU,
            how="left",
        )
        print("세트 분배 쿠팡 판매량 추가 병합 완료.")
    if not component_sales_ownmall.empty:
        df_final = pd.merge(
            df_final,
            component_sales_ownmall.rename(
                columns={COL_SALES_30D_OWN: COL_TEMP_DIST_SALES_OWN}
            ),
            on=COL_SKU,
            how="left",
        )
        print("세트 분배 자사몰/스토어 판매량 추가 병합 완료.")

    # --- 5. 최종 데이터 정제 ---
    # NaN 값을 0으로 채우고, 단품 판매량과 세트에서 분배된 판매량을 합산합니다.
    # [추가] 합산하기 전에 쿠팡 순수 판매량을 별도 컬럼으로 보존합니다 (입고 추천 시 단품 과다 입고 방지용)
    df_final[COL_DIRECT_SALES_30D_COUPANG] = df_final.get(
        COL_SALES_30D_COUPANG, 0
    ).fillna(0)

    df_final[COL_SALES_30D_COUPANG] = df_final.get(COL_SALES_30D_COUPANG, 0).fillna(
        0
    ) + df_final.get(COL_TEMP_DIST_SALES_COUPANG, 0).fillna(0)
    df_final[COL_SALES_30D_OWN] = df_final.get(COL_SALES_30D_OWN, 0).fillna(
        0
    ) + df_final.get(COL_TEMP_DIST_SALES_OWN, 0).fillna(0)

    # [추가] 30일 및 7일 전체 판매량 컬럼 생성
    df_final[COL_SALES_30D_TOTAL] = df_final.get(COL_SALES_30D_OWN, 0) + df_final.get(
        COL_SALES_30D_COUPANG, 0
    )
    df_final[COL_SALES_7D_TOTAL] = df_final.get(COL_SALES_7D_OWN, 0).fillna(
        0
    ) + df_final.get(COL_SALES_7D_COUPANG, 0).fillna(0)

    # 병합 과정에서 생긴 다른 숫자 컬럼들의 NaN(결측치) 값을 0으로 채웁니다.
    # 예를 들어, 쿠팡에만 있는 상품은 '메인창고_재고'가 NaN일 수 있으므로 0으로 변경합니다.
    final_numeric_cols = [
        COL_STOCK_MAIN,
        COL_STOCK_COUPANG,
        COL_SALES_7D_COUPANG,
        COL_SALES_30D_COUPANG,
        COL_SALES_30D_OWN,
        COL_SALES_7D_OWN,
        COL_SALES_30D_TOTAL,
        COL_SALES_7D_TOTAL,
        COL_DIRECT_SALES_30D_COUPANG,
    ]
    for col in final_numeric_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna(0)
            df_final[col] = df_final[col].astype(int)

    # 임시로 사용했던 세트 분배 컬럼들 제거
    if COL_TEMP_DIST_SALES_COUPANG in df_final.columns:
        df_final.drop(columns=[COL_TEMP_DIST_SALES_COUPANG], inplace=True)
    if COL_TEMP_DIST_SALES_OWN in df_final.columns:
        df_final.drop(columns=[COL_TEMP_DIST_SALES_OWN], inplace=True)

    # [추가] set_fhb_ 로 시작하는 상품 제외 (없는 상품 처리)
    if COL_SKU in df_final.columns and EXCLUDED_SKU_PREFIXES:
        df_final = df_final[
            ~df_final[COL_SKU].str.startswith(tuple(EXCLUDED_SKU_PREFIXES), na=False)
        ]

    print("최종 데이터 통합 및 정제 완료.")

    # 세트 상품 SKU 목록 추출
    if df_bom is not None and not df_bom.empty:
        set_item_skus = df_bom[COL_SET_ID].unique().tolist()
    else:
        set_item_skus = []
    return df_final, set_item_skus
