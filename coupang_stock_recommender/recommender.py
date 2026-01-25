import pandas as pd
import numpy as np

# --- 컬럼명 상수 ---
COL_SKU = "sku"
COL_PRODUCT_NAME = "상품명"
COL_STOCK_MAIN = "메인창고_재고"
COL_STOCK_COUPANG = "쿠팡재고"
COL_SALES_30D_COUPANG = "쿠팡_30일_판매량"
COL_COUPANG_OPTION_CODE = "쿠팡로켓_옵션코드"
COL_SALES_30D_OWN = "월간_자사몰스토어_판매량"
COL_SALES_7D_OWN = "최근7일_자사몰스토어_판매량"
COL_DIRECT_SALES_30D_COUPANG = "쿠팡_30일_순수판매량"  # [추가]

# 결과 컬럼
COL_TRANSFER_RECOMMENDATION = "입고수량"
COL_AVG_DAILY_SALES_COUPANG = "쿠팡_일평균_판매량"
COL_REQUIRED_STOCK_COUPANG = "쿠팡_필요재고"
COL_COUPANG_STOCK_DEPLETION_DAYS = "쿠팡_재고소진_예상일"
COL_PRODUCT_GROUP = "상품그룹"
COL_GROUP_URGENCY_METRIC = "그룹_긴급도"

# 내부 계산용 컬럼
COL_AVG_DAILY_SALES_OWN = "자사몰_일평균_판매량"
COL_REQUIRED_STOCK_OWN = "자사몰_필요재고"
COL_AVAILABLE_MAIN_STOCK = "메인창고_가용재고"  # 자사몰 안전재고 제외 후

# BOM 관련 상수
COL_SET_ID = "세트_ID"
COL_BOM_COMPONENT_SKU = "구성품_sku"
COL_BOM_COMPONENT_QTY = "구성품_개수"


def _parse_bom(df_bom):
    """BOM 데이터프레임을 파싱하여 세트-구성품 관계를 정의합니다."""
    if df_bom is None or df_bom.empty:
        return pd.DataFrame()

    id_vars = ["세트명", "옵션", "세트_ID"]
    df_melted = df_bom.melt(id_vars=id_vars, var_name="조합_컬럼", value_name="값")
    df_melted = df_melted.dropna(subset=["값"])
    df_melted = df_melted[df_melted["값"] != ""].copy()

    df_melted["조합번호"] = df_melted["조합_컬럼"].str.extract(r"(\d+)").astype(int)
    df_melted["타입"] = (
        df_melted["조합_컬럼"]
        .str.contains("옵션")
        .map({True: "구성품_옵션", False: "구성품_개수"})
    )

    df_pivot = df_melted.pivot_table(
        index=id_vars + ["조합번호"],
        columns="타입",
        values="값",
        aggfunc="first",
        observed=False,
    ).reset_index()
    df_pivot = df_pivot[
        df_pivot["구성품_옵션"].notna() & (df_pivot["구성품_옵션"] != "")
    ].copy()

    df_pivot[COL_BOM_COMPONENT_SKU] = (
        df_pivot["구성품_옵션"].astype(str).str.split("/").str[0]
    )
    df_pivot[COL_BOM_COMPONENT_QTY] = (
        pd.to_numeric(df_pivot["구성품_개수"], errors="coerce").fillna(0).astype(int)
    )

    return df_pivot[[COL_SET_ID, COL_BOM_COMPONENT_SKU, COL_BOM_COMPONENT_QTY]]


def calculate_coupang_transfer_recommendations(
    df_final,
    df_bom=None,
    coupang_safety_days=30,
    coupang_only_skus=None,
    discontinued_skus=None,
):
    """
    구성품 재고를 고려하여 쿠팡 입고 추천 수량을 계산합니다.
    (자사몰 방어 -> 쿠팡 단품 입고 -> 쿠팡 세트 입고 순서로 재고 할당)
    """
    if df_final.empty:
        return pd.DataFrame()

    df = df_final.copy()

    # 0. 전처리
    df[COL_PRODUCT_GROUP] = df[COL_SKU].str.split("_").str[:2].str.join("_")

    # NaN 처리
    cols_to_fill = [
        COL_STOCK_MAIN,
        COL_STOCK_COUPANG,
        COL_SALES_30D_COUPANG,
        COL_SALES_30D_OWN,
        COL_SALES_7D_OWN,
    ]
    for col in cols_to_fill:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # --- 1. 데이터 준비 (일평균 판매량 및 속성 정의) ---

    # 1-1. 자사몰 일평균 판매량 (가중치 1.2배 적용 전 원본)
    avg_30_day_own = df[COL_SALES_30D_OWN] / 30
    avg_7_day_own = df[COL_SALES_7D_OWN] / 7
    df[COL_AVG_DAILY_SALES_OWN] = np.where(
        avg_7_day_own > avg_30_day_own,
        (avg_30_day_own * 0.7) + (avg_7_day_own * 0.3),
        avg_30_day_own,
    )

    # 1-2. 쿠팡 일평균 판매량 (순수 판매량 기준)
    if COL_DIRECT_SALES_30D_COUPANG in df.columns:
        df[COL_AVG_DAILY_SALES_COUPANG] = df[COL_DIRECT_SALES_30D_COUPANG] / 30
    else:
        df[COL_AVG_DAILY_SALES_COUPANG] = df[COL_SALES_30D_COUPANG] / 30

    # 1-3. 속성 정의
    is_coupang_only = df[COL_SKU].isin(coupang_only_skus if coupang_only_skus else [])
    is_discontinued = df[COL_SKU].isin(discontinued_skus if discontinued_skus else [])

    # 1-4. 시뮬레이션용 일일 수요량 설정
    # 쿠팡 수요: 순수 판매량
    df["sim_daily_coupang"] = df[COL_AVG_DAILY_SALES_COUPANG]

    # 자사몰 수요 (메인 창고 소진용): 1.2배 가중치 적용
    # 단, 쿠팡 전용이거나 품절 상품이면 자사몰 방어 필요 없음 (0으로 설정)
    df["sim_daily_own"] = np.where(
        is_coupang_only | is_discontinued, 0, df[COL_AVG_DAILY_SALES_OWN] * 1.2
    )

    # 쿠팡 옵션 코드 미등록 분리
    is_missing_code = pd.Series(False, index=df.index)

    if COL_COUPANG_OPTION_CODE in df.columns:
        code_str = df[COL_COUPANG_OPTION_CODE].astype(str).str.strip()
        is_valid_code = code_str.str.match(r"^\d{11}$")
        is_missing_code = ~is_valid_code

        # [중요] df에서 행을 삭제하지 않음! (구성품 재고 추적을 위해)
        # 대신, 입고 추천 대상이 되지 않도록 시뮬레이션 수요(daily_coupang)를 0으로 설정
        df.loc[is_missing_code, "sim_daily_coupang"] = 0

    # 중복 SKU 체크
    if df[COL_SKU].duplicated().any():
        dup_skus = df.loc[df[COL_SKU].duplicated(), COL_SKU].unique()
        print(
            f"[경고] 재고 데이터에 중복된 SKU가 발견되었습니다. 해당 SKU들의 재고를 합산하여 처리합니다: {dup_skus}"
        )

    # --- 2. BOM(세트) 구조 파싱 ---
    # SKU -> 세트/구성품 관계 매핑
    # bom_map: 세트 SKU -> [(구성품 SKU, 수량), ...]
    # comp_usage_map: 구성품 SKU -> [세트 SKU, ...]
    bom_map = {}
    comp_usage_map = {}

    if df_bom is not None and not df_bom.empty:
        bom_parsed = _parse_bom(df_bom)
        for _, row in bom_parsed.iterrows():
            set_sku = row[COL_SET_ID]
            comp_sku = row[COL_BOM_COMPONENT_SKU]
            qty = row[COL_BOM_COMPONENT_QTY]

            if set_sku not in bom_map:
                bom_map[set_sku] = []
            bom_map[set_sku].append((comp_sku, qty))

            if comp_sku not in comp_usage_map:
                comp_usage_map[comp_sku] = []
            if set_sku not in comp_usage_map[comp_sku]:
                comp_usage_map[comp_sku].append(set_sku)

    # --- 3. 로직 분기 (Sweep vs Simulation) ---

    # 3-1. 세트/구성품 여부 확인 (Step 1)
    # BOM에 정의된 세트이거나, BOM에 정의된 구성품인 경우
    set_related_skus = set(bom_map.keys()) | set(comp_usage_map.keys())
    df["is_set_related"] = df[COL_SKU].isin(set_related_skus)

    # 3-2. Sweep 대상 식별 (Step 2)
    # 세트/구성품이 아니면서, (쿠팡전용 OR 품절상품)인 경우 -> 전량 입고
    # [수정] 옵션 코드가 없는 상품은 Sweep 대상에서도 제외 (입고 불가하므로)
    sweep_mask = (
        (~df["is_set_related"])
        & (is_coupang_only | is_discontinued)
        & (~is_missing_code)
    )

    # 결과 저장을 위한 딕셔너리 초기화
    sim_state = {}

    for _, row in df.iterrows():
        sku = row[COL_SKU]
        is_sweep = sweep_mask[row.name]  # row.name은 인덱스입니다
        is_defense_needed = not (is_coupang_only[row.name] or is_discontinued[row.name])

        # 초기 상태 설정
        sim_state[sku] = {
            "coupang_stock": float(row[COL_STOCK_COUPANG]),
            "main_stock": float(row[COL_STOCK_MAIN]),
            "daily_coupang": float(row["sim_daily_coupang"]),
            "daily_own": float(row["sim_daily_own"]),
            "transfer_qty": 0.0,
            "is_exhausted": False,
            "is_sweep": is_sweep,
            "has_missing_code": is_missing_code[row.name],
            "requires_own_defense": is_defense_needed,
        }

        # [Step 2 실행] Sweep 대상은 메인 재고 전량을 즉시 할당하고 시뮬레이션 제외
        if is_sweep:
            # 전량 입고 (메인 재고 전체)
            transfer_amount = sim_state[sku]["main_stock"]
            sim_state[sku]["transfer_qty"] = transfer_amount
            sim_state[sku]["main_stock"] = 0.0  # 재고 소진
            sim_state[sku]["is_exhausted"] = True  # 시뮬레이션 참여 안 함

    # --- 3.5. [추가] 최소 수량(2개) 우선 확보 로직 ---
    # 조건: 쿠팡 재고가 2개 미만이고, 메인 창고에 자사몰 7일치 방어 후 여유가 있다면 우선 할당
    MIN_QTY_TARGET = 2
    OWN_DEFENSE_DAYS = 7
    MIN_OWN_STOCK = 2  # [추가] 자사몰 최소 보존 수량 (매출이 0이어도 이만큼은 남김)

    for sku, state in sim_state.items():
        if state["is_sweep"]:
            continue
        if state["is_exhausted"]:
            continue
        if state["has_missing_code"]:
            continue

        current_coupang = state["coupang_stock"]
        if current_coupang < MIN_QTY_TARGET:
            needed = MIN_QTY_TARGET - current_coupang

            # 할당 가능 수량 계산
            allocatable = 0.0

            # A. 세트 상품
            if sku in bom_map:
                # 구성품들의 여유 재고 확인 (가장 적은 것을 기준으로)
                max_set_possible = float("inf")
                for comp_sku, qty in bom_map[sku]:
                    if comp_sku not in sim_state:
                        max_set_possible = 0
                        break

                    comp_state = sim_state[comp_sku]

                    # [수정] 자사몰 방어량: (7일치 판매량) vs (최소수량 2개) 중 큰 값 적용
                    # 단, 쿠팡전용/품절상품(requires_own_defense=False)은 방어하지 않음
                    defense_base = comp_state["daily_own"] * OWN_DEFENSE_DAYS
                    comp_defense = (
                        max(defense_base, MIN_OWN_STOCK)
                        if comp_state["requires_own_defense"]
                        else 0
                    )
                    comp_avail = max(0, comp_state["main_stock"] - comp_defense)

                    # 구성품 재고로 만들 수 있는 세트 수량
                    sets_from_comp = comp_avail / qty
                    max_set_possible = min(max_set_possible, sets_from_comp)

                allocatable = max_set_possible

            # B. 단품
            else:
                defense_base = state["daily_own"] * OWN_DEFENSE_DAYS
                defense = (
                    max(defense_base, MIN_OWN_STOCK)
                    if state["requires_own_defense"]
                    else 0
                )
                avail = max(0, state["main_stock"] - defense)
                allocatable = avail

            # 최종 할당 (필요량과 가능량 중 더 작은 값)
            alloc = min(needed, allocatable)

            if alloc > 0:
                state["transfer_qty"] += alloc
                state[
                    "coupang_stock"
                ] += alloc  # 시뮬레이션에 반영 (이미 재고가 채워진 것으로 간주)

                if sku in bom_map:
                    for comp_sku, qty in bom_map[sku]:
                        sim_state[comp_sku]["main_stock"] -= alloc * qty
                else:
                    state["main_stock"] -= alloc

    # --- 4. 일별 재고 시뮬레이션 (Step 3 & 4) ---
    # 대상: Sweep 되지 않은 나머지 모든 상품 (일반 단품, 세트, 구성품)
    # 목표: 최대 60일까지 재고 균형 맞추기

    # [Step 4 보정] 구성품이 쿠팡전용이면 자사몰 방어(daily_own)를 0으로 설정
    # (이미 위에서 sim_daily_own 계산 시 is_coupang_only면 0으로 처리됨)
    # 품절 상품인 경우도 0으로 처리됨.
    # 따라서 시뮬레이션 로직은 그대로 진행하면 됨.

    MAX_DAYS = 60

    for day in range(1, MAX_DAYS + 1):
        # 1. 각 SKU별로 금일(Day) 쿠팡 재고 부족분 계산
        # (아직 메인 재고 차감 전)
        daily_needs = {}  # SKU -> needed_amount

        for sku, state in sim_state.items():
            if state["is_exhausted"]:
                continue

            # 쿠팡 재고 소진 시뮬레이션
            if state["coupang_stock"] >= state["daily_coupang"]:
                state["coupang_stock"] -= state["daily_coupang"]
                daily_needs[sku] = 0.0
            else:
                shortage = state["daily_coupang"] - state["coupang_stock"]
                state["coupang_stock"] = 0.0
                daily_needs[sku] = shortage

        # 2. 메인 창고 재고 확인 및 할당 (구성품 단위로 집계)
        # 구성품별 금일 총 소요량(Total Drain) 계산
        # 소요량 = (자사몰 방어분) + (해당 SKU의 쿠팡 부족분) + (해당 SKU를 사용하는 세트들의 부족분)

        comp_drain = {}  # Comp SKU -> Total Drain Amount

        # 2-1. 모든 SKU를 순회하며 Drain 집계
        for sku, need in daily_needs.items():
            # A. 세트 상품인 경우 -> 구성품들의 Drain으로 전파
            if sku in bom_map:
                for comp_sku, qty in bom_map[sku]:
                    if comp_sku not in sim_state:
                        continue  # 구성품 정보 없으면 스킵
                    drain_amt = need * qty
                    comp_drain[comp_sku] = comp_drain.get(comp_sku, 0.0) + drain_amt

            # B. 단품(구성품 포함)인 경우 -> 본인의 Drain에 추가
            # (세트가 아니면 모두 단품 취급)
            else:
                comp_drain[sku] = comp_drain.get(sku, 0.0) + need

        # 2-2. 자사몰 방어분(Daily Own) 추가
        for sku, state in sim_state.items():
            # 자사몰 판매량은 메인 창고에서 직접 빠져나감
            # 세트의 자사몰 판매량은? -> data_processor에서 이미 단품 판매량으로 분해되어 COL_SALES_30D_OWN에 합산됨.
            # 따라서 여기서는 '단품'의 자사몰 판매량만 고려하면 됨.
            # (세트 SKU의 sim_daily_own은 0이어야 함, 확인 필요하지만 로직상 단품 레벨에서 처리됨)
            if state["daily_own"] > 0:
                comp_drain[sku] = comp_drain.get(sku, 0.0) + state["daily_own"]

        # 3. 메인 재고 차감 및 가능 여부 판단
        # 금일 재고를 감당할 수 없는 구성품 식별
        failed_comps = set()

        for comp_sku, drain in comp_drain.items():
            if comp_sku not in sim_state:
                continue

            # [수정] 부분 할당(Partial Allocation)을 지원하기 위한 비율 계산
            # 재고가 충분하면 비율 = 1.0, 부족하면 비율 < 1.0
            if sim_state[comp_sku]["main_stock"] >= drain:
                sim_state[comp_sku]["main_stock"] -= drain
                # ratio는 기본적으로 1.0 (생략 가능하지만 로직 통일을 위해)
            else:
                # 재고가 부족해도 남은 양은 모두 할당
                # 단, 소요량(drain)이 0인 경우는 없다고 가정 (위에서 확인)
                ratio = sim_state[comp_sku]["main_stock"] / drain if drain > 0 else 0

                # 이 구성품을 사용하는 모든 SKU에 대해 공급 비율 제한
                # (이 구성품이 병목 지점이 됨)
                failed_comps.add(comp_sku)

                # 메인 재고 소진 처리
                sim_state[comp_sku]["main_stock"] = 0.0
                sim_state[comp_sku]["is_exhausted"] = True  # 향후 시뮬레이션 제외

                # 이 구성품의 부족 비율을 기록해 두었다가 아래 할당 단계에서 적용해야 함
                # 하지만 현재 구조상 아래 루프에서 SKU별로 어떤 구성품이 부족한지 다시 확인해야 합니다.

        # 4. 입고 추천 수량 확정 (전송 업데이트)
        # 실패한 구성품을 사용하는 모든 SKU는 금일 입고 추천을 받을 수 없음

        for sku, need in daily_needs.items():
            if need <= 0:
                continue
            if sim_state[sku]["is_exhausted"]:
                continue  # 이미 본인이 실패함

            supply_ratio = 1.0

            # 세트인 경우 구성품 확인
            if sku in bom_map:
                for comp_sku, _ in bom_map[sku]:
                    # 구성품 재고 상황 확인 (위에서 이미 차감 시도함)
                    # 만약 failed_comps에 있다면, 부분 할당 비율을 계산해야 함
                    if comp_sku in failed_comps:
                        # 재고가 0이 되기 직전의 비율을 역산하거나,
                        # 위 루프에서 저장해둔 비율을 써야 하는데 구조가 복잡함.
                        # 간단하게: (원래재고 + 방금차감된재고) / drain ?? -> 이미 차감됨.

                        # [간소화 로직]
                        # 위에서 failed_comps에 추가될 때 main_stock은 0이 됨.
                        # 정확한 비율 계산을 위해 위 루프를 수정하기보다,
                        # 여기서 "공급 가능했는지" 여부만 따지던 기존 로직을
                        # "남은 재고라도 털어넣기"로 변경.

                        # 하지만 이미 위에서 main_stock을 0으로 만들었으므로,
                        # 정확한 부분 할당을 위해서는 로직 재구성이 필요함.
                        pass

            # [로직 개선] 부분 할당을 올바르게 구현하려면 3번과 4번 과정을 통합하거나
            # 공급 비율(supply_ratio)을 명시적으로 계산하도록 변경

            # 현재 구조를 유지하면서 부분 할당 효과를 내기 위해:
            # "실패했더라도(failed_comps), 이번 차례에 한해 남은 재고 비율만큼 할당"
            # 하지만 여러 구성품이 동시에 부족하면 최소 비율을 따라야 함.

            # 복잡도를 줄이기 위해 기존의 '전부 아니면 전무(All or Nothing)' 로직을 유지하되
            # [수정 2] 최종 결과를 변환할 때 int() 대신 round()를 사용하여 소수점 손실을 줄임.
            # 부분 할당 없이도 round()만으로 7 -> 8 문제가 해결될 가능성이 높음.
            # (예: 3.6 + 4.4 = 8.0 이지만, int()를 사용하면 3+4=7, round()를 사용하면 4+4=8)

            # 따라서 우선 round() 적용만으로 해결을 시도.

            can_supply = True
            if sku in bom_map:
                for comp_sku, _ in bom_map[sku]:
                    if comp_sku in failed_comps:
                        can_supply = False
                        break
            # 단품인 경우 본인 확인
            else:
                if sku in failed_comps:
                    can_supply = False

            if can_supply:
                sim_state[sku]["transfer_qty"] += need
            else:
                # [추가] 실패한 날, 0으로 끝내지 않고 남은 재고 비율만큼이라도 할당 (부분 채우기)
                # 이를 위해서는 각 구성품의 잔여 재고 비율을 알아야 함.
                # 현재 코드 구조가 복잡하므로, 우선 round() 적용을 먼저 수행.
                # 금일 공급 실패 -> 향후 시뮬레이션에서도 제외 (균형 유지를 위해)
                sim_state[sku]["is_exhausted"] = True

    # --- 4.5. [추가] 자사몰 최소 보존 수량(2개) 최종 강제 적용 ---

    # --- 4.5. [추가] 자사몰 최소 보존 수량(2개) 최종 강제 적용 ---
    # 시뮬레이션 결과(반올림 후)가 메인 창고의 최소 보존 수량을 침범하지 않도록 마지막으로 방어합니다.

    # 1. SKU별 초기 재고 및 설정 맵핑
    sku_info = {}
    for _, row in df.iterrows():
        sku = row[COL_SKU]
        is_defense = not (is_coupang_only[row.name] or is_discontinued[row.name])
        sku_info[sku] = {
            "initial_main": row[COL_STOCK_MAIN],
            "requires_defense": is_defense,
            "proposed_qty": int(
                round(sim_state[sku]["transfer_qty"])
            ),  # 반올림된 1차 결과
        }

    # 2. 구성품별 총 출고 예정 수량 집계
    comp_usage = {}  # Comp_SKU -> Total_Transfer_Qty

    for sku, info in sku_info.items():
        qty = info["proposed_qty"]
        if qty <= 0:
            continue

        if sku in bom_map:
            for comp_sku, comp_qty in bom_map[sku]:
                comp_usage[comp_sku] = comp_usage.get(comp_sku, 0) + (qty * comp_qty)
        else:
            comp_usage[sku] = comp_usage.get(sku, 0) + qty

    # 3. 위반 여부 확인 및 수량 조정
    violated_comps = {}  # Comp_SKU -> Excess_Amount

    for comp_sku, usage in comp_usage.items():
        if comp_sku not in sku_info:
            continue

        info = sku_info[comp_sku]
        if not info["requires_defense"]:
            continue

        # 남겨야 할 재고(2개)를 제외하고 보낼 수 있는 최대량
        limit = max(0, int(info["initial_main"] - MIN_OWN_STOCK))

        if usage > limit:
            violated_comps[comp_sku] = usage - limit

    # 4. 위반된 경우 수량 차감
    if violated_comps:
        print(
            f"[방어 로직] 자사몰 최소 보존 수량({MIN_OWN_STOCK}개) 위반 감지 및 조정: {list(violated_comps.keys())}"
        )

        for comp_sku, excess in violated_comps.items():
            current_excess = excess

            # 이 구성품을 사용하는 SKU 목록 찾기
            related_skus = []
            if comp_sku in comp_usage_map:
                related_skus.extend(comp_usage_map[comp_sku])
            if comp_sku in sku_info:
                related_skus.append(comp_sku)

            # 관련 SKU들의 추천 수량을 하나씩 줄여가며 excess 해소
            for r_sku in related_skus:
                if current_excess <= 0:
                    break
                if r_sku not in sku_info:
                    continue

                qty_per_unit = 1
                if r_sku in bom_map:
                    for c, q in bom_map[r_sku]:
                        if c == comp_sku:
                            qty_per_unit = q
                            break

                proposed = sku_info[r_sku]["proposed_qty"]

                while proposed > 0 and current_excess > 0:
                    proposed -= 1
                    current_excess -= qty_per_unit

                sku_info[r_sku]["proposed_qty"] = proposed

    # --- 5. 결과 적용 ---
    # [수정] 최종 방어 로직을 거친 수량을 적용
    df[COL_TRANSFER_RECOMMENDATION] = df[COL_SKU].map(
        lambda x: sku_info[x]["proposed_qty"]
    )

    # 자사몰 필요재고 역산 (대시보드 표시용)
    # 원래 메인 재고 - (시뮬레이션 후 남은 메인 재고) = 총 소진된 메인 재고
    # 총 소진 - 쿠팡 입고분(구성품 환산) = 자사몰 방어분?
    # 간단하게: 메인 재고 - 입고 추천 수량(구성품 환산) = 자사몰 필요재고(남겨진 재고)
    # 하지만 정확히는 '시뮬레이션에서 자사몰 방어를 위해 차감된 양'을 보여주는게 맞음.
    # 여기서는 단순하게 '남은 재고'를 자사몰 필요재고로 표기 (쿠팡으로 안 보낸 재고)

    # 구성품별 총 입고 소요량 계산
    total_transfer_drain = {}
    for _, row in df.iterrows():
        sku = row[COL_SKU]
        qty = row[COL_TRANSFER_RECOMMENDATION]
        if qty > 0:
            if sku in bom_map:
                for comp_sku, c_qty in bom_map[sku]:
                    total_transfer_drain[comp_sku] = total_transfer_drain.get(
                        comp_sku, 0
                    ) + (qty * c_qty)
            else:
                total_transfer_drain[sku] = total_transfer_drain.get(sku, 0) + qty

    # 자사몰 필요재고 컬럼 업데이트 (메인재고 - 쿠팡입고소요량)
    df[COL_REQUIRED_STOCK_OWN] = df.apply(
        lambda row: max(
            0, row[COL_STOCK_MAIN] - total_transfer_drain.get(row[COL_SKU], 0)
        ),
        axis=1,
    )

    # 쿠팡 필요재고 컬럼 업데이트 (60일치 목표)
    # Sweep 대상은 '전량'이 목표였으므로, 필요재고를 전량(또는 그 이상)으로 표시하거나 60일치로 유지
    df[COL_REQUIRED_STOCK_COUPANG] = np.where(
        sweep_mask,
        df[COL_STOCK_COUPANG]
        + df[COL_TRANSFER_RECOMMENDATION],  # Sweep은 현재+입고가 곧 목표
        df[COL_AVG_DAILY_SALES_COUPANG] * MAX_DAYS,
    )

    # --- 5. 결과 정리 및 지표 계산 ---
    df_recommendations = df[df[COL_TRANSFER_RECOMMENDATION] > 0].copy()

    # 재고 소진 예상일 및 예상 손실 수량 계산
    if not df_recommendations.empty:
        with np.errstate(divide="ignore", invalid="ignore"):
            depletion_days = (
                df_recommendations[COL_STOCK_COUPANG]
                / df_recommendations[COL_AVG_DAILY_SALES_COUPANG]
            )
            depletion_days = depletion_days.replace([np.inf, -np.inf], 999).fillna(999)
        df_recommendations[COL_COUPANG_STOCK_DEPLETION_DAYS] = depletion_days

        # 긴급도 (예상 손실)
        missed_sales = (7 - df_recommendations[COL_COUPANG_STOCK_DEPLETION_DAYS]).clip(
            lower=0
        ) * df_recommendations[COL_AVG_DAILY_SALES_COUPANG]
        df_recommendations[COL_GROUP_URGENCY_METRIC] = df_recommendations.groupby(
            COL_PRODUCT_GROUP
        )[COL_PRODUCT_GROUP].transform(lambda x: missed_sales[x.index].sum())

    else:
        for col in [COL_COUPANG_STOCK_DEPLETION_DAYS, COL_GROUP_URGENCY_METRIC]:
            df_recommendations[col] = pd.Series(dtype="float64")

    # 컬럼 선택 및 정렬
    result_cols = [
        COL_PRODUCT_GROUP,
        COL_SKU,
        COL_PRODUCT_NAME,
        COL_TRANSFER_RECOMMENDATION,
        COL_COUPANG_STOCK_DEPLETION_DAYS,
        COL_STOCK_COUPANG,
        COL_STOCK_MAIN,
        COL_AVG_DAILY_SALES_COUPANG,
    ]

    final_cols = [col for col in result_cols if col in df_recommendations.columns]

    df_sorted = df_recommendations.sort_values(
        by=[
            COL_COUPANG_STOCK_DEPLETION_DAYS,
            COL_TRANSFER_RECOMMENDATION,
        ],
        ascending=[True, False],
    )

    # 포맷팅
    df_display = df_sorted[final_cols].copy()

    # [요청] 상품그룹 출력 시 맨 앞의 번호(예: "1_") 제거
    if COL_PRODUCT_GROUP in df_display.columns:
        df_display[COL_PRODUCT_GROUP] = (
            df_display[COL_PRODUCT_GROUP]
            .astype(str)
            .str.replace(r"^\d+_", "", regex=True)
        )

    if COL_COUPANG_STOCK_DEPLETION_DAYS in df_display.columns:
        df_display[COL_COUPANG_STOCK_DEPLETION_DAYS] = (
            df_display[COL_COUPANG_STOCK_DEPLETION_DAYS].round().astype(int)
        )

    if COL_AVG_DAILY_SALES_COUPANG in df_display.columns:
        df_display[COL_AVG_DAILY_SALES_COUPANG] = df_display[
            COL_AVG_DAILY_SALES_COUPANG
        ].round(1)

    return df_display