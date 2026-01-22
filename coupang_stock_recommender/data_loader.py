import gspread
import pandas as pd
import numpy as np
import socket
from config import EXCLUDED_SKU_PREFIXES

# --- 구글 시트 및 컬럼명 상수 ---

# 시트 이름
SHEET_INVENTORY = "재고 시트"
SHEET_ROCKET = "로켓그로스재고(매번입력)"
SHEET_SALES = "매출시트"
SHEET_BOM = "세트구성품"
SHEET_DISCONTINUED = "품절상품"
SHEET_COUPANG_ONLY = "쿠팡전용상품"

# '재고 시트' 원본 컬럼명
SRC_INV_SKU = '옵션ID_이이엘'
SRC_INV_PRODUCT_NAME = '구분값'
SRC_INV_PRODUCT_NAME_OLD = '상품명' # 삭제될 컬럼

# '로켓그로스재고(매번입력)' 원본 컬럼명
SRC_ROCKET_OPTION_ID = 'Option ID'

# '매출시트' 원본 컬럼명
SRC_SALES_SKU = '옵션관리코드'

# '품절상품', '쿠팡전용상품' 원본 컬럼명
SRC_COMMON_SKU = 'sku'


def load_all_data(spreadsheet_name="로켓그로스_입고_발주_수량_관리시트_이이엘타임즈", creds_path='credentials/vocal-airline-291707-6cb22418b6f6.json'):
    """
    Google Sheets에서 재고, 로켓그로스, 매출 데이터를 불러와 DataFrame으로 반환합니다.
    
    :param spreadsheet_name: 연결할 Google 스프레드시트 이름
    :param creds_path: 서비스 계정 인증 파일 경로
    :return: df_inventory, df_rocket, df_sales, df_bom, discontinued_skus, coupang_only_skus 6개의 객체를 담은 튜플
    """
    try:
        # 네트워크 연결 타임아웃 설정 (무한 대기 방지, 120초)
        socket.setdefaulttimeout(120)

        # 서비스 계정 인증 정보 사용하여 구글 시트와 연결
        gc = gspread.service_account(filename=creds_path)
        spreadsheet_doc = gc.open(spreadsheet_name)
        print(f"'{spreadsheet_name}' 스프레드시트에 성공적으로 연결했습니다.")

        # 1-1. '재고 시트' 데이터 불러오기
        inventory_sheet = spreadsheet_doc.worksheet(SHEET_INVENTORY)
        inventory_data = inventory_sheet.get_all_records()
        df_inventory = pd.DataFrame(inventory_data)
        print(f"'{SHEET_INVENTORY}' 데이터를 성공적으로 불러왔습니다.")

        # 1-2. '로켓그로스재고(매번입력)' 데이터 불러오기 및 복잡한 헤더 가공
        rocket_sheet = spreadsheet_doc.worksheet(SHEET_ROCKET)
        rocket_values = rocket_sheet.get_all_values()
        
        if len(rocket_values) < 2:
            print(f"'{SHEET_ROCKET}' 시트에 데이터가 부족하여 처리할 수 없습니다.")
            df_rocket = pd.DataFrame()
        else:
            df_rocket = pd.DataFrame(rocket_values[2:], columns=pd.MultiIndex.from_arrays(rocket_values[:2]))
            new_header_level1 = df_rocket.columns.get_level_values(0).to_series().replace('', np.nan).ffill().fillna('')
            new_header_level2 = df_rocket.columns.get_level_values(1)
            final_headers = [
                f"{h1} {h2}".strip() if h1 and h2 else h1 or h2 for h1, h2 in zip(new_header_level1, new_header_level2)
            ]
            df_rocket.columns = final_headers
            print(f"'{SHEET_ROCKET}' 시트 데이터를 가공하여 성공적으로 불러왔습니다.")

        # 1-3. '매출시트' 데이터 불러오기
        sales_sheet = spreadsheet_doc.worksheet(SHEET_SALES)
        sales_values = sales_sheet.get_all_values()
        
        if len(sales_values) < 3:
            print(f"'{SHEET_SALES}'에 데이터가 부족하여 처리할 수 없습니다.")
            df_sales = pd.DataFrame()
        else:
            df_sales = pd.DataFrame(sales_values[3:], columns=sales_values[2])
            print(f"'{SHEET_SALES}' 데이터를 성공적으로 불러왔습니다.")

        # 1-4. '세트구성품' 데이터 불러오기
        try:
            bom_sheet = spreadsheet_doc.worksheet(SHEET_BOM)
            bom_data = bom_sheet.get_all_records()
            df_bom = pd.DataFrame(bom_data)
            # 모든 sku를 문자열로 변환하여 join 오류 방지
            df_bom = df_bom.astype(str)
            
            # [추가] set_fhb_ 로 시작하는 세트 상품 제외 (BOM 관계 끊기)
            if '세트_ID' in df_bom.columns and EXCLUDED_SKU_PREFIXES:
                df_bom = df_bom[~df_bom['세트_ID'].str.startswith(tuple(EXCLUDED_SKU_PREFIXES), na=False)]

            print(f"'{SHEET_BOM}' 시트 데이터를 성공적으로 불러왔습니다.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"경고: '{SHEET_BOM}' 워크시트를 찾을 수 없습니다. 세트 상품 판매량 분배가 비활성화됩니다.")
            df_bom = None

        # 1-5. '품절상품' 데이터 불러오기
        try:
            discontinued_sheet = spreadsheet_doc.worksheet(SHEET_DISCONTINUED)
            discontinued_data = discontinued_sheet.get_all_records()
            df_discontinued = pd.DataFrame(discontinued_data)
            discontinued_skus = df_discontinued[SRC_COMMON_SKU].astype(str).tolist() if SRC_COMMON_SKU in df_discontinued.columns else []
            print(f"'{SHEET_DISCONTINUED}' 시트에서 {len(discontinued_skus)}개의 SKU를 불러왔습니다.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"경고: '{SHEET_DISCONTINUED}' 워크시트를 찾을 수 없습니다.")
            discontinued_skus = []

        # 1-6. '쿠팡전용상품' 데이터 불러오기
        try:
            coupang_only_sheet = spreadsheet_doc.worksheet(SHEET_COUPANG_ONLY)
            coupang_only_data = coupang_only_sheet.get_all_records()
            df_coupang_only = pd.DataFrame(coupang_only_data)
            coupang_only_skus = df_coupang_only[SRC_COMMON_SKU].astype(str).tolist() if SRC_COMMON_SKU in df_coupang_only.columns else []
            print(f"'{SHEET_COUPANG_ONLY}' 시트에서 {len(coupang_only_skus)}개의 SKU를 불러왔습니다.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"경고: '{SHEET_COUPANG_ONLY}' 워크시트를 찾을 수 없습니다.")
            coupang_only_skus = []

        return df_inventory, df_rocket, df_sales, df_bom, discontinued_skus, coupang_only_skus

    except FileNotFoundError:
        print(f"에러: '{creds_path}' 파일을 찾을 수 없습니다. 서비스 계정 키 파일이 올바른 경로에 있는지 확인하세요.")
        return None, None, None, None, [], []
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"에러: 스프레드시트 '{spreadsheet_name}'을(를) 찾을 수 없습니다. 시트 이름을 정확히 입력했는지, 서비스 계정에 공유했는지 확인하세요.")
        return None, None, None, None, [], []
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"에러: 워크시트를 찾을 수 없습니다. 시트 이름이 정확한지 확인하세요: {e}")
        return None, None, None, None, [], []
    except Exception as e:
        print(f"데이터 로딩 중 알 수 없는 에러가 발생했습니다: {e}")
        return None, None, None, None, [], []
