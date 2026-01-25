# automated_workflow.py

import os
import time
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import subprocess
import re
import glob

# --- 설정 ---
COUPANG_LOGIN_URL = "https://wing.coupang.com/"
COUPANG_INVENTORY_URL = "https://wing.coupang.com/tenants/rfm-inventory/management/list"
GOOGLE_SHEET_NAME = "로켓그로스_입고_발주_수량_관리시트_이이엘타임즈"
TARGET_WORKSHEET_NAME = "로켓그로스재고(매번입력)"

# 스크립트의 현재 위치를 기준으로 경로 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
GSPREAD_CREDS_PATH = os.path.join(
    script_dir, "credentials", "vocal-airline-291707-6cb22418b6f6.json"
)
DOWNLOAD_DIR = os.path.join(
    "coupang_stock_recommender", "downloads" # 특정 다운로드 폴더 사용
)


def get_coupang_credentials():
    """쿠팡 접속 정보를 반환합니다."""
    return "spnteam", "1108ad^^"


def setup_webdriver(download_dir):
    """사용자 지정 다운로드 디렉토리로 Chrome WebDriver를 설정합니다."""
    options = webdriver.ChromeOptions()
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        },
    )
    # options.add_argument("--headless") # 헤드리스 모드로 실행
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def download_latest_inventory_file(driver, username, password):
    """
    쿠팡 Wing에 로그인하고, 재고 관리 페이지로 이동하여 최신 Excel 파일을 다운로드합니다.
    다운로드된 파일의 경로를 반환합니다.
    """
    print("🚀 쿠팡 Wing 로그인 및 파일 다운로드 시작...")

    # 1. 로그인
    driver.get(COUPANG_LOGIN_URL)
    WebDriverWait(driver, 60).until(
        EC.presence_of_element_located((By.ID, "username"))
    )  # 로그인 페이지가 로드될 때까지 대기

    driver.find_element(By.ID, "username").send_keys(username)
    driver.find_element(By.ID, "password").send_keys(password)
    driver.find_element(By.ID, "kc-login").click()

    # 로그인 성공 대기 (대시보드 또는 특정 URL로 리디렉션)
    try:
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "my-user-menu-name"))
        )
        print("✅ 쿠팡 Wing 로그인 성공.")
    except TimeoutException:
        print(
            "❌ 로그인 실패 또는 페이지 로드 시간 초과. ID/PW를 확인하거나 로그인 URL을 확인하세요."
        )
        driver.quit()
        return None

    # 2. 재고 현황 페이지 이동 (이미 이동되어 있을 수 있음, 한 번 더 시도)
    driver.get(COUPANG_INVENTORY_URL)

    # 2.5. 온보딩 팝업 처리
    try:
        # 온보딩 팝업의 '닫기' 버튼이 나타날 때까지 최대 5초 대기 후 Javascript로 클릭
        close_button_xpath = '//*[@id="inventory-management-main-container"]/div[8]/div[1]/div[1]/div/div/p/i'
        close_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, close_button_xpath))
        )
        driver.execute_script("arguments[0].click();", close_button)
        print("ℹ️ 온보딩 팝업을 닫았습니다.")
    except TimeoutException:
        # 팝업이 없으면 그냥 통과
        print("ℹ️ 온보딩 팝업이 나타나지 않았습니다.")
        pass

    # 3. '엑셀 다운로드' 버튼 클릭 (Javascript 클릭으로 우회)
    excel_button_xpath = "//button[contains(@class, 'wing-web-component black') and contains(., '엑셀 다운로드')]"
    # 버튼이 클릭 가능할 때까지 명시적으로 대기합니다.
    excel_button = WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, excel_button_xpath))
    )
    # Javascript를 사용하여 클릭합니다.
    driver.execute_script("arguments[0].click();", excel_button)
    print("클릭: '엑셀 다운로드'")

    # 4. '엑셀 다운로드 요청' 버튼 클릭
    # 팝업 대기 및 버튼 클릭
    try:
        request_button_xpath = '//*[@id="inventory-management-main-container"]/section[1]/div[1]/div[2]/div[6]/div[1]/div/div[1]'
        request_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, request_button_xpath))
        )
        driver.execute_script("arguments[0].click();", request_button)
        print("클릭: '엑셀 다운로드 요청'")
    except TimeoutException:
        print("❌ '엑셀다운로드요청' 버튼을 찾을 수 없거나 클릭할 수 없습니다.")
        driver.quit()
        return None

    # 5. 다운로드 목록 로드 및 최신 파일 대기
    print("🔄 10초 후 새로고침 및 다운로드 목록 확인 시작...")
    time.sleep(10)

    start_time = time.time()
    downloaded_file_path = None

    # 이전에 다운로드된 파일을 정리
    for f in glob.glob(os.path.join(DOWNLOAD_DIR, "inventory_health_sku_info_*.xlsx")):
        os.remove(f)

    # 폴링하여 다운로드 완료 확인
    while time.time() - start_time < 300:  # 최대 5분 대기
        try:
            # '새로고침' 버튼 클릭
            refresh_button_xpath = '//*[@id="inventory-management-main-container"]/section[1]/div[1]/div[2]/div[6]/div[2]/div[1]/div[2]/div/div[1]/button'
            refresh_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, refresh_button_xpath))
            )
            driver.execute_script("arguments[0].click();", refresh_button)

            # 다운로드 목록 테이블이 로드될 때까지 대기
            table_xpath = "//table//tbody"
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, table_xpath))
            )

            rows = driver.find_elements(By.XPATH, f"{table_xpath}/tr")

            if rows:
                latest_row = rows[0]

                # 1. 파일 이름 파싱 (세 번째 <td> 요소)
                file_name_element = latest_row.find_element(By.XPATH, ".//td[3]")
                file_name_match = re.search(
                    r"inventory_health_sku_info_\d{14}\.xlsx", file_name_element.text
                )

                if file_name_match:
                    target_file_name = file_name_match.group(0)
                    expected_file_path = os.path.join(DOWNLOAD_DIR, target_file_name)

                    # 2. 파일이 이미 다운로드되었는지 확인
                    if os.path.exists(expected_file_path):
                        print(f"✅ 파일이 이미 존재합니다: {expected_file_path}")
                        downloaded_file_path = expected_file_path
                        break

                    # 3. 파일이 없다면 다운로드 버튼 클릭 시도
                    print(f"파일 '{target_file_name}' 다운로드 시도...")
                    try:
                        download_button_xpath_absolute = '//*[@id="inventory-management-main-container"]/section[1]/div[1]/div[2]/div[6]/div[2]/div[1]/div[2]/div/div[2]/table/tbody/tr[1]/td[4]/div/button'
                        download_button = WebDriverWait(
                            driver, 2
                        ).until(
                            EC.presence_of_element_located(
                                (By.XPATH, download_button_xpath_absolute)
                            )
                        )
                        driver.execute_script("arguments[0].click();", download_button)
                        print(f"클릭 시도: '{target_file_name}' 다운로드 버튼")
                    except TimeoutException:
                        print("다운로드 버튼을 아직 클릭할 수 없습니다. 재시도합니다.")
                        pass
                else:
                    print("파일 이름을 파싱할 수 없습니다.")

            time.sleep(5)  # 다음 새로고침 전 대기
        except Exception as e:
            print(f"폴링 중 오류 발생: {e}")
            time.sleep(5)  # 오류 발생 시에도 잠시 대기 후 재시도

    if not downloaded_file_path:
        # 최종적으로 파일이 다운로드되었는지 한 번 더 확인
        # 루프가 타임아웃으로 종료되었지만, 마지막 클릭 시도로 파일이 다운로드되었을 수 있음
        if "target_file_name" in locals() and os.path.exists(expected_file_path):
            downloaded_file_path = expected_file_path
        else:
            print("❌ 최신 인벤토리 파일을 다운로드하지 못했습니다.")

    return downloaded_file_path


def upload_to_google_sheet(file_path):
    """
    Excel 파일을 읽어 특정 Google Sheet 워크시트에 내용을 업로드합니다.
    """
    print(f"\n📁 '{os.path.basename(file_path)}' 파일을 Google Sheet에 업로드 중...")
    try:
        gc = gspread.service_account(filename=GSPREAD_CREDS_PATH)
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(TARGET_WORKSHEET_NAME)

        # 1. 엑셀 파일 읽기
        df_excel = pd.read_excel(file_path)

        # 2. 기존 시트 내용 삭제 (첫 행 헤더는 남겨두기)
        # worksheet.clear() # clear()는 모든 내용을 삭제하므로 사용하지 않습니다.
        # 대신, 2번째 행부터 끝까지 삭제합니다.
        if worksheet.row_count > 1:
            worksheet.delete_rows(2, worksheet.row_count)

        # 3. 데이터 업로드
        # 헤더를 포함하여 데이터프레임을 업로드합니다.
        set_with_dataframe(
            worksheet,
            df_excel,
            row=1,
            col=1,
            include_index=False,
            include_column_header=True,
        )
        print(
            f"✅ '{os.path.basename(file_path)}' 파일 내용을 '{GOOGLE_SHEET_NAME}' 스프레드시트의 '{TARGET_WORKSHEET_NAME}' 시트에 성공적으로 업로드했습니다."
        )
        return True
    except Exception as e:
        print(f"❌ Google Sheet 업로드 중 오류 발생: {e}")
        return False


def main():
    # 0. 다운로드 디렉토리 생성
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    username, password = get_coupang_credentials()

    driver = None
    downloaded_file_path = None
    try:
        driver = setup_webdriver(DOWNLOAD_DIR)
        downloaded_file_path = download_latest_inventory_file(
            driver, username, password
        )
    finally:
        if driver:
            driver.quit()

    if downloaded_file_path:
        upload_to_google_sheet(downloaded_file_path)

        # 5. 다운로드된 파일 정리
        try:
            os.remove(downloaded_file_path)
            print(f"🧹 다운로드된 파일 '{downloaded_file_path}' 삭제 완료.")
        except Exception as e:
            print(f"❌ 다운로드된 파일 삭제 중 오류 발생: {e}")


if __name__ == "__main__":
    main()