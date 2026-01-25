# 쿠팡 재고 관리 및 광고 리포트 자동화 봇

## 📖 개요

이 프로젝트는 쿠팡 판매자를 위한 자동화된 재고 관리 및 광고 성과 리포팅을 위한 봇입니다. 주요 기능은 다음과 같습니다:

-   **쿠팡 재고 추천**: 판매 데이터와 현재 재고를 분석하여 쿠팡 로켓그로스 창고로 보내야 할 상품과 수량을 추천합니다.
-   **광고 성과 리포트**: 페이스북 광고의 일일 성과를 요약하여 슬랙으로 보고합니다.
-   **재고 현황 크롤링**: 쿠팡 Wing 사이트에 자동으로 접속하여 최신 재고 현황 엑셀 파일을 다운로드하고, 이를 구글 시트에 업데이트합니다.

---

## ⚙️ 주요 기능

-   **재고 추천 (`coupang_stock_recommender`)**
    -   구글 시트의 판매/재고 데이터를 기반으로 쿠팡 입고 추천 목록 생성
    -   `run_recommender_local.py`: 로컬에서 실행하여 `recommendation_result_local.xlsx`와 `daily_work_stocks.xlsx` 파일 생성
    -   `run_recommender_slack.py`: Github Actions를 통해 실행되며, 즉시 품절 상품 목록을 슬랙으로 알림
-   **광고 리포트 (`daily_ad_reporter`)**
    -   `reporter.py`: 페이스북 광고의 전날 성과(지출, 구매 수, CPP, ROAS 등)를 요약하여 슬랙으로 전송
-   **재고 크롤링 (`update_coupang_rocket_inventory.py`)**
    -   Selenium을 사용하여 쿠팡 Wing에 로그인하고, 로켓그로스 재고 현황 파일을 다운로드하여 지정된 구글 시트에 업로드

---

## 🛠️ 설정 방법

### 사전 요구사항

-   Python 3.9 이상
-   Git

### 1. 저장소 복제

```bash
git clone <repository_url>
cd eel-slack-bot
```

### 2. 의존성 설치

각 기능에 필요한 라이브러리를 설치합니다.

```bash
# 쿠팡 재고 추천 기능용
pip install -r coupang_requirements.txt

# 페이스북 광고 리포트 기능용
pip install -r ads_requirements.txt
```

### 3. 환경 변수 및 인증 파일 설정

이 프로젝트는 외부 서비스(구글 시트, 페이스북, 슬랙)와 연동을 위해 여러 인증 정보가 필요합니다.

#### Google Sheets API 인증

1.  Google Cloud Platform에서 서비스 계정을 생성하고, **`credentials.json`** 이름으로 키 파일을 다운로드합니다.
2.  다운로드한 `credentials.json` 파일을 `coupang_stock_recommender/` 디렉토리 안에 위치시킵니다.
    -   **주의**: 이 파일은 `.gitignore`에 등록되어 있어 Git에 커밋되지 않습니다.

#### 환경 변수

`.env` 파일을 프로젝트 루트에 생성하거나, Github Actions의 경우 Secrets에 아래 변수들을 등록해야 합니다.

-   `SLACK_BOT_TOKEN`: 슬랙 봇 토큰
-   `SLACK_CHANNEL_AD`: 광고 리포트를 보낼 슬랙 채널 ID
-   `SLACK_CHANNEL_ROCKETGROWTH`: 쿠팡 재고 알림을 보낼 슬랙 채널 ID
-   `FB_ACCESS_TOKEN`: 페이스북 API 액세스 토큰
-   `FB_AD_ACCOUNT_ID`: 페이스북 광고 계정 ID

---

## 🚀 사용법

### 로컬에서 재고 추천 실행

터미널에서 아래 명령어를 실행하면 `coupang_stock_recommender` 디렉토리 내에 결과 엑셀 파일이 생성됩니다.

```bash
python coupang_stock_recommender/run_recommender_local.py
```

### 자동화 워크플로우 (Github Actions)

-   **광고 리포트**: 매일 오전 9시(UTC 0시)에 자동으로 실행되어 슬랙으로 리포트를 전송합니다. (`.github/workflows/daily_report.yml`)
-   **재고 추천 알림**: 외부 신호(`repository_dispatch`)나 수동 실행 시, 품절 임박 상품을 슬랙으로 알립니다. (`.github/workflows/run_coupang_recommender.yml`)

### 쿠팡 재고 현황 자동 업데이트

쿠팡 Wing의 최신 재고 현황을 구글 시트로 업데이트합니다.

```bash
python coupang_stock_recommender/update_coupang_rocket_inventory.py
```
