import os
import json
import requests
import datetime
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

load_dotenv()

# 환경 변수 로드
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = f"act_{os.getenv('FB_AD_ACCOUNT_ID')}"
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_AD")


def get_report():
    FacebookAdsApi.init(access_token=ACCESS_TOKEN)
    account = AdAccount(AD_ACCOUNT_ID)

    # 어제 데이터 조회
    fields = ["spend", "clicks", "actions", "action_values"]
    insights = account.get_insights(params={"date_preset": "yesterday"}, fields=fields)

    if not insights:
        return "⚠️ 어제 집계된 광고 데이터가 없습니다."

    data = insights[0]
    spend = float(data.get("spend", 0))
    clicks = int(data.get("clicks", 0))

    # 구매 데이터 추출
    purchases = 0
    purchase_value = 0
    if "actions" in data:
        for action in data["actions"]:
            if action["action_type"] == "purchase":
                purchases = float(action["value"])
    if "action_values" in data:
        for val in data["action_values"]:
            if val["action_type"] == "purchase":
                purchase_value = float(val["value"])

    # 지표 계산
    cpc = spend / clicks if clicks > 0 else 0
    cpp = spend / purchases if purchases > 0 else 0  # 구매당 비용
    roas = (purchase_value / spend * 100) if spend > 0 else 0

    # 슬랙 메시지 구성
    report_text = (
        f"📅 *어제 광고 성과 요약 ({datetime.date.today() - datetime.timedelta(1)})*\n\n"
        f"💰 *총 지출:* {int(spend):,}원\n"
        f"🛒 *총 구매:* {int(purchases)}건\n"
        f"🎯 *구매당 비용 (CPP):* {int(cpp):,}원\n"
        f"🖱️ *평균 CPC:* {int(cpc):,}원\n"
        f"📈 *구매 ROAS:* {int(roas):,}%"
    )
    return report_text


def send_slack(text):
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"channel": SLACK_CHANNEL_ID, "text": text}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes
    print(response.json())  # Print the JSON response from Slack


if __name__ == "__main__":
    report = get_report()
    send_slack(report)
