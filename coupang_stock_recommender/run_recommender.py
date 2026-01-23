import os
import sys
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Add the script's directory to the Python path to ensure local modules can be found.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_loader import load_all_data
from data_processor import process_data
from recommender import calculate_coupang_transfer_recommendations

# --- Configuration ---
SLACK_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.environ.get("TARGET_CHANNEL", "#general")

# Determine the path for the Google Cloud credentials file.
# The workflow will create 'credentials.json' in the script's directory.
script_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(script_dir, "credentials.json")

# For local development, fall back to a specific credential file if the primary one doesn't exist.
if not os.path.exists(creds_path):
    creds_path = os.path.join(script_dir, "credentials", "vocal-airline-291707-6cb22418b6f6.json")


def send_slack_notification(text, file_path=None):
    """Sends a message and optionally uploads a file to a Slack channel."""
    if not SLACK_TOKEN:
        print("Warning: SLACK_BOT_TOKEN not found. Skipping Slack notification.")
        print(f"Message: {text}")
        return

    client = WebClient(token=SLACK_TOKEN)
    try:
        # Send the text message
        client.chat_postMessage(channel=SLACK_CHANNEL, text=text)

        # Upload the file if it exists
        if file_path and os.path.exists(file_path):
            client.files_upload_v2(
                channel=SLACK_CHANNEL,
                file=file_path,
                title="Coupang Shipment Recommendation List",
                initial_comment="Here is the detailed recommendation list in an Excel file.",
            )
        print("Successfully sent Slack notification.")
    except SlackApiError as e:
        print(f"Error sending Slack notification: {e.response['error']}")


def main():
    """Main function to run the stock recommendation process."""
    print("Starting recommendation analysis...")

    # 1. Load data
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
        send_slack_notification(f"Error during data loading: {e}")
        return

    # 2. Process data
    try:
        df_final, _ = process_data(df_inventory, df_rocket, df_sales, df_bom)
    except Exception as e:
        send_slack_notification(f"Error during data processing: {e}")
        return

    # 3. Generate recommendations
    if df_final.empty:
        send_slack_notification("No data available for analysis.")
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
                "No products currently require shipment to Coupang (stock is sufficient)."
            )
        else:
            # Count products with stock depletion days between 0 and 5
            urgent_mask = (df_reco['쿠팡_재고소진_예상일'] >= 0) & (df_reco['쿠팡_재고소진_예상일'] <= 5)
            urgent_count = urgent_mask.sum()

            # Create a new summary message
            msg = f"📦 *Coupang Shipment Recommendations*\n"
            msg += f"🚨 Urgent products (0-5 days to stockout): *{urgent_count}* items\n"
            msg += "Please see the attached Excel file for the full list.\n\n"
            msg += "*Top 5 Urgent Products:*\n"

            # List the top 5 most urgent products
            top_5 = df_reco.head(5)
            for _, row in top_5.iterrows():
                quantity = int(row['추천입고수량'])
                product_name = str(row['상품명'])
                depletion_days = int(row['쿠팡_재고소진_예상일'])
                if len(product_name) > 15:
                    product_name = product_name[:15] + "..."
                msg += f"• *{product_name}*: {quantity} units (Est. stockout in {depletion_days} days)\n"

            # Save the full recommendation list to an Excel file
            excel_path = os.path.join(script_dir, "recommendation_result.xlsx")
            df_reco.to_excel(excel_path, index=False)

            # Send the summary and the Excel file to Slack
            send_slack_notification(msg, file_path=excel_path)

    except Exception as e:
        send_slack_notification(f"Error during recommendation analysis: {e}")


if __name__ == "__main__":
    main()