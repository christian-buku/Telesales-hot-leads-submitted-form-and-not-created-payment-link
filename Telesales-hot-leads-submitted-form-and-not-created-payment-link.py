import base64
import hashlib
import hmac
import json
from datetime import datetime
import time

import pandas as pd
import requests
import streamlit as st

API_BASE = "https://api.mekari.com"

st.set_page_config(page_title="WhatsApp Broadcast (Qontak)", page_icon="💬", layout="wide")

# ------------------------
# Helpers
# ------------------------

def get_secret(key: str, fallback: str = "") -> str:
    """Read secret safely (supports local dev without secrets)."""
    try:
        return st.secrets.get(key, fallback)
    except Exception:
        return fallback


def generate_auth_headers(http_method: str, url_path: str, client_id: str, client_secret: str):
    """Generates HMAC auth headers required by Mekari/Qontak API."""
    date_string = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    request_line = f"{http_method} {url_path} HTTP/1.1"
    string_to_sign = f"date: {date_string}\n{request_line}"

    signature = base64.b64encode(
        hmac.new(client_secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()

    return {
        "Authorization": (
            f'hmac username="{client_id}", algorithm="hmac-sha256", '
            f'headers="date request-line", signature="{signature}"'
        ),
        "Date": date_string,
        "Content-Type": "application/json",
    }


def load_from_public_sheet(sheet_id: str, sheet_name: str) -> pd.DataFrame:
    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    )
    df = pd.read_csv(csv_url)
    return df


def validate_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    required = {"to_number", "to_name"}
    missing = [c for c in required if c not in df.columns]
    return (len(missing) == 0, missing)


def send_whatsapp(
    row: dict,
    template_id: str,
    channel_id: str,
    image_url: str | None,
    image_filename: str | None,
    client_id: str,
    client_secret: str,
    sleep_after_seconds: float = 10.0,
):
    """Send a single WhatsApp message and (optionally) fetch the log."""
    to_number = str(row.get("to_number", "")).strip()
    to_name = str(row.get("to_name", "")).strip()

    post_url_path = "/qontak/chat/v1/broadcasts/whatsapp/direct"
    post_api_url = f"{API_BASE}{post_url_path}"
    headers_post = generate_auth_headers("POST", post_url_path, client_id, client_secret)

    payload = {
        "to_number": to_number,
        "to_name": to_name,
        "message_template_id": template_id,
        "channel_integration_id": channel_id,
        "language": {"code": "id"},
        "parameters": {
            "body": []
        },
    }

    # Header media (optional)
    if image_url:
        payload["parameters"]["header"] = {
            "format": "IMAGE",
            "params": [
                {"key": "url", "value": image_url},
                {"key": "filename", "value": image_filename or "banner.jpg"},
            ],
        }

    result = {
        "to_number": to_number,
        "to_name": to_name,
        "status_code": None,
        "broadcast_id": None,
        "send_response": None,
        "log_status_code": None,
        "log_response": None,
        "error": None,
    }

    try:
        r_post = requests.post(post_api_url, json=payload, headers=headers_post, timeout=60)
        result["status_code"] = r_post.status_code
        result["send_response"] = r_post.text

        if r_post.status_code in (200, 201, 202):
            data = r_post.json()
            broadcast_id = data.get("data", {}).get("id")
            result["broadcast_id"] = broadcast_id

            if broadcast_id:
                # give backend time to create log
                time.sleep(max(0, sleep_after_seconds))

                get_url_path = f"/qontak/chat/v1/broadcasts/{broadcast_id}/whatsapp/log"
                get_api_url = f"{API_BASE}{get_url_path}"
                headers_get = generate_auth_headers("GET", get_url_path, client_id, client_secret)

                r_get = requests.get(get_api_url, headers=headers_get, timeout=60)
                result["log_status_code"] = r_get.status_code
                result["log_response"] = r_get.text
        else:
            try:
                err = r_post.json()
            except Exception:
                err = {"raw": r_post.text}
            result["error"] = json.dumps(err)

    except Exception as e:
        result["error"] = str(e)

    return result


# ------------------------
# UI
# ------------------------

st.title("💬 WhatsApp Broadcast (Qontak/Mekari)")

with st.sidebar:
    st.header("Credentials & Template")
    use_secrets = st.toggle("Use st.secrets (recommended)", value=True)

    if use_secrets:
        client_id = get_secret("CLIENT_ID")
        client_secret = get_secret("CLIENT_SECRET")
        template_id = get_secret("TEMPLATE_ID")
        channel_id = get_secret("CHANNEL_ID")
        if not all([client_id, client_secret, template_id, channel_id]):
            st.warning("Some secrets are missing. Provide them below or set them in app secrets.")
    else:
        client_id = st.text_input("Client ID", value="")
        client_secret = st.text_input("Client Secret", type="password", value="")
        template_id = st.text_input("Template ID", value="")
        channel_id = st.text_input("Channel Integration ID", value="")

    st.divider()
    st.header("Media (optional)")
    # image_url = st.text_input("Image URL (optional)")
    image_url = "https://raw.githubusercontent.com/christian-buku/Telesales-hot-leads-submitted-form-and-not-created-payment-link/refs/heads/main/banner.jpg"
    # image_filename = st.text_input("Image filename", value="banner.jpg")
    image_filename = "banner.jpg"

    st.divider()
    st.header("Source")
    source_mode = st.radio("Choose contact source", ["Google Sheet (public CSV)", "Upload CSV"], index=0)

    sheet_id_default = get_secret("SHEET_ID", "")
    sheet_name_default = get_secret("SHEET_NAME", "hot_created")

    if source_mode == "Google Sheet (public CSV)":
        sheet_id = st.text_input("Sheet ID", value=sheet_id_default)
        sheet_name = st.text_input("Sheet Name", value=sheet_name_default)
        st.caption("Share the Google Sheet as 'Anyone with the link can view'.")
    else:
        uploaded = st.file_uploader("Upload CSV with columns: to_number, to_name", type=["csv"])

    st.divider()
    st.header("Run Options")
    sleep_after_seconds = st.number_input("Wait before fetching log (seconds)", min_value=0.0, max_value=60.0, value=10.0)
    max_rows = st.number_input("Max rows to send (0 = all)", min_value=0, value=0)

# Load data
if source_mode == "Google Sheet (public CSV)":
    df = pd.DataFrame()
    if sheet_id and sheet_name:
        try:
            df = load_from_public_sheet(sheet_id, sheet_name)
            st.success(f"Loaded {len(df)} rows from Google Sheet.")
        except Exception as e:
            st.error(f"Failed to read Google Sheet: {e}")
else:
    df = pd.DataFrame()
    if 'uploaded' in locals() and uploaded is not None:
        try:
            df = pd.read_csv(uploaded)
            st.success(f"Loaded {len(df)} rows from uploaded CSV.")
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")

if not df.empty:
    ok, missing = validate_dataframe(df)
    if not ok:
        st.error(f"Missing required columns: {', '.join(missing)}")
    else:
        st.subheader("Preview")
        st.dataframe(df.head(20))

# Run broadcast
run_clicked = st.button("🚀 Send WhatsApp Messages", type="primary", disabled=(df.empty))

if run_clicked:
    if not all([client_id, client_secret, template_id, channel_id]):
        st.error("Client/Template/Channel credentials are missing. Provide them in the sidebar.")
        st.stop()

    ok, missing = validate_dataframe(df)
    if not ok:
        st.error(f"Missing required columns: {', '.join(missing)}")
        st.stop()

    if max_rows and max_rows > 0:
        df = df.head(int(max_rows))

    st.info(
        "Starting broadcast... Do not close the tab until finished. "
        "Make sure your template is approved and parameters (if any) match."
    )

    results = []
    progress = st.progress(0)
    status_area = st.empty()

    total = len(df)
    for i, row in enumerate(df.to_dict(orient="records"), start=1):
        status_area.write(f"Sending to **{row.get('to_name','')}** ({row.get('to_number','')}) [{i}/{total}] ...")
        result = send_whatsapp(
            row,
            template_id=template_id,
            channel_id=channel_id,
            image_url=image_url.strip() if image_url else None,
            image_filename=image_filename.strip() if image_filename else None,
            client_id=client_id,
            client_secret=client_secret,
            sleep_after_seconds=float(sleep_after_seconds),
        )
        results.append(result)
        progress.progress(i / total)

    status_area.write("✅ Done.")
    res_df = pd.DataFrame(results)
    st.subheader("Results")
    st.dataframe(res_df)

    # Download button
    csv_bytes = res_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download results CSV",
        data=csv_bytes,
        file_name=f"broadcast_results_{int(time.time())}.csv",
        mime="text/csv",
    )

st.caption(
    "Note: This app sends WhatsApp messages using your Mekari/Qontak credentials. "
    "Keep CLIENT_SECRET in Streamlit secrets, not in code. Ensure your use complies with WhatsApp policies."
)