import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric
from google_auth_oauthlib.flow import InstalledAppFlow
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

# Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CLIENT_SECRETS_FILE = os.getenv("CLIENT_SECRETS_FILE")

# Set the path for the analytics_token.json file from environment variables
TOKEN_PATH = os.getenv("TOKEN_PATH")

def authenticate_with_oauth():
    """Authenticate the user via OAuth2."""
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())

    return creds

def convert_date_to_epoch(date_str):
    """Convert a date string (YYYY-MM-DD) to epoch time."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())  

def fetch_sessions_for_date(property_id, start_date, end_date):
    """Fetch session data for a specific date from Google Analytics."""
    creds = authenticate_with_oauth()
    client = BetaAnalyticsDataClient(credentials=creds)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    response = client.run_report(request)

    # Return the number of sessions (default to 0 if not found)
    return int(response.rows[0].metric_values[0].value) if response.rows else 0

def store_session_data_in_db(sessions, start_epoch, end_epoch):
    """Store session data in the Supabase table."""
    try:
        data = {
            "sessions": sessions,  
            "start_epoch": start_epoch, 
            "end_epoch": end_epoch 
        }

        # Insert data into Supabase table
        response = supabase.table("sessions_from_analytics").insert(data).execute()

        # Check for errors
        if response.get('status_code') and response.get('status_code') not in [200, 201]:
            print(f"Error storing data for start_epoch: {start_epoch}: {response.get('error')}")
        elif response.get('data') is None:
            print(f"Error storing data for start_epoch: {start_epoch}: No data returned in response.")
        else:
            print(f"Successfully stored data for start_epoch: {start_epoch}")
    except Exception as e:
        print(f"Exception occurred while storing data for start_epoch: {start_epoch}: {e}")

def get_last_stored_date():
    """Retrieve the last stored date from the Supabase table."""
    try:
        response = supabase.table("sessions_from_analytics").select("end_epoch").order("end_epoch", desc=True).limit(1).execute()
        if response.data:
            last_epoch = response.data[0]["end_epoch"]
            return datetime.fromtimestamp(last_epoch, timezone.utc).strftime("%Y-%m-%d")
        else:
            return None
    except Exception as e:
        print(f"Exception occurred while retrieving the last stored date: {e}")
        return None

def fetch_and_store_data(property_id: str):
    """Fetch and store session data from the last stored date to the current date."""
    try:
        last_stored_date = get_last_stored_date()
        if last_stored_date:
            start_date_dt = datetime.strptime(last_stored_date, "%Y-%m-%d") + timedelta(days=1)
        else:
            start_date_dt = datetime.strptime("2023-01-01", "%Y-%m-%d")  

        end_date_dt = datetime.utcnow() - timedelta(days=1)

        current_date = start_date_dt
        while current_date <= end_date_dt:
            formatted_date = current_date.strftime("%Y-%m-%d")

            # Fetch sessions
            sessions = fetch_sessions_for_date(property_id, formatted_date, formatted_date)

            # Calculate epoch times
            start_epoch = int(current_date.timestamp())
            end_epoch = int((current_date + timedelta(days=1)).timestamp()) - 1

            # Store data in the database
            store_session_data_in_db(sessions, start_epoch, end_epoch)

            current_date += timedelta(days=1)

    except Exception as e:
        print(f"Error fetching and storing data: {e}")

if __name__ == "__main__":
    property_id = os.getenv("GA_PROPERTY_ID")
    fetch_and_store_data(property_id)
    # Log the execution
    with open("/Users/sumuditha/Desktop/Metana/sessions-analytics-to-supabase/logfile.log", "a") as log_file:
        log_file.write(f"Script executed at {datetime.now()}\n")

