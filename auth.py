import os.path
import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
# Scopes required for Gmail reading, Drive upload, Sheets access
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',          # Read emails and attachments
    'https://www.googleapis.com/auth/drive',                 # Upload files to Drive
    'https://www.googleapis.com/auth/spreadsheets'        # Read/write Google Sheets
]

CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

def get_google_credentials():
    """Shows basic usage of the Google APIs authentication flow.
    Returns authorized credentials object.
    Handles token refresh and initial authorization flow.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            print(f"Loaded credentials from {TOKEN_FILE}")
        except Exception as e:
             print(f"Error loading credentials from {TOKEN_FILE}: {e}. Will re-authenticate.")
             creds = None # Force re-authentication
             # Optional: delete the corrupted token file
             # if os.path.exists(TOKEN_FILE):
             #    os.remove(TOKEN_FILE)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Credentials expired, refreshing...")
            try:
                creds.refresh(Request())
                print("Credentials refreshed successfully.")
            except google.auth.exceptions.RefreshError as e:
                 print(f"Error refreshing token: {e}. Need to re-authenticate.")
                 # Likely refresh token revoked or expired, need full re-auth
                 creds = None 
                 # Delete potentially invalid token file
                 if os.path.exists(TOKEN_FILE):
                     print(f"Deleting invalid token file: {TOKEN_FILE}")
                     os.remove(TOKEN_FILE)
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"Error: {CREDENTIALS_FILE} not found. Please download it from Google Cloud Console.")
                return None
            print(f"No valid token found or refresh failed. Starting authentication flow using {CREDENTIALS_FILE}...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            # Run local server flow, this will open a browser window for auth
            creds = flow.run_local_server(port=0)
            print("Authentication successful.")
        # Save the credentials for the next run
        if creds:
            try:
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                print(f"Credentials saved to {TOKEN_FILE}")
            except Exception as e:
                print(f"Error saving token to {TOKEN_FILE}: {e}")

    if not creds or not creds.valid:
         print("Failed to obtain valid credentials.")
         return None

    return creds

def get_service(service_name: str, version: str):
    """Builds and returns an authorized Google API service client.

    Args:
        service_name: Name of the service (e.g., 'gmail', 'drive', 'sheets').
        version: API version (e.g., 'v1', 'v3', 'v4').

    Returns:
        An authorized API service object, or None if authentication fails.
    """
    creds = get_google_credentials()
    if not creds:
        return None
    try:
        service = build(service_name, version, credentials=creds)
        print(f"Successfully built service client for {service_name} {version}")
        return service
    except Exception as e:
        print(f"An error occurred building the service client for {service_name}: {e}")
        return None

if __name__ == '__main__':
    print("Attempting to get Google credentials...")
    credentials = get_google_credentials()
    if credentials:
        print("Successfully obtained credentials.")
        # Example: Try building Gmail service
        gmail_service = get_service('gmail', 'v1')
        if gmail_service:
            print("Gmail service built successfully.")
        else:
            print("Failed to build Gmail service.")
    else:
        print("Failed to obtain credentials.") 