import base64
import os
import shutil
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

import auth
from config import GMAIL_QUERY, PROCESS_EMAILS_AFTER_DATE
from database import is_email_processed

# Directory to temporarily store downloaded attachments
DOWNLOAD_DIR = "temp_downloads"

def get_gmail_service() -> Resource | None:
    """Gets the authenticated Gmail service resource."""
    return auth.get_service('gmail', 'v1')

def find_new_emails(service: Resource) -> list[dict]:
    """Finds new emails matching the query in config.py that haven't been processed yet.

    Args:
        service: Authorized Gmail API service instance.

    Returns:
        A list of message objects (containing id and threadId) for unprocessed emails.
    """
    new_emails = []
    current_query = GMAIL_QUERY

    if PROCESS_EMAILS_AFTER_DATE and str(PROCESS_EMAILS_AFTER_DATE).strip():
        # Gmail API uses YYYY/MM/DD for date queries
        api_date_format = str(PROCESS_EMAILS_AFTER_DATE).strip().replace('-', '/')
        current_query += f" after:{api_date_format}"
        print(f"Extended Gmail query with date filter: after:{api_date_format}")

    try:
        # Initial query to find messages matching the criteria
        print(f"Searching for emails with query: '{current_query}'")
        response = service.users().messages().list(userId='me', q=current_query).execute()
        messages = response.get('messages', [])

        if not messages:
            print("No emails found matching the query.")
            return []

        print(f"Found {len(messages)} potential emails. Checking against database...")

        # Check each message against the database
        for message in messages:
            email_id = message['id']
            if not is_email_processed(email_id):
                new_emails.append(message)
                print(f"Found new email: ID {email_id}")
            # else:
                # print(f"Email ID {email_id} already processed. Skipping.")

        # Handle pagination if necessary (if more results than fit in one page)
        # TODO: Implement pagination if more than 100 messages are expected frequently
        # while 'nextPageToken' in response:
        #     page_token = response['nextPageToken']
        #     response = service.users().messages().list(userId='me', q=GMAIL_QUERY, pageToken=page_token).execute()
        #     messages = response.get('messages', [])
        #     for message in messages:
        #         email_id = message['id']
        #         if not is_email_processed(email_id):
        #             new_emails.append(message)

        print(f"Found {len(new_emails)} new, unprocessed emails.")
        return new_emails

    except HttpError as error:
        print(f"An HTTP error occurred: {error}")
        return []
    except Exception as e:
        print(f"An error occurred finding emails: {e}")
        return []

def download_attachments(service: Resource, message_id: str) -> dict[str, str]:
    """Downloads all attachments from a specific email message.

    Args:
        service: Authorized Gmail API service instance.
        message_id: The ID of the message from which to download attachments.

    Returns:
        A dictionary where keys are original filenames and values are 
        local file paths to the downloaded attachments. Empty if errors occur.
    """
    downloaded_files_map = {}
    try:
        print(f"Fetching email details for ID: {message_id}")
        message = service.users().messages().get(userId='me', id=message_id).execute()
        parts = message['payload'].get('parts', [])

        if not parts:
            print(f"No parts found in message {message_id}. Cannot download attachments.")
            # This might happen for simple text emails included by the query
            return {}

        # Ensure download directory exists
        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR)
            print(f"Created download directory: {DOWNLOAD_DIR}")

        attachment_count = 0
        for part in parts:
            if part.get('filename') and part.get('body') and part['body'].get('attachmentId'):
                filename = part['filename']
                attachment_id = part['body']['attachmentId']
                print(f"Found attachment: '{filename}' (ID: {attachment_id}) in email {message_id}")
                attachment_count += 1

                try:
                    attachment = service.users().messages().attachments().get(userId='me', messageId=message_id, id=attachment_id).execute()
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                    
                    # Sanitize filename slightly (replace spaces, could be more robust)
                    safe_filename = filename.replace(" ", "_") 
                    file_path = os.path.join(DOWNLOAD_DIR, f"{message_id}_{safe_filename}")

                    print(f"Downloading attachment '{filename}' to '{file_path}'...")
                    with open(file_path, 'wb') as f:
                        f.write(file_data)
                    
                    downloaded_files_map[filename] = file_path
                    print(f"Successfully downloaded '{file_path}' (Original: '{filename}')")

                except HttpError as attach_error:
                    print(f"An HTTP error occurred downloading attachment {filename} (ID: {attachment_id}): {attach_error}")
                except Exception as e:
                    print(f"An error occurred processing attachment {filename} (ID: {attachment_id}): {e}")

        if attachment_count == 0:
            print(f"No attachments with attachmentId found in email {message_id}. The query might include emails without downloadable attachments.")
        
        return downloaded_files_map

    except HttpError as error:
        print(f"An HTTP error occurred getting message {message_id}: {error}")
        return {}
    except Exception as e:
        print(f"An error occurred downloading attachments for message {message_id}: {e}")
        return {}

def cleanup_downloads():
    """Removes the temporary download directory and its contents."""
    if os.path.exists(DOWNLOAD_DIR):
        try:
            shutil.rmtree(DOWNLOAD_DIR)
            print(f"Removed temporary download directory: {DOWNLOAD_DIR}")
        except Exception as e:
            print(f"Error removing download directory {DOWNLOAD_DIR}: {e}")
    # else:
        # print(f"Download directory {DOWNLOAD_DIR} does not exist. No cleanup needed.")

# Example usage (optional, for testing)
if __name__ == '__main__':
    print("Testing Gmail Service functions...")
    gmail = get_gmail_service()
    if gmail:
        print("Gmail service obtained.")
        # 1. Find new emails
        new_msgs = find_new_emails(gmail)
        if new_msgs:
            print(f"\nFound {len(new_msgs)} new messages.")
            # 2. Try downloading attachments from the first new message
            first_msg_id = new_msgs[0]['id']
            print(f"\nAttempting to download attachments from message ID: {first_msg_id}")
            downloaded_map = download_attachments(gmail, first_msg_id)
            if downloaded_map:
                print(f"\nSuccessfully downloaded: {downloaded_map}")
                # Remember to clean up!
                # cleanup_downloads() # Uncomment carefully after verifying downloads
            else:
                print(f"\nNo attachments downloaded or error occurred for message {first_msg_id}.")
        else:
            print("\nNo new messages found to test download.")
        
        # Test cleanup (create dummy dir if needed)
        # if not os.path.exists(DOWNLOAD_DIR):
        #     os.makedirs(DOWNLOAD_DIR)
        #     with open(os.path.join(DOWNLOAD_DIR, "dummy.txt"), "w") as f:
        #         f.write("test")
        # print("\nTesting cleanup...")
        # cleanup_downloads()

    else:
        print("Failed to get Gmail service. Ensure credentials.json is present and valid, and run auth flow if needed.") 