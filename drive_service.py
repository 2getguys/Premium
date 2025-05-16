import os
from googleapiclient.discovery import Resource
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import datetime

import auth 
from config import DRIVE_PARENT_FOLDER_NAME, DRIVE_INVOICE_FOLDER_NAME, MONTH_YEAR_FORMAT

# MIME type for Google Drive folder
FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

def get_drive_service() -> Resource | None:
    """Gets the authenticated Google Drive service resource."""
    return auth.get_service('drive', 'v3')

def get_or_create_folder(service: Resource, folder_name: str, parent_id: str = 'root') -> str | None:
    """Checks if a folder exists within a parent folder, creates it if not, and returns its ID.

    Args:
        service: Authorized Google Drive API service instance.
        folder_name: The name of the folder to find or create.
        parent_id: The ID of the parent folder. Defaults to 'root'.

    Returns:
        The ID of the found or created folder, or None if an error occurs.
    """
    try:
        # Search for the folder first
        query = f"name='{folder_name}' and mimeType='{FOLDER_MIME_TYPE}' and '{parent_id}' in parents and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        folders = response.get('files', [])

        if folders:
            folder_id = folders[0].get('id')
            print(f"Folder '{folder_name}' found with ID: {folder_id} inside parent {parent_id}")
            return folder_id
        else:
            # Folder not found, create it
            print(f"Folder '{folder_name}' not found inside parent {parent_id}. Creating...")
            file_metadata = {
                'name': folder_name,
                'mimeType': FOLDER_MIME_TYPE,
                'parents': [parent_id]
            }
            created_folder = service.files().create(body=file_metadata, fields='id').execute()
            folder_id = created_folder.get('id')
            print(f"Folder '{folder_name}' created with ID: {folder_id} inside parent {parent_id}")
            return folder_id

    except HttpError as error:
        print(f"An HTTP error occurred while finding/creating folder '{folder_name}': {error}")
        return None
    except Exception as e:
        print(f"An error occurred while finding/creating folder '{folder_name}': {e}")
        return None

def upload_invoice_to_drive(service: Resource, local_file_path: str, invoice_data: dict) -> dict | None:
    """Uploads an invoice to Google Drive based on invoice data.

    Path: DRIVE_PARENT_FOLDER_NAME / MonthYear (from invoice_date) / Payer / DRIVE_INVOICE_FOLDER_NAME / original_filename

    Args:
        service: Authorized Google Drive API service instance.
        local_file_path: The local path to the invoice file.
        invoice_data: A dictionary containing extracted invoice data from Gemini,
                      expected to have 'invoice_date' (YYYY-MM-DD) and 'payer'.

    Returns:
        A dictionary with 'id' and 'link' of the uploaded file on Google Drive, or None if an error occurs.
    """
    if not os.path.exists(local_file_path):
        print(f"Error: Local file not found for upload: {local_file_path}")
        return None

    try:
        # 1. Get/Create the main parent folder (e.g., "Документи для бухгалтера")
        parent_folder_id = get_or_create_folder(service, DRIVE_PARENT_FOLDER_NAME, 'root')
        if not parent_folder_id:
            print(f"Failed to get or create base folder: {DRIVE_PARENT_FOLDER_NAME}")
            return None

        # 2. Get/Create the MonthYear folder
        try:
            invoice_date_obj = datetime.datetime.strptime(invoice_data['invoice_date'], '%Y-%m-%d')
            month_year_folder_name = invoice_date_obj.strftime(MONTH_YEAR_FORMAT)
        except (KeyError, ValueError) as e:
            print(f"Error parsing invoice_date from invoice_data: {e}. Using generic month_year folder.")
            month_year_folder_name = "Unknown_MonthYear"
        
        month_year_folder_id = get_or_create_folder(service, month_year_folder_name, parent_folder_id)
        if not month_year_folder_id:
            print(f"Failed to get or create month/year folder: {month_year_folder_name}")
            return None

        # 3. Get/Create the Payer folder (Юр лице)
        payer_folder_name = invoice_data.get('payer', 'Unknown_Payer')
        if not payer_folder_name or not isinstance(payer_folder_name, str) or payer_folder_name.isspace():
            payer_folder_name = "Unknown_Payer"
        # Sanitize payer_folder_name to avoid issues with Drive folder names (e.g. slashes)
        payer_folder_name = payer_folder_name.replace('/', '-').replace('\\', '-').strip()
        if not payer_folder_name:
             payer_folder_name = "Invalid_Payer_Name"

        payer_folder_id = get_or_create_folder(service, payer_folder_name, month_year_folder_id)
        if not payer_folder_id:
            print(f"Failed to get or create payer folder: {payer_folder_name}")
            return None

        # 4. Get/Create the final "Фактури" folder
        final_invoices_folder_id = get_or_create_folder(service, DRIVE_INVOICE_FOLDER_NAME, payer_folder_id)
        if not final_invoices_folder_id:
            print(f"Failed to get or create final invoices folder: {DRIVE_INVOICE_FOLDER_NAME}")
            return None

        # 5. Upload the file
        file_name = os.path.basename(local_file_path)
        file_metadata = {
            'name': file_name,
            'parents': [final_invoices_folder_id]
        }
        media = MediaFileUpload(local_file_path, resumable=True)
        
        print(f"Uploading '{file_name}' to Drive folder ID: {final_invoices_folder_id}...")
        uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        file_id = uploaded_file.get('id')
        file_link = uploaded_file.get('webViewLink')
        print(f"File '{file_name}' uploaded successfully to Drive. ID: {file_id}, Link: {file_link}")
        return {'id': file_id, 'link': file_link}

    except HttpError as error:
        print(f"An HTTP error occurred during file upload: {error}")
        return None
    except Exception as e:
        print(f"An error occurred during file upload process: {e}")
        # import traceback
        # traceback.print_exc()
        return None

def delete_file_from_drive(service: Resource, file_id: str) -> bool:
    """Deletes a file from Google Drive.

    Args:
        service: Authorized Google Drive API service instance.
        file_id: The ID of the file to delete.

    Returns:
        True if deletion was successful, False otherwise.
    """
    try:
        print(f"Attempting to delete file with ID: {file_id} from Google Drive...")
        service.files().delete(fileId=file_id).execute()
        print(f"Successfully deleted file with ID: {file_id} from Google Drive.")
        return True
    except HttpError as error:
        print(f"An HTTP error occurred while deleting file ID '{file_id}': {error}")
        if error.resp.status == 404:
            print(f"File with ID '{file_id}' not found. Assuming already deleted or invalid ID.")
            return True # Or False, depending on desired behavior for 404
        return False
    except Exception as e:
        print(f"An unexpected error occurred while deleting file ID '{file_id}': {e}")
        return False

if __name__ == '__main__':
    # This is for testing the drive_service.py module directly
    # Ensure auth.py can provide credentials (e.g., token.json exists or credentials.json for flow)
    # and GOOGLE_API_KEY is set in .env for gemini_analyzer if it's called indirectly.
    print("Testing Drive Service functions...")
    drive = get_drive_service()
    if drive:
        print("Google Drive service obtained.")
        
        # --- Test get_or_create_folder ---
        # test_folder_name = "Test_Faktura_Automation_Folder"
        # test_folder_id = get_or_create_folder(drive, test_folder_name, 'root')
        # if test_folder_id:
        #     print(f"Test folder '{test_folder_name}' ready with ID: {test_folder_id}")
        #     # Try creating a subfolder
        #     # sub_folder_id = get_or_create_folder(drive, "Subfolder1", test_folder_id)
        #     # if sub_folder_id:
        #     #     print(f"Test subfolder 'Subfolder1' ready with ID: {sub_folder_id}")
        # else:
        #     print(f"Failed to create/get test folder '{test_folder_name}'")

        # --- Test upload_invoice_to_drive ---
        # 1. Create a dummy local file for testing upload
        dummy_upload_file_path = "sample_invoice_for_drive.txt"
        with open(dummy_upload_file_path, "w") as f:
            f.write("This is a test invoice file for Google Drive upload.")
        
        # 2. Prepare dummy invoice data (as if from Gemini)
        # Ensure date format matches what Gemini would provide
        current_time = datetime.datetime.now()
        dummy_invoice_info = {
            'invoice_date': current_time.strftime('%Y-%m-%d'), # e.g., "2024-05-13"
            'payer': "TestPayer JDG",
            # other fields like gross_amount, vat_amount, etc. would be here
        }

        print(f"\nAttempting to upload '{dummy_upload_file_path}' with data: {dummy_invoice_info}")
        uploaded_file_details = upload_invoice_to_drive(drive, dummy_upload_file_path, dummy_invoice_info)

        if uploaded_file_details:
            print(f"\nSUCCESS: Dummy file uploaded. ID: {uploaded_file_details['id']}, Link: {uploaded_file_details['link']}")
        else:
            print(f"\nFAILURE: Dummy file upload failed.")
        
        # Clean up dummy file
        if os.path.exists(dummy_upload_file_path):
            os.remove(dummy_upload_file_path)
            print(f"Removed dummy local file: {dummy_upload_file_path}")

    else:
        print("Failed to get Google Drive service. Check auth setup.") 