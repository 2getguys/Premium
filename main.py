import os
import time
import json
from dotenv import load_dotenv # Import load_dotenv
import logging # Import logging module
from apscheduler.schedulers.background import BackgroundScheduler # Corrected import

# Load environment variables from .env file at the start
load_dotenv()

# --- Basic Logging Configuration ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # Add a logger for main.py
# --- End Logging Configuration ---

import database
import auth
import gmail_service
import gemini_analyzer
import drive_service
import config # MODIFIED: Was "from config import EMAIL_CHECK_INTERVAL_SECONDS, MONTH_YEAR_FORMAT, DATE_FORMAT"
import trello_service
import sheets_service # Uncommented
import vat_calculator # For VAT Calculation Scheduling

# --- Helper function for comparing invoice data ---
def _are_invoices_identical(new_data_gemini: dict, existing_db_invoice: dict) -> bool:
    """Compares key fields of a new invoice (from Gemini) and an existing DB record."""
    if not new_data_gemini or not existing_db_invoice:
        return False

    # Helper to safely convert Gemini string amounts to float for comparison
    def to_float(value):
        if isinstance(value, (float, int)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.replace(',', '.')) # Handle comma as decimal if any
            except ValueError:
                return None # Cannot convert
        return None

    # Compare invoice_number (should always be present and match if this function is called appropriately)
    if str(new_data_gemini.get('invoice_number')) != str(existing_db_invoice.get('invoice_number')):
        # This check is more of a safeguard; typically, this function is called when numbers already match.
        return False 

    # Compare dates (YYYY-MM-DD string format)
    if new_data_gemini.get('invoice_date') != existing_db_invoice.get('invoice_date'):
        return False
    if new_data_gemini.get('due_date') != existing_db_invoice.get('due_date'):
        return False
    
    # Compare textual fields (case-sensitive, assuming consistency)
    if new_data_gemini.get('payer') != existing_db_invoice.get('payer'):
        return False
    if new_data_gemini.get('issuer') != existing_db_invoice.get('issuer'):
        return False

    # Compare amounts (handle potential precision issues by comparing with a small tolerance if needed,
    # but direct float comparison is often okay if data sources are consistent)
    new_gross = to_float(new_data_gemini.get('gross_amount'))
    db_gross = to_float(existing_db_invoice.get('gross_amount'))
    if new_gross is None or db_gross is None or abs(new_gross - db_gross) > 0.001: # Comparing floats
        return False

    new_vat = to_float(new_data_gemini.get('vat_amount'))
    db_vat = to_float(existing_db_invoice.get('vat_amount'))
    if new_vat is None or db_vat is None or abs(new_vat - db_vat) > 0.001: # Comparing floats
        return False

    # Compare boolean is_fuel_related (Gemini gives bool, DB stores int 0 or 1)
    gemini_fuel_related = bool(new_data_gemini.get('is_fuel_related'))
    db_fuel_related = bool(existing_db_invoice.get('is_fuel_related')) # SQLite int 0/1 converts to bool
    if gemini_fuel_related != db_fuel_related:
        return False
        
    return True


# --- Main Logic ---
def process_single_invoice(file_path: str, email_id: str, attachment_filename: str, drive_service_instance) -> bool:
    """
    Processes a single downloaded invoice file.
    Handles Gemini analysis, Drive upload, Trello card creation, and DB storage.
    Manages duplicate and modification detection.
    """
    logger.info(f"--- Processing invoice file: {attachment_filename} (from email {email_id}) ---")
    
    # Перевіряємо чи файл не є ZIP архівом
    if file_path.lower().endswith('.zip'):
        logger.info(f"Skipping ZIP file: {attachment_filename}")
        return True  # Повертаємо True, бо це не помилка, а навмисне пропускання
    
    analysis_result = gemini_analyzer.analyze_invoice(file_path)

    if analysis_result is None:
        # Лог з gemini_analyzer вже пояснив причину (наприклад, "is not a standard invoice or receipt")
        # Тому тут ми просто підтверджуємо, що файл пропущено коректно.
        logger.info(f"Document {attachment_filename} was intentionally skipped by Gemini analyzer (e.g., not a standard invoice/receipt). This is expected behavior.")
        return True # Повертаємо True, оскільки це не помилка обробки, а коректний пропуск.
    
    # Ensure all required keys from Gemini are present, including invoice_number
    # Note: This check might seem redundant if Gemini returns None for non-standard invoices above,
    # but it's a good safeguard if analyze_invoice changes its return for non-skipped but incomplete data.
    # Required keys are now checked within gemini_analyzer.py before returning, so this specific block might be simplified
    # or removed if we trust gemini_analyzer to always return None or a complete dict for processable types.
    # For now, keeping it as a defense layer.
    required_gemini_keys = {'document_type', 'is_paid', 'invoice_number', 'invoice_date', 'due_date', 'payer', 'issuer', 'gross_amount', 'vat_amount', 'is_fuel_related'}
    # Payer_nip is not strictly required here as it can be derived.
    if not required_gemini_keys.issubset(analysis_result.keys()):
        logger.error(f"[FAILED] Gemini analysis for {attachment_filename} missing one or more required keys: {required_gemini_keys - set(analysis_result.keys())}. Raw: {analysis_result}")
        return False
    if not analysis_result.get('invoice_number'): # Specifically check for invoice_number
        logger.error(f"[FAILED] Gemini analysis for {attachment_filename} did not return an 'invoice_number'. Raw: {analysis_result}")
        return False

    logger.info(f"[SUCCESS] Gemini analysis for {attachment_filename} successful. Invoice Number: {analysis_result.get('invoice_number')}")
    # logger.debug(json.dumps(analysis_result, indent=2, ensure_ascii=False)) # Make debug for less verbose logs

    current_invoice_details_from_gemini = analysis_result.copy()
    invoice_num = current_invoice_details_from_gemini.get('invoice_number')

    # --- Duplicate / Modification Check ---
    existing_db_invoices = database.find_invoices_by_number(invoice_num)
    
    if existing_db_invoices:
        is_exact_duplicate = False
        for db_invoice_record in existing_db_invoices:
            if _are_invoices_identical(current_invoice_details_from_gemini, db_invoice_record):
                is_exact_duplicate = True
                logger.info(f"[DUPLICATE] Invoice {invoice_num} ({attachment_filename}) is an exact duplicate of DB record ID {db_invoice_record['id']}. Skipping.")
                break 
        
        if is_exact_duplicate:
            return True # Duplicate handled successfully

        # If not an exact duplicate, but invoice_number existed, it's a modification. Delete all old versions.
        logger.info(f"[MODIFICATION] Invoice {invoice_num} ({attachment_filename}) is a new version. Deleting {len(existing_db_invoices)} old version(s).")
        for old_db_invoice in existing_db_invoices:
            logger.info(f"Deleting resources for old invoice version (DB ID: {old_db_invoice['id']})...")
            if old_db_invoice.get('google_drive_file_id') and drive_service_instance:
                del_drive = drive_service.delete_file_from_drive(drive_service_instance, old_db_invoice['google_drive_file_id'])
                logger.info(f"  Old Google Drive file {old_db_invoice['google_drive_file_id']} deletion status: {del_drive}")
            if old_db_invoice.get('trello_card_id'):
                del_trello = trello_service.delete_trello_card(old_db_invoice['trello_card_id'])
                logger.info(f"  Old Trello card {old_db_invoice['trello_card_id']} deletion status: {del_trello}")
            # Delete from Google Sheets
            if old_db_invoice.get('google_sheets_row_id'):
                logger.info(f"Attempting to delete old Google Sheets row: {old_db_invoice['google_sheets_row_id']}")
                spreadsheet_id_for_delete = os.getenv(config.GOOGLE_SHEET_ID_FAKTURY_ENV)
                if spreadsheet_id_for_delete:
                    del_sheets = sheets_service.delete_invoice_row_by_range(
                        spreadsheet_id_for_delete, 
                        old_db_invoice['google_sheets_row_id']
                    )
                    logger.info(f"  Old Google Sheets row {old_db_invoice['google_sheets_row_id']} deletion status: {del_sheets}")
                else:
                    logger.warning(f"  Could not delete Google Sheets row: {config.GOOGLE_SHEET_ID_FAKTURY_ENV} not set.")
            
            db_del_success = database.delete_invoice(old_db_invoice['id'])
            logger.info(f"  Old DB record {old_db_invoice['id']} deletion status: {db_del_success}")
        logger.info(f"Finished deleting old versions for invoice {invoice_num}.")
    
    # --- Process as New Invoice (or Modification after cleanup) ---
    logger.info(f"Processing invoice {invoice_num} ({attachment_filename}) as new or modified version.")

    # 1. Upload to Google Drive
    drive_file_data = None
    if drive_service_instance:
        logger.info(f"Attempting to upload {attachment_filename} to Google Drive...")
        drive_file_data = drive_service.upload_invoice_to_drive(drive_service_instance, file_path, current_invoice_details_from_gemini)
        if not drive_file_data or not drive_file_data.get('id'):
            logger.error(f"[FAILED] Google Drive upload failed for {attachment_filename} or file ID not found.")
            return False # Critical failure
        logger.info(f"[SUCCESS] File {attachment_filename} uploaded to Google Drive. ID: {drive_file_data.get('id')}, Link: {drive_file_data.get('link')}")
    else:
        logger.warning("[SKIPPED] Google Drive service not available. Cannot upload.")
        # Depending on requirements, this might be a critical failure if Drive upload is essential
        # For now, let's assume it might proceed if other essential parts are okay or if Drive is optional for some workflows.
        # Consider returning False if Drive is absolutely mandatory for all documents.
        pass # Or return False if Drive is mandatory

    # 2. Create Trello Card - only if the document is not paid and is a standard invoice
    trello_card_id_val = None
    is_paid_status = current_invoice_details_from_gemini.get('is_paid', False) # Default to False if key is missing
    doc_type_for_trello = current_invoice_details_from_gemini.get('document_type')

    if doc_type_for_trello == 'standard_invoice' and not is_paid_status:
        if drive_file_data and drive_file_data.get('link'): # Requires Drive link
            logger.info(f"Attempting to create Trello card for unpaid standard invoice {attachment_filename}...")
            trello_card_id_val = trello_service.create_trello_card(
                invoice_data=current_invoice_details_from_gemini,
                invoice_file_path=file_path, # For attachment
                drive_file_link=drive_file_data['link']
            )
            if trello_card_id_val:
                logger.info(f"[SUCCESS] Trello card created for {attachment_filename}. ID: {trello_card_id_val}")
            else:
                logger.warning(f"[WARNING] Failed to create Trello card for {attachment_filename}. This is non-critical for DB entry.")
        else:
            logger.info(f"[SKIPPED] Trello card creation for {attachment_filename} due to missing Drive link (even though it's an unpaid invoice).")
    else:
        if doc_type_for_trello != 'standard_invoice':
            logger.info(f"[SKIPPED] Trello card creation for {attachment_filename} because document type is '{doc_type_for_trello}' (not 'standard_invoice').")
        elif is_paid_status:
            logger.info(f"[SKIPPED] Trello card creation for {attachment_filename} because it is marked as paid (is_paid: {is_paid_status}).")

    # 3. Store in Database
    invoice_to_save_in_db = current_invoice_details_from_gemini.copy() # Start with Gemini data
    invoice_to_save_in_db['google_drive_file_id'] = drive_file_data.get('id') if drive_file_data else None
    invoice_to_save_in_db['google_drive_file_weblink'] = drive_file_data.get('link') if drive_file_data else None
    invoice_to_save_in_db['trello_card_id'] = trello_card_id_val
    invoice_to_save_in_db['original_email_id'] = email_id
    invoice_to_save_in_db['attachment_filename'] = attachment_filename
    # 'google_sheets_row_id' will be set after attempting to append to Sheets

    # --- Google Sheets Entry ---
    google_sheets_row_id_val = None
    logger.info(f"Attempting to add entry to Google Sheets for {attachment_filename}...")
    if current_invoice_details_from_gemini and drive_file_data and drive_file_data.get('link'):
        google_sheets_row_id_val = sheets_service.append_invoice_to_sheet(
            invoice_data=current_invoice_details_from_gemini, 
            drive_file_link=drive_file_data['link']
        )
        if google_sheets_row_id_val:
            logger.info(f"[SUCCESS] Data for {attachment_filename} added to Google Sheets. Range: {google_sheets_row_id_val}")
        else:
            logger.warning(f"[WARNING] Failed to add data for {attachment_filename} to Google Sheets. DB record will not have sheet row ID.")
    else:
        logger.warning(f"[SKIPPED] Google Sheets entry for {attachment_filename} due to missing analysis data or Drive link.")
    
    invoice_to_save_in_db['google_sheets_row_id'] = google_sheets_row_id_val # Store even if None

    logger.info(f"Attempting to save invoice {invoice_num} ({attachment_filename}) to database with Sheets ID: {google_sheets_row_id_val}...")
    new_db_invoice_id = database.add_invoice(invoice_to_save_in_db)

    if not new_db_invoice_id:
        logger.error(f"[FAILED] Could not save invoice {invoice_num} ({attachment_filename}) to database.")
        # Potentially attempt to rollback Drive upload / Trello card if this fails? Complex.
        return False # Critical failure
    
    logger.info(f"[SUCCESS] Invoice {invoice_num} ({attachment_filename}) saved to database with ID: {new_db_invoice_id}.")
    
    # --- Google Sheets Entry (Commented Out for now) ---
    # logger.info(f"Attempting to add entry to Google Sheets for {attachment_filename}...")
    # if analysis_result and drive_file_data and drive_file_data.get('link'):
    # # This would need invoice_db_id or new_db_invoice_id for future linking
    # # sheets_success = sheets_service.append_invoice_to_sheet(
    # # invoice_data=current_invoice_details_from_gemini, # or invoice_to_save_in_db
    # # drive_file_link=drive_file_data['link']
    # # # Potentially add invoice_db_id=new_db_invoice_id here
    # # )
    # # if sheets_success:
    # # logger.info(f"[SUCCESS] Data for {attachment_filename} added to Google Sheets.")
    # # else:
    # # logger.warning(f"[WARNING] Failed to add data for {attachment_filename} to Google Sheets.")
    # else:
    #     logger.info(f"[SKIPPED] Google Sheets entry for {attachment_filename} due to missing data.")

    return True # All critical steps succeeded for this file

def main_loop():
    """Main loop to check emails and process invoices."""
    logger.info("Starting Faktura Processing Automation...")
    
    logger.info("Initializing database...")
    database.init_db()

    logger.info("Authenticating with Google...")
    gmail = gmail_service.get_gmail_service()
    drive = drive_service.get_drive_service()
    
    if not gmail or not drive:
        logger.error("Failed to get required Google services (Gmail or Drive). Exiting.")
        return
    
    logger.info("Authentication successful. Entering main loop for email checking...")

    try:
        while True:
            logger.info(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Checking for new emails...")
            
            new_emails = gmail_service.find_new_emails(gmail)

            if not new_emails:
                logger.info("No new emails to process.")
            else:
                logger.info(f"Found {len(new_emails)} new email(s) requiring processing.")
                for email_info in new_emails:
                    email_id = email_info['id']
                    logger.info(f"\nProcessing Email ID: {email_id}")
                    
                    downloaded_files_map = gmail_service.download_attachments(gmail, email_id) # Returns dict
                    
                    if not downloaded_files_map:
                        logger.info(f"No attachments found or failed to download for email {email_id}. Skipping email.")
                        # If an email had no attachments but was queried, we might mark it as processed.
                        # For now, if download_attachments returns empty/None, we assume it's not an invoice email
                        # or an issue occurred. If it's consistently picked up, gmail_query might need refinement.
                        # Let's mark it as processed to avoid re-picking an empty email.
                        logger.info(f"Marking email {email_id} as processed as no valid attachments were found for processing.")
                        database.add_processed_email(email_id)
                        continue

                    all_attachments_handled_successfully = True # Renamed for clarity
                    for original_filename, file_path in downloaded_files_map.items():
                        if not os.path.exists(file_path): # Double check file exists
                            logger.error(f"Downloaded file path {file_path} for {original_filename} does not exist. Skipping.")
                            all_attachments_handled_successfully = False
                            continue

                        success = process_single_invoice(file_path, email_id, original_filename, drive)
                        if not success:
                            all_attachments_handled_successfully = False
                            logger.error(f"Critical processing failed for attachment {original_filename} from email {email_id}.")
                            # We continue processing other attachments in the same email,
                            # but the email won't be marked as fully processed if any attachment fails critically.
                    
                    logger.info(f"Cleaning up temporary files for email {email_id}...")
                    for file_path in downloaded_files_map.values():
                        if os.path.exists(file_path): # Check before removing
                            try:
                                os.remove(file_path)
                            except OSError as e:
                                logger.error(f"Error removing temp file {file_path}: {e}")
                    
                    # Attempt to remove the email's temporary sub-directory
                    if downloaded_files_map: # If there were files, a directory might have been made
                        # Assuming download_attachments creates a per-email_id folder inside a main download dir
                        # And that file_path is inside that per-email_id folder.
                        # This needs to align with how gmail_service.download_attachments and cleanup_downloads work.
                        # For simplicity, relying on a global cleanup in gmail_service for now,
                        # or individual file removal is sufficient if no per-email subfolder is used that needs specific cleanup here.
                        # The current `gmail_service.cleanup_downloads()` in `finally` should handle the main download folder.
                        pass


                    if all_attachments_handled_successfully:
                        logger.info(f"All attachments for email {email_id} were handled successfully (processed, duplicate, or modified).")
                    else:
                        logger.warning(f"One or more attachments in email {email_id} could not be fully processed (e.g., unsupported file type, analysis error). See logs for details.")
                    
                    logger.info(f"Marking email {email_id} as processed to prevent further attempts on this email.")
                    database.add_processed_email(email_id)

            logger.info(f"\nWaiting for {config.EMAIL_CHECK_INTERVAL_SECONDS} seconds before next check...")
            time.sleep(config.EMAIL_CHECK_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("\nProcess interrupted by user. Exiting gracefully...")
    except Exception as e:
        logger.exception(f"\n[FATAL ERROR] An unexpected error occurred in the main loop: {e}")
    finally:
        logger.info("Performing final cleanup...")
        gmail_service.cleanup_downloads() 
        logger.info("Exiting Faktura Processing Automation.")


if __name__ == '__main__':
    bg_scheduler = BackgroundScheduler(timezone="Europe/Kiev")
    bg_scheduler.add_job(
        vat_calculator.calculate_and_record_vat_summary, 
        'cron', 
        day=15, 
        hour=9, 
        minute=0,
        misfire_grace_time=3600 
    )
    bg_scheduler.start()
    logger.info("Background scheduler for VAT calculation started. (15th of month, 9:00 AM Europe/Kiev).")
    
    logger.info("Starting main email processing loop...")
    main_loop() 