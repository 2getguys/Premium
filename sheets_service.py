import logging
import os
import re # Added for parsing row range
from datetime import datetime
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

import auth
import config

logger = logging.getLogger(__name__)

# Define the headers for the sheet
SHEET_HEADERS = [
    "Дата виставлення", "Виставив", "Дата оплати", "Платник",
    "Сума (брутто)", "VAT", "Пов'язано з авто/паливом", "Посилання на Google Drive"
]

def get_sheets_service() -> Resource | None:
    """Gets the authenticated Google Sheets service resource."""
    return auth.get_service('sheets', 'v4')

def _get_sheet_id_by_title(service, spreadsheet_id, sheet_title):
    """Gets the ID of a sheet (tab) by its title. Returns None if not found."""
    try:
        spreadsheet_properties = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet_properties.get('sheets', [])
        for sheet in sheets:
            if sheet.get('properties', {}).get('title') == sheet_title:
                return sheet.get('properties', {}).get('sheetId')
        return None
    except HttpError as e:
        logger.error(f"Error getting sheet ID for title '{sheet_title}': {e}")
        return None

def _create_sheet_tab(service, spreadsheet_id, sheet_title):
    """Creates a new sheet (tab) with the given title. Returns sheetId or None."""
    try:
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_title
                    }
                }
            }]
        }
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        # Assuming the new sheet is the last one in the response of addSheet requests
        created_sheet_properties = response.get('replies')[0].get('addSheet').get('properties')
        logger.info(f"Created new sheet tab: '{sheet_title}' with ID {created_sheet_properties.get('sheetId')}")
        return created_sheet_properties.get('sheetId')
    except HttpError as e:
        logger.error(f"Error creating sheet tab '{sheet_title}': {e}")
        return None

def _ensure_sheet_tab_with_headers(service: Resource, spreadsheet_id: str, sheet_title: str, headers: list[str]) -> bool:
    """Ensures a sheet (tab) with the given title and headers exists. Creates it if not."""
    try:
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        existing_sheet = next((s for s in sheets if s.get('properties', '').get('title', '') == sheet_title), None)

        if existing_sheet:
            logger.info(f"Sheet '{sheet_title}' already exists.")
            # Optionally, check if headers are present and correct, though append will add them if sheet is empty
            # For simplicity, we assume if it exists, it's usable or append will handle headers on empty sheet.
            return True
        else:
            logger.info(f"Sheet '{sheet_title}' not found. Creating it...")
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_title
                        }
                    }
                }]
            }
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            logger.info(f"Sheet '{sheet_title}' created.")
            
            # Add headers to the new sheet
            header_body = {
                'values': [headers]
            }
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_title}'!A1",
                valueInputOption='USER_ENTERED',
                body=header_body
            ).execute()
            logger.info(f"Headers added to sheet '{sheet_title}'.")
            return True
    except HttpError as error:
        logger.error(f"Error ensuring sheet '{sheet_title}': {error}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error ensuring sheet '{sheet_title}': {e}")
        return False

def _ensure_header_row(service, spreadsheet_id, sheet_title):
    """Ensures the header row exists for the main invoice sheets. (Uses predefined SHEET_HEADERS from config)"""
    # Use SHEET_HEADERS from config.py for consistency
    return _ensure_sheet_tab_with_headers(service, spreadsheet_id, sheet_title, config.SHEET_HEADERS)

def read_sheet_data(spreadsheet_id: str, sheet_name_with_range: str) -> list[list[str]] | None:
    """Reads data from a specific sheet and range."""
    service = get_sheets_service()
    if not service:
        logger.error("Sheets service not available for reading data.")
        return None
    try:
        logger.info(f"Reading data from spreadsheet '{spreadsheet_id}', range '{sheet_name_with_range}'")
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name_with_range
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.info(f"No data found in '{sheet_name_with_range}'.")
            return [] # Return empty list not None if no data, to distinguish from error
        return values
    except HttpError as error:
        logger.error(f"Error reading data from sheet '{sheet_name_with_range}': {error}")
        return None # Error case
    except Exception as e_read:
        logger.error(f"Unexpected error reading data from '{sheet_name_with_range}': {e_read}")
        return None

def append_invoice_to_sheet(invoice_data: dict, drive_file_link: str) -> str | None:
    """Appends invoice data to the appropriate monthly sheet in Google Sheets.

    Args:
        invoice_data: A dictionary containing extracted invoice data.
                      Expected keys: invoice_date, issuer, payer, gross_amount, etc.,
                                     and the new 'invoice_number'.
        drive_file_link: The Google Drive link for the uploaded invoice.

    Returns:
        The range where data was appended (e.g., "05.2025!A10"), or None if an error occurred.
    """
    spreadsheet_id = os.getenv(config.GOOGLE_SHEET_ID_FAKTURY_ENV)
    if not spreadsheet_id:
        logger.error(f"Google Sheet ID ({config.GOOGLE_SHEET_ID_FAKTURY_ENV}) not found in .env. Cannot append to sheet.")
        return None

    service = get_sheets_service()
    if not service:
        logger.error("Failed to get Google Sheets service. Cannot append.")
        return None

    try:
        # Determine sheet title (e.g., "05.2025")
        invoice_date_str = invoice_data.get('invoice_date')
        if not invoice_date_str:
            logger.error("Invoice date missing in invoice_data. Cannot determine sheet title.")
            return None
        invoice_date_obj = datetime.strptime(invoice_date_str, config.DATE_FORMAT) # %Y-%m-%d
        sheet_title = invoice_date_obj.strftime(config.MONTH_YEAR_FORMAT) # %m.%Y
    except ValueError as ve:
        logger.error(f"Error parsing invoice_date '{invoice_date_str}' to determine sheet title: {ve}")
        return None

    # Ensure sheet and headers exist
    if not _ensure_sheet_tab_with_headers(service, spreadsheet_id, sheet_title, config.SHEET_HEADERS):
        logger.error(f"Failed to ensure sheet '{sheet_title}' with headers. Aborting append.")
        return None

    # Prepare row data according to config.SHEET_HEADERS
    # SHEET_HEADERS = ["Номер фактури", "Дата виставлення", "Виставив", "Дата оплати", "Платник", 
    #                  "Сума (брутто)", "VAT", "Пов'язано з авто/паливом", "Посилання на Google Drive"]
    row_values = []
    try:
        row_values = [
            str(invoice_data.get('invoice_number', '')),
            str(invoice_data.get('invoice_date', '')),
            str(invoice_data.get('issuer', '')),
            str(invoice_data.get('due_date', '')),
            str(invoice_data.get('payer', '')),
            str(invoice_data.get('payer_nip', '')),
            str(invoice_data.get('gross_amount', '')).replace('.', ','), # Sheets often prefers comma for decimal in some locales
            str(invoice_data.get('vat_amount', '')).replace('.', ','),   # Sheets often prefers comma for decimal in some locales
            "Так" if invoice_data.get('is_fuel_related') else "Ні",
            str(drive_file_link if drive_file_link else '')
        ]
    except Exception as e_format:
        logger.error(f"Error formatting row data for Google Sheets: {e_format}. Data: {invoice_data}")
        return None
        
    body = {
        'values': [row_values]
    }
    
    try:
        logger.info(f"Appending data to sheet: '{sheet_title}', row: {row_values}")
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_title}'!A1", # Append after the last row with data in this range
            valueInputOption='USER_ENTERED', # So Sheets interprets values as numbers, dates if possible
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        logger.info(f"Data appended successfully to '{sheet_title}'. Result: {result}")
        updated_range = result.get('updates', {}).get('updatedRange')
        return updated_range # e.g., "05.2025!A10:I10"
    except HttpError as error:
        logger.error(f"Error appending data to sheet '{sheet_title}': {error}")
        return None
    except Exception as e_append:
        logger.error(f"Unexpected error appending data to '{sheet_title}': {e_append}")
        return None

def delete_invoice_row_by_range(spreadsheet_id: str, row_range_to_delete: str) -> bool:
    """
    Deletes a row from a Google Sheet based on its A1 notation range.

    Args:
        spreadsheet_id: The ID of the Google Spreadsheet.
        row_range_to_delete: The A1 notation of the range to delete (e.g., "05.2025!A10:I10" or "Sheet1!A5").
                             It's assumed this range refers to a single row or part of a single row.

    Returns:
        True if deletion was successful or row_range_to_delete was invalid, False otherwise.
    """
    if not row_range_to_delete:
        logger.warning("No row_range_to_delete provided. Skipping deletion.")
        return True # Not an error, just nothing to do

    service = get_sheets_service()
    if not service:
        logger.error("Failed to get Google Sheets service. Cannot delete row.")
        return False

    try:
        # Parse sheet_title and row_index from row_range_to_delete
        # Example: "05.2025!A10:I10" or "'Sheet Name with Spaces'!A10"
        match = re.match(r"^(?:'(.*)'|([^'!]+))!(?:[A-Z]+)(\d+)(?::[A-Z]+\d+)?$", row_range_to_delete)
        if not match:
            logger.error(f"Could not parse sheet title and row index from range: '{row_range_to_delete}'")
            return False

        sheet_title = match.group(1) or match.group(2) # Group 1 for quoted, Group 2 for unquoted
        row_number_1_indexed = int(match.group(3))
        
        if not sheet_title or row_number_1_indexed <= 0:
            logger.error(f"Invalid sheet title or row number parsed from '{row_range_to_delete}'. Title: '{sheet_title}', Row: {row_number_1_indexed}")
            return False

        logger.info(f"Attempting to delete row {row_number_1_indexed} from sheet '{sheet_title}' (range: '{row_range_to_delete}')")

        sheet_id = _get_sheet_id_by_title(service, spreadsheet_id, sheet_title)
        if sheet_id is None:
            logger.error(f"Could not find sheetId for title '{sheet_title}'. Cannot delete row.")
            # If the sheet itself doesn't exist, we can consider the "deletion" successful
            # as the row is effectively not there. Or return False if explicit deletion failed.
            # For now, let's say if sheet is gone, row is gone.
            logger.warning(f"Sheet '{sheet_title}' not found. Assuming row is already effectively deleted.")
            return True 

        row_index_0_based = row_number_1_indexed - 1

        requests = [{
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row_index_0_based,
                    "endIndex": row_index_0_based + 1  # Deletes one row starting at startIndex
                }
            }
        }]

        body = {'requests': requests}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        logger.info(f"Successfully deleted row {row_number_1_indexed} (index {row_index_0_based}) from sheet '{sheet_title}' (ID: {sheet_id}).")
        return True

    except HttpError as error:
        logger.error(f"Error deleting row from sheet '{row_range_to_delete}': {error}")
        # Specific check for "Unable to parse range" which can happen if sheet was deleted
        # or if the range refers to something outside existing sheet dimensions after other deletions.
        if "Unable to parse range" in str(error) or "range (gridRange.startRowIndex)" in str(error):
             logger.warning(f"Could not delete row {row_range_to_delete}. It might have been already deleted or sheet structure changed. Treating as non-critical.")
             return True # Non-critical if row/sheet is already gone or range is invalid due to prior changes
        return False
    except ValueError as ve: # For int conversion error
        logger.error(f"ValueError parsing row index from '{row_range_to_delete}': {ve}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error deleting row '{row_range_to_delete}': {e}")
        return False

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv() # For testing

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Ensure GOOGLE_SHEET_ID_FAKTURY is set in .env for testing
    if not os.getenv(config.GOOGLE_SHEET_ID_FAKTURY_ENV):
        logger.warning(f"Skipping sheets_service.py test: {config.GOOGLE_SHEET_ID_FAKTURY_ENV} not set in .env")
    else:
        logger.info(f"Testing sheets_service.py with Spreadsheet ID: {os.getenv(config.GOOGLE_SHEET_ID_FAKTURY_ENV)}")
        sample_invoice_data = {
            'invoice_date': datetime.now().strftime(config.DATE_FORMAT), # Today's date
            'issuer': 'Тестовий Постачальник ТОВ',
            'due_date': (datetime.now() + datetime.timedelta(days=14)).strftime(config.DATE_FORMAT),
            'payer': 'Тестовий Платник ФОП',
            'gross_amount': 1230.00,
            'vat_amount': 230.00,
            'is_fuel_related': True
        }
        sample_drive_link = "https://docs.google.com/document/d/example_drive_link_123"

        # Test with current month/year
        success = append_invoice_to_sheet(sample_invoice_data, sample_drive_link)
        if success:
            logger.info("Test append_invoice_to_sheet (current month) SUCCEEDED.")
        else:
            logger.error("Test append_invoice_to_sheet (current month) FAILED.")

        # Test with a different month/year to check sheet creation
        from datetime import timedelta
        sample_invoice_data_next_month = sample_invoice_data.copy()
        next_month_date = datetime.now() + timedelta(days=35) # Ensure it's next month
        sample_invoice_data_next_month['invoice_date'] = next_month_date.strftime(config.DATE_FORMAT)
        sample_invoice_data_next_month['is_fuel_related'] = False
        
        logger.info(f"Testing with next month date: {sample_invoice_data_next_month['invoice_date']}")
        success_next = append_invoice_to_sheet(sample_invoice_data_next_month, "https://drive.google.com/another_link")
        if success_next:
            logger.info("Test append_invoice_to_sheet (next month) SUCCEEDED.")
        else:
            logger.error("Test append_invoice_to_sheet (next month) FAILED.") 