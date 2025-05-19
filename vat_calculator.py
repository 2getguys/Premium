import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import config
import sheets_service

logger = logging.getLogger(__name__)

# Column indices from config.SHEET_HEADERS (for reading monthly invoice data)
# config.SHEET_HEADERS = [
#     "Номер фактури", "Дата виставлення", "Виставив", "Дата оплати", "Платник",
#     "NIP Платника", "Сума (брутто)", "VAT", "Пов'язано з авто/паливом", "Посилання на Google Drive"
# ]
COL_IDX_INVOICE_NUMBER = config.SHEET_HEADERS.index("Номер фактури")
COL_IDX_INVOICE_DATE = config.SHEET_HEADERS.index("Дата виставлення")
COL_IDX_GROSS_AMOUNT = config.SHEET_HEADERS.index("Сума (брутто)")
COL_IDX_VAT = config.SHEET_HEADERS.index("VAT")
COL_IDX_IS_FUEL_RELATED = config.SHEET_HEADERS.index("Пов'язано з авто/паливом")

def get_previous_month_sheet_title():
    """Determines the sheet title for the previous month (e.g., '04.2024')."""
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month.strftime(config.MONTH_YEAR_FORMAT)

def parse_decimal(value_str: str) -> Decimal | None:
    """Safely parses a string to a Decimal, handling potential errors."""
    if isinstance(value_str, (int, float, Decimal)):
        return Decimal(value_str)
    try:
        return Decimal(str(value_str).replace(',', '.').strip())
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(f"Could not parse '{value_str}' as Decimal.")
        return None

def calculate_and_record_vat_summary():
    """Calculates VAT summary for the previous month and records it to the summary sheet."""
    logger.info("Starting VAT calculation for the previous month...")
    spreadsheet_id = os.getenv(config.GOOGLE_SHEET_ID_FAKTURY_ENV)
    if not spreadsheet_id:
        logger.error(f"Spreadsheet ID {config.GOOGLE_SHEET_ID_FAKTURY_ENV} not found in .env. Aborting VAT calculation.")
        return

    prev_month_sheet_title = get_previous_month_sheet_title()
    logger.info(f"Reading data from sheet: '{prev_month_sheet_title}' for VAT calculation.")

    data = sheets_service.read_sheet_data(spreadsheet_id, prev_month_sheet_title)

    if data is None:
        logger.error(f"Failed to read data from sheet '{prev_month_sheet_title}'. Aborting VAT calculation.")
        return
    if not data or len(data) <= 1: # <=1 to account for only header row or empty
        logger.info(f"No data (or only headers) found in sheet '{prev_month_sheet_title}'. Nothing to calculate.")
        _write_vat_summary_row(spreadsheet_id, prev_month_sheet_title, Decimal(0), Decimal(0), Decimal(0)) # Write zero summary
        return

    header_row_from_sheet = data[0]
    invoice_rows = data[1:]

    # Validate headers from the sheet against config.SHEET_HEADERS
    if header_row_from_sheet != config.SHEET_HEADERS:
        logger.warning(
            f"Header row in '{prev_month_sheet_title}' ({header_row_from_sheet}) "
            f"does not match expected headers ({config.SHEET_HEADERS}). "
            f"Proceeding based on column indices derived from config.SHEET_HEADERS."
        )
        # Potential issue: if columns are actually different, indices might be wrong.
        # For now, we trust the indices derived from config at the top of the file.

    # --- Deduplication (based on Invoice Number, Invoice Date, Gross Amount) ---
    processed_invoices = set()
    unique_invoice_rows = []
    for row in invoice_rows:
        try:
            # Ensure row has enough columns before accessing by index
            if len(row) > max(COL_IDX_INVOICE_NUMBER, COL_IDX_INVOICE_DATE, COL_IDX_GROSS_AMOUNT):
                invoice_number = row[COL_IDX_INVOICE_NUMBER].strip()
                invoice_date = row[COL_IDX_INVOICE_DATE].strip()
                gross_amount_str = row[COL_IDX_GROSS_AMOUNT].strip()
                invoice_key = (invoice_number, invoice_date, gross_amount_str)
                if invoice_key not in processed_invoices:
                    unique_invoice_rows.append(row)
                    processed_invoices.add(invoice_key)
                else:
                    logger.info(f"Duplicate invoice found and skipped: {invoice_key}")
            else:
                logger.warning(f"Skipping row due to insufficient columns: {row}")
        except Exception as e:
            logger.error(f"Error during deduplication for row {row}: {e}")
            continue
    
    logger.info(f"Processed {len(unique_invoice_rows)} unique invoices after deduplication.")

    total_vat_before_deduction = Decimal(0)
    total_fuel_auto_vat = Decimal(0)

    for row in unique_invoice_rows:
        try:
            if len(row) > max(COL_IDX_VAT, COL_IDX_IS_FUEL_RELATED):
                vat_amount_str = row[COL_IDX_VAT]
                is_fuel_str = row[COL_IDX_IS_FUEL_RELATED].strip().lower()
                
                vat_amount = parse_decimal(vat_amount_str)
                if vat_amount is None:
                    logger.warning(f"Could not parse VAT amount for row: {row}. Skipping VAT for this row.")
                    continue

                total_vat_before_deduction += vat_amount
                if is_fuel_str == 'так':
                    total_fuel_auto_vat += vat_amount
            else:
                logger.warning(f"Skipping row in VAT calculation due to insufficient columns: {row}")
        except Exception as e:
            logger.error(f"Unexpected error processing row {row} for VAT calculation: {e}")
            continue
            
    logger.info(f"Total VAT (before deductions): {total_vat_before_deduction}")
    logger.info(f"Total Fuel/Auto VAT (100%): {total_fuel_auto_vat}")

    vat_fuel_auto_50_percent_to_deduct = total_fuel_auto_vat / Decimal(2)
    final_vat_payable = total_vat_before_deduction - vat_fuel_auto_50_percent_to_deduct

    logger.info(f"50% Fuel/Auto VAT (for deduction): {vat_fuel_auto_50_percent_to_deduct}")
    logger.info(f"Final VAT Payable (after deductions): {final_vat_payable}")

    # Write to summary sheet
    _write_vat_summary_row(spreadsheet_id, prev_month_sheet_title, total_vat_before_deduction, total_fuel_auto_vat, final_vat_payable)

def _write_vat_summary_row(spreadsheet_id, report_month_year, total_vat_before_deduction, total_fuel_auto_vat_100, final_vat_payable):
    """Writes a single summary row to the VAT Summary sheet with 4 columns."""
    summary_sheet_title = config.VAT_SUMMARY_SHEET_NAME
    service = sheets_service.get_sheets_service()

    if not service:
        logger.error("Sheets service not available for writing VAT summary.")
        return

    if not sheets_service._ensure_sheet_tab_with_headers(service, spreadsheet_id, summary_sheet_title, config.VAT_SUMMARY_HEADERS):
        logger.error(f"Failed to ensure VAT summary sheet '{summary_sheet_title}' with headers. Cannot write summary.")
        return

    # Data for the new 4-column structure:
    # config.VAT_SUMMARY_HEADERS = [
    #     "Звітний Місяць.Рік",
    #     "Загальна сума VAT (до вирахувань)",
    #     "VAT Авто (100%)",
    #     "VAT до сплати (після вирахувань)"
    # ]
    summary_row_data = [
        report_month_year,
        f"{total_vat_before_deduction:.2f}",
        f"{total_fuel_auto_vat_100:.2f}",
        f"{final_vat_payable:.2f}"
    ]

    try:
        existing_data = sheets_service.read_sheet_data(spreadsheet_id, summary_sheet_title)
        row_index_to_update = -1
        if existing_data and len(existing_data) > 1:
            for i, row in enumerate(existing_data[1:], start=2):
                if row and len(row) > 0 and row[0] == report_month_year:
                    row_index_to_update = i
                    break
        
        if row_index_to_update != -1:
            logger.info(f"Updating existing VAT summary row for '{report_month_year}' at row {row_index_to_update}.")
            range_to_update = f"'{summary_sheet_title}'!A{row_index_to_update}"
            body = {'values': [summary_row_data]}
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_to_update,
                valueInputOption='USER_ENTERED', body=body
            ).execute()
        else:
            logger.info(f"Appending new VAT summary row for '{report_month_year}'.")
            body = {'values': [summary_row_data]}
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=f"'{summary_sheet_title}'!A1",
                valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body=body
            ).execute()
        logger.info(f"Successfully wrote VAT summary for '{report_month_year}' to '{summary_sheet_title}'.")
    except HttpError as e:
        logger.error(f"Error writing VAT summary to sheet '{summary_sheet_title}': {e}")
    except Exception as e_gen:
        logger.error(f"Generic error writing VAT summary for '{report_month_year}': {e_gen}")

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Manually triggering VAT calculation for previous month...")
    calculate_and_record_vat_summary()
    logger.info("VAT calculation process finished.") 