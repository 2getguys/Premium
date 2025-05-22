import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from collections import defaultdict # Added for grouping

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
COL_IDX_PAYER = config.SHEET_HEADERS.index("Платник")
COL_IDX_PAYER_NIP = config.SHEET_HEADERS.index("NIP Платника")
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
    """Calculates VAT summary for the previous month for each payer and records it."""
    logger.info("Starting VAT calculation for the previous month by payer...")
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
        # No overall zero summary needed as we report per payer.
        # If a summary sheet exists, it won't be updated for this month if no data.
        return

    header_row_from_sheet = data[0]
    invoice_rows = data[1:]

    if header_row_from_sheet != config.SHEET_HEADERS:
        logger.warning(
            f"Header row in '{prev_month_sheet_title}' ({header_row_from_sheet}) "
            f"does not match expected headers ({config.SHEET_HEADERS}). "
            f"Proceeding based on column indices derived from config.SHEET_HEADERS."
        )

    # --- Deduplication and Grouping by Payer (using NIP as the primary key for payer) ---
    # Dictionary to hold unique invoices grouped by (payer_nip, payer_name)
    # Key: (payer_nip, payer_name), Value: set of (invoice_number, invoice_date, gross_amount_str)
    grouped_unique_invoices_keys = defaultdict(set)
    # Dictionary to hold the actual row data for unique invoices, keyed by (payer_nip, payer_name)
    # Key: (payer_nip, payer_name), Value: list of [row_data]
    payer_invoices_data = defaultdict(list)

    for row in invoice_rows:
        try:
            # Ensure row has enough columns before accessing by index
            if len(row) > max(COL_IDX_INVOICE_NUMBER, COL_IDX_INVOICE_DATE, COL_IDX_GROSS_AMOUNT, COL_IDX_PAYER, COL_IDX_PAYER_NIP, COL_IDX_VAT, COL_IDX_IS_FUEL_RELATED):
                payer_name = str(row[COL_IDX_PAYER]).strip()
                payer_nip = str(row[COL_IDX_PAYER_NIP]).strip()
                
                if not payer_nip and not payer_name: # Skip if no payer identifier
                    logger.warning(f"Skipping row due to missing Payer Name and Payer NIP: {row}")
                    continue
                
                # Use NIP if available, otherwise Payer Name as part of the group key.
                # Prefer NIP as it's more unique. If NIP is missing, group by name but log warning.
                if not payer_nip:
                    logger.warning(f"Payer NIP is missing for payer '{payer_name}'. Grouping by name only for this entry.")
                
                payer_group_key = (payer_nip if payer_nip else "NIP_UNKNOWN", payer_name)


                invoice_number = str(row[COL_IDX_INVOICE_NUMBER]).strip()
                invoice_date = str(row[COL_IDX_INVOICE_DATE]).strip()
                gross_amount_str = str(row[COL_IDX_GROSS_AMOUNT]).strip()
                
                invoice_dedup_key = (invoice_number, invoice_date, gross_amount_str)

                if invoice_dedup_key not in grouped_unique_invoices_keys[payer_group_key]:
                    payer_invoices_data[payer_group_key].append(row)
                    grouped_unique_invoices_keys[payer_group_key].add(invoice_dedup_key)
                else:
                    logger.info(f"Duplicate invoice found for payer '{payer_name}' (NIP: {payer_nip}) and skipped: {invoice_dedup_key}")
            else:
                logger.warning(f"Skipping row due to insufficient columns: {row}")
        except Exception as e:
            logger.error(f"Error during deduplication/grouping for row {row}: {e}", exc_info=True)
            continue
    
    if not payer_invoices_data:
        logger.info(f"No unique invoices found after deduplication and grouping for sheet '{prev_month_sheet_title}'.")
        return

    logger.info(f"Processing VAT for {len(payer_invoices_data)} unique payers/groups.")

    for (payer_nip_key, payer_name_key), unique_rows_for_payer in payer_invoices_data.items():
        logger.info(f"--- Calculating VAT for Payer: {payer_name_key} (NIP: {payer_nip_key if payer_nip_key != 'NIP_UNKNOWN' else 'N/A'}) ---")
        logger.info(f"Found {len(unique_rows_for_payer)} unique invoices for this payer.")

        total_vat_before_deduction = Decimal(0)
        total_fuel_auto_vat = Decimal(0)

        for row_data in unique_rows_for_payer:
            try:
                # Already checked length during grouping, but good practice
                vat_amount_str = row_data[COL_IDX_VAT]
                is_fuel_str = str(row_data[COL_IDX_IS_FUEL_RELATED]).strip().lower()
                
                vat_amount = parse_decimal(vat_amount_str)
                if vat_amount is None:
                    logger.warning(f"Could not parse VAT amount for row: {row_data} (Payer: {payer_name_key}). Skipping VAT for this row.")
                    continue

                total_vat_before_deduction += vat_amount
                if is_fuel_str == 'так':
                    total_fuel_auto_vat += vat_amount
            except IndexError:
                 logger.warning(f"Skipping row in VAT calculation for payer {payer_name_key} due to insufficient columns: {row_data}")
            except Exception as e_calc:
                logger.error(f"Unexpected error processing row {row_data} for VAT calculation (Payer: {payer_name_key}): {e_calc}", exc_info=True)
                continue
        
        logger.info(f"Payer: {payer_name_key} (NIP: {payer_nip_key}) - Total VAT (before deductions): {total_vat_before_deduction}")
        logger.info(f"Payer: {payer_name_key} (NIP: {payer_nip_key}) - Total Fuel/Auto VAT (100%): {total_fuel_auto_vat}")

        vat_fuel_auto_50_percent_to_deduct = total_fuel_auto_vat / Decimal(2)
        final_vat_payable = total_vat_before_deduction - vat_fuel_auto_50_percent_to_deduct

        logger.info(f"Payer: {payer_name_key} (NIP: {payer_nip_key}) - 50% Fuel/Auto VAT (for deduction): {vat_fuel_auto_50_percent_to_deduct}")
        logger.info(f"Payer: {payer_name_key} (NIP: {payer_nip_key}) - Final VAT Payable (after deductions): {final_vat_payable}")

        _write_vat_summary_row(
            spreadsheet_id, 
            prev_month_sheet_title, # This is the "Звітний Місяць.Рік"
            payer_name_key,
            payer_nip_key if payer_nip_key != 'NIP_UNKNOWN' else "", # Pass empty string if NIP was unknown
            total_vat_before_deduction, 
            total_fuel_auto_vat, 
            final_vat_payable
        )

def _write_vat_summary_row(spreadsheet_id, report_month_year, payer_name, payer_nip, total_vat_before_deduction, total_fuel_auto_vat_100, final_vat_payable):
    """Writes a single summary row to the VAT Summary sheet for a specific payer."""
    summary_sheet_title = config.VAT_SUMMARY_SHEET_NAME
    service = sheets_service.get_sheets_service()

    if not service:
        logger.error("Sheets service not available for writing VAT summary.")
        return

    if not sheets_service._ensure_sheet_tab_with_headers(service, spreadsheet_id, summary_sheet_title, config.VAT_SUMMARY_HEADERS):
        logger.error(f"Failed to ensure VAT summary sheet '{summary_sheet_title}' with headers. Cannot write summary.")
        return
    
    # Data for the new multi-column structure:
    # config.VAT_SUMMARY_HEADERS = [
    #     "Звітний Місяць.Рік", "Платник", "NIP Платника", 
    #     "Загальна сума VAT (до вирахувань)", "VAT Авто (100%)", "VAT до сплати (після вирахувань)"
    # ]
    summary_row_data = [
        report_month_year,
        payer_name,
        payer_nip,
        f"{total_vat_before_deduction:.2f}",
        f"{total_fuel_auto_vat_100:.2f}",
        f"{final_vat_payable:.2f}"
    ]

    try:
        existing_data = sheets_service.read_sheet_data(spreadsheet_id, summary_sheet_title)
        row_index_to_update = -1
        if existing_data and len(existing_data) > 1: # Headers + data
            for i, row in enumerate(existing_data[1:], start=2): # Start from 2 for 1-based indexing in Sheets
                # Check for matching report_month_year AND payer_nip (or payer_name if NIP is empty)
                if row and len(row) >=3 and row[0] == report_month_year:
                    sheet_payer_nip = str(row[2]).strip()
                    sheet_payer_name = str(row[1]).strip()
                    
                    # Match primarily on NIP if available, otherwise on name.
                    # This handles cases where NIP might have been missing for the payer_group_key
                    match_found = False
                    if payer_nip and sheet_payer_nip == payer_nip:
                        match_found = True
                    elif not payer_nip and not sheet_payer_nip and sheet_payer_name == payer_name: # Both NIPs are empty, match by name
                        match_found = True
                    # Add a fallback if one NIP is present and the other is not, but names match?
                    # For now, strict NIP match if present, or name match if both NIPs are absent.
                    
                    if match_found:
                        row_index_to_update = i
                        break
        
        if row_index_to_update != -1:
            logger.info(f"Updating existing VAT summary row for '{report_month_year}', Payer: '{payer_name}' (NIP: {payer_nip}) at row {row_index_to_update}.")
            # Update the entire row for the specific payer and month
            range_to_update = f"'{summary_sheet_title}'!A{row_index_to_update}" 
            body = {'values': [summary_row_data]}
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_to_update,
                valueInputOption='USER_ENTERED', body=body
            ).execute()
        else:
            logger.info(f"Appending new VAT summary row for '{report_month_year}', Payer: '{payer_name}' (NIP: {payer_nip}).")
            body = {'values': [summary_row_data]}
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=f"'{summary_sheet_title}'!A1",
                valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body=body
            ).execute()
        logger.info(f"Successfully wrote VAT summary for '{report_month_year}', Payer: '{payer_name}' to '{summary_sheet_title}'.")

    except HttpError as e: # Make sure HttpError is imported or handle googleapiclient.errors.HttpError
        logger.error(f"Google API HttpError writing VAT summary to sheet '{summary_sheet_title}' for Payer '{payer_name}': {e}", exc_info=True)
    except Exception as e_gen:
        logger.error(f"Generic error writing VAT summary for '{report_month_year}', Payer '{payer_name}': {e_gen}", exc_info=True)

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv() # Ensure .env is loaded for direct script execution
    
    # Configure logging for direct script execution
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Create a logger for this specific execution context if needed, or use the root logger
    script_logger = logging.getLogger("vat_calculator_manual_run")
    script_logger.info("Manually triggering VAT calculation for previous month...")
    
    calculate_and_record_vat_summary()
    
    script_logger.info("VAT calculation process finished.") 