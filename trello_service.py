import os
from trello import TrelloClient
import logging
import config # Import the config module

# Configure logging
logger = logging.getLogger(__name__)

def get_trello_client():
    """Initializes and returns a TrelloClient instance."""
    api_key = os.getenv(config.TRELLO_API_KEY_ENV)
    api_token = os.getenv(config.TRELLO_API_TOKEN_ENV)

    # --- DEBUG LOGGING: Display Trello credentials ---
    logger.info(f"Attempting to use Trello API Key (from {config.TRELLO_API_KEY_ENV}): '{api_key[:5]}...{api_key[-5:] if api_key and len(api_key) > 10 else api_key}'")
    logger.info(f"Attempting to use Trello API Token (from {config.TRELLO_API_TOKEN_ENV}): '{api_token[:5]}...{api_token[-5:] if api_token and len(api_token) > 10 else api_token}'")
    # --- END DEBUG LOGGING ---

    if not all([api_key, api_token]):
        logger.error(f"Trello API Key (env var: {config.TRELLO_API_KEY_ENV}) or Token (env var: {config.TRELLO_API_TOKEN_ENV}) not found in environment variables.")
        raise ValueError(f"Missing Trello API Key or Token in .env file. Ensure {config.TRELLO_API_KEY_ENV} and {config.TRELLO_API_TOKEN_ENV} are set.")
    
    client = TrelloClient(
        api_key=api_key,
        token=api_token
    )
    return client

def create_trello_card(invoice_data: dict, invoice_file_path: str, drive_file_link: str) -> str | None:
    """
    Creates a Trello card for an invoice.

    Args:
        invoice_data (dict): A dictionary containing extracted invoice information.
                             Expected keys: due_date, payer, issuer, gross_amount, 
                                            vat_amount, invoice_date, invoice_number.
        invoice_file_path (str): The local path to the original invoice file.
        drive_file_link (str): The Google Drive link for the uploaded invoice.

    Returns:
        str: The ID of the created Trello card, or None if creation failed.
    """
    try:
        client = get_trello_client()
        board_id = os.getenv(config.TRELLO_BOARD_ID_ENV)
        invoice_list_id = os.getenv(config.TRELLO_INVOICE_LIST_ID_ENV)

        # --- DEBUG LOGGING: Display Trello IDs ---
        logger.info(f"Attempting to use Trello Board ID (from {config.TRELLO_BOARD_ID_ENV}): '{board_id}'")
        logger.info(f"Attempting to use Trello Invoice List ID (from {config.TRELLO_INVOICE_LIST_ID_ENV}): '{invoice_list_id}'")
        # --- END DEBUG LOGGING ---

        if not all([board_id, invoice_list_id]):
            logger.error(f"Trello Board ID (env var: {config.TRELLO_BOARD_ID_ENV}) or Invoice List ID (env var: {config.TRELLO_INVOICE_LIST_ID_ENV}) not found in environment variables.")
            raise ValueError(f"Missing Trello Board ID or Invoice List ID in .env file. Ensure {config.TRELLO_BOARD_ID_ENV} and {config.TRELLO_INVOICE_LIST_ID_ENV} are set.")

        # Get the board
        board = client.get_board(board_id)
        if not board:
            logger.error(f"Trello board with ID '{board_id}' (from env var {config.TRELLO_BOARD_ID_ENV}) not found.")
            return None

        # Get the list
        invoice_list = board.get_list(invoice_list_id)
        if not invoice_list:
            logger.error(f"Trello list with ID '{invoice_list_id}' (from env var {config.TRELLO_INVOICE_LIST_ID_ENV}) not found on board '{board_id}'.")
            return None

        # Prepare card details
        due_date_str = invoice_data.get("due_date", "N/A")
        card_name = f"Оплатити до: {due_date_str}"
        
        is_fuel_related_str = "Так" if invoice_data.get("is_fuel_related") else "Ні"

        # Align description with Google Sheets columns (config.SHEET_HEADERS)
        # SHEET_HEADERS = ["Номер фактури", "Дата виставлення", "Виставив", "Дата оплати", "Платник", 
        # "Сума (брутто)", "VAT", "Пов'язано з авто/паливом", "Посилання на Google Drive"]
        description_parts = [
            f"{config.SHEET_HEADERS[0]}: {invoice_data.get('invoice_number', 'N/A')}", # Номер фактури
            f"{config.SHEET_HEADERS[1]}: {invoice_data.get('invoice_date', 'N/A')}",   # Дата виставлення
            f"{config.SHEET_HEADERS[2]}: {invoice_data.get('issuer', 'N/A')}",         # Виставив
            f"{config.SHEET_HEADERS[3]}: {invoice_data.get('due_date', 'N/A')}",           # Дата оплати
            f"{config.SHEET_HEADERS[4]}: {invoice_data.get('payer', 'N/A')}",          # Платник
            f"{config.SHEET_HEADERS[5]}: {invoice_data.get('payer_nip', 'N/A')}",      # NIP Платника
            f"{config.SHEET_HEADERS[6]}: {invoice_data.get('gross_amount', 'N/A')}",   # Сума (брутто)
            f"{config.SHEET_HEADERS[7]}: {invoice_data.get('vat_amount', 'N/A')}",          # VAT
            f"{config.SHEET_HEADERS[8]}: {is_fuel_related_str}",                         # Пов'язано з авто/паливом
            f"{config.SHEET_HEADERS[9]}: {drive_file_link if drive_file_link else 'N/A'}" # Посилання на Google Drive
        ]
        description = "\n".join(description_parts)

        # Create the card
        new_card = invoice_list.add_card(name=card_name, desc=description)
        logger.info(f"Trello card '{new_card.name}' created successfully. ID: {new_card.id}, URL: {new_card.url}")

        # Attach the invoice file
        if os.path.exists(invoice_file_path):
            with open(invoice_file_path, 'rb') as f:
                new_card.attach(name=os.path.basename(invoice_file_path), file=f)
            logger.info(f"Attached file '{os.path.basename(invoice_file_path)}' to card '{new_card.name}'.")
        else:
            logger.warning(f"Invoice file not found at path: {invoice_file_path}. Cannot attach to Trello card.")

        return new_card.id # Return the card ID

    except Exception as e:
        logger.error(f"Error creating Trello card: {e}", exc_info=True)
        return None

def delete_trello_card(card_id: str) -> bool:
    """
    Deletes a Trello card by its ID.

    Args:
        card_id (str): The ID of the Trello card to delete.

    Returns:
        bool: True if the card was deleted successfully, False otherwise.
    """
    try:
        client = get_trello_client()
        card = client.get_card(card_id)
        if card:
            card.delete()
            logger.info(f"Trello card with ID '{card_id}' deleted successfully.")
            return True
        else:
            # This case should ideally not be reached if get_card raises an exception for not found.
            # However, if get_card returns None (though unlikely for py-trello), this handles it.
            logger.warning(f"Trello card with ID '{card_id}' not found for deletion (get_card returned None).")
            return False # Or True if not found means effectively deleted for our purpose
    except Exception as e: # Catches exceptions from get_card (e.g., TrelloResourceUnavailable if not found) or delete()
        logger.error(f"Error deleting Trello card with ID '{card_id}': {e}", exc_info=True)
        # Check if the error indicates the card was not found (common for Trello API)
        # py-trello might raise TrelloResourceUnavailable (subclass of TrelloError)
        if "card not found" in str(e).lower() or (hasattr(e, 'response') and e.response and e.response.status_code == 404):
            logger.info(f"Trello card with ID '{card_id}' was not found, considering it deleted.")
            return True
        return False

if __name__ == '__main__':
    # This is for testing purposes.
    # You'll need to set up your .env file with Trello credentials and IDs.
    # And have a sample file to attach.
    from dotenv import load_dotenv
    load_dotenv() # Load environment variables from .env file

    # Check if required env variables are set using config names
    required_env_vars_for_test = [
        config.TRELLO_API_KEY_ENV,
        config.TRELLO_API_TOKEN_ENV,
        config.TRELLO_BOARD_ID_ENV,
        config.TRELLO_INVOICE_LIST_ID_ENV
    ]

    if not all(os.getenv(var) for var in required_env_vars_for_test):
        print("Skipping Trello test: Required Trello environment variables are not set.")
        print(f"Please set the following in your .env file: {', '.join(required_env_vars_for_test)}")
    else:
        # Create a dummy invoice file for testing
        sample_file_path = "sample_invoice_trello_test.txt"
        with open(sample_file_path, "w") as f:
            f.write("This is a test invoice file for Trello.")

        sample_invoice_data = {
            "due_date": "2024-12-31",
            "payer": "Test Payer Company",
            "issuer": "Test Issuer Inc.",
            "gross_amount": "1230.00 PLN",
            "vat_amount": "230.00 PLN",
            "invoice_date": "2024-12-01",
            "invoice_number": "INV-TRELLO-TEST-001", # Added for testing
            "is_fuel_related": True # Added for testing
        }
        sample_drive_link = "https://docs.google.com/document/d/mock_drive_link"

        print(f"Attempting to create Trello card with sample data...")
        created_card_id = create_trello_card(sample_invoice_data, sample_file_path, sample_drive_link)

        if created_card_id:
            print(f"Trello card created successfully! ID: {created_card_id}")
            
            # Test deleting the card
            # print(f"\nAttempting to delete Trello card with ID: {created_card_id}...")
            # delete_success = delete_trello_card(created_card_id)
            # if delete_success:
            #     print(f"Trello card {created_card_id} deleted successfully.")
            # else:
            #     print(f"Failed to delete Trello card {created_card_id}. Check logs.")
            
            # # Try deleting again to test 404 handling
            # print(f"\nAttempting to delete Trello card with ID: {created_card_id} again (should be not found)...")
            # delete_again_success = delete_trello_card(created_card_id)
            # if delete_again_success:
            #     print(f"Trello card {created_card_id} reported as deleted (already gone).")
            # else:
            #     print(f"Deletion check for already deleted card {created_card_id} failed unexpectedly. Check logs.")

        else:
            print("Failed to create Trello card. Check logs for details.")
        
        # Clean up dummy file
        if os.path.exists(sample_file_path):
            os.remove(sample_file_path)
            print(f"Cleaned up {sample_file_path}") 