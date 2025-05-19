import sqlite3
import os
from config import DB_NAME

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    if not os.path.exists(DB_NAME):
        print(f"Database '{DB_NAME}' does not exist. Creating...")
    
    conn = get_db_connection()
    try:
        # Create processed_emails table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS processed_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_id TEXT UNIQUE NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("Table 'processed_emails' created successfully or already exists.")

        # Create invoices table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT NOT NULL,
                invoice_date TEXT,
                issuer TEXT,
                due_date TEXT,
                payer TEXT,
                payer_nip TEXT,
                gross_amount REAL,
                vat_amount REAL,
                is_fuel_related INTEGER, -- 0 for false, 1 for true
                google_drive_file_id TEXT,
                google_drive_file_weblink TEXT,
                trello_card_id TEXT,
                google_sheets_row_id TEXT, -- For future use
                original_email_id TEXT, 
                attachment_filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
            )
        ''')
        print("Table 'invoices' created successfully or already exists.")

        # Trigger to update 'updated_at' timestamp on invoices table update
        conn.execute('''
            CREATE TRIGGER IF NOT EXISTS update_invoices_updated_at
            AFTER UPDATE ON invoices
            FOR EACH ROW
            BEGIN
                UPDATE invoices SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
            END;
        ''')
        print("Trigger 'update_invoices_updated_at' created successfully or already exists.")
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error initializing database: {e}")
    finally:
        conn.close()

def add_processed_email(email_id: str):
    """Adds a processed email ID to the database."""
    conn = get_db_connection()
    try:
        conn.execute("INSERT INTO processed_emails (email_id) VALUES (?)", (email_id,))
        conn.commit()
        print(f"Added email ID {email_id} to processed list.")
        return True
    except sqlite3.IntegrityError:
        print(f"Email ID {email_id} already processed.")
        return False
    except sqlite3.Error as e:
        print(f"Error adding email ID {email_id}: {e}")
        return False
    finally:
        conn.close()

def is_email_processed(email_id: str) -> bool:
    """Checks if an email ID has already been processed."""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT 1 FROM processed_emails WHERE email_id = ?", (email_id,))
        result = cursor.fetchone()
        return result is not None
    except sqlite3.Error as e:
        print(f"Error checking email ID {email_id}: {e}")
        return False
    finally:
        conn.close()

def add_invoice(invoice_data: dict) -> int | None:
    """Adds a new invoice record to the database.
    Args:
        invoice_data: A dictionary containing invoice details. 
                      Expected keys: invoice_number, invoice_date, issuer, due_date, 
                                     payer, gross_amount, vat_amount, is_fuel_related,
                                     google_drive_file_id, google_drive_file_weblink, 
                                     trello_card_id, google_sheets_row_id (optional),
                                     original_email_id, attachment_filename.
    Returns:
        The ID of the newly inserted invoice, or None if an error occurred.
    """
    conn = get_db_connection()
    try:
        # Ensure boolean is_fuel_related is stored as integer
        is_fuel_related_int = 1 if invoice_data.get('is_fuel_related') else 0

        cursor = conn.execute('''
            INSERT INTO invoices (
                invoice_number, invoice_date, issuer, due_date, payer, 
                payer_nip, gross_amount, vat_amount, is_fuel_related,
                google_drive_file_id, google_drive_file_weblink, trello_card_id,
                google_sheets_row_id, original_email_id, attachment_filename
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_data.get('invoice_number'), invoice_data.get('invoice_date'),
            invoice_data.get('issuer'), invoice_data.get('due_date'),
            invoice_data.get('payer'), invoice_data.get('payer_nip'),
            invoice_data.get('gross_amount'), invoice_data.get('vat_amount'),
            is_fuel_related_int,
            invoice_data.get('google_drive_file_id'), 
            invoice_data.get('google_drive_file_weblink'),
            invoice_data.get('trello_card_id'), 
            invoice_data.get('google_sheets_row_id'),
            invoice_data.get('original_email_id'),
            invoice_data.get('attachment_filename')
        ))
        conn.commit()
        print(f"Added invoice with number: {invoice_data.get('invoice_number')} to database. ID: {cursor.lastrowid}")
        return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"Error adding invoice {invoice_data.get('invoice_number')}: {e}")
        return None
    finally:
        conn.close()

def find_invoice(details: dict) -> dict | None:
    """
    Finds an invoice based on a set of key details.
    Searches for an exact match on: invoice_number, invoice_date, issuer, due_date, 
                                   payer, gross_amount, vat_amount, is_fuel_related.
    If multiple records match these criteria (e.g., due to reprocessing without cleanup),
    it will return the most recently created one.
    Args:
        details: A dictionary containing the key fields to search for.
    Returns:
        A dictionary representing the invoice row if found, otherwise None.
    """
    conn = get_db_connection()
    try:
        # Ensure boolean is_fuel_related is converted to int for query
        is_fuel_related_int = 1 if details.get('is_fuel_related') else 0
        
        # Convert amounts to float if they are strings, for consistent comparison.
        # SQLite REAL type can sometimes have precision issues with direct float comparison
        # but for this use case, it should be acceptable.
        # Ensure values from Gemini (string numbers) are compared correctly.
        gross_amount = float(details.get('gross_amount')) if details.get('gross_amount') is not None else None
        vat_amount = float(details.get('vat_amount')) if details.get('vat_amount') is not None else None


        query = '''
            SELECT * FROM invoices 
            WHERE invoice_number = ? AND invoice_date = ? AND issuer = ? 
            AND due_date = ? AND payer = ? AND payer_nip = ? AND gross_amount = ? 
            AND vat_amount = ? AND is_fuel_related = ?
            ORDER BY created_at DESC 
            LIMIT 1 
        '''
        cursor = conn.execute(query, (
            details.get('invoice_number'), details.get('invoice_date'),
            details.get('issuer'), details.get('due_date'),
            details.get('payer'), details.get('payer_nip'),
            gross_amount, vat_amount, is_fuel_related_int
        ))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        print(f"Error finding invoice with number {details.get('invoice_number')}: {e}")
        return None
    except (TypeError, ValueError) as conv_err: # Handle errors from float conversion
        print(f"Error converting amount for invoice {details.get('invoice_number')} during search: {conv_err}")
        return None
    finally:
        conn.close()

def find_invoices_by_number(invoice_number: str) -> list[dict]:
    """
    Finds all invoices matching a given invoice number, ordered by most recent first.
    This is useful for identifying if any version of an invoice with this number exists.
    Args:
        invoice_number: The invoice number to search for.
    Returns:
        A list of dictionaries, each representing an invoice row. Empty if none found.
    """
    conn = get_db_connection()
    invoices_list = []
    try:
        cursor = conn.execute("SELECT * FROM invoices WHERE invoice_number = ? ORDER BY created_at DESC", (invoice_number,))
        rows = cursor.fetchall()
        for row in rows:
            invoices_list.append(dict(row))
        return invoices_list
    except sqlite3.Error as e:
        print(f"Error finding invoices by number {invoice_number}: {e}")
        return []
    finally:
        conn.close()


def delete_invoice(invoice_id: int) -> bool:
    """Deletes an invoice from the database by its ID."""
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM invoices WHERE id = ?", (invoice_id,))
        conn.commit()
        print(f"Deleted invoice with ID: {invoice_id} from database.")
        return True
    except sqlite3.Error as e:
        print(f"Error deleting invoice with ID {invoice_id}: {e}")
        return False
    finally:
        conn.close()

if __name__ == '__main__':
    print("Running DB initialization...")
    init_db()
    print("DB initialization complete.")

    # Example Usage (Uncomment to test)
    # print("\n--- Testing add_invoice ---")
    # test_invoice_data_1 = {
    #     'invoice_number': "INV-2024-001", 'invoice_date': "2024-07-01",
    #     'issuer': "Test Issuer Inc.", 'due_date': "2024-07-15",
    #     'payer': "Test Payer LLC", 'gross_amount': 1200.50,
    #     'vat_amount': 200.50, 'is_fuel_related': False,
    #     'google_drive_file_id': "drive_id_1", 'google_drive_file_weblink': "drive_link_1",
    #     'trello_card_id': "trello_id_1", 'original_email_id': "email_abc_123",
    #     'attachment_filename': "invoice_A.pdf"
    # }
    # new_id = add_invoice(test_invoice_data_1)
    # if new_id:
    #     print(f"Added invoice with ID: {new_id}")

    # print("\n--- Testing find_invoice (exact match) ---")
    # found_invoice = find_invoice(test_invoice_data_1)
    # if found_invoice:
    #     print(f"Found invoice: {found_invoice['invoice_number']}, ID: {found_invoice['id']}")
    # else:
    #     print("Invoice not found by exact match.")

    # print("\n--- Testing find_invoices_by_number ---")
    # invoices_by_num = find_invoices_by_number("INV-2024-001")
    # if invoices_by_num:
    #     print(f"Found {len(invoices_by_num)} invoice(s) with number INV-2024-001:")
    #     for inv in invoices_by_num:
    #         print(f"  ID: {inv['id']}, Date: {inv['invoice_date']}, Payer: {inv['payer']}")
    # else:
    #     print("No invoices found with number INV-2024-001.")
    
    # print("\n--- Testing modified invoice data (different gross_amount) ---")
    # modified_invoice_data = test_invoice_data_1.copy()
    # modified_invoice_data['gross_amount'] = 1300.00
    # found_modified = find_invoice(modified_invoice_data)
    # if found_modified:
    #      print(f"Found modified invoice (should not match if amount differs): {found_modified['invoice_number']}")
    # else:
    #     print("Modified invoice details did not find an exact match (as expected).")


    # # Test adding another one to see if find_invoice gets the latest
    # # test_invoice_data_1_v2 = test_invoice_data_1.copy()
    # # test_invoice_data_1_v2['attachment_filename'] = "invoice_A_v2.pdf" # only one change
    # # add_invoice(test_invoice_data_1_v2)
    # # found_invoice_v2_check = find_invoice(test_invoice_data_1)
    # # if found_invoice_v2_check:
    # #      print(f"Found invoice (should be latest): {found_invoice_v2_check['invoice_number']}, attachment: {found_invoice_v2_check['attachment_filename']}, ID: {found_invoice_v2_check['id']}")


    # if new_id: # From first add_invoice
    #     print(f"\n--- Testing delete_invoice for ID: {new_id} ---")
    #     delete_success = delete_invoice(new_id)
    #     print(f"Deletion status for ID {new_id}: {delete_success}")
    #     found_after_delete = find_invoice(test_invoice_data_1)
    #     if not found_after_delete:
    #         print(f"Invoice with ID {new_id} successfully deleted and not found.")
    #     else:
    #         print(f"Error: Invoice with ID {new_id} found after deletion.")
    
    # print("\n--- Test is_email_processed ---")
    # email_to_test = "unique_email_id_for_db_test@example.com"
    # print(f"Is '{email_to_test}' processed initially? {is_email_processed(email_to_test)}")
    # add_processed_email(email_to_test)
    # print(f"Is '{email_to_test}' processed after adding? {is_email_processed(email_to_test)}")
    # add_processed_email(email_to_test) # Try adding again 