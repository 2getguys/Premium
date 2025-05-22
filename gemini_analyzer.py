import os
import pathlib
import google.generativeai as genai
import mimetypes
import json
import base64
import payer_mapping
import datetime # Ensure datetime is imported for timedelta

# DEBUG print statements removed
# print("--- Attributes of genai module ---") # DEBUG
# print(dir(genai)) # DEBUG
# print("----------------------------------") # DEBUG

# Load API key from environment or placeholder
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    print("Warning: GOOGLE_API_KEY environment variable not set. Using placeholder.")
    API_KEY = "YOUR_GOOGLE_API_KEY_PLACEHOLDER" # Ensure this is a distinct placeholder

# Налаштування API ключа
genai.configure(api_key=API_KEY)

MODEL_NAME = "gemini-2.5-pro-preview-05-06" # Or "gemini-2.0-flash" as per docs, let's stick to 1.5 for now unless issues arise

def analyze_invoice(file_path: str) -> dict | None:
    """Analyzes an invoice file (PDF, image) using Gemini API.
    Args:
        file_path: Path to the invoice file.
    Returns:
        A dictionary containing extracted invoice data, or None if analysis fails.
    """
    if API_KEY == "YOUR_GOOGLE_API_KEY_PLACEHOLDER" or not API_KEY:
        print("Error: Gemini API key is not configured with a real value. Skipping analysis.")
        return None
        
    print(f"Analyzing invoice: {file_path} using model {MODEL_NAME}")
    path = pathlib.Path(file_path)
    if not path.exists():
        print(f"Error: File not found at {file_path}")
        return None

    mime_type, _ = mimetypes.guess_type(path)
    if not mime_type:
        if file_path.lower().endswith('.pdf'):
            mime_type = 'application/pdf'
        elif file_path.lower().endswith(('.png', '.jpg', '.jpeg', '.webp', '.heic', '.heif')):
            mime_type = f'image/{path.suffix[1:].lower()}'
        else:
             print(f"Error: Could not determine MIME type for {file_path}")
             return None
        print(f"Guessed MIME type as: {mime_type}")

    prompt = f""" 
    Analyze the provided invoice document ({path.name}). Extract the following information and return it as a JSON object:
    1.  **document_type**: Classify the document. Is it a standard VAT invoice ('standard_invoice'), a proforma invoice ('proforma'), an offer ('offer'), a receipt ('receipt'), or other ('other')?
    2.  **is_paid**: A boolean (true/false). Set to true if the document is a receipt, or if the invoice explicitly states it has been paid (e.g., contains "Zapłacono", "Zapłacone", "Paid", or similar language, or if the document structure is clearly that of a receipt like a fuel receipt). If it appears to be an unpaid invoice requiring payment, set to false.
    3.  **invoice_date**: The date the invoice was issued (format YYYY-MM-DD). If it's not an invoice, this might be a document date.
    4.  **due_date**: The payment due date (format YYYY-MM-DD). If explicitly stated as a date, extract it. If not stated or if payment term is in days, this can be null.
    5.  **payment_terms_days**: An integer. If the payment term is specified in a number of days (e.g., "14 days", "Termin płatności: 7 dni"), extract the number of days. If a specific due_date is present, or if terms are not in days (e.g. "cash"), this should be null.
    6.  **payer**: The name of the company or person who needs to pay this invoice (Nabywca). Might not be relevant for some paid receipts.
    7.  **payer_nip**: The NIP (tax identification number) of the payer. Look for fields like "NIP Nabywcy" or similar. Extract it as accurately as possible, including prefixes if present.
    8.  **issuer**: The name of the company or person who issued this invoice (Sprzedawca).
    9.  **gross_amount**: The total amount including VAT (as a number, use '.' as decimal separator).
    10. **vat_amount**: The total VAT amount (as a number, use '.' as decimal separator).
    11. **is_fuel_related**: Is this invoice related to fuel or auto expenses? (true/false).
    12. **invoice_number**: The unique invoice identifier. Might be absent on some receipts.

    Important:
    - Prioritize extracting **due_date** if it's a specific calendar date. If payment terms are given in days, extract that into **payment_terms_days** and **due_date** might be null initially.
    - If the document is NOT a 'standard_invoice' requiring payment (e.g., it's a proforma, offer, receipt, or an already paid invoice), ensure 'is_paid' is true. For such documents, fields like 'due_date', 'payer' might be less relevant or absent; return null or empty string for them if not applicable.
    - For standard, unpaid invoices, 'is_paid' should be false. 'due_date' or 'payment_terms_days' should generally be present.
    - Return the data strictly as a JSON object. Do not include any introductory text or explanations outside the JSON structure.
    - Ensure all specified fields (document_type, is_paid, invoice_date, due_date, payment_terms_days, payer, payer_nip, issuer, gross_amount, vat_amount, is_fuel_related, invoice_number) are present in the JSON, even if their values are null or empty strings when not applicable.
    """

    try:
        file_bytes = path.read_bytes()
        
        # Створюємо модель за допомогою genai.GenerativeModel
        model = genai.GenerativeModel(MODEL_NAME)
        
        print(f"Sending request to Gemini model ({MODEL_NAME})...")
        
        # Створюємо вміст з файлом та промптом у правильному форматі
        content = [
            {"mime_type": mime_type, "data": base64.b64encode(file_bytes).decode()},
            {"text": prompt}
        ]
        
        # Генеруємо відповідь з правильними параметрами
        response = model.generate_content(content)
        
        print("Received response from Gemini.")
        
        cleaned_response = response.text.strip()
        if cleaned_response.startswith("```json"):
            cleaned_response = cleaned_response[7:]
        if cleaned_response.endswith("```"):
            cleaned_response = cleaned_response[:-3]
        cleaned_response = cleaned_response.strip()
        
        extracted_data = json.loads(cleaned_response)
        print(f"Initial extracted data from Gemini: {extracted_data}") # Log before cleaning NIP
        
        # Clean payer_nip - remove non-digit characters
        payer_nip_raw = extracted_data.get('payer_nip')
        if isinstance(payer_nip_raw, str):
            cleaned_nip = ''.join(filter(str.isdigit, payer_nip_raw))
            if payer_nip_raw != cleaned_nip:
                print(f"Cleaned payer_nip: '{payer_nip_raw}' -> '{cleaned_nip}'")
            extracted_data['payer_nip'] = cleaned_nip
        elif payer_nip_raw is not None: # If it's not a string but not None (e.g. a number already)
            extracted_data['payer_nip'] = str(payer_nip_raw) # Ensure it's a string for consistency, then it will be digits only

        print(f"Successfully parsed and NIP-cleaned data: {extracted_data}")
        
        required_keys = {
            'document_type', 'is_paid', 'invoice_date', 'due_date', 'payment_terms_days',
            'payer', 'payer_nip', 'issuer', 'gross_amount', 'vat_amount',
            'is_fuel_related', 'invoice_number'
        }
        if not required_keys.issubset(extracted_data.keys()):
             missing_keys = required_keys - set(extracted_data.keys())
             print(f"Error: Extracted JSON is missing required keys. Required: {required_keys}. Got: {extracted_data.keys()}. Missing: {missing_keys}")
             return None 

        # Check if Gemini classified the document as a standard invoice OR a receipt (as receipts also need processing for DB/Sheets)
        document_type = extracted_data.get("document_type")
        # Allow 'standard_invoice' and 'receipt' to proceed further for data storage.
        # Other types like 'proforma', 'offer', 'other' will be skipped.
        if document_type not in ["standard_invoice", "receipt"]:
            print(f"Document {path.name} is not a standard invoice or receipt (type: {document_type}). Skipping analysis.")
            return None 

        # Calculate due_date if payment_terms_days is provided
        due_date_str = extracted_data.get('due_date')
        payment_terms_days_val = extracted_data.get('payment_terms_days')
        invoice_date_str = extracted_data.get('invoice_date')

        # Try to parse due_date_str to see if it's already a valid date
        valid_due_date_present = False
        if due_date_str:
            try:
                datetime.datetime.strptime(due_date_str, '%Y-%m-%d')
                valid_due_date_present = True
            except ValueError:
                # due_date_str is present but not a valid date, might need calculation or is invalid
                print(f"Warning: due_date '{due_date_str}' is present but not in YYYY-MM-DD format. Will attempt calculation if payment_terms_days is available.")
                # We will proceed to check payment_terms_days

        if not valid_due_date_present and payment_terms_days_val is not None and invoice_date_str:
            try:
                invoice_date_obj = datetime.datetime.strptime(invoice_date_str, '%Y-%m-%d')
                # Ensure payment_terms_days_val is an integer
                days_to_add = int(payment_terms_days_val)
                calculated_due_date = invoice_date_obj + datetime.timedelta(days=days_to_add)
                extracted_data['due_date'] = calculated_due_date.strftime('%Y-%m-%d')
                print(f"Calculated due_date: {extracted_data['due_date']} from invoice_date: {invoice_date_str} and payment_terms_days: {days_to_add}")
            except ValueError as e:
                print(f"Error converting payment_terms_days ('{payment_terms_days_val}') to int or parsing invoice_date ('{invoice_date_str}'): {e}. Due date may remain as is or null.")
            except TypeError as e: # Handles if payment_terms_days_val is not suitable for int()
                 print(f"Error with payment_terms_days type ('{payment_terms_days_val}'): {e}. Due date may remain as is or null.")
        
        # If it is a standard invoice or receipt, proceed with payer identification and data enrichment
        identified_payer = None
        payer_nip = extracted_data.get("payer_nip")
        if payer_nip:
            identified_payer = payer_mapping.identify_payer_by_nip(payer_nip)
            if identified_payer:
                print(f"Payer identified by NIP ('{payer_nip}'): '{identified_payer}'")
                extracted_data['payer'] = identified_payer
            else:
                # NIP was extracted by Gemini, but not found in our mapping.
                # Keep the original payer name from the invoice.
                print(f"Warning: Payer NIP '{payer_nip}' (cleaned) extracted from invoice, but not found in payer_mapping. Using payer name from invoice: '{extracted_data.get('payer')}'.")
        else:
            # Спробуємо знайти NIP за назвою платника
            payer_name = extracted_data.get('payer')
            if payer_name:
                print(f"No NIP found by Gemini and no payer name to look up NIP in {path.name}. Attempting to find NIP based on payer name: '{payer_name}'")
                nip_from_mapping = payer_mapping.get_payer_nip(payer_name)
                if nip_from_mapping:
                    print(f"NIP ('{nip_from_mapping}') found for payer '{payer_name}' via mapping. Adding to extracted data.")
                    extracted_data['payer_nip'] = nip_from_mapping # Add the NIP found via name mapping
                else:
                    print(f"No NIP found for payer '{payer_name}' via mapping.")
            else:
                print(f"No NIP found by Gemini and no payer name to look up NIP in {path.name}.")
        
        return extracted_data
    except json.JSONDecodeError as json_err:
        print(f"Error: Failed to decode JSON response from Gemini: {json_err}")
        print(f"Gemini Raw Response: {response.text if 'response' in locals() else 'Response not available'}")
        return None
    except AttributeError as attr_err: # To catch issues like Client not having 'models'
        print(f"AttributeError during Gemini analysis: {attr_err}")
        return None
    except FileNotFoundError:
        print(f"Error: File not found during read: {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred during Gemini analysis for {file_path}: {e}")
        # import traceback
        # traceback.print_exc() # For more detailed error logging
        return None
