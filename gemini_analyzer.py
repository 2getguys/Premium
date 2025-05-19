import os
import pathlib
import google.generativeai as genai
import mimetypes
import json
import base64
import payer_mapping

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
    1.  **invoice_date**: The date the invoice was issued (format YYYY-MM-DD).
    2.  **due_date**: The payment due date (format YYYY-MM-DD).
    3.  **payer**: The name of the company or person who needs to pay this invoice.
    4.  **payer_nip**: The NIP (tax identification number) of the payer. Look for fields like "NIP Nabywcy" or similar.
    5.  **issuer**: The name of the company or person who issued this invoice.
    6.  **gross_amount**: The total amount including VAT (as a number, use '.' as decimal separator).
    7.  **vat_amount**: The total VAT amount (as a number, use '.' as decimal separator).
    8.  **is_fuel_related**: A boolean (true/false). Set to true if the invoice contains items related to fuel (e.g., gasoline, diesel, petrol, 'paliwo') or car maintenance/parts. Otherwise, set to false.
    9.  **invoice_number**: The unique identification number of the invoice.
    If any information cannot be found, use null for that field in the JSON object.
    Ensure the output is ONLY the JSON object, without any introductory text or markdown formatting.
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
        print(f"Successfully parsed extracted data: {extracted_data}")
        
        required_keys = {'invoice_date', 'due_date', 'payer', 'payer_nip', 'issuer', 'gross_amount', 'vat_amount', 'is_fuel_related', 'invoice_number'}
        if not required_keys.issubset(extracted_data.keys()):
             print("Error: Extracted JSON is missing required keys.")
             return None 

        # Ідентифікуємо платника за NIP
        payer_nip = extracted_data.get('payer_nip')
        if payer_nip:
            identified_payer = payer_mapping.identify_payer_by_nip(payer_nip)
            if identified_payer:
                print(f"Identified payer by NIP {payer_nip}: {identified_payer}")
                extracted_data['payer'] = identified_payer
            else:
                print(f"Warning: Could not identify payer by NIP {payer_nip}")
        else:
            # Спробуємо знайти NIP за назвою платника
            payer_name = extracted_data.get('payer')
            if payer_name:
                payer_nip = payer_mapping.get_payer_nip(payer_name)
                if payer_nip:
                    print(f"Found NIP {payer_nip} for payer {payer_name}")
                    extracted_data['payer_nip'] = payer_nip
        
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
