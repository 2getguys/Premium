# Configuration settings

# Google Drive
DRIVE_PARENT_FOLDER_NAME = "Документи для бухгалтера"
DRIVE_INVOICE_FOLDER_NAME = "Фактури"

# Database
DB_NAME = "db_data/processed_emails.db"

# Trello (Values should be loaded from .env file)
# These constants define the NAMES of the environment variables to be read.
# The actual values must be set in the .env file.
TRELLO_API_KEY_ENV = "TRELLO_API_KEY"
TRELLO_API_TOKEN_ENV = "TRELLO_API_TOKEN"
TRELLO_BOARD_ID_ENV = "TRELLO_BOARD_ID"
TRELLO_INVOICE_LIST_ID_ENV = "TRELLO_INVOICE_LIST_ID" # For new invoices

# Google Sheets
GOOGLE_SHEET_ID_FAKTURY_ENV = "GOOGLE_SHEET_ID_FAKTURY" # Main spreadsheet for invoices

# Headers for individual month sheets (e.g., "05.2024")
# Пов'язано з авто/паливом
SHEET_HEADERS = [
    "Номер фактури", "Дата виставлення", "Виставив", "Дата оплати", "Платник",
    "Сума (брутто)", "VAT", "Пов\'язано з авто/паливом", "Посилання на Google Drive"
]

VAT_SUMMARY_SHEET_NAME = "VAT Звіт" # Sheet name for VAT summaries
VAT_SUMMARY_HEADERS = [
    "Звітний Місяць.Рік",                  # report_month_year
    "Загальна сума VAT (до вирахувань)",   # total_vat_before_deduction
    "VAT Авто (100%)",                     # total_fuel_auto_vat_100
    "VAT до сплати (після вирахувань)"     # final_vat_payable
]

# Gmail
GMAIL_QUERY = "has:attachment label:inbox"
# Optional: Process emails received only after this date (YYYY-MM-DD).
# If None or empty, all emails matching GMAIL_QUERY will be considered.
PROCESS_EMAILS_AFTER_DATE = "2025-05-14" # Example: "2024-01-01"

# Other
MONTH_YEAR_FORMAT = "%m.%Y"
DATE_FORMAT = "%Y-%m-%d"
EMAIL_CHECK_INTERVAL_SECONDS = 20 # Check email interval in seconds 