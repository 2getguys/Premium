"""
Модуль для ідентифікації платників за їх NIP номерами.
"""

# Маппінг NIP до назв платників
PAYER_NIP_MAPPING = {
    "5214052965": "Bohdan Yeromin - Premium Kawior",
    "5214032313": "Premium Kawior spolka z o. o.",
    "5253033512": "Premium Maksym Yeromin"
}

def identify_payer_by_nip(nip: str) -> str | None:
    """
    Ідентифікує платника за його NIP номером.
    
    Args:
        nip: NIP номер платника
        
    Returns:
        Назва платника, якщо NIP знайдено в маппінгу, інакше None
    """
    if not nip:
        return None
        
    # Видаляємо всі нецифрові символи з NIP
    clean_nip = ''.join(filter(str.isdigit, str(nip)))
    
    return PAYER_NIP_MAPPING.get(clean_nip)

def get_payer_nip(payer_name: str) -> str | None:
    """
    Отримує NIP номер за назвою платника.
    
    Args:
        payer_name: Назва платника
        
    Returns:
        NIP номер, якщо платник знайдено в маппінгу, інакше None
    """
    if not payer_name:
        return None
        
    # Шукаємо NIP за назвою платника
    for nip, name in PAYER_NIP_MAPPING.items():
        if name.lower() == payer_name.lower():
            return nip
            
    return None 