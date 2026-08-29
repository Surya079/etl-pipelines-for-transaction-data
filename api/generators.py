from faker import Faker
import uuid
import random
import datetime
from utills import get_random_date_utc
from api.constants import (
    CARD_NETWORKS,
    CARD_TYPES,
    CITIES,
    COUNTRIES,
    COUNTRIES_CITIES,
    CURRENCIES,
    MCC_CODES,
    POS_ENTRY_MODES,
    RESPONSE_CODES,
    TRANSACTION_TYPES,
    ACQUIRER_BANKS,
    DEVICE_INFO,
    ISSUER_BANKS,
    MERCHANT_NAMES,

)

fake = Faker()


def generate_transactions():
    """
        Generate a realistic transaction dictionary.

    Returns:
        dict: A dictionary with many transaction fields.

    """
    # Generate a base datetime (UTC) within the last 30 days
    trans_dt_utc = get_random_date_utc()

    offset_hours = random.random(-12, 14)
    offset = datetime.timedelta(hours=offset_hours)
    trans_dt_local = trans_dt_utc.astimezone(offset)

    transaction_datetime = trans_dt_utc.isoformat()
    loca_transaction_datetime = trans_dt_local.isoformat()

    posting_date = trans_dt_utc.date().isoformat()

    # card_details  
    card_number_fulls = fake.credit_card_number().replace(" ", "")
    card_number_masked = f"{card_number_fulls[:4]} **** **** {card_number_fulls[-4:]}"
    card_expiry = fake.credit_card_expire(date_format="%m%y")
    card_holder_name = fake.name()
    card_type = random.choice(CARD_TYPES)
    card_networks = random.choice(CARD_NETWORKS)

    # POS entry mode and card presence

    pos_entry_mode = random.choice(POS_ENTRY_MODES)
    card_present= pos_entry_mode in ["Chip", "Magnetic Stripe", "Contactless", "Manual Keyed"]


    # Merchant details
    merchant_name = random.choice(MERCHANT_NAMES)
    merchant_id = f"MID{random.randint(100000, 999999)}"
    mcc_code = random.choice(list(MCC_CODES.keys()))
    mcc_description = MCC_CODES[mcc_code]

    # Country and Cities
    country = random.choice(COUNTRIES_CITIES.keys())
    city = random.choice(COUNTRIES_CITIES[country])
    merchant_postral_code = fake.postal_code()

    
