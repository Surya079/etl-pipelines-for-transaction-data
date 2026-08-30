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
    local_transaction_datetime = trans_dt_local.isoformat()

    posting_date = trans_dt_utc.date().isoformat()

    # card_details
    card_number_fulls = fake.credit_card_number().replace(" ", "")
    card_number_masked = f"{card_number_fulls[:4]} **** **** {card_number_fulls[-4:]}"
    card_expiry = fake.credit_card_expire(date_format="%m%y")
    card_holder_name = fake.name()
    card_type = random.choice(CARD_TYPES)
    card_network = random.choice(CARD_NETWORKS)

    # POS entry mode and card presence

    pos_entry_mode = random.choice(POS_ENTRY_MODES)
    card_present = pos_entry_mode in [
        "Chip",
        "Magnetic Stripe",
        "Contactless",
        "Manual Keyed",
    ]

    # Merchant details
    merchant_name = random.choice(MERCHANT_NAMES)
    merchant_id = f"MID{random.randint(100000, 999999)}"
    mcc_code = random.choice(list(MCC_CODES.keys()))
    mcc_description = MCC_CODES[mcc_code]

    # Country and Cities
    country = random.choice(COUNTRIES_CITIES.keys())
    city = random.choice(COUNTRIES_CITIES[country])
    merchant_postral_code = fake.postal_code()

    # Transaction amount and currency

    amount = round(random.uniform(1.0, 1000.0), 2)
    currency = random.choice(CURRENCIES)
    original_amount = amount

    # Transaction type
    transaction_type = random.choice(TRANSACTION_TYPES)

    # Terminal ID
    terminal_id = f"T{random.randint(100000, 999999)}"

    # Network processing
    acquirer_bank = random.choice(ACQUIRER_BANKS)
    issuer_bank = random.choice(ISSUER_BANKS)
    network = card_network

    # Authorization response code and status

    if random.random() < 0.90:
        auth_response_code = "00"
    else:

        # Decline codes (excluding 00)
        decline_codes = [k for k in RESPONSE_CODES.keys() if k != "00"]
        auth_response_code = random.choice(decline_codes)

    # Determin transaction code based on response code

    if auth_response_code == "00":
        if random.random() == 0.03:
            transaction_status = "Pending"
        else:
            transaction_status = "Approved"
    else:
        transaction_status = "Declined"

    # Authorization code (only if approved or pending)

    if transaction_status in ["Approved", "Pending"]:
        authorization_code = f"{random.randint(0,999999):06d}"
    else:
        authorization_code = None

    # Fraud/risk fields

    fraud_label = "Yes" if random.random() < 0.05 else "No"
    if fraud_label == "Yes":
        fraud_label = random.randint(60, 100)
    else:
        fraud_score = random.randint(0, 30)

    # Risk indicators (optional, filled if fraud)

    risk_indicators = []

    if fraud_label == "Yes":
        possible_indicators = [
            "high_amount",
            "foreign_country",
            "unusual_location",
            "rapid_transactions",
            "card_not_present",
            "new_device",
            "odd_hour",
            "multiple_attempts",
        ]
        risk_indicators = random.sample(possible_indicators, k=random.randint(1, 3))

    # Additional details

    device_info = random.choice(DEVICE_INFO)
    ip_address = fake.ipv4()
    user_id = str(uuid.uuid4())
    account_id = str(uuid.uuid4())

    # Build the final dictionary

    transaction ={

        #Transaction identifier
        "transaction_id":str(uuid.uuid4()),
        "trace_number":f"{random.random(0, 999999):06d}",
        "retriveval_reference_number":''.join(random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', K=12)),

        #Timestamps
        "transaction_datetime": transaction_datetime,
        "local_transaction_datetime":local_transaction_datetime,
        "posting_date":posting_date,


        # Card details    
        "card_number":card_number_masked,
        "card_expiry":card_expiry,
        "card_holder_name":card_holder_name,
        "card_type":card_type,
        "card_network":card_network,
        "card_present":card_present,

        # Merchant details
        "merchant_name":merchant_name,
        "merchant_id":merchant_id,
        "mcc":mcc_code,
        "mcc_description":mcc_description,
        "merchant_city":city,
        "merchant_country":country,
        "merchant_postal_code":merchant_postral_code,

        # Transaction details
        "amount":amount,
        "currency":currency,
        "original_amount":original_amount,
        "transaction_type":transaction_type,
        "pos_entry_mode":pos_entry_mode,
        "terminal_id":terminal_id,

        # Network / processing

        "acquirer_bank":acquirer_bank,
        "issuer_bank":issuer_bank,
        "network":network,
        "authorization_response_code":auth_response_code,
        "transaction_status":transaction_status,

        # Fraud and risk
        "fraud_score":fraud_score,
        "fraud_label":fraud_label,
        "risk_indicators":risk_indicators,

        # Additional fields
        "device_info":device_info,
        "ip_address":ip_address,
        "user_id":user_id,
        "account_id":account_id
    }

    return transaction


    