from typing import Optional, List
from pydantic import BaseModel, Field


class Transaction(BaseModel):
    """

    Schema representing a synthetic financial transaction.

    All file correspond to the output of `generate_transaction()`.

    """

    # Transaction identifiers
    transaction_id: str = Field(..., description="Unique transaction ID (UUID string).")
    trace_number: str = Field(
        ..., description="6-digit system trace audit number (STAN)."
    )
    retrieval_reference_number: str = Field(
        ..., description="12-character alphanumeric retrieval reference number."
    )
    authorization_code: Optional[str] = Field(
        None,
        description="6-digit authorization code, present only if approved/pending.",
    )

    # Timestamps
    transaction_datetime: str = Field(
        ..., description="ISO 8601 timestamp of transaction in UTC."
    )
    local_transaction_datetime: str = Field(
        ..., description="ISO 8601 timestamp with local timezone offset."
    )
    posting_date: str = Field(
        ..., description="Date (YYYY-MM-DD) when transaction is posted."
    )

    # Card details
    card_number: str = Field(
        ...,
        description="Masked card number, e.g., '4532 **** **** 1234'.",
        example="4532 **** **** 1234",
    )
    card_expiry: str = Field(
        ..., description="Card expiry in MM/YY format.", example="09/25"
    )
    cardholder_name: str = Field(..., description="Full name of cardholder.")
    card_type: str = Field(..., description="Card type: Credit, Debit, or Prepaid.")
    card_network: str = Field(
        ..., description="Card network: Visa, Mastercard, American Express, Discover."
    )
    card_present: bool = Field(
        ...,
        description="True if card was physically present (chip, stripe, contactless, manual keyed).",
    )

    # Merchant details
    merchant_name: str = Field(..., description="Name of the merchant.")
    merchant_id: str = Field(..., description="Merchant identification code.")
    mcc: str = Field(
        ..., description="Merchant Category Code (4 digits).", example="5411"
    )
    mcc_description: str = Field(..., description="Description of the MCC.")
    merchant_city: str = Field(..., description="City where the merchant is located.")
    merchant_country: str = Field(
        ..., description="Country where the merchant is located."
    )
    merchant_postal_code: str = Field(..., description="Postal code of the merchant.")

    # Transaction details
    amount: float = Field(..., description="Transaction amount.", example=23.45)
    currency: str = Field(..., description="3-letter currency code.", example="USD")
    original_amount: float = Field(
        ...,
        description="Original amount before conversion (same as amount if no conversion).",
    )
    transaction_type: str = Field(
        ...,
        description="Type of transaction: Purchase, Refund, ATM Withdrawal, Balance Inquiry, Transfer.",
    )
    pos_entry_mode: str = Field(
        ...,
        description="Point-of-sale entry mode: Chip, Magnetic Stripe, Contactless, Manual Keyed, E-commerce.",
    )
    terminal_id: str = Field(..., description="Terminal identifier.")

    # Network / processing
    acquirer_bank: str = Field(..., description="Name of the acquiring bank.")
    issuer_bank: str = Field(..., description="Name of the issuing bank.")
    network: str = Field(
        ..., description="Network used for processing (usually same as card network)."
    )
    authorization_response_code: str = Field(
        ..., description="ISO 8583 response code, e.g., '00' for approved."
    )
    transaction_status: str = Field(
        ..., description="Final status: Approved, Declined, or Pending."
    )

    # Fraud and risk
    fraud_score: int = Field(
        ..., ge=0, le=100, description="Fraud risk score from 0 to 100."
    )
    fraud_label: str = Field(..., description="Fraud label: 'Yes' or 'No'.")
    risk_indicators: List[str] = Field(
        default_factory=list,
        description="List of risk indicators (empty if not fraud).",
    )

    # Additional fields
    device_info: str = Field(..., description="Information about the device used.")
    ip_address: str = Field(..., description="IPv4 address of the device.")
    user_id: str = Field(..., description="Unique user identifier (UUID).")
    account_id: str = Field(..., description="Unique account identifier (UUID).")

    class Config:
        schema_extra = {
            "example": {
                "transaction_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                "trace_number": "123456",
                "retrieval_reference_number": "A1B2C3D4E5F6",
                "authorization_code": "483920",
                "transaction_datetime": "2025-03-15T14:23:45+00:00",
                "local_transaction_datetime": "2025-03-15T09:23:45-05:00",
                "posting_date": "2025-03-15",
                "card_number": "4532 **** **** 1234",
                "card_expiry": "09/25",
                "cardholder_name": "John Doe",
                "card_type": "Credit",
                "card_network": "Visa",
                "card_present": True,
                "merchant_name": "Starbucks",
                "merchant_id": "MID123456",
                "mcc": "5812",
                "mcc_description": "Eating Places",
                "merchant_city": "New York",
                "merchant_country": "United States",
                "merchant_postal_code": "10001",
                "amount": 4.75,
                "currency": "USD",
                "original_amount": 4.75,
                "transaction_type": "Purchase",
                "pos_entry_mode": "Contactless",
                "terminal_id": "T123456",
                "acquirer_bank": "JPMorgan Chase",
                "issuer_bank": "Chase",
                "network": "Visa",
                "authorization_response_code": "00",
                "transaction_status": "Approved",
                "fraud_score": 5,
                "fraud_label": "No",
                "risk_indicators": [],
                "device_info": "iPhone 15 Pro",
                "ip_address": "192.168.1.100",
                "user_id": "3f2b1c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
                "account_id": "9c8b7a6f-5e4d-3c2b-1a0f-9e8d7c6b5a4f",
            }
        }
