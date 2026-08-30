"""

FastAPI application generating synthetic transaction data.
"""

from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict
import uuid

from .generators import generate_transactions
from .schema import Transaction, TransactionResponse

app = FastAPI(
    title="Synthetic Transaction Generator API",
    description="Generate realistic synthetic financial transaction data for testing and simulation.",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    """
    Health check endpoint.
    Return a simple status.
    """

    return {"status": "ok"}


@app.get("/transactions", response_model=TransactionResponse)
def get_transactions(
    page: int = Query(1, ge=1, description="Page number, starting from 1."),
    limit: int = Query(10, ge=1, description="Number of items per page (max 100)."),
):
    """
    Generate a list of synthetic transactions

    Pagination is simulated; each request generates fresh random transactions.
    The `total` field is a fixed large number to represent a large dataset.
    """

    total_simulated = 100000

    data = [generate_transactions() for _ in range(limit)]

    return {"page": page, "limit": limit, "total": total_simulated, "data": data}


@app.get("/transaction/{transaction_id}", response_model=Transaction)
def get_transaction(transaction_id: str):
    """
    Generate a single transaction and override its transaction_id with the provided path parameter.
    This is for simulation; no actual storage is used.
    """
    transaction = generate_transactions()
    transaction["transaction_id"] = transaction_id
    return transaction
