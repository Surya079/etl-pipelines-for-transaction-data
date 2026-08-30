# DATA ENGINEERING DATA ETL PIPELINE

## Synthetic Transaction Generator API

A FastAPI application that generates realistic synthetic financial transaction data for testing, simulation, and development purposes. The API returns random transactions with many fields covering card details, merchant information, amounts, timestamps, and fraud indicators.

## Features

- Generate single or multiple random transactions.
- Rich set of fields modeled after real payment processing data.
- Uses [Faker](https://faker.readthedocs.io/) for realistic names, addresses, and other personal data.
- Pydantic schema for validation and automatic OpenAPI documentation.
- No database required – all data is generated on the fly.

## Requirements

- Python 3.9+
- Dependencies listed in `requirements.txt` (install with `pip install -r requirements.txt`).

## Installation and Local Run

1. **Clone the repository** (or download the code).

2. **Create and activate a virtual environment** (recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate        # Linux/macOS
   venv\Scripts\activate           # Windows
   ```

=========================================== END ===============================

# Git Permission error resolution when pushing code, step by step:

## STEP 1

### Understand the error message

'''
ERROR: Permission to <owner>/<repo>.git denied to <authenticated-user>.
'''

    - GitHub is telling you that your SSH key belongs to <authenticated-user>, but that account doesn’t have write access to the repository.

## STEP 2

### Check which GitHub account SSH is currently using

Run : ''' ssh -T git@github.com '''

Expected output: ''' Hi <authenticated-user>! You've successfully authenticated... '''

    -   This confirms the account your current SSH key is tied to. If it’s the wrong one, continue.

## STEP 3

### List all SSH keys on your machine

Run : ''' ls -al ~/.ssh '''

    - You’ll see files like id_rsa, id_ed25519, or custom names. Each public key (*.pub) corresponds to a private key that can be added to a GitHub account.

    - Explanation: You may have multiple keys for different accounts. The system may be offering a key that belongs to the wrong account.

## STEP 4

### Find out which exact key is being offered to GitHub

Run : - ''' ssh -vT git@github.com '''

    - Look for lines containing Offering public key. The path shown is the key file currently being used.

    - Explanation: This verbose output tells you which private key SSH is trying. If it’s not the one you want, you need to force SSH to use the correct key.

## STEP 5

### Obtain or create the correct SSH key

- Option A – You already have a key for the correct account
  - If a key exists (e.g., id_ed25519_surya079) but isn’t being used, skip to Step 6.
- Option B – Generate a new key for the correct GitHub account
  - ''' ssh-keygen -t ed25519 -C "your_email@example.com" -f ~/.ssh/id_ed25519_correct_account '''

  - -C adds a comment (usually your email) to identify the key.

  - -f sets a custom filename so you don’t overwrite existing keys.
    Press Enter to accept the default location (the custom name) and choose a passphrase (optional).

- Explanation: This creates a new private/public key pair specifically for the account you want to use.

## STEP 6

### Add the public key to the correct GitHub account

- Copy the public key:
  - cat ~/.ssh/id_ed25519_correct_account.pub
- Go to GitHub → Settings → SSH and GPG keys → New SSH key.
- Paste the key and save.

- Explanation: GitHub needs to know that this key is allowed to authenticate as the desired account.

## STEP 7

### Force SSH to use the correct key for GitHub

- Create or edit ~/.ssh/config:
  - nano ~/.ssh/config

- Add (or modify) these lines:
  '''
  Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_correct_account
  IdentitiesOnly yes
  '''

- IdentityFile points to the private key you want to use.

- IdentitiesOnly yes tells SSH to only try that key and ignore others.

Save and exit (Ctrl+O, Enter, Ctrl+X in nano).

- Explanation: By default, SSH may try several keys and pick the first one that GitHub accepts. This forces it to use exactly the key you specify.

## STEP 8

### Test the SSH authentication again

Run : ''' ssh -T git@github.com '''

Now it should say: \* ''' Hi <correct-account>! You've successfully authenticated... '''

- If it still shows the wrong account, double‑check that:
  - The key file path in ~/.ssh/config is correct.

  - The public key was added to the correct GitHub account.

  - IdentitiesOnly yes is present.

## STEP 9

### Push again

    ''' git push -u origin main '''

==================================================== END ===========================================================
