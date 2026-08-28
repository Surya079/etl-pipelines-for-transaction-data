"""

This module stores lists and dictionaries of realistic values that can be
randomly sampled from when generating synthetic transaction data.

All constants are defined at module level for easy import.

"""

# ----------------------------------------------------------------------
# MCC (Merchant Category Code) - Code: Description
# ----------------------------------------------------------------------

MCC_CODES = {
    "5411": "Grocery Stores",
    "5812": "Eating Places",
    "5814": "Fast Food Restaurants",
    "5912": "Drug Stores",
    "5541": "Service Stations",
    "5310": "Discount Stores",
    "5311": "Department Stores",
    "5331": "Variety Stores",
    "5942": "Book Stores",
    "5941": "Sporting Goods Stores",
    "5947": "Gift, Card, Novelty, and Souvenir Shops",
    "5813": "Drinking Places (Alcoholic Beverages)",
    "5811": "Caterers",
    "5815": "Digital Goods: Media, Books, Movies, Music",
    "5816": "Digital Goods: Games",
    "5817": "Digital Goods: Applications (Excludes Games)",
    "5818": "Digital Goods: Large Digital Goods Merchant",
    "5810": "Package Stores, Beer, Wine, and Liquor",
    "5819": "Miscellaneous Food Stores",
    "5913": "Health and Beauty Spas",
    "5914": "Cosmetic Stores",
    "5921": "Package Stores, Beer, Wine, and Liquor",
    "5931": "Used Merchandise and Secondhand Stores",
    "5932": "Antique Shops",
    "5933": "Pawn Shops",
    "5935": "Wrecking and Salvage Yards",
    "5937": "Antique Reproduction Stores",
    "5940": "Bicycle Shops",
    "5943": "Stationery, Office Supply, Printing and Writing Paper Stores",
    "5944": "Jewelry, Watch, Clock, and Silverware Stores",
    "5945": "Hobby, Toy, and Game Shops",
    "5946": "Camera and Photographic Supply Stores",
    "5948": "Luggage and Leather Goods Stores",
    "5949": "Sewing, Needlework, Fabric, and Piece Goods Stores",
    "5950": "Glassware, Crystal Stores",
    "5960": "Direct Marketing - Insurance Services",
    "5961": "Direct Marketing - Catalog Merchants",
    "5962": "Direct Marketing - Travel Related Arrangement Services",
    "5963": "Door-to-Door Sales",
    "5964": "Direct Marketing - Catalog Merchants",
    "5965": "Direct Marketing - Combination Catalog and Retail Merchant",
    "5966": "Direct Marketing - Outbound Telemarketing Merchant",
    "5967": "Direct Marketing - Inbound Telemarketing Merchant",
    "5968": "Direct Marketing - Continuity/Subscription Merchant",
    "5969": "Direct Marketing - Other Direct Marketers",
    "5999": "Miscellaneous and Specialty Retail Stores",
    "6010": "Financial Institutions - Manual Cash Disbursements",
    "6011": "Financial Institutions - Automated Cash Disbursements",
    "6012": "Financial Institutions - Merchandise and Services",
    "6051": "Non-Financial Institutions - Foreign Currency, Money Orders, and Travelers Cheques",
    "6211": "Security Brokers/Dealers",
    "6300": "Insurance Sales, Underwriting, and Premiums",
    "7011": "Lodging - Hotels, Motels, Resorts",
    "7032": "Sporting and Recreational Camps",
    "7033": "Trailer Parks and Campgrounds",
    "7210": "Laundry, Cleaning, and Garment Services",
    "7211": "Laundry Services - Family and Commercial",
    "7216": "Dry Cleaners",
    "7217": "Carpet and Upholstery Cleaning",
    "7221": "Photographic Studios",
    "7230": "Barber and Beauty Shops",
    "7296": "Clothing Rental - Costumes, Uniforms, and Formal Wear",
    "7297": "Massage Parlors",
    "7298": "Health and Beauty Spas",
    "7299": "Miscellaneous Personal Services",
    "7311": "Advertising Services",
    "7333": "Commercial Photography, Art and Graphics",
    "7338": "Quick Copy, Reproduction and Blueprinting Services",
    "7339": "Stenographic and Secretarial Support Services",
    "7342": "Exterminating and Disinfecting Services",
    "7349": "Cleaning and Maintenance, Janitorial Services",
    "7392": "Management, Consulting, and Public Relations Services",
    "7393": "Detective Agencies, Protective Agencies, and Security Services",
    "7394": "Equipment Rental and Leasing Services, Furniture and Tool Rental",
    "7395": "Photofinishing Laboratories, Photo Developing",
    "7399": "Business Services - Not Elsewhere Classified",
    "7512": "Automobile Rental Agency",
    "7513": "Truck and Utility Trailer Rental",
    "7519": "Motor Home and Recreational Vehicle Rental",
    "7523": "Parking Lots, Parking Garages",
    "7531": "Automotive Body Repair Shops",
    "7534": "Tire Retreading and Repair Shops",
    "7535": "Automotive Paint Shops",
    "7538": "Automotive Service Shops (Non-Repair)",
    "7542": "Car Washes",
    "7549": "Towing Services",
    "7622": "Electronics Repair Shops",
    "7623": "Air Conditioning and Refrigeration Repair Shops",
    "7629": "Electrical and Small Appliance Repair Shops",
    "7631": "Watch, Clock, and Jewelry Repair Shops",
    "7641": "Furniture Reupholstery, Repair, and Refinishing",
    "7692": "Welding Services",
    "7699": "Repair Shops and Related Services - Miscellaneous",
    "7832": "Motion Picture Theaters",
    "7841": "Video Tape Rental Stores",
    "7911": "Dance Halls, Studios, and Schools",
    "7922": "Theatrical Producers (Except Motion Pictures) and Ticket Agencies",
    "7929": "Bands, Orchestras, and Miscellaneous Entertainers",
    "7932": "Billiard and Pool Establishments",
    "7933": "Bowling Alleys",
    "7941": "Commercial Sports, Professional Sports Clubs, Athletic Fields",
    "7991": "Tourist Attractions and Exhibits",
    "7993": "Video Amusement Game Supplies",
    "7994": "Video Game Arcades/Establishments",
    "7995": "Betting, including Lottery Tickets, Casino Gaming Chips, Off-Track Betting, and Wagers",
    "7996": "Amusement Parks, Circuses, Carnivals, and Fortune Tellers",
    "7997": "Membership Clubs (Sports, Recreation, Athletic), Country Clubs, Private Golf Courses",
    "7998": "Aquariums, Dolphinariums, Zoos, and Seaquariums",
    "7999": "Recreation Services - Not Elsewhere Classified",
    "8011": "Doctors and Physicians (Not Elsewhere Classified)",
    "8021": "Dentists, Orthodontists, and Dental Surgeons",
    "8031": "Osteopathic Physicians",
    "8041": "Chiropractors",
    "8042": "Optometrists and Ophthalmologists",
    "8043": "Opticians, Optical Goods, and Eyeglasses",
    "8049": "Podiatrists, Chiropodists",
    "8050": "Nursing and Personal Care Facilities",
    "8062": "Hospitals",
    "8071": "Medical and Dental Laboratories",
    "8099": "Medical Services and Health Practitioners - Not Elsewhere Classified",
    "8111": "Legal Services, Attorneys",
    "8211": "Elementary and Secondary Schools",
    "8220": "Colleges, Universities, Junior Colleges, and Professional Schools",
    "8241": "Correspondence Schools",
    "8244": "Business and Secretarial Schools",
    "8249": "Vocational and Trade Schools",
    "8299": "Schools and Educational Services - Not Elsewhere Classified",
    "8351": "Child Day Care Services",
    "8398": "Charitable and Social Service Organizations",
    "8641": "Civic, Social, Fraternal Associations",
    "8651": "Political Organizations",
    "8661": "Religious Organizations",
    "8675": "Automobile Associations",
    "8699": "Membership Organizations - Not Elsewhere Classified",
    "8734": "Testing Laboratories (Non-Medical)",
    "8911": "Architectural, Engineering, and Surveying Services",
    "8931": "Accounting, Auditing, and Bookkeeping Services",
    "8999": "Professional Services - Not Elsewhere Classified",
    "9211": "Court Costs, Including Alimony and Child Support",
    "9222": "Fines",
    "9223": "Bail and Bond Payments",
    "9311": "Tax Payments",
    "9399": "Government Services - Not Elsewhere Classified",
    "9402": "Postal Services - Government Only",
    "9405": "U.S. Federal Government - Not Elsewhere Classified",
    "9700": "Automated Referral Service",
    "9701": "Visa Credential Server",
    "9702": "GCAS (Global Cardholder Authentication Service)",
    "9950": "Intra-Company Purchases",
}


# ----------------------------------------------------------------------
# Card networks
# ----------------------------------------------------------------------
CARD_NETWORKS = [
    "Visa",
    "Mastercard",
    "American Express",
    "Discover",
]


# ----------------------------------------------------------------------
# Card types
# ----------------------------------------------------------------------
CARD_TYPES = [
    "Credit",
    "Debit",
    "Prepaid",
]

# ----------------------------------------------------------------------
# Transaction types
# ----------------------------------------------------------------------
TRANSACTION_TYPES = [
    "Purchase",
    "Refund",
    "ATM Withdrawal",
    "Balance Inquiry",
    "Transfer",
]

# ----------------------------------------------------------------------
# POS entry modes
# ----------------------------------------------------------------------
POS_ENTRY_MODES = [
    "Chip",
    "Magnetic Stripe",
    "Contactless",
    "Manual Keyed",
    "E-commerce",
]

# ----------------------------------------------------------------------
# Response codes - Code: Description
# ----------------------------------------------------------------------
RESPONSE_CODES = {
    "00": "Approved",
    "01": "Refer to card issuer",
    "02": "Refer to card issuer, special condition",
    "03": "Invalid merchant",
    "04": "Pick-up card",
    "05": "Do not honor",
    "06": "Error",
    "07": "Pick-up card, special condition",
    "10": "Partial approval",
    "11": "Approved (VIP)",
    "12": "Invalid transaction",
    "13": "Invalid amount",
    "14": "Invalid card number",
    "15": "No such issuer",
    "16": "Approved, update track 3",
    "19": "Re-enter transaction",
    "21": "No action taken",
    "25": "Unable to locate record on file",
    "28": "File temporarily not available for update",
    "39": "No credit account",
    "41": "Lost card, pick-up",
    "43": "Stolen card, pick-up",
    "51": "Insufficient funds",
    "54": "Expired card",
    "55": "Incorrect PIN",
    "57": "Transaction not permitted to cardholder",
    "58": "Transaction not permitted to terminal",
    "61": "Exceeds withdrawal amount limit",
    "62": "Restricted card",
    "63": "Security violation",
    "65": "Exceeds withdrawal frequency limit",
    "68": "Response received too late",
    "75": "Allowable number of PIN entry tries exceeded",
    "76": "Unable to locate previous message",
    "77": "Previous message located for a repeat or reversal, but repeat data are inconsistent",
    "78": "Invalid/nonexistent account specified (general)",
    "79": "Already reversed",
    "80": "Visa transactions: credit issuer unavailable",
    "81": "PIN cryptographic error found by the security module",
    "82": "Negative CAM, dCVV, iCVV, or CVV results",
    "83": "Unable to verify PIN",
    "84": "Invalid authorization life cycle",
    "85": "No reason to decline a request for account number verification",
    "86": "Cannot verify PIN",
    "87": "Purchase amount only, no cash back allowed",
    "88": "Cryptographic failure",
    "89": "Unacceptable PIN - transaction declined",
    "91": "Issuer or switch is inoperative",
    "92": "Financial institution or intermediate network facility cannot be found for routing",
    "93": "Transaction cannot be completed; violation of law",
    "94": "Duplicate transaction",
    "96": "System malfunction",
    "97": "Terminal error",
    "98": "Exceeds cash limit",
    "99": "Reserved for national use",
}

# ----------------------------------------------------------------------
# Currencies
# ----------------------------------------------------------------------
CURRENCIES = [
    "USD",
    "EUR",
    "GBP",
    "INR",
    "CAD",
    "AUD",
    "JPY",
    "CNY",
    "CHF",
    "SGD",
    "AED",
    "BRL",
    "MXN",
    "ZAR",
    "SEK",
    "NOK",
]


# ----------------------------------------------------------------------
# Countries and Cities (sample of major ones)
# ----------------------------------------------------------------------
# We provide a dictionary mapping country to a list of major cities.
# This can be extended or replaced with Faker-generated data if needed.
COUNTRIES_CITIES = {
    "United States": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Liverpool", "Edinburgh"],
    "India": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
    "Brazil": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"],
    "United Arab Emirates": [
        "Dubai",
        "Abu Dhabi",
        "Sharjah",
        "Ajman",
        "Ras Al Khaimah",
    ],
    "Singapore": ["Singapore"],
    "Netherlands": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"],
    "Switzerland": ["Zurich", "Geneva", "Basel", "Bern", "Lausanne"],
    "Sweden": ["Stockholm", "Gothenburg", "Malmö", "Uppsala", "Västerås"],
    "Norway": ["Oslo", "Bergen", "Trondheim", "Stavanger", "Drammen"],
}



# Optionally, provide flat lists for convenience.
COUNTRIES = list(COUNTRIES_CITIES.keys())
CITIES = [city for cities in COUNTRIES_CITIES.values() for city in cities]


# Additional static lists for variety

MERCHANT_NAMES = [
    "Amazon", "Walmart", "Starbucks", "Uber", "Netflix", "Shell", "McDonald's",
    "Apple Store", "Target", "Best Buy", "CVS Pharmacy", "Home Depot", "Kroger",
    "Whole Foods", "Delta Airlines", "Expedia", "Airbnb", "Subway", "Pizza Hut",
    "Lowe's", "Costco", "7-Eleven", "Walgreens", "Trader Joe's", "Safeway",
    "Publix", "Aldi", "Lidl", "Tesco", "Carrefour", "Aldi", "Zara", "H&M",
    "IKEA", "Nike", "Adidas", "Samsung Store", "Sony Store", "Microsoft Store",
]

ACQUIRER_BANKS = [
    "JPMorgan Chase", "Bank of America", "Wells Fargo", "Citibank", "HSBC",
    "Barclays", "Deutsche Bank", "ICICI Bank", "HDFC Bank", "State Bank of India",
    "BBVA", "Santander", "BNP Paribas", "Credit Suisse", "UBS",
    "Royal Bank of Canada", "Toronto-Dominion Bank", "Mitsubishi UFJ Financial Group",
    "Sumitomo Mitsui Banking Corporation", "Commonwealth Bank", "Westpac",
    "ANZ", "National Australia Bank", "DBS Bank", "OCBC Bank", "United Overseas Bank",
]

ISSUER_BANKS = [
    "Chase", "Capital One", "American Express", "Citibank", "Bank of America",
    "Barclays", "HSBC", "Discover", "USAA", "Navy Federal Credit Union",
    "Wells Fargo", "Synchrony Bank", "TD Bank", "PNC Bank", "Ally Bank",
    "ICICI Bank", "HDFC Bank", "Axis Bank", "State Bank of India",
    "Standard Chartered", "DBS Bank", "Citibank Singapore", "HSBC Singapore",
]

DEVICE_INFO = [
    "iPhone 15 Pro", "iPhone 14", "iPhone 13", "iPhone SE",
    "Samsung Galaxy S24", "Samsung Galaxy S23", "Samsung Galaxy Z Fold5",
    "Google Pixel 8", "Google Pixel 7", "OnePlus 12", "OnePlus 11",
    "Xiaomi 14", "Xiaomi 13", "Oppo Find X7", "Vivo X100",
    "iPad Pro", "iPad Air", "iPad Mini",
    "MacBook Pro", "MacBook Air", "Dell XPS 13", "HP Spectre x360",
    "Lenovo ThinkPad X1 Carbon", "Microsoft Surface Laptop 5",
    "Samsung Galaxy Tab S9", "Amazon Fire HD 10",
    "Windows Desktop", "iMac", "Linux Workstation",
    "Smart TV", "PlayStation 5", "Xbox Series X", "Nintendo Switch",
]

