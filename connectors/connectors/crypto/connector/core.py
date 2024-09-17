
def get_contract_size(spot_symbol):
    if spot_symbol == 'BTC' or spot_symbol == 'BTCUSDT':
        contract_size = 0.001
    elif spot_symbol == 'ETH' or spot_symbol == 'ETHUSDT':
        contract_size = 0.01
    elif spot_symbol == 'BNB' or spot_symbol == 'BNBUSDT':
        contract_size = 0.1
    else:
        contract_size = 1
    return contract_size

def is_fiat(symbol):
    if symbol in FIAT_CURRENCIES:
        return True
    return False

def is_stable_coin(symbol):
    if symbol in STABLE_COINS:
        return True
    return False

FIAT_CURRENCIES = {
    "AED": "United Arab Emirates dirham",
    "AFN": "Afghan Afghani",
    "AMD": "Armenian Dram",
    "ARS": "Argentine Peso",
    "AUD": "Australian Dollar",
    "BHD": "Bahraini Dinar",
    "BND": "Brunei Dollar",
    "BRL": "Brazilian Real",
    "CAD": "Canadian Dollar",
    "CHF": "schweizer Franken",
    "COP": "Colombian Peso",
    "CZK": "Czech Koruny",
    "DKK": "Danish Kroner",
    "DZD": "Algerian Dinar",
    "ETB": "Ethiopian Birr",
    "EUR": "Euro",
    "GBP": "Pound Sterling",
    "GHS": "Ghanaian cedi",
    "HKD": "Hong Kong Dollar",
    "HUF": "Hungarian Forint",
    "IDR": "Indonesian Rupiah",
    "JOD": "Jordanian Dinar",
    "JPY": "Japanese Yen",
    "KES": "Kenya Shilling",
    "KGS": "Kyrgyzstani Som",
    "KHR": "Cambodian riel",
    "KWD": "Kuwaiti Dinar",
    "KZT": "Kazakhstani Tenge",
    "LAK": "Laotian Kip",
    "MNT": "Mongolian Tughrik",
    "MXN": "Mexican Peso",
    "NGN": "Nigerian Naira",
    "NOK": "Norwegian Kroner",
    "NZD": "New Zealand Dollar",
    "OMR": "Oman Rial",
    "PEN": "Peruvian sol",
    "PHP": "Philippine Peso",
    "PLN": "Polish Zlotych",
    "RUB": "Russian Ruble",
    "SDG": "Sudanese Pound",
    "SEK": "Swedish Kroner",
    "TMT": "Turkmenistani Manat",
    "TND": "Tunisian Dinar",
    "TRY": "Turkish Lira",
    "TZS": "Tanzanian Shilling",
    "UAH": "Ukraine Hryvnia",
    "UGX": "Uganda Shilling",
    "USD": "United States Dollar",
    "UZS": "Uzbekistani Sum",
    "VND": "Vietnamese Dong",
    "XAF": "Central African CFA franc",
    "XOF": "West African CFA",
    "ZAR": "South African Rand",
}

STABLE_COINS = {
    "BUSD": "Binance USD",
    "USDC": "USD Coin",
    "USDT": "USD Tether",
    "LDUSDT": "USD Tether EARN",
}