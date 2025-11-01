coin_ohlc_schema = {
    "type": "array",
    "items": {
        "type": "array",
        "prefixItems": [
            {"type": "integer"},  # timestamp in ms
            {"type": "number"},   # open price
            {"type": "number"},   # high price
            {"type": "number"},   # low price
            {"type": "number"}    # close price
        ],
        "items": False
    }
}