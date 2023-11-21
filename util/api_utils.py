from typing import Dict
import hashlib
import hmac


def add_signature(params: Dict, api_secret: str):
    params_string = "&".join(f"{key}={value}" for key, value in params.items())

    signature = hmac.new(
        api_secret.encode(),
        params_string.encode(),
        hashlib.sha256,
    ).hexdigest()

    params["signature"] = signature
