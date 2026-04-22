import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlencode

from config import (
    ROBOKASSA_LOGIN,
    ROBOKASSA_PASSWORD1,
    ROBOKASSA_PASSWORD2,
    ROBOKASSA_PAYMENT_METHOD,
    ROBOKASSA_PAYMENT_OBJECT,
    ROBOKASSA_PAYMENT_URL,
    ROBOKASSA_SNO,
    ROBOKASSA_TAX,
    ROBOKASSA_TEST_MODE,
)


def format_amount(amount):
    value = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(value, "f")


def is_configured():
    return bool(ROBOKASSA_LOGIN and ROBOKASSA_PASSWORD1 and ROBOKASSA_PASSWORD2)


def build_receipt(product_name, amount):
    out_sum = format_amount(amount)
    return {
        "sno": ROBOKASSA_SNO,
        "items": [
            {
                "name": product_name[:128],
                "quantity": 1,
                "sum": out_sum,
                "tax": ROBOKASSA_TAX,
                "payment_method": ROBOKASSA_PAYMENT_METHOD,
                "payment_object": ROBOKASSA_PAYMENT_OBJECT,
            }
        ],
    }


def _md5(value):
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def build_payment_url(order_id, product_name, amount):
    if not is_configured():
        raise RuntimeError("Robokassa credentials are not configured")

    out_sum = format_amount(amount)
    receipt_json = json.dumps(
        build_receipt(product_name, out_sum),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    signature = _md5(
        f"{ROBOKASSA_LOGIN}:{out_sum}:{order_id}:{receipt_json}:{ROBOKASSA_PASSWORD1}"
    )
    params = {
        "MerchantLogin": ROBOKASSA_LOGIN,
        "OutSum": out_sum,
        "InvId": order_id,
        "Description": f"Оплата: {product_name}",
        "Receipt": receipt_json,
        "SignatureValue": signature,
    }
    if ROBOKASSA_TEST_MODE:
        params["IsTest"] = 1
    return f"{ROBOKASSA_PAYMENT_URL}?{urlencode(params)}"


def verify_result_signature(out_sum, inv_id, signature):
    signature = str(signature).lower()
    exact = _md5(f"{out_sum}:{inv_id}:{ROBOKASSA_PASSWORD2}").lower()
    normalized = _md5(f"{format_amount(out_sum)}:{inv_id}:{ROBOKASSA_PASSWORD2}").lower()
    return signature in (exact, normalized)
