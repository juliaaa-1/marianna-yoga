import os


def _load_dotenv():
    path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


TOKEN = _get_required_env("VK_TOKEN")
GROUP_ID = int(_get_required_env("VK_GROUP_ID"))

ADMIN_IDS = [
    int(admin_id.strip())
    for admin_id in _get_required_env("ADMIN_IDS").split(",")
    if admin_id.strip()
]

SHOP_ID = os.getenv("ROBOKASSA_SHOP_ID", "")
SHOP_KEY = os.getenv("ROBOKASSA_SHOP_KEY", "")

ROBOKASSA_LOGIN = os.getenv("ROBOKASSA_LOGIN", "")
ROBOKASSA_PASSWORD1 = os.getenv("ROBOKASSA_PASSWORD1", "")
ROBOKASSA_PASSWORD2 = os.getenv("ROBOKASSA_PASSWORD2", "")
ROBOKASSA_TEST_MODE = os.getenv("ROBOKASSA_TEST_MODE", "1") == "1"
ROBOKASSA_PAYMENT_URL = os.getenv(
    "ROBOKASSA_PAYMENT_URL",
    "https://auth.robokassa.ru/Merchant/Index.aspx",
)
ROBOKASSA_SNO = os.getenv("ROBOKASSA_SNO", "usn_income")
ROBOKASSA_TAX = os.getenv("ROBOKASSA_TAX", "none")
ROBOKASSA_PAYMENT_METHOD = os.getenv("ROBOKASSA_PAYMENT_METHOD", "full_payment")
ROBOKASSA_PAYMENT_OBJECT = os.getenv("ROBOKASSA_PAYMENT_OBJECT", "service")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")
PAYMENT_SERVER_HOST = os.getenv("PAYMENT_SERVER_HOST", "0.0.0.0")
PAYMENT_SERVER_PORT = int(os.getenv("PAYMENT_SERVER_PORT", os.getenv("PORT", "8080")))
MODERATION_MODE = os.getenv("MODERATION_MODE", "1") == "1"
MODERATION_SECRET = os.getenv("MODERATION_SECRET", "robokassa-test-2026").lower()
MODERATION_ACCESS_MINUTES = int(os.getenv("MODERATION_ACCESS_MINUTES", "30"))

APP_ID = int(os.getenv("VK_APP_ID", "54471577"))
APP_URL = os.getenv("VK_APP_URL", "https://vk.com/app54471577")
