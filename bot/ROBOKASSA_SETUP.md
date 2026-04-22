# Robokassa setup

## Environment variables

Copy `.env.example` to `.env` on the server and fill:

- `ROBOKASSA_LOGIN` - MerchantLogin from Robokassa.
- `ROBOKASSA_PASSWORD1` - password 1 for creating payment links.
- `ROBOKASSA_PASSWORD2` - password 2 for ResultURL signature verification.
- `ROBOKASSA_TEST_MODE=1` for test mode, `0` for production.
- `PUBLIC_BASE_URL` - public HTTPS address of the bot backend.
- `PAYMENT_SERVER_PORT` - local port for Robokassa callbacks, default `8080`.

Fiscal defaults:

- `ROBOKASSA_SNO=usn_income`
- `ROBOKASSA_TAX=none`
- `ROBOKASSA_PAYMENT_METHOD=full_payment`
- `ROBOKASSA_PAYMENT_OBJECT=service`

Confirm fiscal values with the accountant or Robokassa manager before production.

## Robokassa technical settings

Set URLs in the Robokassa shop:

- Result URL: `https://your-domain.ru/robokassa/result`
- Method: `POST`
- Success URL: the VK Mini App or a static success page
- Fail URL: the VK Mini App or a static failure page

The backend responds with `OK{InvId}` after a valid payment notification.

## Flow

1. The bot creates an order in SQLite.
2. The bot generates a Robokassa payment link with receipt nomenclature.
3. Robokassa calls `ResultURL` after payment.
4. The payment server verifies the signature and marks the order as paid.
5. The bot background task sends the product and marks the order as delivered.

## Moderation mode

Before production, keep:

```env
MODERATION_MODE=1
MODERATION_SECRET=robokassa-test-2026
MODERATION_ACCESS_MINUTES=30
```

In this mode regular users cannot receive payment links. If they click a VK product, the bot says that payment is being connected and offers support.

For Robokassa/VK review, give the reviewer this scenario:

1. Write `robokassa-test-2026` to the bot.
2. Open a product in the VK community.
3. Click "Write to seller".
4. The bot will create a test Robokassa payment link for 30 minutes.

After activation and final testing, set:

```env
MODERATION_MODE=0
```
