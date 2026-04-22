from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlparse

from config import PAYMENT_SERVER_HOST, PAYMENT_SERVER_PORT
from database import mark_order_paid
from robokassa import verify_result_signature


class PaymentHandler(BaseHTTPRequestHandler):
    def _read_params(self):
        params = {}
        query = parse_qs(urlparse(self.path).query)
        for key, value in query.items():
            if value:
                params[key] = value[0]

        length = int(self.headers.get("Content-Length") or 0)
        if length:
            body = self.rfile.read(length).decode("utf-8")
            form = parse_qs(body)
            for key, value in form.items():
                if value:
                    params[key] = value[0]
        return params

    def _send_text(self, status, text):
        encoded = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        self._handle_result()

    def do_POST(self):
        self._handle_result()

    def _handle_result(self):
        if urlparse(self.path).path != "/robokassa/result":
            self._send_text(404, "Not found")
            return

        params = self._read_params()
        out_sum = params.get("OutSum")
        inv_id = params.get("InvId")
        signature = params.get("SignatureValue")
        robokassa_invoice_id = params.get("IncCurrLabel") or params.get("EMail")

        if not out_sum or not inv_id or not signature:
            self._send_text(400, "Missing required payment parameters")
            return

        if not verify_result_signature(out_sum, inv_id, signature):
            self._send_text(403, "Invalid signature")
            return

        try:
            order_id = int(inv_id)
        except ValueError:
            self._send_text(400, "Invalid InvId")
            return

        mark_order_paid(order_id, robokassa_invoice_id)
        self._send_text(200, f"OK{order_id}")

    def log_message(self, format, *args):
        print(f"PAYMENT SERVER: {format % args}")


def start_payment_server():
    server = ThreadingHTTPServer((PAYMENT_SERVER_HOST, PAYMENT_SERVER_PORT), PaymentHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(
        f"Payment server started on http://{PAYMENT_SERVER_HOST}:{PAYMENT_SERVER_PORT}/robokassa/result"
    )
    return server

