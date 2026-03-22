# TaxoGPT API Specification (v3.2.0)

This document defines the standardized API paths, payloads, and response structures for the TaxoGPT production environment.

## 1. Global Standards
- **Base URL**: `http://localhost:8000/api/v1`
- **Auth**: Most routes require `Authorization: Bearer <JWT_TOKEN>` header.
- **Naming**: Use lowercase with hyphens for paths.
- **Response Format**: JSON. Errors use `{ "detail": "string" }`.

---

## 2. Authentication Router (`/auth`)

| Method | Path | Description | Payload Example |
| :--- | :--- | :--- | :--- |
| `POST` | `/auth/register` | Register a new user | `{"email": "...", "password": "...", "full_name": "..."}` |
| `POST` | `/auth/login` | Login to get JWT | `{"email": "...", "password": "..."}` |
| `POST` | `/auth/google` | Social Login | `{"credential": "TOKEN"}` |
| `POST` | `/auth/facebook` | Social Login | `{"access_token": "TOKEN"}` |
| `POST` | `/auth/verify-email` | Verify email | `{"token": "UUID"}` |
| `GET` | `/auth/me` | Get Profile + Credits | N/A (Response includes `usage` object) |
| `GET` | `/auth/credits` | Get Just Credit Balances | N/A (Response: `{"simple_query_balance": 10, ...}`) |
| `PATCH` | `/auth/me` | Update Preferences | `{"dynamic_summary": "...", "preferences": {...}}` |
| `POST` | `/auth/logout` | Invalidate Session | N/A |

---

## 3. Chat & Sessions Router (`/`)

| Method | Path | Description | Payload Example |
| :--- | :--- | :--- | :--- |
| `POST` | `/chat/ask/stream/simple` | Stream AI response | `{"question": "what is gst?", "session_id": "optional-uuid"}` |
| `POST` | `/chat/ask/stream/draft` | Stream Draft mode AI | `{"question": "redraft this...", "session_id": "uuid"}` |
| `GET` | `/sessions` | List user sessions | N/A (Returns array of session objects) |
| `GET` | `/sessions/{id}/history`| Get session history | N/A (Returns flat array of messages) |
| `DELETE` | `/sessions/{id}` | Delete session | N/A |
| `POST` | `/sessions/feedback` | Submit message rating | `{"message_id": 123, "rating": 5, "comment": "..."}` |
| `POST` | `/chat/share/session/{id}`| Create share link | N/A (Returns `shared_id`) |
| `GET` | `/chat/share/{shared_id}` | View public shared chat | N/A (Returns message history) |

---

## 4. Payments Router (`/payments`)

| Method | Path | Description | Payload Example |
| :--- | :--- | :--- | :--- |
| `GET` | `/payments/packages` | List active packages | N/A |
| `POST` | `/payments/create-order` | Start payment | `{"package_name": "basic", "coupon_code": "..."}` |
| `POST` | `/payments/verify` | Verify Razorpay SIG | `{"razorpay_order_id": "...", "razorpay_payment_id": "...", ...}` |
| `GET` | `/payments/history` | Transaction logs | N/A |
| `POST` | `/payments/validate-coupon`| Check coupon code | `{"coupon_code": "SAVE10", "package_name": "..."}` |

---

## 5. Administrative Router (`/admin`)

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/admin/analytics` | Total users and transaction counts. |
| `GET` | `/admin/analytics/users` | List top users by token usage and queries. |
| `GET` | `/admin/users` | Master list of all registered users. |
| `PATCH` | `/admin/users/{id}` | Modify user role or session limits. |
| `GET` | `/admin/payments/packages` | Master list of all packages (incl. inactive). |
| `PATCH`| `/admin/payments/packages/{id}` | Edit an existing package description/price. |
| `GET` | `/admin/payments/coupons` | Master list of all coupons. |
| `DELETE`| `/admin/payments/coupons/{id}` | Permanently remove a coupon. |

---

## 6. Response Structures (Common)

### Success Profile (`GET /auth/me`)
```json
{
  "user": {
    "id": 1,
    "email": "user@example.com",
    "credits": {
      "simple_query_balance": 100,
      "draft_reply_balance": 5
    }
  }
}
```

### Stream Message Object (`POST /chat/ask/stream/*`)
Each line of the stream is a JSON object.
```json
{"type": "content", "delta": "He"}
{"type": "content", "delta": "llo"}
{"type": "completion", "session_id": "...", "message_id": 123}
```
