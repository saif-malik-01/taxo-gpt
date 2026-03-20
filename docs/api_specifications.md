# TaxoGPT API Specification (v3.1.0)

This document defines the standardized API paths and best practices for the TaxoGPT production environment.

## 1. Global Standards
- **Base URL**: `http://localhost:8000/api/v1`
- **Version Prefix**: All routes must be prefixed with `/api/v1`.
- **Naming Convention**: Use lowercase with hyphens for paths (e.g. `/verify-email`).
- **Response Format**: JSON only. Standard errors use `{ "detail": "error message" }`.

---

## 2. Authentication Router (`/auth`)

| Path | Method | Auth | Description |
| :--- | :--- | :--- | :--- |
| `/auth/register` | POST | Public | Register a new user with email. |
| `/auth/login` | POST | Public | Login with email/password to get JWT. |
| `/auth/google` | POST | Public | Login/Register via Google. |
| `/auth/facebook` | POST | Public | Login/Register via Facebook. |
| `/auth/verify-email`| POST | Public | Verify account using token. |
| `/auth/resend-verification` | POST | Public | Resend verification email. |
| `/auth/logout` | POST | Bearer | Invalidate session in Redis. |
| `/auth/me` | GET | Bearer | Get current user profile details. |
| `/auth/me` | PATCH | Bearer | Update profile (summary, prefs). |

---

## 3. Chat & RAG Router (`/chat`)

| Path | Method | Auth | Description |
| :--- | :--- | :--- | :--- |
| `/chat/health` | GET | Public | Health check specifically for chat service. |
| `/chat/ask` | POST | Bearer | Primary RAG query endpoint. |
| `/chat/stream` | POST | Bearer | Streaming version of RAG query. |

---

## 4. Payments Router (`/payments`)

| Path | Method | Auth | Description |
| :--- | :--- | :--- | :--- |
| `/payments/packages` | GET | Public | List active credit packages. |
| `/payments/create-order`| POST | Bearer | Create Razorpay order for a package. |
| `/payments/verify` | POST | Public | Verify Razorpay payment signature. |
| `/payments/history` | GET | Bearer | Get user credit/transaction history. |

---

## 5. Administrative Router (`/admin`)
*Requires `admin_guard` (role="admin" or internal API key)*

| Path | Method | Description |
| :--- | :--- | :--- |
| `/admin/users` | GET | List all users in system. |
| `/admin/users/{id}` | GET | Get details of a specific user. |
| `/admin/users/{id}` | PATCH | Update user role or session limit. |
| `/admin/users/{id}` | DELETE | Remove user from system. |
| `/admin/payments/packages`| POST | Create a new credit package. |
| `/admin/payments/coupons` | POST | Create a new discount coupon. |
| `/admin/analytics` | GET | View system-wide metrics. |

---

## 6. Best Practices Implemented
- **Layered Imports**: All files use absolute imports from `apps.api.src`.
- **Graceful Failures**: High-complexity services (LLM, Email) use try-except blocks to prevent global crashes.
- **Session Enforcement**: `add_session` in Redis ensures users don't exceed `max_sessions`.
- **Centralized Config**: `core/config.py` uses Pydantic Settings for type-safe environment loading.
