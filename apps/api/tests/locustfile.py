"""
TaxoGPT API - Production Load Test Suite
=========================================
Covers:
  - Auth  (login, /me, credits, heartbeat)
  - Chat  (simple-stream, session listing/history/delete, feedback, sharing)
  - Payments (packages, coupon validation, history, transactions)
  - Health  (shallow + deep)

Load Shape
----------
Uses StepLoadShape to ramp up gradually:
  Step 1 →  5 users  (0–60 s)     — warm-up / smoke
  Step 2 → 10 users  (60–120 s)   — light load
  Step 3 → 20 users  (120–180 s)  — moderate load
  Step 4 → 30 users  (180–240 s)  — medium-high load
  Step 5 → 50 users  (240–300 s)  — peak load
  After 300 s the test stops automatically.

Usage
-----
# Interactive UI (shape is defined in code – user count is controlled by the shape)
locust -f apps/api/tests/locustfile.py --host http://localhost:8000

# Headless CI run (shape drives the ramp; --run-time is a safety ceiling)
locust -f apps/api/tests/locustfile.py --host http://localhost:8000 \
       --headless --run-time 6m \
       --html reports/load_test_report.html \
       --csv  reports/load_test

# Env-var overrides (avoid hard-coding credentials)
LOCUST_TEST_EMAIL=user@example.com \
LOCUST_TEST_PASSWORD=secret \
locust -f apps/api/tests/locustfile.py --headless --run-time 6m

Environment Variables
---------------------
LOCUST_TEST_EMAIL     – email for the pre-existing test account (default: test@example.com)
LOCUST_TEST_PASSWORD  – password for that account              (default: TestPass@123)
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from typing import Optional

from locust import HttpUser, LoadTestShape, TaskSet, between, events, task
from locust.exception import StopUser

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEST_EMAIL = os.getenv("LOCUST_TEST_EMAIL", "admin@gst.com")
TEST_PASSWORD = os.getenv("LOCUST_TEST_PASSWORD", "admin1234")

logger = logging.getLogger("taxogpt.loadtest")

# ---------------------------------------------------------------------------
# Sample GST / Tax questions for realistic workload
# ---------------------------------------------------------------------------

GST_QUESTIONS = [
    "What is the GST rate on restaurant services?",
    "How do I file GSTR-1 return?",
    "What is the threshold limit for GST registration?",
    "Explain input tax credit rules under GST.",
    "What is the difference between CGST and SGST?",
    "How is GST calculated on mixed supply?",
    "What are the penalties for late GST filing?",
    "Explain reverse charge mechanism under GST.",
    "What documents are required for GST registration?",
    "How does the composition scheme work under GST?",
    "What is the time of supply rule under GST?",
    "Explain the GST refund process for exporters.",
    "What is the HSN code system under GST?",
    "How does GST apply to e-commerce transactions?",
    "What are the rules for place of supply in GST?",
]




# ---------------------------------------------------------------------------
# Shared token store (populated at user on_start)
# ---------------------------------------------------------------------------

class AuthToken:
    """Simple holder for the Bearer token and discovered session data."""

    def __init__(self, token: str):
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}"}
        self.session_id: Optional[str] = None   # set after first chat
        self.message_id: Optional[int] = None   # last bot message id


# ---------------------------------------------------------------------------
# Helper mixin
# ---------------------------------------------------------------------------

class _AuthMixin:
    """Provides login helper and authenticated request wrappers."""

    auth: Optional[AuthToken] = None

    def _login(self):
        """Authenticate and store the Bearer token. Raises StopUser on failure."""
        with self.client.post(
            "/api/v1/auth/login",
            json={"email": TEST_EMAIL, "password": TEST_PASSWORD, "identifier": "locust-load-test"},
            name="[AUTH] Login",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                token = resp.json().get("access_token")
                if token:
                    self.auth = AuthToken(token)
                    resp.success()
                    return
            resp.failure(f"Login failed: {resp.status_code} – {resp.text[:200]}")
            raise StopUser()

    def _get(self, url: str, name: str, **kwargs):
        return self.client.get(url, headers=self.auth.headers, name=name, **kwargs)

    def _post(self, url: str, name: str, **kwargs):
        return self.client.post(url, headers=self.auth.headers, name=name, **kwargs)

    def _patch(self, url: str, name: str, **kwargs):
        return self.client.patch(url, headers=self.auth.headers, name=name, **kwargs)

    def _delete(self, url: str, name: str, **kwargs):
        return self.client.delete(url, headers=self.auth.headers, name=name, **kwargs)


# ===========================================================================
# Task Set – Auth Flows
# ===========================================================================

class AuthTasks(TaskSet, _AuthMixin):
    """
    Exercises the auth endpoints that are cheap to hit frequently:
    /me, /credits, /heartbeat, session listing.
    """

    def on_start(self):
        self._login()

    @task(5)
    def get_me(self):
        with self._get("/api/v1/auth/me", name="[AUTH] GET /me", catch_response=True) as r:
            if r.status_code == 200:
                r.success()
            else:
                r.failure(f"GET /me → {r.status_code}")

    @task(3)
    def get_credits(self):
        with self._get("/api/v1/auth/credits", name="[AUTH] GET /credits", catch_response=True) as r:
            if r.status_code == 200:
                r.success()
            else:
                r.failure(f"GET /credits → {r.status_code}")

    @task(4)
    def heartbeat(self):
        with self._post("/api/v1/auth/heartbeat", name="[AUTH] POST /heartbeat", catch_response=True) as r:
            if r.status_code in (200, 401):  # 401 is acceptable if session really expired
                r.success()
            else:
                r.failure(f"Heartbeat → {r.status_code}")

    @task(2)
    def list_auth_sessions(self):
        with self._get("/api/v1/auth/sessions", name="[AUTH] GET /sessions", catch_response=True) as r:
            if r.status_code == 200:
                r.success()
            else:
                r.failure(f"Auth sessions → {r.status_code}")

    @task(1)
    def update_profile_noop(self):
        """Patch profile with current values to exercise the PATCH endpoint."""
        with self._patch(
            "/api/v1/auth/me",
            name="[AUTH] PATCH /me",
            json={"full_name": "Load Test User"},
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                r.success()
            else:
                r.failure(f"PATCH /me → {r.status_code}")


# ===========================================================================
# Task Set – Chat / Simple Stream
# ===========================================================================

class SimpleChatTasks(TaskSet, _AuthMixin):
    """
    Exercises the simple-mode streaming endpoint.
    After each successful chat the session_id and message_id are cached
    so that subsequent tasks (history, feedback, share) can reuse them.
    """

    def on_start(self):
        self._login()

    # ---- primary load driver -----------------------------------------------

    @task(10)
    def ask_simple(self):
        """
        WHY TWO SEPARATE METRICS?
        --------------------------
        LLM streaming responses take 10–40 seconds to complete. If Locust
        measures the whole stream as ONE request, the report shows enormous
        response times and near-zero RPS — which looks like a failure but is
        entirely normal for streaming AI endpoints.

        Instead we fire TWO custom events into Locust's stats engine:
          1. [CHAT] TTFT /ask/stream/simple
             → Time To First Token: how fast the server STARTS sending data.
               This is the real latency users feel. Should be < 2–3 s.

          2. [CHAT] Total Duration /ask/stream/simple
             → Wall-clock time until the full answer is received.
               Useful for SLA checks. Typically 10–40 s for LLMs.

        The HTTP request itself is marked as successful immediately (so Locust
        doesn't report 30-second response times and inflate p95/p99).
        """
        question = random.choice(GST_QUESTIONS)
        body = {"question": question}
        if self.auth.session_id:
            body["session_id"] = self.auth.session_id

        t_start = time.monotonic()
        ttft_ms: Optional[float] = None       # time to first content token (ms)
        total_ms: Optional[float] = None      # full stream duration (ms)
        got_completion = False
        error_msg: Optional[str] = None

        with self._post(
            "/api/v1/chat/ask/stream/simple",
            name="[CHAT] POST /ask/stream/simple (connection)",
            json=body,
            stream=True,
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:200]}")
                return

            # Mark the HTTP connection itself as succeeded immediately —
            # the meaningful stats are reported separately below.
            resp.success()

            # ── Consume the NDJSON stream ──────────────────────────────────
            for raw_line in resp.iter_lines():
                if not raw_line:
                    continue
                try:
                    event = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue

                etype = event.get("type")

                if etype == "content" and ttft_ms is None:
                    # First content chunk received — record TTFT
                    ttft_ms = (time.monotonic() - t_start) * 1000

                elif etype == "completion":
                    self.auth.session_id = event.get("session_id") or self.auth.session_id
                    self.auth.message_id = event.get("message_id") or self.auth.message_id
                    total_ms = (time.monotonic() - t_start) * 1000
                    got_completion = True

                elif etype == "error":
                    error_msg = event.get("message", "unknown stream error")
                    break

        # ── Fire custom stats events into Locust ──────────────────────────
        if error_msg:
            self.user.environment.events.request.fire(
                request_type="STREAM",
                name="[CHAT] TTFT /ask/stream/simple",
                response_time=ttft_ms or (time.monotonic() - t_start) * 1000,
                response_length=0,
                exception=Exception(error_msg),
            )
            logger.warning("Stream error from server: %s", error_msg)
            return

        if ttft_ms is not None:
            self.user.environment.events.request.fire(
                request_type="STREAM",
                name="[CHAT] TTFT /ask/stream/simple",
                response_time=ttft_ms,
                response_length=0,
                exception=None,
            )
            logger.debug("TTFT: %.0f ms", ttft_ms)

        if got_completion and total_ms is not None:
            self.user.environment.events.request.fire(
                request_type="STREAM",
                name="[CHAT] Total Duration /ask/stream/simple",
                response_time=total_ms,
                response_length=0,
                exception=None,
            )
            logger.debug("Total stream: %.0f ms", total_ms)
        elif not got_completion:
            self.user.environment.events.request.fire(
                request_type="STREAM",
                name="[CHAT] Total Duration /ask/stream/simple",
                response_time=(time.monotonic() - t_start) * 1000,
                response_length=0,
                exception=Exception("Stream ended without completion event"),
            )
            logger.warning("Stream did not receive a completion event")

    # ---- read-heavy tasks ---------------------------------------------------

    @task(5)
    def list_sessions(self):
        with self._get("/api/v1/sessions", name="[CHAT] GET /sessions", catch_response=True) as r:
            if r.status_code == 200:
                sessions = r.json()
                # Cache a session_id if not already set
                if sessions and not self.auth.session_id:
                    self.auth.session_id = sessions[0]["id"]
                r.success()
            else:
                r.failure(f"List sessions → {r.status_code}")

    @task(4)
    def get_session_history(self):
        if not self.auth.session_id:
            return
        with self._get(
            f"/api/v1/sessions/{self.auth.session_id}/history",
            name="[CHAT] GET /sessions/{id}/history",
            catch_response=True,
        ) as r:
            if r.status_code in (200, 404):
                r.success()
            else:
                r.failure(f"Session history → {r.status_code}")

    @task(2)
    def submit_feedback(self):
        if not self.auth.message_id:
            return
        with self._post(
            "/api/v1/sessions/feedback",
            name="[CHAT] POST /sessions/feedback",
            json={
                "message_id": self.auth.message_id,
                "rating": random.choice([1, 2, 3, 4, 5]),
                "comment": "Automated load test feedback",
            },
            catch_response=True,
        ) as r:
            if r.status_code in (200, 404):
                r.success()
            else:
                r.failure(f"Feedback → {r.status_code}")

    @task(1)
    def share_session(self):
        if not self.auth.session_id:
            return
        with self._post(
            f"/api/v1/chat/share/session/{self.auth.session_id}",
            name="[CHAT] POST /chat/share/session/{id}",
            catch_response=True,
        ) as r:
            if r.status_code in (200, 404):
                r.success()
            else:
                r.failure(f"Share session → {r.status_code}")

    @task(1)
    def share_message(self):
        if not self.auth.message_id:
            return
        with self._post(
            f"/api/v1/chat/share/message/{self.auth.message_id}",
            name="[CHAT] POST /chat/share/message/{id}",
            catch_response=True,
        ) as r:
            if r.status_code in (200, 404):
                r.success()
            else:
                r.failure(f"Share message → {r.status_code}")





# ===========================================================================
# Task Set – Session Lifecycle (with cleanup)
# ===========================================================================

class SessionLifecycleTasks(TaskSet, _AuthMixin):
    """
    Full lifecycle: create session via chat → read history → delete.
    Useful for detecting connection-pool exhaustion and session leak bugs.
    """

    def on_start(self):
        self._login()
        self._tmp_session_id: Optional[str] = None

    @task(3)
    def full_session_lifecycle(self):
        # 1. Start a new session
        question = random.choice(GST_QUESTIONS)
        with self._post(
            "/api/v1/chat/ask/stream/simple",
            name="[LIFECYCLE] Create new session",
            json={"question": question},
            stream=True,
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"Lifecycle create → {resp.status_code}")
                return

            session_id = None
            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("type") == "completion":
                    session_id = event.get("session_id")
                elif event.get("type") == "error":
                    resp.failure(f"Lifecycle error: {event.get('message')}")
                    return

            if session_id:
                self._tmp_session_id = session_id
                resp.success()
            else:
                resp.failure("No session_id in completion event")
                return

        # 2. Fetch history
        if self._tmp_session_id:
            with self._get(
                f"/api/v1/sessions/{self._tmp_session_id}/history",
                name="[LIFECYCLE] Read session history",
                catch_response=True,
            ) as r:
                if r.status_code in (200, 404):
                    r.success()
                else:
                    r.failure(f"Lifecycle history → {r.status_code}")

        # 3. Delete the session
        if self._tmp_session_id:
            with self._delete(
                f"/api/v1/sessions/{self._tmp_session_id}",
                name="[LIFECYCLE] Delete session",
                catch_response=True,
            ) as r:
                if r.status_code in (200, 404):
                    r.success()
                    self._tmp_session_id = None
                else:
                    r.failure(f"Lifecycle delete → {r.status_code}")


# ===========================================================================
# Task Set – Payments
# ===========================================================================

class PaymentTasks(TaskSet, _AuthMixin):
    """
    Read-only payment endpoints. (Order creation / verification are NOT
    executed – they would create real Razorpay orders and deduct credits.)
    """

    def on_start(self):
        self._login()

    @task(5)
    def list_packages(self):
        with self.client.get(
            "/api/v1/payments/packages",
            name="[PAYMENTS] GET /packages",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                r.success()
            else:
                r.failure(f"List packages → {r.status_code}")

    @task(3)
    def validate_coupon_invalid(self):
        """Stress the coupon validation endpoint with an invalid code."""
        with self._post(
            "/api/v1/payments/validate-coupon",
            name="[PAYMENTS] POST /validate-coupon",
            json={"coupon_code": "LOADTEST_INVALID", "package_name": "basic"},
            catch_response=True,
        ) as r:
            # Both 200 (invalid coupon detail) and 400 are valid responses
            if r.status_code in (200, 400):
                r.success()
            else:
                r.failure(f"Validate coupon → {r.status_code}")

    @task(4)
    def get_credit_history(self):
        with self._get(
            "/api/v1/payments/history",
            name="[PAYMENTS] GET /history",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                r.success()
            else:
                r.failure(f"Credit history → {r.status_code}")

    @task(3)
    def get_transactions(self):
        with self._get(
            "/api/v1/payments/transactions",
            name="[PAYMENTS] GET /transactions",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                r.success()
            else:
                r.failure(f"Transactions → {r.status_code}")


# ===========================================================================
# Task Set – Health Checks
# ===========================================================================

class HealthTasks(TaskSet):
    """
    Lightweight health-check hammering – useful for baseline latency and
    ensuring the API stays up under concurrent load.
    """

    @task(10)
    def shallow_health(self):
        with self.client.get(
            "/api/v1/health",
            name="[HEALTH] GET /health",
            catch_response=True,
        ) as r:
            if r.status_code == 200 and r.json().get("status") == "ok":
                r.success()
            else:
                r.failure(f"Shallow health → {r.status_code}: {r.text[:100]}")

    @task(2)
    def deep_health(self):
        """Deep health is expensive (hits DB, Redis, Qdrant) – hit sparingly."""
        with self.client.get(
            "/api/v1/health/deep",
            name="[HEALTH] GET /health/deep",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                payload = r.json()
                if payload.get("status") == "ok":
                    r.success()
                else:
                    r.failure(f"Deep health degraded: {payload}")
            else:
                r.failure(f"Deep health → {r.status_code}")


# ===========================================================================
# User Classes
# ===========================================================================

class RegularUser(HttpUser):
    """
    Simulates a typical end-user:
    heavy on simple chat, reads sessions, occasional feedback.
    """

    weight = 6
    wait_time = between(2, 8)  # realistic think time between requests
    tasks = {
        SimpleChatTasks: 6,
        AuthTasks: 2,
        PaymentTasks: 1,
        HealthTasks: 1,
    }

    def on_start(self):
        # Warm-up: verify the server is reachable before heavy tasks run
        r = self.client.get("/api/v1/health")
        if r.status_code != 200:
            logger.error("Health check failed on start – aborting user.")
            raise StopUser()




class SessionLifecycleUser(HttpUser):
    """
    Dedicated user for detecting connection-pool exhaustion bugs.
    Low spawn weight – only a few of these run at a time.
    """

    weight = 1
    wait_time = between(1, 3)
    tasks = [SessionLifecycleTasks]

    def on_start(self):
        r = self.client.get("/api/v1/health")
        if r.status_code != 200:
            raise StopUser()


class MonitorUser(HttpUser):
    """
    Simulates an ops dashboard polling health endpoints.
    Very lightweight – high RPS to stress health-check path.
    """

    weight = 1
    wait_time = between(0.5, 2)
    tasks = [HealthTasks]


# ===========================================================================
# Step Load Shape  –  gradual ramp-up
# ===========================================================================

class StepLoadShape(LoadTestShape):
    """
    Flat load at exactly 5 concurrent users for focused analysis.

    Timeline:
      0 s  →  5 users   @ spawn rate 1/s   (analysis window)
    300 s  →  test stops automatically

    To later add more stages, expand the `stages` list:
        stages = [
            (300,  5,  1),   # current — 5-user analysis
            (420, 10,  2),   # next step: add 5 more users
            (600, 20,  5),   # peak
        ]
    """

    # (duration_seconds, target_users, spawn_rate)
    stages = [
        (300, 5, 1),  # 5 minutes at exactly 5 users
    ]

    def tick(self):
        run_time = self.get_run_time()
        for duration, users, spawn_rate in self.stages:
            if run_time < duration:
                return (users, spawn_rate)
        return None  # signals Locust to stop the test


# ===========================================================================
# Event hooks for enhanced reporting
# ===========================================================================

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    host = environment.host or "http://localhost:8000"
    logger.info("=" * 70)
    logger.info("TaxoGPT Load Test Starting")
    logger.info(f"  Target: {host}")
    logger.info(f"  Test user: {TEST_EMAIL}")
    logger.info("=" * 70)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats
    logger.info("=" * 70)
    logger.info("TaxoGPT Load Test Finished")
    logger.info(f"  Total requests : {stats.total.num_requests}")
    logger.info(f"  Failures       : {stats.total.num_failures}")
    logger.info(f"  Failure rate   : {stats.total.fail_ratio * 100:.1f}%")
    logger.info(f"  Avg response   : {stats.total.avg_response_time:.0f}ms")
    logger.info(f"  95th percentile: {stats.total.get_response_time_percentile(0.95):.0f}ms")
    logger.info("=" * 70)


@events.request.add_listener
def on_request(
    request_type, name, response_time, response_length, exception, **kwargs
):
    """Log any request that takes more than 10 seconds as a warning."""
    if response_time and response_time > 10_000:
        logger.warning(
            "SLOW REQUEST: %s %s took %.1fs",
            request_type,
            name,
            response_time / 1000,
        )
