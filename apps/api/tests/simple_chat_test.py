"""
TaxoGPT — Dedicated Simple Stream Load Test
=============================================
Tests ONLY the /api/v1/chat/ask/stream/simple endpoint
through the complete user lifecycle:

  Phase 1 — Auth
    ✓  POST /auth/login         → acquire Bearer token

  Phase 2 — First message (new session)
    ✓  POST /ask/stream/simple  → no session_id, server creates one
       Measures:
         • Connection time       (HTTP 200 received)
         • TTFT                  (first "content" chunk)
         • Total stream duration (completion event)
         • Chunks received       (logged, not a Locust stat)

  Phase 3 — Follow-up message (same session, tests context window)
    ✓  POST /ask/stream/simple  → with session_id (follow-up question)
       Same 3 metrics as Phase 2, tagged "(follow-up)"

  Phase 4 — Post-stream reads (what the frontend does after streaming)
    ✓  GET  /sessions                        → list all sessions
    ✓  GET  /sessions/{id}/history           → read full history
    ✓  POST /sessions/feedback               → rate the bot message
    ✓  POST /chat/share/session/{id}         → generate share link
    ✓  POST /chat/share/message/{message_id} → share single message

  Phase 5 — Cleanup
    ✓  DELETE /sessions/{id}                 → remove test session

Load Shape
----------
  0 s  →  5 users  @ 1 user/s   (5 min flat — focused analysis)
  300s →  test stops

Usage
-----
# Interactive (watch the Locust UI at http://localhost:8089)
locust -f apps/api/tests/stream_simple_test.py --host http://localhost:8000

# Headless CI
locust -f apps/api/tests/stream_simple_test.py \\
       --host http://localhost:8000 \\
       --headless --run-time 6m \\
       --html reports/stream_simple_report.html \\
       --csv  reports/stream_simple

Environment Variables
---------------------
LOCUST_TEST_EMAIL     — verified test account email    (default: test@example.com)
LOCUST_TEST_PASSWORD  — password for the test account  (default: TestPass@123)
STREAM_TIMEOUT_S      — max seconds to wait for stream (default: 120)
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from typing import Optional

from locust import HttpUser, LoadTestShape, task, between, events
from locust.exception import StopUser

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TEST_EMAIL = os.getenv("LOCUST_TEST_EMAIL", "admin@gst.com")
TEST_PASSWORD = os.getenv("LOCUST_TEST_PASSWORD", "admin1234")
STREAM_TIMEOUT_S = int(os.getenv("STREAM_TIMEOUT_S", "180"))

logger = logging.getLogger("taxogpt.stream_simple")

# ---------------------------------------------------------------------------
# Realistic question bank
# ---------------------------------------------------------------------------

FIRST_QUESTIONS = [
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

# Follow-up questions that reference the prior answer —
# tests Stage 1 clarification (DEPENDENT query rewrite)
FOLLOW_UP_QUESTIONS = [
    "Can you explain that in simpler terms?",
    "What are the exceptions to this rule?",
    "Give me a practical example of the above.",
    "What happens if someone does not comply with this?",
    "Is this rule the same for small businesses?",
    "When did this come into effect?",
    "How does this interact with import/export?",
]


# ---------------------------------------------------------------------------
# Helper: fire a custom Locust stat event
# ---------------------------------------------------------------------------

def _fire(env, name: str, response_time_ms: float, success: bool, err: str = ""):
    env.events.request.fire(
        request_type="STREAM",
        name=name,
        response_time=response_time_ms,
        response_length=0,
        exception=None if success else Exception(err),
    )


# ---------------------------------------------------------------------------
# Helper: consume an NDJSON stream and return rich telemetry
# ---------------------------------------------------------------------------

def _consume_stream(resp, env, label: str) -> dict:
    """
    Reads every line of an NDJSON streaming response.

    Returns
    -------
    {
        "success":        bool,
        "session_id":     str | None,
        "message_id":     int | None,
        "ttft_ms":        float | None,   # time to first content chunk
        "total_ms":       float | None,   # full stream wall-clock time
        "chunks":         int,            # number of content events received
        "error":          str | None,     # server-side error message if any
    }
    """
    t_start = time.monotonic()
    ttft_ms: Optional[float] = None
    total_ms: Optional[float] = None
    session_id: Optional[str] = None
    message_id: Optional[int] = None
    chunks = 0
    error: Optional[str] = None
    got_completion = False

    for raw in resp.iter_lines():
        if not raw:
            continue
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("%s — unparseable line: %r", label, raw[:120])
            continue

        etype = event.get("type")

        if etype == "content":
            chunks += 1
            if ttft_ms is None:
                # ── First token! ───────────────────────────────────────────
                ttft_ms = (time.monotonic() - t_start) * 1000
                logger.info("%s — TTFT: %.0f ms", label, ttft_ms)

        elif etype == "retrieval":
            logger.debug(
                "%s — retrieval: %d sources, usage=%s",
                label,
                len(event.get("sources", [])),
                event.get("usage", {}),
            )

        elif etype == "completion":
            got_completion = True
            total_ms = (time.monotonic() - t_start) * 1000
            session_id = event.get("session_id")
            message_id = event.get("message_id")
            logger.info(
                "%s — completion in %.0f ms | session=%s | msg_id=%s | chunks=%d",
                label, total_ms, session_id, message_id, chunks,
            )

        elif etype == "error":
            error = event.get("message", "unknown stream error")
            logger.error("%s — server error event: %s", label, error)
            break

    # ── Emit Locust stats ──────────────────────────────────────────────────
    elapsed_fallback = (time.monotonic() - t_start) * 1000

    if ttft_ms is not None:
        _fire(env, f"[STREAM] TTFT {label}", ttft_ms, error is None)
    else:
        # Never got a content chunk at all
        _fire(env, f"[STREAM] TTFT {label}", elapsed_fallback, False,
              error or "no content chunks received")

    if got_completion and total_ms is not None:
        _fire(env, f"[STREAM] Total Duration {label}", total_ms, True)
    else:
        _fire(env, f"[STREAM] Total Duration {label}", elapsed_fallback, False,
              error or "stream closed without completion event")

    return {
        "success":    got_completion and error is None,
        "session_id": session_id,
        "message_id": message_id,
        "ttft_ms":    ttft_ms,
        "total_ms":   total_ms,
        "chunks":     chunks,
        "error":      error,
    }


# ---------------------------------------------------------------------------
# The User
# ---------------------------------------------------------------------------

class SimpleStreamUser(HttpUser):
    """
    One virtual user that runs the complete simple-stream lifecycle
    sequentially in a single task. Each iteration = one full user session.
    """

    wait_time = between(2, 5)          # think time between full iterations

    # State persisted across the iteration
    _token: Optional[str] = None
    _headers: dict = {}
    _chat_session_id: Optional[str] = None
    _last_message_id: Optional[int] = None

    # ── Startup: authenticate once ──────────────────────────────────────────

    def on_start(self):
        """Login and store the Bearer token. Hard-stop if login fails."""
        # Quick server sanity check
        hc = self.client.get("/api/v1/health", name="[HEALTH] /health")
        if hc.status_code != 200:
            logger.error("Health check failed — stopping user")
            raise StopUser()

        with self.client.post(
            "/api/v1/auth/login",
            json={
                "email":      TEST_EMAIL,
                "password":   TEST_PASSWORD,
                "identifier": "locust-stream-test",
            },
            name="[AUTH] POST /auth/login",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                tok = r.json().get("access_token")
                if tok:
                    self._token = tok
                    self._headers = {"Authorization": f"Bearer {tok}"}
                    r.success()
                    logger.info("Auth OK for %s", TEST_EMAIL)
                    return
            r.failure(f"Login failed: {r.status_code} — {r.text[:200]}")
            raise StopUser()

    # ── Main task: full lifecycle ───────────────────────────────────────────

    @task
    def full_stream_lifecycle(self):
        """
        Runs the complete lifecycle sequentially:
          1. New session chat (stream)
          2. Follow-up chat  (stream, same session)
          3. List sessions
          4. Fetch session history
          5. Submit feedback
          6. Share session
          7. Share message
          8. Delete session (cleanup)
        """
        # Reset per-iteration state
        self._chat_session_id = None
        self._last_message_id = None

        # ── Phase 2: First message (new session) ──────────────────────────
        success = self._phase_first_message()
        if not success:
            # Can't continue without a valid session
            return

        # ── Phase 3: Follow-up message (same session) ─────────────────────
        self._phase_follow_up()

        # ── Phase 4: Post-stream reads ────────────────────────────────────
        self._phase_list_sessions()
        self._phase_get_history()
        self._phase_feedback()
        self._phase_share_session()
        self._phase_share_message()

        # ── Phase 5: Cleanup ──────────────────────────────────────────────
        self._phase_delete_session()

    # ── Phase implementations ───────────────────────────────────────────────

    def _phase_first_message(self) -> bool:
        """
        POST /ask/stream/simple with NO session_id.
        Server generates a new session automatically.
        """
        question = random.choice(FIRST_QUESTIONS)
        logger.info("Phase 2 — new session | Q: %s", question[:60])

        with self.client.post(
            "/api/v1/chat/ask/stream/simple",
            headers=self._headers,
            json={"question": question},       # no session_id → new session
            name="[CHAT:NEW] POST /ask/stream/simple (connection)",
            stream=True,
            catch_response=True,
            timeout=STREAM_TIMEOUT_S,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:300]}")
                return False

            # Connection accepted — success for this metric row
            resp.success()

            result = _consume_stream(
                resp, self.environment, "/ask/stream/simple (new)"
            )

        if result["success"]:
            self._chat_session_id = result["session_id"]
            self._last_message_id = result["message_id"]
            logger.info(
                "Phase 2 done — session=%s msg_id=%s chunks=%d",
                self._chat_session_id, self._last_message_id, result["chunks"]
            )
            return True
        else:
            logger.error("Phase 2 failed — %s", result["error"])
            return False

    def _phase_follow_up(self):
        """
        POST /ask/stream/simple WITH the session_id from Phase 2.
        Tests Stage 1 DEPENDENT query rewrite in the RAG pipeline.
        """
        if not self._chat_session_id:
            return

        question = random.choice(FOLLOW_UP_QUESTIONS)
        logger.info("Phase 3 — follow-up | Q: %s", question)

        with self.client.post(
            "/api/v1/chat/ask/stream/simple",
            headers=self._headers,
            json={
                "question":   question,
                "session_id": self._chat_session_id,
            },
            name="[CHAT:FOLLOWUP] POST /ask/stream/simple (connection)",
            stream=True,
            catch_response=True,
            timeout=STREAM_TIMEOUT_S,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:300]}")
                return

            resp.success()

            result = _consume_stream(
                resp, self.environment, "/ask/stream/simple (follow-up)"
            )

        if result["success"]:
            # Cache the latest message_id for feedback / share
            self._last_message_id = result["message_id"] or self._last_message_id
            logger.info(
                "Phase 3 done — chunks=%d total=%.0fs",
                result["chunks"], (result["total_ms"] or 0) / 1000
            )
        else:
            logger.warning("Phase 3 follow-up failed — %s", result["error"])

    def _phase_list_sessions(self):
        """GET /sessions — verify session appears in list."""
        with self.client.get(
            "/api/v1/sessions",
            headers=self._headers,
            name="[READ] GET /sessions",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                sessions = r.json()
                ids = [s.get("id") for s in sessions]
                if self._chat_session_id and self._chat_session_id not in ids:
                    r.failure(
                        f"Created session {self._chat_session_id} not found in list"
                    )
                else:
                    r.success()
                    logger.debug("Sessions in list: %d", len(sessions))
            else:
                r.failure(f"GET /sessions → {r.status_code}")

    def _phase_get_history(self):
        """GET /sessions/{id}/history — validate messages are persisted."""
        if not self._chat_session_id:
            return

        with self.client.get(
            f"/api/v1/sessions/{self._chat_session_id}/history",
            headers=self._headers,
            name="[READ] GET /sessions/{id}/history",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                history = r.json()
                msg_count = len(history) if isinstance(history, list) else 0
                logger.debug(
                    "History for session %s: %d messages",
                    self._chat_session_id, msg_count
                )
                # We expect at least 2 messages (user + bot) from Phase 2
                if msg_count < 2:
                    r.failure(
                        f"Expected ≥2 messages in history, got {msg_count}"
                    )
                else:
                    r.success()
            elif r.status_code == 404:
                r.failure(f"Session history not found (404) — session may have been deleted")
            else:
                r.failure(f"GET history → {r.status_code}")

    def _phase_feedback(self):
        """POST /sessions/feedback — submit a rating for the last bot message."""
        if not self._last_message_id:
            return

        with self.client.post(
            "/api/v1/sessions/feedback",
            headers=self._headers,
            json={
                "message_id": self._last_message_id,
                "rating":     random.choice([3, 4, 5]),
                "comment":    "Stream load test automated feedback",
            },
            name="[WRITE] POST /sessions/feedback",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                r.success()
                logger.debug("Feedback submitted for msg_id=%s", self._last_message_id)
            elif r.status_code == 404:
                r.failure(f"Message {self._last_message_id} not found for feedback")
            else:
                r.failure(f"POST /sessions/feedback → {r.status_code}")

    def _phase_share_session(self):
        """POST /chat/share/session/{id} — generate a public link for the session."""
        if not self._chat_session_id:
            return

        with self.client.post(
            f"/api/v1/chat/share/session/{self._chat_session_id}",
            headers=self._headers,
            name="[WRITE] POST /chat/share/session/{id}",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                shared_id = r.json().get("shared_id")
                logger.debug("Session shared — shared_id=%s", shared_id)
                r.success()
            elif r.status_code == 404:
                r.failure("Session not found for sharing (404)")
            else:
                r.failure(f"POST share/session → {r.status_code}")

    def _phase_share_message(self):
        """POST /chat/share/message/{id} — generate a public link for one message."""
        if not self._last_message_id:
            return

        with self.client.post(
            f"/api/v1/chat/share/message/{self._last_message_id}",
            headers=self._headers,
            name="[WRITE] POST /chat/share/message/{id}",
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                shared_id = r.json().get("shared_id")
                logger.debug("Message shared — shared_id=%s", shared_id)
                r.success()
            elif r.status_code == 404:
                r.failure("Message not found for sharing (404)")
            else:
                r.failure(f"POST share/message → {r.status_code}")

    def _phase_delete_session(self):
        """DELETE /sessions/{id} — clean up so the DB doesn't accumulate test data."""
        if not self._chat_session_id:
            return

        with self.client.delete(
            f"/api/v1/sessions/{self._chat_session_id}",
            headers=self._headers,
            name="[CLEANUP] DELETE /sessions/{id}",
            catch_response=True,
        ) as r:
            if r.status_code in (200, 404):
                r.success()
                logger.debug("Session %s deleted", self._chat_session_id)
                self._chat_session_id = None
                self._last_message_id = None
            else:
                r.failure(f"DELETE session → {r.status_code}")


# ---------------------------------------------------------------------------
# Load Shape — flat 5 users for focused analysis
# ---------------------------------------------------------------------------

class StreamSimpleShape(LoadTestShape):
    """
    Flat 5 users for 5 minutes then stops.

    To ramp up later, expand stages:
        stages = [
            (300,  5, 1),   # current analysis window
            (420, 10, 2),   # bump to 10
            (600, 20, 5),   # bump to 20
        ]
    """

    stages = [
        (300, 20, 1),   # 5 minutes at exactly 5 users
    ]

    def tick(self):
        t = self.get_run_time()
        for duration, users, rate in self.stages:
            if t < duration:
                return (users, rate)
        return None  # stop the test


# ---------------------------------------------------------------------------
# Event hooks
# ---------------------------------------------------------------------------

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    logger.info("=" * 70)
    logger.info("TaxoGPT — Simple Stream Dedicated Test")
    logger.info(f"  Target  : {environment.host}")
    logger.info(f"  Account : {TEST_EMAIL}")
    logger.info(f"  Timeout : {STREAM_TIMEOUT_S}s per stream")
    logger.info("=" * 70)
    logger.info("Metrics to watch:")
    logger.info("  [STREAM] TTFT /ask/stream/simple (new)        → target < 5s")
    logger.info("  [STREAM] TTFT /ask/stream/simple (follow-up)  → target < 8s (extra Stage-1 LLM call)")
    logger.info("  [STREAM] Total Duration /ask/stream/simple     → 15–60s is normal for LLMs")
    logger.info("  [WRITE]  failure rate                          → must be 0%")
    logger.info("=" * 70)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats
    logger.info("=" * 70)
    logger.info("TaxoGPT — Simple Stream Test Finished")
    logger.info(f"  Total requests  : {stats.total.num_requests}")
    logger.info(f"  Failures        : {stats.total.num_failures}")
    logger.info(f"  Failure rate    : {stats.total.fail_ratio * 100:.1f}%")

    # Per-row breakdown
    for name, entry in sorted(stats.entries.items()):
        if entry.num_requests == 0:
            continue
        p50  = entry.get_response_time_percentile(0.50)
        p95  = entry.get_response_time_percentile(0.95)
        fails = entry.num_failures
        logger.info(
            "  %-58s  p50=%6.0fms  p95=%6.0fms  fails=%d",
            name[1],   # name is a (method, route) tuple
            p50, p95, fails,
        )
    logger.info("=" * 70)


@events.request.add_listener
def on_slow_request(request_type, name, response_time, response_length, exception, **kwargs):
    """Warn on any non-stream request taking over 5 seconds."""
    if request_type != "STREAM" and response_time and response_time > 5_000:
        logger.warning(
            "SLOW %s %s → %.1fs", request_type, name, response_time / 1000
        )
