"""
ingestion_api/tests/test_cgst_section.py

End-to-end tests for the cgst_section ingestion flow.

Run with:
    pytest ingestion_api/tests/test_cgst_section.py -v

Prerequisites:
    - Redis running on localhost:6379
    - Qdrant running on localhost:6333
    - .env loaded with valid AWS credentials
    - ADMIN_USERNAME + ADMIN_PASSWORD_HASH set in .env
"""

from __future__ import annotations

import json
import time

import pytest
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)

# ── Sample data ───────────────────────────────────────────────────────────────

SAMPLE_CGST_SECTION = {
    "chunk_type": "cgst_section",
    "data": {
        "ext": {
            "section_number": "16",
            "section_title":  "Eligibility and conditions for taking input tax credit",
            "chapter_number": "V",
            "chapter_title":  "Input Tax Credit",
        },
        "text": (
            "16. Eligibility and conditions for taking input tax credit.—"
            "(1) Every registered person shall, subject to such conditions and "
            "restrictions as may be prescribed and in the manner specified in "
            "section 49, be entitled to take credit of input tax charged on any "
            "supply of goods or services or both to him which are used or intended "
            "to be used in the course or furtherance of his business and the said "
            "amount shall be credited to the electronic credit ledger of such person.\n\n"
            "(2) Notwithstanding anything contained in this section, no registered "
            "person shall be entitled to the credit of any input tax in respect of "
            "any supply of goods or services or both to him unless,—\n"
            "(a) he is in possession of a tax invoice or debit note issued by a "
            "supplier registered under this Act, or such other tax paying documents "
            "as may be prescribed;\n"
            "(b) he has received the goods or services or both;\n"
            "(c) subject to the provisions of section 41, the tax charged in respect "
            "of such supply has been actually paid to the Government, either in cash "
            "or through utilisation of input tax credit admissible in respect of "
            "the said supply; and\n"
            "(d) he has furnished the return under section 39."
        ),
        "summary": "",     # will be autofilled
        "keywords": [],    # will be autofilled
    },
}


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_token() -> str:
    import os
    resp = client.post("/auth/login", json={
        "username": os.getenv("ADMIN_USERNAME", "admin"),
        "password": os.getenv("ADMIN_PASSWORD", "testpassword"),
    })
    assert resp.status_code == 200, f"Login failed: {resp.text}"
    return resp.json()["access_token"]


@pytest.fixture
def auth_headers() -> dict:
    return {"Authorization": f"Bearer {get_token()}"}


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestAuth:
    def test_login_success(self):
        import os
        resp = client.post("/auth/login", json={
            "username": os.getenv("ADMIN_USERNAME", "admin"),
            "password": os.getenv("ADMIN_PASSWORD", "testpassword"),
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert data["expires_in"] > 0

    def test_login_wrong_password(self):
        resp = client.post("/auth/login", json={
            "username": "admin",
            "password": "wrongpassword",
        })
        assert resp.status_code == 401

    def test_protected_route_without_token(self):
        resp = client.get("/chunks/schema/cgst_section")
        assert resp.status_code == 403   # HTTPBearer returns 403 when no header


class TestSchema:
    def test_get_cgst_section_schema(self, auth_headers):
        resp = client.get("/chunks/schema/cgst_section", headers=auth_headers)
        assert resp.status_code == 200
        schema = resp.json()

        assert schema["chunk_type"] == "cgst_section"
        assert schema["authority_level"] == 1
        assert schema["namespace"] == "statutory_law"
        assert schema["has_supersession"] is True

        # Check anchor fields are present
        field_paths = [f["path"] for f in schema["fields"]]
        assert "ext.section_number" in field_paths
        assert "text" in field_paths
        assert "summary" in field_paths   # autofill field

        # Check tiers
        anchor_fields   = [f for f in schema["fields"] if f["tier"] == "anchor"]
        autofill_fields = [f for f in schema["fields"] if f["tier"] == "autofill"]
        assert len(anchor_fields) >= 5
        assert len(autofill_fields) >= 8

    def test_unknown_chunk_type(self, auth_headers):
        resp = client.get("/chunks/schema/unknown_type", headers=auth_headers)
        assert resp.status_code == 404


class TestAutofill:
    def test_autofill_cgst_section(self, auth_headers):
        """
        Integration test — requires valid AWS credentials and Bedrock access.
        Skip in unit-test-only environments.
        """
        resp = client.post(
            "/chunks/autofill",
            headers=auth_headers,
            json={
                "chunk_type":  "cgst_section",
                "anchor_data": SAMPLE_CGST_SECTION["data"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()

        assert data["chunk_type"] == "cgst_section"
        assert len(data["fields"]) > 0
        assert data["latency_ms"] > 0

        # Check expected fields are returned
        field_paths = [f["path"] for f in data["fields"]]
        assert "summary" in field_paths
        assert "keywords" in field_paths
        assert "ext.provision_type" in field_paths

        # Summary should be a non-empty string
        summary_field = next(f for f in data["fields"] if f["path"] == "summary")
        assert isinstance(summary_field["value"], str)
        assert len(summary_field["value"]) > 20

        # Keywords should be a non-empty list
        kw_field = next((f for f in data["fields"] if f["path"] == "keywords"), None)
        if kw_field:
            assert isinstance(kw_field["value"], list)
            assert len(kw_field["value"]) >= 3


class TestSubmit:
    def test_submit_missing_anchor_field(self, auth_headers):
        bad_data = {
            "chunk_type": "cgst_section",
            "data": {
                "ext": {"section_number": "16"},
                # Missing section_title, chapter_number, chapter_title, text
            },
        }
        resp = client.post("/chunks/submit", headers=auth_headers, json=bad_data)
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert "missing_fields" in detail
        assert "text" in detail["missing_fields"]

    def test_submit_unknown_chunk_type(self, auth_headers):
        resp = client.post(
            "/chunks/submit",
            headers=auth_headers,
            json={"chunk_type": "not_a_real_type", "data": {}},
        )
        assert resp.status_code == 422   # Pydantic validator catches this

    def test_submit_queues_job(self, auth_headers):
        """
        Integration test — requires Qdrant + Redis + Celery worker running.
        """
        resp = client.post(
            "/chunks/submit",
            headers=auth_headers,
            json=SAMPLE_CGST_SECTION,
        )
        # May be 202 (queued) or 409 (duplicate from previous test run)
        assert resp.status_code in (202, 409)

        if resp.status_code == 202:
            data = resp.json()
            assert "job_id" in data
            assert "chunk_id" in data
            assert data["status"] == "queued"

    def test_submit_force_override_on_duplicate(self, auth_headers):
        """
        Submit same section twice — second submit with force_override=True.
        """
        # First submit
        resp1 = client.post("/chunks/submit", headers=auth_headers,
                            json=SAMPLE_CGST_SECTION)
        # First may succeed (202) or already exist (409)

        # Second submit with force_override
        override_body = {**SAMPLE_CGST_SECTION, "force_override": True}
        resp2 = client.post("/chunks/submit", headers=auth_headers,
                            json=override_body)
        assert resp2.status_code == 202


class TestJobPolling:
    def test_poll_nonexistent_job(self, auth_headers):
        resp = client.get("/jobs/nonexistent-job-id", headers=auth_headers)
        # Celery returns PENDING for unknown task IDs
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_poll_real_job(self, auth_headers):
        """Integration test — polls until job completes or times out."""
        resp = client.post(
            "/chunks/submit",
            headers=auth_headers,
            json={**SAMPLE_CGST_SECTION, "force_override": True},
        )
        if resp.status_code != 202:
            pytest.skip("Submit failed — skipping job poll test")

        job_id = resp.json()["job_id"]
        deadline = time.time() + 60  # 60s timeout

        while time.time() < deadline:
            poll = client.get(f"/jobs/{job_id}", headers=auth_headers)
            assert poll.status_code == 200
            status = poll.json()["status"]
            if status in ("success", "failed"):
                assert status == "success", f"Job failed: {poll.json().get('error')}"
                assert poll.json()["progress"] == 100
                return
            time.sleep(2)

        pytest.fail("Job did not complete within 60 seconds")