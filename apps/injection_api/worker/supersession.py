"""
ingestion_api/worker/supersession.py

SupersessionEngine — detects when a new chunk supersedes an existing one
and applies the necessary Qdrant payload updates.

Currently implemented:
  - cgst_section / igst_section: "amendment" supersession
    If a new section chunk has the same section_number as an existing one,
    the old chunk's temporal.is_current is flipped to False.

Wired for future types:
  - notification:  amends_notification → flip old notification is_current,
                   cascade to hsn_code/sac_code rate_notification refs
  - judgment:      overruled_by → flip old judgment current_status
  - circular:      same circular_number → flip old circular is_current
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from utils.logger import get_logger

logger = get_logger("supersession")


class SupersessionEngine:

    def __init__(self, qdrant_manager):
        """
        qdrant_manager: instance of models.qdrant_manager.QdrantManager
        The engine uses search_by_payload() and update_payload() —
        both must be present on the manager.
        """
        self.qdrant = qdrant_manager

    def check_and_apply(
        self,
        chunk: dict[str, Any],
        spec:  dict[str, Any],
    ) -> list[dict]:
        """
        Main entry point. Called BEFORE the new chunk is upserted to Qdrant.

        Returns a list of supersession log entries — one per affected chunk.
        Empty list means nothing was superseded.

        Each log entry:
          {
            "action":           "flipped_is_current" | "flipped_current_status" | ...,
            "affected_chunk_id": str,
            "affected_type":    str,
            "field_updated":    str,
            "old_value":        Any,
            "new_value":        Any,
            "reason":           str,
          }
        """
        sup_config = spec.get("supersession_check", {})
        if not sup_config.get("enabled", False):
            return []

        sup_type = sup_config.get("type")
        log = []

        if sup_type == "amendment":
            log += self._handle_amendment(chunk, spec, sup_config)
        elif sup_type == "notification_supersession":
            log += self._handle_notification(chunk, spec, sup_config)
        elif sup_type == "judgment_overrule":
            log += self._handle_judgment(chunk, spec, sup_config)
        elif sup_type == "circular_supersession":
            log += self._handle_circular(chunk, spec, sup_config)

        return log

    # ── Amendment (cgst_section, igst_section) ────────────────────────────────

    def _handle_amendment(
        self,
        new_chunk:  dict,
        spec:       dict,
        sup_config: dict,
    ) -> list[dict]:
        """
        If a chunk with the same section_number already exists and is current,
        flip temporal.is_current = False on the old one and record the amendment.
        """
        match_field = sup_config.get("match_field", "ext.section_number")
        match_value = _get_nested(new_chunk, match_field)

        if not match_value:
            logger.debug("Amendment check: match_field value is empty — skipping.")
            return []

        # Search Qdrant for existing chunk with same section_number that is current
        existing_chunks = self.qdrant.search_by_payload(
            filters={
                "must": [
                    {"key": match_field,           "match": {"value": str(match_value)}},
                    {"key": "temporal.is_current", "match": {"value": True}},
                    {"key": "chunk_type",          "match": {"value": new_chunk.get("chunk_type")}},
                ]
            },
            limit=5,  # should be 1 but handle data quality issues
        )

        if not existing_chunks:
            logger.debug(
                f"Amendment check: no existing current chunk for "
                f"{match_field}='{match_value}' — treating as new."
            )
            return []

        log = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for old_chunk in existing_chunks:
            old_id = old_chunk.get("id")
            if not old_id:
                logger.warning("Found existing chunk with no id — skipping.")
                continue

            # Flip is_current + record what superseded it
            patch = {
                "temporal": {
                    "is_current":       False,
                    "superseded_date":  now_iso,
                },
                "legal_status": {
                    "current_status": "modified",
                },
                "_superseded_by_chunk_id": new_chunk.get("id"),
            }

            success = self.qdrant.update_payload(chunk_id=old_id, payload_patch=patch)

            if success:
                logger.info(
                    f"  Supersession: flipped {old_id} is_current=False "
                    f"(amended by {new_chunk.get('id')})"
                )
                log.append({
                    "action":            "flipped_is_current",
                    "affected_chunk_id": old_id,
                    "affected_type":     old_chunk.get("chunk_type"),
                    "field_updated":     "temporal.is_current",
                    "old_value":         True,
                    "new_value":         False,
                    "reason": (
                        f"New {new_chunk.get('chunk_type')} submitted for "
                        f"{match_field}='{match_value}' — old version marked superseded."
                    ),
                })
            else:
                logger.error(f"  Supersession update FAILED for chunk {old_id}")

        return log

    # ── Notification supersession (stub — implement when building notification type) ──

    def _handle_notification(
        self,
        new_chunk:  dict,
        spec:       dict,
        sup_config: dict,
    ) -> list[dict]:
        """
        Logic for Notifications:
        1. Rescission: If ext.rescinds_notification is set, flip ALL chunks of that notif.
        2. Amendment (Row-level): If amends_notification + chunk_subtype=rate_entry + Serial Number.
        3. Simple Amendment: If same notification_number (Full replacement).
        """
        notif_num = _get_nested(new_chunk, "ext.notification_number")
        if not notif_num:
            return []

        log = []
        now_iso = datetime.now(timezone.utc).isoformat()

        # Case 4: Full Rescission
        rescinds = _get_nested(new_chunk, "ext.rescinds_notification")
        if rescinds:
            # Clean up number (e.g. 5/2017 -> 5/2017)
            old_chunks = self.qdrant.search_by_payload(
                filters={
                    "must": [
                        {"key": "ext.notification_number", "match": {"value": str(rescinds)}},
                        {"key": "chunk_type",          "match": {"value": "notification"}},
                        {"key": "temporal.is_current", "match": {"value": True}},
                    ]
                },
                limit=100, # notifications usually don't have >100 chunks
            )
            for old in old_chunks:
                old_id = old.get("id")
                patch = {
                    "temporal": {
                        "is_current":       False,
                        "superseded_date":  now_iso,
                    },
                    "legal_status": {
                        "current_status": "rescinded",
                    },
                    "_superseded_by_chunk_id": new_chunk.get("id"),
                }
                if self.qdrant.update_payload(old_id, patch):
                    log.append({
                        "action":            "rescinded_notification",
                        "affected_chunk_id": old_id,
                        "affected_type":     "notification",
                        "reason":            f"Notification rescinded by {new_chunk.get('id')}",
                    })

        # Case 3: Row-level Substitution
        amends = _get_nested(new_chunk, "ext.amends_notification")
        subtype = _get_nested(new_chunk, "ext.chunk_subtype")
        sl_no = _get_nested(new_chunk, "ext.row_data.Serial Number")

        if amends and subtype == "rate_entry" and sl_no:
            # Find the specific row in the parent notification
            match_row = self.qdrant.search_by_payload(
                filters={
                    "must": [
                        {"key": "ext.notification_number",     "match": {"value": str(amends)}},
                        {"key": "temporal.is_current",          "match": {"value": True}},
                        {"key": "ext.row_data.Serial Number",  "match": {"value": str(sl_no)}},
                    ]
                },
                limit=5
            )
            for old in match_row:
                old_id = old.get("id")
                patch = {
                    "temporal": {
                        "is_current":       False,
                        "superseded_date":  now_iso,
                    },
                    "legal_status": {
                        "current_status": "substituted",
                    },
                    "_superseded_by_chunk_id": new_chunk.get("id"),
                }
                if self.qdrant.update_payload(old_id, patch):
                    log.append({
                        "action":            "substituted_rate_row",
                        "affected_chunk_id": old_id,
                        "reason":            f"Rate entry substituted by {new_chunk.get('id')}",
                    })

        # Case 1: Simple Amendment (Full Replacement if same number and not already handled)
        # We only do direct replacement if it's NOT explicitly amending a DIFFERENT notification
        if not amends:
            log += self._handle_amendment(new_chunk, spec, {
                **sup_config,
                "match_field": "ext.notification_number",
            })

        return log

    # ── Judgment overruling (stub — implement when building judgment type) ─────

    def _handle_judgment(
        self,
        new_chunk:  dict,
        spec:       dict,
        sup_config: dict,
    ) -> list[dict]:
        """
        Stub. Implement when building judgment type.
        """
        logger.debug("Judgment overrule check — not yet implemented.")
        return []

    # ── Circular supersession (Replacement + Corrigenda) ──────────────────────

    def _handle_circular(
        self,
        new_chunk:  dict,
        spec:       dict,
        sup_config: dict,
    ) -> list[dict]:
        """
        Logic for Circulars:
        1. If it's a new version of same circular_number → Flip all old is_current=False.
        2. If it's a Corrigendum → Patch parent chunks with is_disputed=True + note.
        """
        circ_num = _get_nested(new_chunk, "ext.circular_number")
        if not circ_num:
            return []

        # Check for Case 3: Corrigendum
        subject = new_chunk.get("ext", {}).get("subject", "").lower()
        is_corrigendum = "corrigendum" in subject

        log = []
        now_iso = datetime.now(timezone.utc).isoformat()

        if is_corrigendum:
            # Case 3: Corrigendum — Patch the parents referenced in cross-references
            parents = new_chunk.get("cross_references", {}).get("circulars", [])
            for parent_num in parents:
                existing = self.qdrant.search_by_payload(
                    filters={
                        "must": [
                            {"key": "ext.circular_number", "match": {"value": str(parent_num)}},
                            {"key": "chunk_type",          "match": {"value": "circular"}},
                            {"key": "temporal.is_current", "match": {"value": True}},
                        ]
                    },
                    limit=20,
                )
                for old_chunk in existing:
                    old_id = old_chunk.get("id")
                    patch = {
                        "legal_status": {
                            "is_disputed":  True,
                            "dispute_note": f"Corrected by Corrigendum dated {new_chunk.get('ext', {}).get('circular_date')}",
                        }
                    }
                    if self.qdrant.update_payload(old_id, patch):
                        log.append({
                            "action":            "patched_with_corrigendum",
                            "affected_chunk_id": old_id,
                            "affected_type":     "circular",
                            "reason":            f"Marked as disputed due to corrigendum in {new_chunk.get('id')}",
                        })
            return log

        # Case 2: Full Replacement (same circular_number)
        return self._handle_amendment(new_chunk, spec, {
            "match_field": "ext.circular_number",
        })


# ── Utility ───────────────────────────────────────────────────────────────────

def _get_nested(d: dict, path: str, default=None):
    """Read from dot-notation path."""
    keys = path.split(".")
    cursor = d
    for k in keys:
        if not isinstance(cursor, dict) or k not in cursor:
            return default
        cursor = cursor[k]
    return cursor