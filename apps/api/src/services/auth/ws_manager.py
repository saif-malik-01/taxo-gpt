import logging
from typing import Dict, List
from fastapi import WebSocket

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self):
        # session_id -> List of active WebSocket connections
        self.active_sessions: Dict[str, List[WebSocket]] = {}
        # temp_uid -> email (for verification bridge)
        self.verification_pending: Dict[str, List[WebSocket]] = {}

    async def connect_session(self, session_id: str, websocket: WebSocket):
        await websocket.accept()
        if session_id not in self.active_sessions:
            self.active_sessions[session_id] = []
        self.active_sessions[session_id].append(websocket)
        logger.info(f"WS: Session connected {session_id}")

    def disconnect_session(self, session_id: str, websocket: WebSocket):
        if session_id in self.active_sessions:
            if websocket in self.active_sessions[session_id]:
                self.active_sessions[session_id].remove(websocket)
            if not self.active_sessions[session_id]:
                del self.active_sessions[session_id]
        logger.info(f"WS: Session disconnected {session_id}")

    async def notify_session(self, session_id: str, message: dict):
        """Notify all active tabs of a single session (e.g., 'KICKED')"""
        if session_id in self.active_sessions:
            for connection in self.active_sessions[session_id]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.error(f"WS: Failed to notify session {session_id}: {e}")

    # --- Verification Bridge Logic ---

    async def connect_verification(self, temp_uid: str, websocket: WebSocket):
        await websocket.accept()
        if temp_uid not in self.verification_pending:
            self.verification_pending[temp_uid] = []
        self.verification_pending[temp_uid].append(websocket)
        logger.info(f"WS: Verification tab waiting {temp_uid}")

    def disconnect_verification(self, temp_uid: str, websocket: WebSocket):
        if temp_uid in self.verification_pending:
            if websocket in self.verification_pending[temp_uid]:
                self.verification_pending[temp_uid].remove(websocket)
            if not self.verification_pending[temp_uid]:
                del self.verification_pending[temp_uid]

    async def broadcast_verification(self, temp_uid: str, message: dict):
        """Send verification success to the waiting original tab"""
        if temp_uid in self.verification_pending:
            for connection in self.verification_pending[temp_uid]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.error(f"WS: Failed to broadcast verification {temp_uid}: {e}")

manager = ConnectionManager()
