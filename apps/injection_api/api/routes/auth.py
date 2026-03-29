"""
ingestion_api/api/routes/auth.py

POST /auth/login  →  returns JWT access token
"""

from fastapi import APIRouter, HTTPException, status

from api.auth import authenticate_user, create_access_token
from api.req_models import LoginRequest, TokenResponse

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse, summary="Obtain JWT access token")
def login(body: LoginRequest) -> TokenResponse:
    """
    Authenticate with username + password.
    Returns a Bearer token valid for JWT_EXPIRE_MINUTES (default 8 hours).

    Use the token in all subsequent requests:
        Authorization: Bearer <token>
    """
    username = authenticate_user(body.username, body.password)
    if username is None:
        # Deliberately vague — don't reveal which field was wrong
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials.",
        )

    token, expires_in = create_access_token(subject=username)
    return TokenResponse(
        access_token=token,
        expires_in=expires_in,
    )