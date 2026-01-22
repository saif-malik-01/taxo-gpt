from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.auth.jwt import create_access_token

router = APIRouter(prefix="/auth", tags=["Auth"])

USERS = {
    "admin@gst.com": {
        "password": "admin123",
        "role": "admin"
    },
    "user@gst.com": {
        "password": "user123",
        "role": "user"
    }
}


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest):
    user = USERS.get(payload.email)

    if not user or user["password"] != payload.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({
        "sub": payload.email,
        "role": user["role"]
    })

    return {"access_token": token}
