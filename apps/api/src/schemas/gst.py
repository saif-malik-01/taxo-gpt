from pydantic import BaseModel

class GSTVerifySchema(BaseModel):
    gstin: str

class GSTResponseSchema(BaseModel):
    gstin: str
    legal_name: str | None
    trade_name: str | None
    status: str | None
    address: str | None
    state: str | None
    pincode: str | None
