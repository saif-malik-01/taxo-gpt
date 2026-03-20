from pydantic import BaseModel
from typing import Optional

class ProfileUpdate(BaseModel):
    dynamic_summary: Optional[str] = None
    preferences: Optional[dict] = None
