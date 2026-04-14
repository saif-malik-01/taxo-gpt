from fastapi import APIRouter, Depends, HTTPException
from apps.api.src.services.auth.deps import auth_guard
from apps.api.src.services.gst import GSTService
from apps.api.src.schemas.gst import GSTVerifySchema, GSTResponseSchema

router = APIRouter(prefix="/gst", tags=["GST"])

@router.post("/verify", response_model=GSTResponseSchema)
async def verify_gstin(payload: GSTVerifySchema, user=Depends(auth_guard)):
    """
    Verify GSTIN and get user details for dashboard/invoicing.
    """
    details = await GSTService.verify_gstin(payload.gstin)
    if not details:
        raise HTTPException(status_code=400, detail="Unable to verify GSTIN or invalid GSTIN provided.")
    return details
