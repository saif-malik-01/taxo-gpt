import httpx
import logging
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

class GSTService:
    @staticmethod
    async def verify_gstin(gstin: str) -> dict | None:
        """
        Verify GSTIN using TaxoCredit API and return relevant details.
        """
        url = "https://taxocredit.com/api/verify-public-gst-number"
        headers = {
            "accept": "application/json",
            "apikey": settings.TAXO_API_KEY,
            "Content-Type": "application/json"
        }
        payload = {"gstin": gstin}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()

                if result.get("code") == 200 and "data" in result:
                    data = result["data"]
                    
                    # Extract relevant details
                    pradr_addr = data.get("pradr", {}).get("addr", {})
                    
                    # Format address
                    addr_parts = [
                        pradr_addr.get("bno"),
                        pradr_addr.get("bnm"),
                        pradr_addr.get("loc"),
                        pradr_addr.get("st"),
                        pradr_addr.get("locality"),
                        pradr_addr.get("dst"),
                        pradr_addr.get("stcd"),
                        pradr_addr.get("pncd")
                    ]
                    formatted_address = ", ".join([p for p in addr_parts if p and str(p).strip()])

                    return {
                        "legal_name": data.get("lgnm"),
                        "trade_name": data.get("tradeNam"),
                        "status": data.get("sts"),
                        "address": formatted_address,
                        "state": pradr_addr.get("stcd"),
                        "pincode": pradr_addr.get("pncd"),
                        "gstin": data.get("gstin")
                    }
                else:
                    logger.warning(f"GST verification failed for {gstin}: {result.get('response')}")
                    return None

        except Exception as e:
            logger.error(f"Error verifying GSTIN {gstin}: {e}")
            return None
