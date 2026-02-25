import razorpay
from api.config import settings
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from services.models import PaymentTransaction, UserUsage, CreditPackage
import logging

logger = logging.getLogger(__name__)

# Initialize Razorpay client
client = razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))

async def create_razorpay_order(user_id: int, package_name: str, db: AsyncSession):
    # Fetch package from DB
    res = await db.execute(select(CreditPackage).where(CreditPackage.name == package_name, CreditPackage.is_active == True))
    package = res.scalars().first()
    
    if not package:
        raise ValueError("Invalid or inactive package name")
    
    order_data = {
        "amount": package.amount,
        "currency": package.currency,
        "payment_capture": 1 # Auto capture
    }
    
    try:
        razorpay_order = client.order.create(data=order_data)
        
        # Save to DB
        transaction = PaymentTransaction(
            user_id=user_id,
            order_id=razorpay_order['id'],
            amount=package.amount,
            package_id=package.id,
            credits_added=package.credits_added,
            status="pending"
        )
        db.add(transaction)
        await db.commit()
        
        return razorpay_order
    except Exception as e:
        logger.error(f"Error creating Razorpay order: {e}")
        raise

async def verify_payment(order_id: str, payment_id: str, signature: str, db: AsyncSession):
    try:
        # Verify signature
        client.utility.verify_payment_signature({
            'razorpay_order_id': order_id,
            'razorpay_payment_id': payment_id,
            'razorpay_signature': signature
        })
        
        # Update transaction
        res = await db.execute(select(PaymentTransaction).where(PaymentTransaction.order_id == order_id))
        transaction = res.scalars().first()
        
        if not transaction or transaction.status == "completed":
            return False
        
        transaction.payment_id = payment_id
        transaction.status = "completed"
        
        # Add credits to user wallet
        res = await db.execute(select(UserUsage).where(UserUsage.user_id == transaction.user_id))
        usage = res.scalars().first()
        
        if not usage:
            usage = UserUsage(user_id=transaction.user_id)
            db.add(usage)
            await db.flush()
            
        usage.draft_reply_balance += transaction.credits_added
        
        await db.commit()
        return True
    except Exception as e:
        logger.error(f"Payment verification failed: {e}")
        return False
