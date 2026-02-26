import razorpay
from api.config import settings
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from services.models import PaymentTransaction, UserUsage, CreditPackage, Coupon, CreditLog
import logging
from datetime import datetime, timezone
import uuid
import logging

logger = logging.getLogger(__name__)

# Initialize Razorpay client
client = razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))

async def create_razorpay_order(user_id: int, package_name: str, coupon_code: str | None, db: AsyncSession):
    # Fetch package from DB
    res = await db.execute(select(CreditPackage).where(CreditPackage.name == package_name, CreditPackage.is_active == True))
    package = res.scalars().first()
    
    if not package:
        raise ValueError("Invalid or inactive package name")
        
    final_amount = package.amount
    discount_amount = 0
    coupon = None
    
    if coupon_code:
        res_c = await db.execute(select(Coupon).where(Coupon.code == coupon_code, Coupon.is_active == True))
        coupon = res_c.scalars().first()
        
        if not coupon:
            raise ValueError("Invalid coupon code")
            
        now = datetime.now(timezone.utc)
        if coupon.valid_from and coupon.valid_from > now:
            raise ValueError("Coupon is not valid yet")
        if coupon.valid_until and coupon.valid_until < now:
            raise ValueError("Coupon has expired")
        if coupon.max_uses and coupon.current_uses >= coupon.max_uses:
            raise ValueError("Coupon maximum usage limit reached")
            
        if coupon.discount_type == 'fixed':
            discount_amount = coupon.discount_value
        elif coupon.discount_type == 'percentage':
            discount_amount = int(package.amount * (coupon.discount_value / 100))
            
        final_amount = max(0, package.amount - discount_amount)
    
    # 100% Discount / Free Case
    if final_amount == 0:
        # Bypass Razorpay
        order_id = f"free_{uuid.uuid4().hex}"
        transaction = PaymentTransaction(
            user_id=user_id,
            order_id=order_id,
            payment_id="free_activation",
            amount=final_amount,
            package_id=package.id,
            credits_added=package.credits_added,
            coupon_id=coupon.id if coupon else None,
            discount_amount=discount_amount,
            status="completed"
        )
        db.add(transaction)
        
        # Add credits immediately
        res_u = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
        usage = res_u.scalars().first()
        
        if not usage:
            usage = UserUsage(user_id=user_id)
            db.add(usage)
            await db.flush()
            
        usage.draft_reply_balance += package.credits_added
        
        # Log credit addition
        log = CreditLog(
            user_id=user_id,
            amount=package.credits_added,
            credit_type="draft",
            transaction_type="purchase",
            reference_id=order_id
        )
        db.add(log)
        
        if coupon:
            coupon.current_uses += 1
            
        await db.commit()
        return {"status": "success", "is_free": True, "order_id": order_id}

    # Standard Razorpay Flow
    order_data = {
        "amount": final_amount,
        "currency": package.currency,
        "payment_capture": 1 # Auto capture
    }
    
    try:
        razorpay_order = client.order.create(data=order_data)
        
        # Save to DB
        transaction = PaymentTransaction(
            user_id=user_id,
            order_id=razorpay_order['id'],
            amount=final_amount,
            package_id=package.id,
            credits_added=package.credits_added,
            coupon_id=coupon.id if coupon else None,
            discount_amount=discount_amount,
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
        
        # Log credit addition
        log = CreditLog(
            user_id=transaction.user_id,
            amount=transaction.credits_added,
            credit_type="draft",
            transaction_type="purchase",
            reference_id=transaction.order_id
        )
        db.add(log)
        
        # If a coupon was used, increment usage
        if transaction.coupon_id:
            res_c = await db.execute(select(Coupon).where(Coupon.id == transaction.coupon_id))
            coupon = res_c.scalars().first()
            if coupon:
                coupon.current_uses += 1
        
        await db.commit()
        return True
    except Exception as e:
        logger.error(f"Payment verification failed: {e}")
        return False
