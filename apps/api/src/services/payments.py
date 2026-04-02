import razorpay
import logging
import uuid
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from apps.api.src.core.config import settings
from apps.api.src.db.models.base import PaymentTransaction, UserUsage, CreditPackage, Coupon, CreditLog, User
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.services.email import EmailService
from apps.api.src.services.invoice import InvoiceGenerator

logger = logging.getLogger(__name__)

# Initialize Razorpay client
client = razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))

async def create_razorpay_order(user_id: int, package_name: str, coupon_code: str | None, db: AsyncSession):
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
    
    if final_amount == 0:
        order_id = f"free_{uuid.uuid4().hex}"
        transaction = PaymentTransaction(
            user_id=user_id, order_id=order_id, payment_id="free_activation",
            amount=final_amount, package_id=package.id, credits_added=package.credits_added,
            coupon_id=coupon.id if coupon else None, discount_amount=discount_amount, status="completed"
        )
        db.add(transaction)
        
        res_u = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
        usage = res_u.scalars().first()
        if not usage:
            usage = UserUsage(user_id=user_id); db.add(usage); await db.flush()
            
        usage.draft_reply_balance += package.credits_added
        
        # Reset expiration to 1 year from now on purchase
        usage.credits_expire_at = datetime.now(timezone.utc) + timedelta(days=365)
        
        db.add(CreditLog(
            user_id=user_id, amount=package.credits_added, credit_type="draft",
            transaction_type="purchase", reference_id=order_id
        ))
        
        if coupon: coupon.current_uses += 1
        await db.commit()

        return {"status": "success", "is_free": True, "order_id": order_id}

    order_data = {"amount": final_amount, "currency": package.currency, "payment_capture": 1}
    try:
        razorpay_order = client.order.create(data=order_data)
        transaction = PaymentTransaction(
            user_id=user_id, order_id=razorpay_order['id'], amount=final_amount,
            package_id=package.id, credits_added=package.credits_added,
            coupon_id=coupon.id if coupon else None, discount_amount=discount_amount, status="pending"
        )
        db.add(transaction); await db.commit()
        return razorpay_order
    except Exception as e:
        logger.error(f"Error creating Razorpay order: {e}")
        raise

async def verify_payment(order_id: str, payment_id: str, signature: str, db: AsyncSession):
    try:
        client.utility.verify_payment_signature({
            'razorpay_order_id': order_id, 'razorpay_payment_id': payment_id, 'razorpay_signature': signature
        })
        
        res = await db.execute(select(PaymentTransaction).where(PaymentTransaction.order_id == order_id))
        transaction = res.scalars().first()
        if not transaction or transaction.status == "completed": return False
        
        transaction.payment_id = payment_id; transaction.status = "completed"
        
        res = await db.execute(select(UserUsage).where(UserUsage.user_id == transaction.user_id))
        usage = res.scalars().first()
        if not usage:
            usage = UserUsage(user_id=transaction.user_id); db.add(usage); await db.flush()
            
        usage.draft_reply_balance += transaction.credits_added
        
        # Reset expiration to 1 year from now on purchase
        usage.credits_expire_at = datetime.now(timezone.utc) + timedelta(days=365)
        db.add(CreditLog(
            user_id=transaction.user_id, amount=transaction.credits_added, credit_type="draft",
            transaction_type="purchase", reference_id=transaction.order_id
        ))
        
        if transaction.coupon_id:
            res_c = await db.execute(select(Coupon).where(Coupon.id == transaction.coupon_id))
            coupon = res_c.scalars().first()
            if coupon: coupon.current_uses += 1
        
        await db.commit()
        return True
    except Exception as e:
        logger.error(f"Payment verification failed: {e}")
        return False

async def send_invoice_background(order_id: str):
    """
    Background task to generate and send invoice.
    Uses a fresh DB session to ensure data is available.
    """
    async with AsyncSessionLocal() as db:
        try:
            res_full = await db.execute(
                select(PaymentTransaction)
                .options(joinedload(PaymentTransaction.user), joinedload(PaymentTransaction.package))
                .where(PaymentTransaction.order_id == order_id)
            )
            full_tx = res_full.scalars().first()
            if full_tx and full_tx.user:
                current_time = datetime.now(timezone.utc)
                
                transaction_info = {
                    "order_id": full_tx.order_id,
                    "payment_id": full_tx.payment_id or "N/A",
                    "date": full_tx.created_at or current_time,
                    "user_name": full_tx.user.full_name,
                    "user_email": full_tx.user.email,
                    "package_name": full_tx.package.title if full_tx.package else "Credit Pack",
                    "amount": full_tx.amount or 0,
                    "discount": full_tx.discount_amount or 0,
                    "credits": full_tx.credits_added or 0
                }
                
                pdf_bytes = InvoiceGenerator.generate_invoice_pdf(transaction_info)
                EmailService.send_invoice_email(
                    email=full_tx.user.email,
                    invoice_pdf=pdf_bytes,
                    order_id=full_tx.order_id,
                    amount=full_tx.amount or 0,
                    full_name=full_tx.user.full_name
                )
        except Exception as e:
            logger.error(f"Background invoice task failed for {order_id}: {e}")

async def validate_coupon_logic(coupon_code: str, package_name: str, db: AsyncSession):
    """
    Validates a coupon against a package and returns calculation details.
    """
    res_c = await db.execute(select(Coupon).where(Coupon.code == coupon_code, Coupon.is_active == True))
    coupon = res_c.scalars().first()
    if not coupon: raise ValueError("Invalid or inactive coupon")
    
    res_p = await db.execute(select(CreditPackage).where(CreditPackage.name == package_name))
    pkg = res_p.scalars().first()
    if not pkg: raise ValueError("Invalid package name")
    
    discount = 0
    if coupon.discount_type == 'fixed': discount = coupon.discount_value
    else: discount = int(pkg.amount * (coupon.discount_value / 100))
    
    return {"valid": True, "discount_amount": discount, "final_amount": max(0, pkg.amount - discount)}
