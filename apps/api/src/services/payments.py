import razorpay
import logging
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, func
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

async def get_welcome_package_settings(db: AsyncSession):
    """
    Retrieves the credits and validity settings from the default package.
    Falls back to hardcoded config settings if no default package is found.
    """
    res = await db.execute(select(CreditPackage).where(CreditPackage.is_default == True, CreditPackage.is_active == True))
    default_pkg = res.scalars().first()
    
    if default_pkg:
        return {
            "simple_credits": default_pkg.simple_credits,
            "draft_credits": default_pkg.draft_credits,
            "validity_days": default_pkg.validity_days
        }
    
    return {
        "simple_credits": settings.DEFAULT_SIMPLE_CREDITS,
        "draft_credits": settings.DEFAULT_DRAFT_CREDITS,
        "validity_days": settings.DEFAULT_VALIDITY_DAYS
    }

async def initialize_user_credits(user_id: int, db: AsyncSession):
    """
    Creates or updates UserUsage for a new user using the Welcome Package settings.
    """
    data = await get_welcome_package_settings(db)
    
    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    usage = res.scalars().first()
    
    if not usage:
        usage = UserUsage(user_id=user_id)
        db.add(usage)
        await db.flush()
        
    usage.simple_query_balance = data["simple_credits"]
    usage.draft_reply_balance = data["draft_credits"]
    usage.credits_expire_at = datetime.now(timezone.utc) + timedelta(days=data["validity_days"])
    
    return usage

async def assign_invoice_number(db: AsyncSession, transaction: PaymentTransaction):
    """
    Generates and assigns an invoice number following the TB/YY-YY/NNN pattern.
    Resets NNN for each financial year (April - March).
    """
    now = datetime.now(timezone.utc)
    # Financial Year Logic (India)
    if now.month >= 4:
        fy_start = now.year
        fy_end = now.year + 1
    else:
        fy_start = now.year - 1
        fy_end = now.year
    
    fy_str = f"{str(fy_start)[2:]}-{str(fy_end)[2:]}"
    prefix = f"TB/{fy_str}/"
    
    # Query count of completed transactions with same FY prefix
    res = await db.execute(
        select(func.count(PaymentTransaction.id))
        .where(PaymentTransaction.invoice_number.like(f"{prefix}%"))
    )
    count = res.scalar() or 0
    next_number = f"{prefix}{str(count + 1).zfill(3)}"
    
    transaction.invoice_number = next_number
    return next_number

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
            amount=final_amount, package_id=package.id, draft_credits_added=package.draft_credits,
            coupon_id=coupon.id if coupon else None, discount_amount=discount_amount, status="completed"
        )
        db.add(transaction)
        await db.flush() # Get id
        await assign_invoice_number(db, transaction)
        
        res_u = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
        usage = res_u.scalars().first()
        if not usage:
            usage = UserUsage(user_id=user_id); db.add(usage); await db.flush()
            
        usage.draft_reply_balance += package.draft_credits
        
        # Calculate expiration based on package validity (default 365)
        days = package.validity_days or 365
        usage.credits_expire_at = datetime.now(timezone.utc) + timedelta(days=days)
        
        db.add(CreditLog(
            user_id=user_id, amount=package.draft_credits, credit_type="draft",
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
            package_id=package.id, draft_credits_added=package.draft_credits,
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
        
        res = await db.execute(
            select(PaymentTransaction)
            .options(joinedload(PaymentTransaction.package))
            .where(PaymentTransaction.order_id == order_id)
        )
        transaction = res.scalars().first()
        if not transaction or transaction.status == "completed": return False
        
        transaction.payment_id = payment_id; transaction.status = "completed"
        await assign_invoice_number(db, transaction)
        
        res = await db.execute(select(UserUsage).where(UserUsage.user_id == transaction.user_id))
        usage = res.scalars().first()
        if not usage:
            usage = UserUsage(user_id=transaction.user_id); db.add(usage); await db.flush()
            
        usage.draft_reply_balance += transaction.draft_credits_added
        
        # Calculate expiration based on package validity (default 365)
        days = 365
        if transaction.package:
            days = transaction.package.validity_days or 365
            
        usage.credits_expire_at = datetime.now(timezone.utc) + timedelta(days=days)
        db.add(CreditLog(
            user_id=transaction.user_id, amount=transaction.draft_credits_added, credit_type="draft",
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
                    "invoice_number": full_tx.invoice_number or full_tx.order_id, # Fallback
                    "payment_id": full_tx.payment_id or "N/A",
                    "date": full_tx.created_at or current_time,
                    "user_name": full_tx.user.full_name,
                    "user_email": full_tx.user.email,
                    "package_name": full_tx.package.title if full_tx.package else "Credit Pack",
                    "amount": full_tx.amount or 0,
                    "discount": full_tx.discount_amount or 0,
                    "credits": full_tx.draft_credits_added or 0
                }
                
                pdf_bytes = InvoiceGenerator.generate_invoice_pdf(transaction_info)
                EmailService.send_invoice_email(
                    email=full_tx.user.email,
                    invoice_pdf=pdf_bytes,
                    order_id=full_tx.order_id,
                    invoice_num=full_tx.invoice_number,
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
    
    now = datetime.now(timezone.utc)
    if coupon.valid_from and coupon.valid_from > now:
        raise ValueError("Coupon is not valid yet")
    if coupon.valid_until and coupon.valid_until < now:
        raise ValueError("Coupon has expired")
    if coupon.max_uses and coupon.current_uses >= coupon.max_uses:
        raise ValueError("Coupon maximum usage limit reached")
    
    res_p = await db.execute(select(CreditPackage).where(CreditPackage.name == package_name, CreditPackage.is_active == True))
    pkg = res_p.scalars().first()
    if not pkg: raise ValueError("Invalid package name")
    
    discount = 0
    if coupon.discount_type == 'fixed': discount = coupon.discount_value
    else: discount = int(pkg.amount * (coupon.discount_value / 100))
    
    return {"valid": True, "discount_amount": discount, "final_amount": max(0, pkg.amount - discount)}
