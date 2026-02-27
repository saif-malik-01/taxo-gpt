from sqlalchemy import Column, Integer, BigInteger, String, Boolean, ForeignKey, DateTime, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy import func
from services.database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String, nullable=True)
    email = Column(String, unique=True, index=True, nullable=False)
    mobile_number = Column(String, nullable=True)
    country = Column(String, nullable=True)
    password_hash = Column(String, nullable=True)
    google_id = Column(String, unique=True, index=True, nullable=True)
    facebook_id = Column(String, unique=True, index=True, nullable=True)
    role = Column(String, default="user")
    max_sessions = Column(Integer, default=1)  # Dynamic session limit
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    profile = relationship("UserProfile", back_populates="user", uselist=False)
    sessions = relationship("ChatSession", back_populates="user")


class UserProfile(Base):
    __tablename__ = "user_profiles"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    dynamic_summary = Column(Text, nullable=True) # AI-generated summary of user
    preferences = Column(JSON, default={}) # e.g. {"language": "en"}
    
    user = relationship("User", back_populates="profile")


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id = Column(String, primary_key=True, index=True) # UUID
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    title = Column(String, nullable=True)
    session_type = Column(String, default="simple") # 'simple' or 'draft'
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="sessions")
    messages = relationship("ChatMessage", back_populates="session", cascade="all, delete-orphan")
    shared_links = relationship("SharedSession", back_populates="session", cascade="all, delete-orphan")


class UserUsage(Base):
    __tablename__ = "user_usage"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    
    # Balances (What is left to spend)
    # Using -1 or a very high number for "Unlimited"
    simple_query_balance = Column(Integer, default=1000000) 
    draft_reply_balance = Column(Integer, default=3)
    
    # Lifetime usage (For analytics)
    simple_query_used = Column(Integer, default=0)
    draft_reply_used = Column(Integer, default=0)
    
    last_updated = Column(DateTime(timezone=True), onupdate=func.now())
    
    user = relationship("User")


class CreditPackage(Base):
    __tablename__ = "credit_packages"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True) # slug e.g. "draft-20"
    title = Column(String)
    description = Column(Text, nullable=True)
    amount = Column(Integer) # In paise
    currency = Column(String, default="INR")
    credits_added = Column(Integer)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class PaymentTransaction(Base):
    __tablename__ = "payment_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    order_id = Column(String, unique=True, index=True) # Razorpay Order ID
    payment_id = Column(String, nullable=True) # Razorpay Payment ID
    amount = Column(Integer) # In paise/cents
    currency = Column(String, default="INR")
    
    package_id = Column(Integer, ForeignKey("credit_packages.id"), nullable=True)
    credits_added = Column(Integer)
    
    coupon_id = Column(Integer, ForeignKey("coupons.id"), nullable=True)
    discount_amount = Column(Integer, default=0) # In paise
    
    status = Column(String, default="pending") # pending, completed, failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User")
    package = relationship("CreditPackage")
    coupon = relationship("Coupon")


class Coupon(Base):
    __tablename__ = "coupons"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True, nullable=False)
    discount_type = Column(String, nullable=False) # 'percentage' or 'fixed'
    discount_value = Column(Integer, nullable=False) # In paise or 0-100 percentage
    max_uses = Column(Integer, nullable=True)
    current_uses = Column(Integer, default=0)
    valid_from = Column(DateTime(timezone=True), nullable=True)
    valid_until = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(BigInteger, primary_key=True, index=True) # Changed to BigInteger
    session_id = Column(String, ForeignKey("chat_sessions.id"), nullable=False)
    role = Column(String, nullable=False) # 'user' or 'assistant'
    content = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

    session = relationship("ChatSession", back_populates="messages")
    feedback = relationship("Feedback", back_populates="message", uselist=False, cascade="all, delete-orphan")


class Feedback(Base):
    __tablename__ = "feedback"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(BigInteger, ForeignKey("chat_messages.id"), unique=True, nullable=False) # Changed to BigInteger
    rating = Column(Integer, nullable=False) # 1-5 or -1/1
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    message = relationship("ChatMessage", back_populates="feedback")


class SharedSession(Base):
    __tablename__ = "shared_sessions"

    id = Column(String, primary_key=True, index=True) # Obfuscated ID (e.g., short UUID or random string)
    session_id = Column(String, ForeignKey("chat_sessions.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=True)

    session = relationship("ChatSession", back_populates="shared_links")


class CreditLog(Base):
    __tablename__ = "credit_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    amount = Column(Integer, nullable=False) # Positive for credit, negative for debit
    credit_type = Column(String, nullable=False) # 'draft' or 'simple'
    transaction_type = Column(String, nullable=False) # 'purchase', 'usage', 'admin_adjustment'
    
    reference_id = Column(String, nullable=True) # e.g. order_id or session_id
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User")

