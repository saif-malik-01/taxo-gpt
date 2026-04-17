import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import (
    User, ChatSession, ChatMessage, UserUsage, PaymentTransaction
)
from apps.api.src.services.email import EmailService
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

RECIPIENT_EMAIL = settings.FEEDBACK_RECIPIENT_EMAIL


async def get_daily_mis_data():
    """Fetch all MIS metrics for the last 24 hours + cumulative totals."""
    async with AsyncSessionLocal() as session:
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)

        # ── 1. Growth ─────────────────────────────────────────────────────────

        new_signups = (await session.execute(
            select(func.count(User.id)).where(User.created_at >= yesterday)
        )).scalar() or 0

        total_users = (await session.execute(
            select(func.count(User.id))
        )).scalar() or 0

        verified_count = (await session.execute(
            select(func.count(User.id)).where(User.is_verified == True)
        )).scalar() or 0

        unverified_count = total_users - verified_count

        # ── 2. Active Users (24h) ─────────────────────────────────────────────
        # Unique users who sent at least 1 message in the last 24h

        active_users_24h = (await session.execute(
            select(func.count(func.distinct(ChatSession.user_id)))
            .join(ChatMessage, ChatMessage.session_id == ChatSession.id)
            .where(ChatMessage.timestamp >= yesterday)
        )).scalar() or 0

        # ── 3. Sessions ───────────────────────────────────────────────────────

        query_sessions_24h = (await session.execute(
            select(func.count(ChatSession.id))
            .where(ChatSession.session_type == "simple", ChatSession.created_at >= yesterday)
        )).scalar() or 0

        draft_sessions_24h = (await session.execute(
            select(func.count(ChatSession.id))
            .where(ChatSession.session_type == "draft", ChatSession.created_at >= yesterday)
        )).scalar() or 0

        total_query_sessions = (await session.execute(
            select(func.count(ChatSession.id)).where(ChatSession.session_type == "simple")
        )).scalar() or 0

        total_draft_sessions = (await session.execute(
            select(func.count(ChatSession.id)).where(ChatSession.session_type == "draft")
        )).scalar() or 0

        # ── 4. Avg messages per session (24h sessions only) ───────────────────

        sessions_24h_ids = (await session.execute(
            select(ChatSession.id).where(ChatSession.created_at >= yesterday)
        )).scalars().all()

        avg_msgs_per_session = 0.0
        if sessions_24h_ids:
            total_msgs_in_24h_sessions = (await session.execute(
                select(func.count(ChatMessage.id))
                .where(ChatMessage.session_id.in_(sessions_24h_ids))
            )).scalar() or 0
            avg_msgs_per_session = total_msgs_in_24h_sessions / len(sessions_24h_ids)

        # ── 5. Revenue ────────────────────────────────────────────────────────

        payments_24h_res = (await session.execute(
            select(
                func.count(PaymentTransaction.id),
                func.coalesce(func.sum(PaymentTransaction.amount), 0)
            )
            .where(
                PaymentTransaction.status == "completed",
                PaymentTransaction.created_at >= yesterday,
                PaymentTransaction.amount > 0  # exclude free orders
            )
        )).first()
        payments_count_24h = payments_24h_res[0] or 0
        revenue_24h_paise = payments_24h_res[1] or 0

        total_revenue_res = (await session.execute(
            select(func.coalesce(func.sum(PaymentTransaction.amount), 0))
            .where(
                PaymentTransaction.status == "completed",
                PaymentTransaction.amount > 0
            )
        )).scalar() or 0

        # ── 6. Credit Health ─────────────────────────────────────────────────

        users_zero_query = (await session.execute(
            select(func.count(UserUsage.id))
            .where(UserUsage.simple_query_balance == 0)
        )).scalar() or 0

        users_zero_draft = (await session.execute(
            select(func.count(UserUsage.id))
            .where(UserUsage.draft_reply_balance == 0)
        )).scalar() or 0

        users_expired = (await session.execute(
            select(func.count(UserUsage.id))
            .where(
                UserUsage.credits_expire_at.isnot(None),
                UserUsage.credits_expire_at < now
            )
        )).scalar() or 0

        return {
            "timestamp": now,
            # Growth
            "new_signups": new_signups,
            "total_users": total_users,
            "verified_count": verified_count,
            "unverified_count": unverified_count,
            # Engagement
            "active_users_24h": active_users_24h,
            "query_sessions_24h": query_sessions_24h,
            "draft_sessions_24h": draft_sessions_24h,
            "total_query_sessions": total_query_sessions,
            "total_draft_sessions": total_draft_sessions,
            "avg_msgs_per_session": round(avg_msgs_per_session, 1),
            # Revenue
            "payments_count_24h": payments_count_24h,
            "revenue_24h_inr": revenue_24h_paise / 100,
            "total_revenue_inr": total_revenue_res / 100,
            # Credit Health
            "users_zero_query": users_zero_query,
            "users_zero_draft": users_zero_draft,
            "users_expired": users_expired,
        }


def _row(label: str, value: str, highlight: bool = False) -> str:
    """Single data row — fully inline CSS for all email clients."""
    bg         = "#fff7ed" if highlight else "#f8fafc"
    border_clr = "#fed7aa" if highlight else "#e2e8f0"
    val_color  = "#ea580c" if highlight else "#1e293b"
    return (
        f'<tr>'
        f'<td width="65%" style="padding:11px 16px;background-color:{bg};'
        f'border-bottom:1px solid {border_clr};border-right:1px solid {border_clr};'
        f'color:#475569;font-size:14px;font-family:Arial,Helvetica,sans-serif;">'
        f'{label}</td>'
        f'<td width="35%" align="right" style="padding:11px 16px;background-color:{bg};'
        f'border-bottom:1px solid {border_clr};'
        f'color:{val_color};font-size:14px;font-weight:bold;'
        f'font-family:Arial,Helvetica,sans-serif;">'
        f'{value}</td>'
        f'</tr>'
    )


def _section(title: str, rows_html: str) -> str:
    """Section block with a dark header and data table — inline CSS only."""
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="margin-bottom:20px;border-collapse:collapse;">'
        # Section header row
        f'<tr><td colspan="2" style="background-color:#1e293b;color:#f8fafc;'
        f'padding:10px 16px;font-size:12px;font-weight:bold;letter-spacing:1px;'
        f'text-transform:uppercase;font-family:Arial,Helvetica,sans-serif;">'
        f'{title}</td></tr>'
        # Data rows
        f'{rows_html}'
        f'</table>'
    )


def format_mis_html(data: dict) -> str:
    date_str = data["timestamp"].strftime("%B %d, %Y  %H:%M UTC")
    verified_pct = (
        round(data["verified_count"] / data["total_users"] * 100, 1)
        if data["total_users"] else 0
    )

    # ── Build row groups ──────────────────────────────────────────────────────

    growth_rows = (
        _row("New Signups (24h)",  str(data["new_signups"]))
        + _row("Total Users",      f"{data['total_users']:,}")
        + _row("Verified Users",   f"{data['verified_count']:,} ({verified_pct}%)")
        + _row("Unverified Users", str(data["unverified_count"]),
               highlight=data["unverified_count"] > 0)
    )

    engagement_rows = (
        _row("Active Users (24h)",               str(data["active_users_24h"]))
        + _row("Query Sessions (24h)",            str(data["query_sessions_24h"]))
        + _row("Draft Sessions (24h)",            str(data["draft_sessions_24h"]))
        + _row("Total Query Sessions (All Time)", f"{data['total_query_sessions']:,}")
        + _row("Total Draft Sessions (All Time)", f"{data['total_draft_sessions']:,}")
        + _row("Avg Messages / Session (24h)",    str(data["avg_msgs_per_session"]))
    )

    revenue_rows = (
        _row("Payments Received (24h)", str(data["payments_count_24h"]))
        + _row("Revenue (24h)",         f"INR {data['revenue_24h_inr']:,.2f}",
               highlight=data["revenue_24h_inr"] > 0)
        + _row("Total Revenue (All Time)", f"INR {data['total_revenue_inr']:,.2f}")
    )

    credit_rows = (
        _row("Users with 0 Query Credits",  str(data["users_zero_query"]),
             highlight=data["users_zero_query"] > 0)
        + _row("Users with 0 Draft Credits", str(data["users_zero_draft"]),
               highlight=data["users_zero_draft"] > 0)
        + _row("Users with Expired Credits", str(data["users_expired"]),
               highlight=data["users_expired"] > 0)
    )

    year = data["timestamp"].year

    # ── Full email — 100% inline CSS, table-based layout ─────────────────────
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Daily MIS Report</title>
</head>
<body style="margin:0;padding:0;background-color:#f1f5f9;">

  <!--[if mso]>
  <table width="100%" cellpadding="0" cellspacing="0"><tr><td>
  <![endif]-->

  <!-- Outer wrapper -->
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#f1f5f9;padding:32px 16px;">
    <tr>
      <td align="center">

        <!-- Inner container -->
        <table width="600" cellpadding="0" cellspacing="0" border="0"
               style="max-width:600px;width:100%;">

          <!-- ── HEADER ── -->
          <tr>
            <td align="center"
                style="background-color:#0f172a;padding:32px 24px;
                       border-radius:12px;margin-bottom:20px;">
              <div style="color:#fb923c;font-size:26px;font-weight:bold;
                          font-family:Arial,Helvetica,sans-serif;letter-spacing:-0.5px;">
                TaxoBuddy
              </div>
              <div style="color:#f8fafc;font-size:17px;font-weight:600;
                          font-family:Arial,Helvetica,sans-serif;margin-top:6px;">
                Daily MIS Report
              </div>
              <div style="color:#94a3b8;font-size:13px;
                          font-family:Arial,Helvetica,sans-serif;margin-top:4px;">
                {date_str}
              </div>
            </td>
          </tr>

          <!-- Spacer -->
          <tr><td height="20"></td></tr>

          <!-- ── SECTIONS ── -->
          <tr><td>{_section("&#x1F4C8; Growth", growth_rows)}</td></tr>
          <tr><td>{_section("&#x1F4AC; Engagement", engagement_rows)}</td></tr>
          <tr><td>{_section("&#x1F4B0; Revenue", revenue_rows)}</td></tr>
          <tr><td>{_section("&#x26A0;&#xFE0F; Credit Health", credit_rows)}</td></tr>

          <!-- ── FOOTER ── -->
          <tr>
            <td align="center"
                style="padding:16px;border-top:1px solid #e2e8f0;
                       color:#94a3b8;font-size:12px;
                       font-family:Arial,Helvetica,sans-serif;">
              &copy; {year} TaxoBuddy &middot; Automated Internal Report &middot; Do not reply
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>

  <!--[if mso]>
  </td></tr></table>
  <![endif]-->

</body>
</html>"""


async def send_daily_mis_report():
    try:
        logger.info("Starting Daily MIS Report Job")
        data = await get_daily_mis_data()
        subject = f"Daily MIS Report -- {data['timestamp'].strftime('%B %d, %Y')}"
        EmailService.send_email(subject, format_mis_html(data), RECIPIENT_EMAIL)
        logger.info("Daily MIS report sent successfully")
    except Exception as e:
        logger.error(f"Error in MIS job: {e}", exc_info=True)
