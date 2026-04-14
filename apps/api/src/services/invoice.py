from fpdf import FPDF
from datetime import datetime
import io

class InvoiceGenerator:
    @staticmethod
    def generate_invoice_pdf(transaction_data: dict) -> bytes:
        """
        Generate a professional PDF invoice using fpdf.
        transaction_data: {
            "order_id": "...",
            "payment_id": "...",
            "date": datetime,
            "user_name": "...",
            "user_email": "...",
            "package_name": "...",
            "amount": 10000, # in paise
            "discount": 500, # in paise
            "credits": 20
        }
        """
        pdf = FPDF()
        pdf.add_page()
        
        # --- Company Details (Seller) ---
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 10, "Astrazure E Ventures Pvt. Ltd.", ln=True)
        
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(60, 60, 60)
        pdf.cell(0, 5, "C9, Block C, Sushant Lok Phase-I", ln=True)
        pdf.cell(0, 5, "Sector 30 Main Wide Road", ln=True)
        pdf.cell(0, 5, "Gurugram, Haryana-122002", ln=True)
        pdf.cell(0, 5, "UDYAM : UDYAM-DL-11-0027475 (Micro)", ln=True)
        pdf.cell(0, 5, "GSTIN/UIN: 06AAOCA0669C1ZT", ln=True)
        pdf.cell(0, 5, "State Name : Haryana, Code : 06", ln=True)
        pdf.ln(10)
        
        # --- Invoice Info ---
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 10, f"TAX INVOICE", ln=True)
        
        pdf.set_font("Helvetica", "", 10)
        date_obj = transaction_data.get("date") or datetime.now()
        current_date = date_obj.strftime("%d-%m-%Y")
        invoice_num = transaction_data.get("invoice_number", "N/A")
        order_id = transaction_data.get("order_id", "N/A")
        payment_id = transaction_data.get("payment_id", "N/A")
        
        pdf.cell(100, 7, f"Invoice No: {invoice_num}", ln=False)
        pdf.cell(0, 7, f"Order ID: {order_id}", ln=True, align="R")
        pdf.cell(100, 7, f"Invoice Date: {current_date}", ln=False)
        pdf.cell(0, 7, f"Payment ID: {payment_id}", ln=True, align="R")
        pdf.ln(10)
        
        # --- Billing Details (Buyer) ---
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 10, "Bill To:", ln=True)
        pdf.set_font("Helvetica", "", 11)
        
        user_name = transaction_data.get("user_name") or "Valued Customer"
        user_email = transaction_data.get("user_email") or "Not Provided"
        user_gst = transaction_data.get("user_gst") or ""
        user_address = transaction_data.get("user_address")
        
        pdf.cell(0, 6, f"Name: {user_name}", ln=True)
        pdf.cell(0, 6, f"Email: {user_email}", ln=True)
        if user_gst and user_gst != "N/A":
            pdf.cell(0, 6, f"GSTIN/UIN: {user_gst}", ln=True)
        if user_address:
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 6, f"Address: {user_address}")
        pdf.ln(10)
        
        # --- Table Header ---
        pdf.set_fill_color(240, 240, 240)
        pdf.set_font("Helvetica", "B", 10)
        # Total width: 190
        pdf.cell(80, 10, " Description", border=1, fill=True)
        pdf.cell(30, 10, " HSN/SAC", border=1, fill=True, align="C")
        pdf.cell(30, 10, " Quantity", border=1, fill=True, align="C")
        pdf.cell(50, 10, " Amount (INR)", border=1, fill=True, align="R")
        pdf.ln()
        
        # --- Table Body ---
        pdf.set_font("Helvetica", "", 10)
        package_title = transaction_data.get("package_name") or "Credit Package"
        
        draft_count = transaction_data.get("draft_credits") 
        simple_count = transaction_data.get("simple_credits")
        
        def format_credit(val):
            if val == -1: return "Unlimited"
            return str(val or 0)

        desc_parts = []
        if draft_count: desc_parts.append(f"{format_credit(draft_count)} Draft")
        if simple_count: desc_parts.append(f"{format_credit(simple_count)} Tax Intelligence")
        
        credit_str = ", ".join(desc_parts) if desc_parts else ""
        full_desc = f"{package_title}"
        if credit_str:
            full_desc += f" ({credit_str})"
            
        # Get coordinates for flexible row height
        x, y = pdf.get_x(), pdf.get_y()
        
        # Description cell with wrapping
        pdf.multi_cell(80, 10, full_desc, border=1)
        h = pdf.get_y() - y
        
        # Re-position for other columns in the same row
        pdf.set_xy(x + 80, y)
        pdf.cell(30, h, " 998439", border=1, align="C")
        pdf.cell(30, h, " 1", border=1, align="C")
        
        amount_paise = transaction_data.get("amount") or 0
        discount_paise = transaction_data.get("discount") or 0
        
        # Extract base package amount, fallback to old math for backward compatibility
        base_package_amount_paise = transaction_data.get("base_package_amount")
        if not base_package_amount_paise:
            base_package_amount_paise = amount_paise + discount_paise
            
        base_amount = base_package_amount_paise / 100
        pdf.cell(50, h, f" {base_amount:,.2f} ", border=1, align="R")
        pdf.set_xy(x, y + h) # Move to next line
        
        # --- Calculations ---
        pdf.ln(5)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(140, 10, "Subtotal ", border=0, align="R")
        pdf.cell(50, 10, f"INR {base_amount:,.2f} ", border=1, align="R")
        pdf.ln()
        
        taxable_amount_paise = base_package_amount_paise
        if discount_paise > 0:
            pdf.set_text_color(200, 0, 0)
            discount_amount = discount_paise / 100
            pdf.cell(140, 10, "Discount ", border=0, align="R")
            pdf.cell(50, 10, f"- INR {discount_amount:,.2f} ", border=1, align="R")
            pdf.ln()
            pdf.set_text_color(0, 0, 0)
            
        taxable_amount_paise -= discount_paise
        taxable_amount = taxable_amount_paise / 100
        if discount_paise > 0:
            pdf.cell(140, 10, "Taxable Value ", border=0, align="R")
            pdf.cell(50, 10, f"INR {taxable_amount:,.2f} ", border=1, align="R")
            pdf.ln()
            
        gst_amount_paise = amount_paise - taxable_amount_paise
        if gst_amount_paise > 0:
            gst_amount = gst_amount_paise / 100
            
            # Logic: If GSTIN starts with "06" (Haryana), show CGST/SGST, else IGST
            # User mentioned using "06" for CGST/SGST calculation
            gstin_to_check = str(user_gst).strip()
            if gstin_to_check.startswith("06"):
                half_gst = gst_amount / 2
                pdf.cell(140, 10, "CGST (9%) ", border=0, align="R")
                pdf.cell(50, 10, f"INR {half_gst:,.2f} ", border=1, align="R")
                pdf.ln()
                pdf.cell(140, 10, "SGST (9%) ", border=0, align="R")
                pdf.cell(50, 10, f"INR {half_gst:,.2f} ", border=1, align="R")
                pdf.ln()
            else:
                pdf.cell(140, 10, "IGST (18%) ", border=0, align="R")
                pdf.cell(50, 10, f"INR {gst_amount:,.2f} ", border=1, align="R")
                pdf.ln()
            
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_fill_color(230, 230, 230)
        pdf.set_text_color(0, 0, 0)
        final_amount = amount_paise / 100
        pdf.cell(140, 12, "TOTAL AMOUNT PAID (Rounded) ", border=0, align="R")
        pdf.cell(50, 12, f"INR {final_amount:,.2f} ", border=1, fill=True, align="R")
        pdf.ln(20)
        
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(150, 150, 150)
        pdf.multi_cell(0, 5, "This is an electronically generated invoice and does not require a physical signature. Thank you for choosing Astrazure E Ventures Pvt. Ltd.", align="C")
        
        return pdf.output()
