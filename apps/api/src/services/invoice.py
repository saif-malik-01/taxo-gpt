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
        
        # --- Header ---
        pdf.set_font("Helvetica", "B", 24)
        pdf.set_text_color(251, 146, 60) # TaxoBuddy Orange
        pdf.cell(0, 20, "TaxoBuddy", ln=True, align="L")
        
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 5, "Professional Tax Intelligence", ln=True)
        pdf.ln(10)
        
        # --- Invoice Info ---
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 10, f"INVOICE", ln=True)
        
        pdf.set_font("Helvetica", "", 10)
        date_obj = transaction_data.get("date") or datetime.now()
        current_date = date_obj.strftime("%B %d, %Y")
        invoice_num = transaction_data.get("invoice_number", "N/A")
        order_id = transaction_data.get("order_id", "N/A")
        payment_id = transaction_data.get("payment_id", "N/A")
        
        pdf.cell(100, 7, f"Invoice No: {invoice_num}", ln=False)
        pdf.cell(0, 7, f"Order ID: {order_id}", ln=True, align="R")
        pdf.cell(100, 7, f"Invoice Date: {current_date}", ln=False)
        pdf.cell(0, 7, f"Payment ID: {payment_id}", ln=True, align="R")
        pdf.ln(10)
        
        # --- Billing Details ---
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 10, "Bill To:", ln=True)
        pdf.set_font("Helvetica", "", 11)
        
        user_name = transaction_data.get("user_name") or "Valued Customer"
        user_email = transaction_data.get("user_email") or "Not Provided"
        
        pdf.cell(0, 6, str(user_name), ln=True)
        pdf.cell(0, 6, str(user_email), ln=True)
        pdf.ln(15)
        
        # --- Table Header ---
        pdf.set_fill_color(245, 245, 245)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(110, 10, " Description", border=1, fill=True)
        pdf.cell(30, 10, " Quantity", border=1, fill=True, align="C")
        pdf.cell(50, 10, " Amount (INR)", border=1, fill=True, align="R")
        pdf.ln()
        
        # --- Table Body ---
        pdf.set_font("Helvetica", "", 10)
        package_title = transaction_data.get("package_name") or "Credit Package"
        credits_count = transaction_data.get("credits") or 0
        pdf.cell(110, 12, f" {package_title} ({credits_count} Credits)", border=1)
        pdf.cell(30, 12, " 1", border=1, align="C")
        
        amount_paise = transaction_data.get("amount") or 0
        discount_paise = transaction_data.get("discount") or 0
        base_amount = (amount_paise + discount_paise) / 100
        pdf.cell(50, 12, f" {base_amount:,.2f} ", border=1, align="R")
        pdf.ln()
        
        # --- Calculations ---
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(140, 10, "Subtotal ", border=0, align="R")
        pdf.cell(50, 10, f"INR {base_amount:,.2f} ", border=1, align="R")
        pdf.ln()
        
        if discount_paise > 0:
            pdf.set_text_color(200, 0, 0)
            discount_amount = discount_paise / 100
            pdf.cell(140, 10, "Discount ", border=0, align="R")
            pdf.cell(50, 10, f"- INR {discount_amount:,.2f} ", border=1, align="R")
            pdf.ln()
            pdf.set_text_color(0, 0, 0)
            
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_fill_color(251, 146, 60)
        pdf.set_text_color(255, 255, 255)
        final_amount = amount_paise / 100
        pdf.cell(140, 12, "TOTAL PAID ", border=0, align="R")
        pdf.cell(50, 12, f"INR {final_amount:,.2f} ", border=1, fill=True, align="R")
        pdf.ln(20)
        
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(150, 150, 150)
        pdf.multi_cell(0, 5, "This is an electronically generated invoice and does not require a physical signature. Thank you for choosing TaxoBuddy for your tax compliance needs.", align="C")
        
        return pdf.output()
