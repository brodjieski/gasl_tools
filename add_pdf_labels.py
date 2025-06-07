#!/usr/bin/env python3
"""
Script to add labels to the top of PDF files.
Creates a new PDF for each label with red, centered text at font size 10.
"""

import os
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import red
from reportlab.lib.units import inch
from io import BytesIO


def create_label_overlay(label_text, page_width, page_height):
    """
    Create a PDF overlay with the label text at the top center.
    
    Args:
        label_text (str): Text to display as label
        page_width (float): Width of the page
        page_height (float): Height of the page
    
    Returns:
        BytesIO: PDF overlay as bytes
    """
    packet = BytesIO()
    can = canvas.Canvas(packet, pagesize=(page_width, page_height))
    
    # Set font, size, and color
    can.setFont("Helvetica-Bold", 10)
    can.setFillColor(red)
    
    # Calculate center position for text
    text_width = can.stringWidth(label_text, "Helvetica-Bold", 10)
    x_position = (page_width - text_width) / 2
    y_position = page_height - 20  # 20 points from top
    
    # Draw the text
    can.drawString(x_position, y_position, label_text.upper())
    can.save()
    
    packet.seek(0)
    return packet


def add_label_to_pdf(input_pdf_path, label_text, output_pdf_path):
    """
    Add a label to the first page of a PDF and save to new file.
    
    Args:
        input_pdf_path (str): Path to input PDF file
        label_text (str): Text to add as label
        output_pdf_path (str): Path for output PDF file
    """
    # Read the existing PDF
    reader = PdfReader(input_pdf_path)
    writer = PdfWriter()
    
    # Get the first page
    first_page = reader.pages[0]
    page_width = float(first_page.mediabox.width)
    page_height = float(first_page.mediabox.height)
    
    # Create label overlay
    overlay_packet = create_label_overlay(label_text, page_width, page_height)
    overlay_reader = PdfReader(overlay_packet)
    overlay_page = overlay_reader.pages[0]
    
    # Merge the overlay with the first page
    first_page.merge_page(overlay_page)
    writer.add_page(first_page)
    
    # Add remaining pages unchanged
    for page_num in range(1, len(reader.pages)):
        writer.add_page(reader.pages[page_num])
    
    # Write the output PDF
    with open(output_pdf_path, 'wb') as output_file:
        writer.write(output_file)


def main():
    """Main function to process PDF with all labels."""
    # Define labels
    labels = ["announcer", "lane organizer", "clerk of course", "kid finder", "head coach"]
    
    # Input PDF file (assumes there's one PDF in the directory)
    pdf_files = [f for f in os.listdir('.') if f.endswith('.pdf')]
    
    if not pdf_files:
        print("No PDF files found in the current directory.")
        return
    
    if len(pdf_files) > 1:
        print("Multiple PDF files found. Using the first one:", pdf_files[0])
    
    input_pdf = pdf_files[0]
    base_name = os.path.splitext(input_pdf)[0]
    
    print(f"Processing: {input_pdf}")
    
    # Create labeled versions
    for label in labels:
        output_filename = f"{base_name}_{label.replace(' ', '_')}.pdf"
        try:
            add_label_to_pdf(input_pdf, label, output_filename)
            print(f"Created: {output_filename}")
        except Exception as e:
            print(f"Error creating {output_filename}: {e}")
    
    print("Done!")


if __name__ == "__main__":
    main()