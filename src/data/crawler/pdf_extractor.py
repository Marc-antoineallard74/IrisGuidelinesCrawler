import fitz  # PyMuPDF

def extract_text(doc, out, margin_top=40, margin_bottom=40):
    """
    Extracts text from the document while excluding footnotes and page numbers
    based on their position on the page.
    
    :param doc: The PyMuPDF document object.
    :param out: File object to write the text to.
    :param margin_top: Margin from the top of the page to exclude content (e.g., page numbers).
    :param margin_bottom: Margin from the bottom of the page to exclude content (e.g., footnotes).
    """
    for page in doc:  
        page_rect = page.rect
        content_rect = fitz.Rect(
            page_rect.x0, margin_top, page_rect.x1, page_rect.y1 - margin_bottom
        )

        blocks = page.get_text("blocks")
        for block in blocks:
            block_rect = fitz.Rect(block[:4])
            if content_rect.contains(block_rect):
                text = block[4].encode("utf8")
                out.write(text)  
        out.write("\f".encode("utf8"))  # page delimiter

if __name__ == "__main__":
    PDF_DIR = "/Users/marc-antoineallard/Desktop/Msc-LIGHT-WHO/LLM4MedicalGuideline/PDF/9786177152063_Guidelines_ukr.pdf"
    output_path = "output.txt"

    with open(output_path, "wb") as out:
        try:
            doc = fitz.open(PDF_DIR)  # open the PDF document
        except Exception as e:  # Catch any exception
            out.write(b"CORRUPTED\n")  # Write "CORRUPTED" to the output file
        else:
            print("==== METADATA ====\n")
            print(doc.metadata)
            print()

            extract_text(doc, out)

            print("==== PLAIN TEXT ====\n")
            # Print output path instead of text (since the output is in the file)
            print(f"Text extracted to {output_path}\n")