import argparse
from magic_pdf.rw.DiskReaderWriter import DiskReaderWriter
from magic_pdf.pipe.UNIPipe import UNIPipe
import os

if __name__ == "__main__":
    # Argument parser setup
    parser = argparse.ArgumentParser(description="Convert PDF to markdown and text")
    parser.add_argument("pdf_path", type=str, help="Path to the input PDF file")
    parser.add_argument("output_dir", type=str, help="Directory to store the output")

    args = parser.parse_args()

    # Use the provided arguments
    pdf_path = args.pdf_path
    output_dir = args.output_dir
    
    pdf_name = os.path.basename(pdf_path).split(".")[0]
    output_path = os.path.join(output_dir, pdf_name)
    output_image_path = os.path.join(output_path, 'images')

    # Ensure output directories exist
    os.makedirs(output_image_path, exist_ok=True)

    image_writer = DiskReaderWriter(output_image_path)
    image_dir = str(os.path.basename(output_image_path))
    jso_useful_key = {"_pdf_type": "", "model_list": []}
    pdf_bytes = open(pdf_path, "rb").read()

    pipe = UNIPipe(pdf_bytes, jso_useful_key, image_writer)
    pipe.pipe_classify()
    pipe.pipe_analyze()
    pipe.pipe_parse()
    
    md_content = pipe.pipe_mk_markdown(image_dir, drop_mode="none")
    print(md_content)
