import argparse
import os
import re
import time
from tqdm import tqdm
from io import BytesIO

from concurrent.futures import ThreadPoolExecutor, as_completed
from ftlangdetect import detect
from langcodes import *
import fitz  # PyMuPDF
import random
from datasets import Dataset, DatasetDict

def extract_text(pdf_path, margin_top=40, margin_bottom=40):
    """
    Extracts text from the document while excluding footnotes and page numbers
    based on their position on the page.

    :param pdf_path: Path to the PDF file.
    :param margin_top: Margin from the top of the page to exclude content (e.g., page numbers).
    :param margin_bottom: Margin from the bottom of the page to exclude content (e.g., footnotes).

    :Return: Dictionary with pdf_name and text.
    """
    try:
        doc = fitz.open(pdf_path)  # open the PDF document
        extracted_text = []

        # Loop through each page in the document and extract relevant text
        for page in doc:
            page_rect = page.rect
            content_rect = fitz.Rect(
                page_rect.x0, margin_top, page_rect.x1, page_rect.y1 - margin_bottom
            )

            blocks = page.get_text("blocks")
            for block in blocks:
                block_rect = fitz.Rect(block[:4])
                if content_rect.contains(block_rect):
                    extracted_text.append(block[4])  # Append the text content

        return {'pdf_name': os.path.basename(pdf_path), 'text': " ".join(extracted_text)}

    except Exception as e:
        # If any exception occurs (e.g., corrupted PDF), return 'CORRUPTED' as the text
        return {'pdf_name': os.path.basename(pdf_path), 'text': "CORRUPTED"}

def extract_lang_type(file_name):
    """
    Extracts language code if present from PDF name, otherwise returns "unknown".
    
    :param file_name: The name of the PDF file.
    
    :Return: ISO standardized language code or 'unknown'.
    """
    pattern = r'[-_](eng|engl|en|[a-z]{2,4})(?=\.(pdf))'
    match = re.search(pattern, file_name)

    return standardize_tag(match.group(1)) if match else "unknown"

def get_random_chunks(text, num_chunks=4, words_per_chunk=6):
    """
    Extracts random chunks of text from the document.

    :param text: The full text from the document.
    :param num_chunks: The number of random chunks to extract.
    :param words_per_chunk: The number of words per chunk.

    :Return: A concatenation of the random chunks.
    """
    words = text.split()
    total_words = len(words)
    chunks = []

    if total_words < words_per_chunk * num_chunks:
        return " ".join(words) 
    for _ in range(num_chunks):
        start_idx = random.randint(0, total_words - words_per_chunk)
        chunks.append(" ".join(words[start_idx:start_idx + words_per_chunk]))

    return " ".join(chunks)

def language_extractor(data):
    """
    Associates each document to an ISO lang code or marks it as 'CORRUPT' if corrupted.

    :param data: A list of dictionaries with 'pdf_name' and 'text'.

    :Return: A dictionary where key = 'lang code' and value = list of {PDF name, Plain text}.
    """
    pdf_split_by_lang = {}

    # First extract language using the PDF name 
    for doc in data:
        print(f"Processing PDF: {doc['pdf_name']}")
        if doc['text'] == "CORRUPTED":
            lang_code = "CORRUPT"
        else:
            lang_code = extract_lang_type(doc['pdf_name'])
            if lang_code == "unknown":
                # Extract random chunks from plain text for language detection
                chunk = get_random_chunks(doc['text'])
                # Detect language using the random chunk and fasttext
                lang_code = standardize_tag(detect(chunk, low_memory=True)['lang'])
        
        if lang_code in pdf_split_by_lang:
            pdf_split_by_lang[lang_code].append(doc)
        else:
            pdf_split_by_lang[lang_code] = [doc]

    return pdf_split_by_lang

def save_to_hf_dataset(pdf_split_by_lang, output_dir="hf_datasets"):
    """
    Saves the PDF data categorized by language as Hugging Face dataset.
    
    :param pdf_split_by_lang: Dictionary where key is lang and value is list of {pdf_name, text}.
    :param output_dir: The output directory to save the datasets.
    """
    dataset_dict = {}

    for lang_code, pdfs in pdf_split_by_lang.items():
        # Transform list of dicts to dict of lists
        transformed_pdfs = {
            "pdf_name": [pdf["pdf_name"] for pdf in pdfs],
            "text": [pdf["text"] for pdf in pdfs]
        }
        
        # Create dataset from the transformed dictionary
        dataset = Dataset.from_dict(transformed_pdfs)
        dataset_dict[lang_code] = dataset

    # Create a DatasetDict to hold datasets for each language
    dataset_splits = DatasetDict(dataset_dict)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save dataset to disk
    dataset_path = os.path.join(output_dir, f"hf_parallel_iris_corpus")
    dataset_splits.save_to_disk(dataset_path)


if __name__ == "__main__":
    PDF_directory = "/Users/marc-antoineallard/Desktop/Msc-LIGHT-WHO/LLM4MedicalGuideline/PDF"
    output_dir = "/Users/marc-antoineallard/Desktop/Msc-LIGHT-WHO/LLM4MedicalGuideline/"
    pdf_data = []

    start_time = time.time()
    total_files = sum(len(files) for _, _, files in os.walk(PDF_directory))  # Get total number of PDF files
    print(f"Total number of PDF files: {total_files}\n")

    with tqdm(total=total_files, desc="Processing PDFs") as pbar:
        for root, _, files in os.walk(PDF_directory):
            for file in files:
                if file.endswith(".pdf"):
                    pdf_path = os.path.join(root, file)
                    # 1- Extract: {pdf_name, text} 
                    pdf_data.append(extract_text(pdf_path))
                    # Update progress bar
                    pbar.update(1)

    end_time = time.time()
    print(f"=== Text Extraction Done ===")
    print(f"Time taken for text extraction: {end_time - start_time:.2f} seconds\n")

    start_time = time.time()

    # 2- Language detection and extraction: {lang, [{pdf_name, text},...]}
    pdf_split_by_lang = language_extractor(pdf_data)

    end_time = time.time()
    print(f"=== Language Extraction Done ===")
    print(f"Time taken for language extraction: {end_time - start_time:.2f} seconds\n")

    for lang_code, documents in pdf_split_by_lang.items():
        num_docs = len(documents)
        print(f"Language: {lang_code}, Number of documents: {num_docs}")

    save_to_hf_dataset(pdf_split_by_lang)