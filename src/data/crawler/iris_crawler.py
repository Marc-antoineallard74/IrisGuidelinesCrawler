import argparse
import requests
from bs4 import BeautifulSoup
import os
import re
import time
import json
import threading
from io import BytesIO
import PyPDF2
from concurrent.futures import ThreadPoolExecutor, as_completed
from datasets import Dataset

# Global variables
PDF_STORAGE_PATH = "../pdf_who"  
JSON_STORAGE_PATH = "../json_who" 
PDF_DATASET = {}
stop_crawling = False  # Flag to control crawling
MAX_WORKERS = 16

def sanitize_filename(filename):
    """Replace invalid characters in filename with underscores."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def extract_pdf_text(pdf_url):
    """Extract text from a PDF at the provided URL and return the PDF name and text."""
    try:
        pdf_response = requests.get(pdf_url)
        pdf_response.raise_for_status()  
        
        title = pdf_url.split("/")[-1].split("?")[0]
        title = sanitize_filename(title)

        pdf_file = BytesIO(pdf_response.content)
        reader = PyPDF2.PdfReader(pdf_file)
        pdf_text = ''
        for page in reader.pages:
            pdf_text += page.extract_text() if page.extract_text() else '' 
        
        return {'title': title, 'pdf_text': pdf_text}

    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF from {pdf_url}: {e}")
        return {'title': pdf_url.split('/')[-1], 'pdf_text': ''}
    except Exception as e:
        print(f"Error extracting text from PDF {pdf_url}: {e}")
        return {'title': pdf_url.split('/')[-1], 'pdf_text': ''}
    
def download_pdf(pdf_url):
    """Download PDF manually to a given directory."""
    try:

        pdf_response = requests.get(pdf_url)
        pdf_response.raise_for_status()  # Raise an error if the response status is not OK
        
        # Extract a sanitized title from the URL
        title = pdf_url.split("/")[-1].split("?")[0]
        title = sanitize_filename(title)

        # Save the PDF file to the PDF_STORAGE_PATH
        pdf_file = BytesIO(pdf_response.content)
        os.makedirs(PDF_STORAGE_PATH, exist_ok=True)

        file_path = os.path.join(PDF_STORAGE_PATH, f"{title}")
        print(f"Downloading and saving at: {file_path}")
        with open(file_path, "wb") as f:
            f.write(pdf_file.getbuffer())
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF from {pdf_url}: {e}")

def crawl_document_page(document_url, get_children = True):
    """Crawl the document page to find and extract text from the PDFs."""
    global PDF_DATASET  
    if stop_crawling:
        return  

    pdf_urls = {} 
    main_urls = []
    children_urls = []
    
    try:
        response = requests.get(document_url)
        response.raise_for_status()  
        soup = BeautifulSoup(response.content, 'html.parser')

        # Select all anchor tags that link to PDFs on the main page
        pdf_link_elements = soup.select("#aspect_artifactbrowser_ItemViewer_div_item-view a")  
        for pdf_link_element in pdf_link_elements:
            if 'href' in pdf_link_element.attrs and 'pdf' in pdf_link_element['href'].lower():
                pdf_url = f"https://iris.who.int{pdf_link_element['href']}"
                print(f"PDF FOUND: {pdf_url}")
                main_urls.append(pdf_url)

        pdf_urls['mainpage'] = main_urls

        if get_children:
            document_links = set()
            language_link_elements = soup.select("#aspect_artifactbrowser_ItemViewer_div_item-view a")    
            pattern = re.compile(r"https://iris\.who\.int/handle/10665/\d+")
            for link in language_link_elements:
                if 'href' in link.attrs and pattern.match(link['href']):
                    children_url = f"{link['href']}"
                    print(f"CHILDREN PAGE FOUND: {children_url}")
                    if children_url != document_url:
                        document_links.add(children_url)
            
            # Recursively crawl child pages to get their PDFs
            for link in document_links:
                children_page_pdfs = crawl_document_page(link, get_children=False)['mainpage']
                if children_page_pdfs:
                    children_urls.extend(children_page_pdfs)
            
            pdf_urls['childrenpage'] = children_urls
        else:
            pdf_urls['childrenpage'] = []  # No children crawling if `get_children=0`
        
        return pdf_urls

    except Exception as e:
        print(f"Error crawling document page {document_url}: {e}")
        return {'mainpage': [], 'childrenpage': []}
    
def crawl_main_page(base_url, start_page, last_page):
    """Crawl the main page to find document links and paginate through pages."""
    global stop_crawling  
    
    for page_id in range(start_page, last_page + 1):
        if stop_crawling:
            return  
        try:
            current_url = base_url.format(page=page_id)
            response = requests.get(current_url)
            response.raise_for_status()  
            soup = BeautifulSoup(response.content, 'html.parser')

            document_links = set()
            for link in soup.select("a[href^='/handle/']"):
                document_url = f"https://iris.who.int{link['href']}"
                document_links.add(document_url)
                print(f"Document Link Found: {document_url}")

            pdf_urls = []
            for document_url in document_links:
                if stop_crawling:
                    return  
                pdf_url = crawl_document_page(document_url)
                if pdf_url:
                    pdf_urls.append(pdf_url)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_pdf = {executor.submit(extract_pdf_text, pdf_url): pdf_url for pdf_url in pdf_urls}
                
                for future in as_completed(future_to_pdf):
                    pdf_data = future.result()
                    PDF_DATASET[pdf_data['title']] = pdf_data['pdf_text']
                    print(f"Processed PDF: {pdf_data['title']}")

            print(f"Finished crawling page {page_id}")

        except Exception as e:
            print(f"Error crawling main page {current_url}: {e}")
            break  

def crawl_main_page_for_downloading(base_url, id2pdfurls, start_page, last_page):
    """Crawl the main page to find document links and paginate through pages."""
    global stop_crawling  
    for page_id in range(start_page, last_page + 1):
        if stop_crawling:
            return id2pdfurls  # Return early if stop_crawling is set
        try:
            current_url = base_url.format(page=page_id)
            response = requests.get(current_url)
            response.raise_for_status()  
            soup = BeautifulSoup(response.content, 'html.parser')

            document_links = set()
            for link in soup.select("a[href^='/handle/']"):
                document_url = f"https://iris.who.int{link['href']}"
                document_links.add(document_url)
                print(f"Document Link Found: {document_url}")

            pdf_urls = []
            for doc_id, document_url in enumerate(document_links):
                if stop_crawling:
                    return id2pdfurls  # Return early if stop_crawling is set  
                # Crawl the page to get all PDF URLs 
                all_pdf_dict = crawl_document_page(document_url)  
                pdf_urls_from_doc = all_pdf_dict['mainpage'] # download only main not children
                # Update the dict of {unique id: [pdf_urls crawl from that mainpage and childrenpage]}
                id2pdfurls[document_url] = (all_pdf_dict['mainpage'], all_pdf_dict['childrenpage'])
                if pdf_urls_from_doc:
                    pdf_urls.extend(pdf_urls_from_doc)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_pdf = {executor.submit(download_pdf, pdf_url): pdf_url for pdf_url in pdf_urls}
                
                for future in as_completed(future_to_pdf):
                    try:
                        pdf_data = future.result()
                    except Exception as e:
                        print(f"Error downloading PDF: {future_to_pdf[future]}: {e}")

            print(f"Finished crawling page {page_id}")
        except Exception as e:
            print(f"Error crawling main page {current_url}: {e}")
            break
    return id2pdfurls

def listen_for_stop():
    """Listen for user input to stop crawling."""
    global stop_crawling
    while True:
        user_input = input().strip().lower()
        if user_input == 'q':
            stop_crawling = True
            print("Crawling stopped by user.")
            break

def remove_invalid_character(text):
    # safety method to remove surrogate characters that are invalid in UTF-8
    return re.sub(r'[\ud800-\udfff]', '', text)

def save_to_hf_dataset(start_page, last_page):
    """Save the global PDF_DATASET to a Hugging Face dataset."""
    if not PDF_DATASET:
        print("No PDF data to save.")
        return
    
    data = {'title': [], 'text': []}
    for title, pdf_text in PDF_DATASET.items():
        clean_title = remove_invalid_character(title)
        clean_text = remove_invalid_character(pdf_text)

        data['title'].append(clean_title)
        data['text'].append(clean_text)
    
    hf_dataset = Dataset.from_dict(data)
    
    os.makedirs(PDF_STORAGE_PATH, exist_ok=True)
    dataset_path = os.path.join(PDF_STORAGE_PATH, f"pdf_dataset_from_{start_page}_to_{last_page}")
    hf_dataset.save_to_disk(dataset_path)
    print(f"Dataset saved to {dataset_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl IRIS pages to download or extract text from PDFs.")
    parser.add_argument('start_page', type=int, help="The starting page number to crawl.")
    parser.add_argument('last_page', type=int, help="The last page number to crawl.")
    parser.add_argument('mode', choices=['download', 'read'], help="Mode of operation: 'download' to download PDFs, 'read' to only crawl and extract text.")

    args = parser.parse_args()

    # Base URL
    BASE_URL = "https://iris.who.int/discover?rpp=10&etal=0&query=Guidelines&scope=/&group_by=none&page={page}"

    print(f"Crawling from page {args.start_page} to {args.last_page} in '{args.mode}' mode.")

    start_time = time.time()

    # Decide based on the mode
    if args.mode == 'download':
        id2pdfurls = {}
        id2pdfurls = crawl_main_page_for_downloading(BASE_URL, id2pdfurls, args.start_page, args.last_page)
        with open(f'{JSON_STORAGE_PATH}/id2pdfurls{args.start_page}_to_{args.last_page}.json', 'w') as json_file:
            json.dump(id2pdfurls, json_file, indent=4)
        print("SAVE AS JSON")
    elif args.mode == 'read':
        crawl_main_page(BASE_URL, args.start_page, args.last_page)
        save_to_hf_dataset(args.start_page, args.last_page)

    elapsed_time = time.time() - start_time
    print(f"Time taken to crawl from page {args.start_page} to {args.last_page}: {elapsed_time:.2f} seconds")
