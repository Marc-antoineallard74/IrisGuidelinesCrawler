import requests
from bs4 import BeautifulSoup

# Base URL for page 1
BASE_URL = "https://iris.who.int/discover?rpp=10&etal=0&query=Guidelines&scope=/&group_by=none&page=1"

def get_total_pages(base_url):
    """Fetches and prints the total number of pages from the first page of the URL."""
    try:
        # Make a request to the base URL
        response = requests.get(base_url)
        response.raise_for_status()  # Check if the request was successful

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Look for the last page number in the pagination section
        last_page_li = soup.find('li', class_='last-page-link')
        
        if last_page_li:
            last_page_anchor = last_page_li.find('a')
            last_page_number = int(last_page_anchor['href'].split('page=')[-1])
            print(f"Total number of pages: {last_page_number}")
        else:
            print("Could not find the total number of pages. It may be a single-page result.")

    except Exception as e:
        print(f"Error fetching total number of pages: {e}")

if __name__ == "__main__":
    get_total_pages(BASE_URL)