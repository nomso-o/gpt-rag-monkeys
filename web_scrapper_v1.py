import requests 
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import Optional
import aiohttp
import asyncio
import backoff
import pandas as pd
import requests
from bs4 import BeautifulSoup
logging.basicConfig(level=logging.INFO)



URL = "https://en.wikipedia.org/wiki/Cloud-computing_comparison" # Replace with the URL of the website you want to scrape
def _parse_table(html: str, columns: Optional[list], output_doc_name: Optional[str]) -> Optional[pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")
    logging.info("Successfully fetched the webpage: %s", soup.title.string if soup.title else "N/A")
    table = soup.find("table", class_="wikitable")
    if table is None:
        logging.error("Expected table not found; update the selector.")
        return None
    rows = table.find_all("tr")
    data = []
    for row in rows:
        cells = row.find_all("td") if columns else row.find_all(["td", "th"])
        if not cells:
            continue
        data.append([cell.get_text(strip=True) for cell in cells])
    if not data:
        logging.error("No rows extracted from the table.")
        return None
    if columns:
        df = pd.DataFrame(data, columns=columns)
    else:
        df = pd.DataFrame(data)
    logging.info("Data extracted and converted to DataFrame")
    if output_doc_name:
        df.to_csv(output_doc_name, index=False)
        logging.info("Data saved to %s", output_doc_name)
    return df
@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def fetch_table_with_backoff(url, headers=None, columns: Optional[list] = None, output_doc_name: Optional[str] = "scraped_data.csv"):
    """
    Fetch the content of a URL with exponential backoff on failure (synchronous).
    """
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return _parse_table(response.content, columns, output_doc_name)


# Backoff wrapper works with exceptions from aiohttp
@backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=5)
async def fetch_table_with_backoff_async(session, url, headers=None, columns: Optional[list] = None, output_doc_name: Optional[str] = "scraped_data.csv"):
    """
    Fetch the content of a URL with exponential backoff on failure (asynchronous).
    """
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, headers=headers, timeout=timeout) as response:
            response.raise_for_status()
            html = await response.text()
    finally:
        if close_session:
            await session.close()
    return _parse_table(html, columns, output_doc_name)


# Downloading multiple Documents

@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, aiohttp.ClientError), max_tries=5)
def fetch_multiple_documents(urls : list, headers=None):
    """
    Fetch multiple URLs concurrently with exponential backoff on failure.
    """
    response = requests.get(url=urls, headers=headers, timeout=10)
    response.raise_for_status()
    logging.info("Successfully fetched the webpage: %s", response.url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
    else:
        logging.error("Failed to retrieve the webpage. Status code: %s", response.status_code)
        exit()
    
    # Find the document link
    document_links = soup.findall("a", {"class": "download-link"})["href"]

    # Display document link to verify
    logging.info("Document link found: %s", document_links)

    # Loop through and download each document
    for i, document_link in enumerate(document_links):
        logging.info("Downloading document %d from %s", i + 1, document_link)       
        document_response = requests.get(document_link, headers=headers, timeout=10)
        document_response.raise_for_status()

    
    if document_response.status_code == 200:
        # Save each document with a unique name
        file_name = f"report_{i + 1}.pdf"
        with open(file_name, "wb") as file:
            file.write(document_response.content)
        logging.info(f"Document {i+1} downloaded successfully as report.pdf")
    else:
        logging.error(f"Failed to download document {i+1}. Status code:", document_response.status_code)

