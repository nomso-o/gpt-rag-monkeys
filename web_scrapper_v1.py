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





# if __name__ == "__main__":
#     asyncio.run(main())


# if __name__ == "__main__":
#     fetch_with_backoff(URL)
