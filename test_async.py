import asyncio
import aiohttp
from web_scrapper_v1 import fetch_table_with_backoff_async


async def main():
    headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}
    columns =['Provider', 'Launched', 'Block-storage', 'Assignable IPs',
                                         'SMTP support', 'IOPS Guaranteed minimum','Security',
                                           'Locations', 'Notes']
    async with aiohttp.ClientSession() as session:
        df = await fetch_table_with_backoff_async(session, "https://en.wikipedia.org/wiki/Cloud-computing_comparison", headers, columns)
        print(df.head())

if __name__ == "__main__":
    asyncio.run(main())