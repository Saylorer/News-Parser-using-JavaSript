import asyncio
from urllib.parse import urlparse
import aiosqlite
import aiohttp
import logging
import re
import random
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Константы
DATABASE_NAME = "the_daily.db"
PHRASES = ["the", "people"]
START_YEAR = 2025
END_YEAR = 2025



SITEMAP_URL = "https://www.dailystar.co.uk/sitemaps/sitemap_index.xml"
MAX_CONCURRENT_PAGES = 5
MAX_CONCURRENT_TASKS = 5
REQUEST_TIMEOUT = 45000
MAX_RETRIES = 2


TAGS = ["news"]  # Добавлен список тегов для фильтрации



# User-Agent'ы для подмены
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
]

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


async def setup_database():
    async with aiosqlite.connect(DATABASE_NAME) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS articles 
                            (url TEXT, 
                             title TEXT,
                             phrase TEXT,
                             mentions INTEGER,
                             date_published TEXT,
                             PRIMARY KEY(url, phrase))''')
        await db.commit()


async def parse_content(html, url):
    soup = BeautifulSoup(html, "lxml")
    title = soup.find('h1') or soup.find('meta', property='og:title')
    title = title.get_text(strip=True) if title else "No Title"

    content = ""
    for element in soup.find_all(['article', 'div', 'section'], class_=re.compile(r'article|content')):
        if len(element.text) > 500:
            content = element.get_text()
            break

    if not content:
        return []

    results = []
    date = extract_date(html)

    # Проверяем все фразы
    for phrase in PHRASES:
        mentions = len(re.findall(rf'\b{re.escape(phrase)}\b', content, re.I))
        if mentions > 0:
            results.append({
                "url": url,
                "title": title,
                "phrase": phrase,
                "mentions": mentions,
                "date_published": date
            })

    return results


async def process_article(browser_context, url):
    for attempt in range(MAX_RETRIES + 1):
        page = None
        try:
            page = await browser_context.new_page()
            await stealth_mode(page)
            await page.set_extra_http_headers({"User-Agent": random.choice(USER_AGENTS)})
            await asyncio.sleep(random.uniform(1.5, 4.0))
            await page.goto(url, timeout=45000, wait_until="domcontentloaded")

            if await page.query_selector('text=/access denied|blocked|cloudflare/i'):
                logging.warning(f"Blocked detected: {url}")
                with open('blocked_urls.txt', 'a') as f:
                    f.write(f"{url}\n")
                return []

            await page.wait_for_selector('article, div.article-body, .post-content', timeout=20000)
            html = await page.content()
            return await parse_content(html, url)

        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed: {type(e).__name__} - {str(e)[:200]}")
            if attempt == MAX_RETRIES:
                logging.error(f"Final failure for {url}")
                with open('blocked_urls.txt', 'a') as f:
                    f.write(f"{url}\n")
                return []
            await asyncio.sleep(2 ** attempt)
        finally:
            if page:
                await page.close()


async def main_processor(browser, urls):
    user_agent = random.choice(USER_AGENTS)
    context = await browser.new_context(
        user_agent=user_agent,
        viewport={"width": 1920, "height": 1080},
        java_script_enabled=True
    )
    try:
        all_results = []
        batch_size = MAX_CONCURRENT_TASKS
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            tasks = [process_article(context, url) for url in batch]
            batch_results = await asyncio.gather(*tasks)
            # Объединяем вложенные списки
            valid_results = [res for sublist in batch_results for res in sublist if res]
            all_results.extend(valid_results)
            logging.info(f"Progress: {len(all_results)}/{len(urls) * len(PHRASES)} possible entries")
        return all_results
    finally:
        await context.close()

def extract_tag(url):
    """Извлекает первый сегмент пути из URL для фильтрации по тегам."""
    try:
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        if not path:
            return None
        segments = path.split('/')
        return segments[0].lower()  # Приводим к нижнему регистру для единообразия
    except Exception:
        return None

async def parse_sitemap(url, session):
    # Существующий код без изменений
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
            xml_data = await response.text()
            root = ET.fromstring(xml_data)
            return [loc.text for loc in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
    except Exception as e:
        logging.error(f"Sitemap error: {e}")
        return []


async def stealth_mode(page):
    # Существующий код без изменений
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    """)

def extract_date(html):
    # Существующий код без изменений
    soup = BeautifulSoup(html, "lxml")
    date_element = soup.find('meta', {'property': 'article:published_time'}) or \
                   soup.find('time', {'datetime': True})
    if date_element:
        date_str = date_element.get('content') or date_element.get('datetime')
        return date_str.split('T')[0] if date_str else "Unknown"
    return "Unknown"


async def main():
    await setup_database()
    async with aiohttp.ClientSession() as session:
        sitemap_urls = await parse_sitemap(SITEMAP_URL, session)
        article_sitemaps = [
            url for url in sitemap_urls
            if any(str(year) in url for year in range(START_YEAR, END_YEAR + 1))
        ]
        article_urls = []
        for sitemap in article_sitemaps:
            article_urls += await parse_sitemap(sitemap, session)

        # Фильтрация URL по тегам
        if TAGS:
            filtered_urls = []
            for url in article_urls:
                tag = extract_tag(url)
                if tag in TAGS:
                    filtered_urls.append(url)
            article_urls = filtered_urls
            logging.info(f"Filtered to {len(article_urls)} articles after tag filtering")

        logging.info(f"Found {len(article_urls)} articles for processing")
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--disable-gpu', '--no-sandbox'], timeout=60000)
            results = await main_processor(browser, article_urls)
            async with aiosqlite.connect(DATABASE_NAME) as db:
                await db.executemany('''INSERT OR REPLACE INTO articles 
                                        VALUES (:url, :title, :phrase, :mentions, :date_published)''',
                                     results)
                await db.commit()
    logging.info(f"Processing complete! Saved {len(results)} articles")


if __name__ == "__main__":
    asyncio.run(main())