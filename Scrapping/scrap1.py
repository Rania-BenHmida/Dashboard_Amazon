import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import os

# ------------------------------
# CONFIGURATION
# ------------------------------
PROXIES = [
    # Replace with real proxies if needed
    # 'http://123.456.789.001:8080',
    # 'http://222.333.444.555:3128',
    # Leave empty for now, or populate with working ones
]

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.5993.90 Safari/537.36',
]

BASE_CATEGORY_URL = "https://www.amazon.com/s?i=electronics-intl-ship&rh=n%3A16225007011&fs=true&page={}"
SAVE_FILE = "amazon_data/electronics_data.csv"
LAST_PAGE_FILE = "last_scraped_page.txt"

# ------------------------------
# UTILITIES
# ------------------------------
def get_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9'
    })
    return session

def request_with_retry(session, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            proxy = random.choice(PROXIES) if PROXIES else None
            proxies = {"http": proxy, "https": proxy} if proxy else None
            response = session.get(url, proxies=proxies, timeout=15)
            if "api-services-support@amazon.com" in response.text or "Enter the characters" in response.text:
                raise requests.exceptions.RequestException("Blocked by CAPTCHA page")
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Attempt {attempt+1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(30, 60))
            else:
                return None

def extract_product_links(session, url):
    response = request_with_retry(session, url)
    if not response:
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    product_links = []
    results = soup.select('div.s-main-slot div.s-result-item[data-asin]')
    for item in results:
        asin = item.get('data-asin')
        if asin:
            product_links.append(f"https://www.amazon.com/dp/{asin}")
    return product_links

def scrape_product_page(session, url):
    time.sleep(random.uniform(5, 10))
    response = request_with_retry(session, url)
    if not response:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    product = {'url': url, 'category': "Computers & Accessories"}

    def get(selector, attr='text'):
        el = soup.select_one(selector)
        return el.get(attr) if el and attr != 'text' else (el.get_text(strip=True) if el else None)

    product['name'] = get('#productTitle')
    product['brand'] = get('#poExpander tr.po-brand td.a-span9 span')
    product['price'] = get('.a-price .a-price-whole')
    product['reduction_percentage'] = get('.savingsPercentage') or get('.a-span12.a-color-price')
    product['average_review'] = get('[data-hook=rating-out-of-text]')
    product['review_stars'] = get('[data-hook=average-star-rating] i', 'class')
    product['number_of_reviews'] = get('#acrCustomerReviewText')
    product['image_url'] = get('#landingImage', 'src')
    product['availability'] = get('#availability span')
    product['last_month_sales'] = get('#social-proofing-faceout-title-tk_bought span.a-text-bold')

    if product['review_stars']:
        product['review_stars'] = product['review_stars'].split()[-1]

    product['reviews'] = scrape_reviews(session, url)
    return product

def scrape_reviews(session, product_url):
    reviews = []
    review_url = product_url.replace('/dp/', '/product-reviews/') + "?reviewerType=all_reviews"
    response = request_with_retry(session, review_url)
    if not response:
        return reviews

    soup = BeautifulSoup(response.text, 'html.parser')
    blocks = soup.select('div[data-hook=review]')
    for block in blocks[:3]:
        r = {}
        r['profile_name'] = block.select_one('.a-profile-name') and block.select_one('.a-profile-name').get_text(strip=True)
        r['rating'] = block.select_one('[data-hook=review-star-rating]') and block.select_one('[data-hook=review-star-rating]').get_text(strip=True).split()[0]
        r['title'] = block.select_one('[data-hook=review-title] span') and block.select_one('[data-hook=review-title] span').get_text(strip=True)
        r['comment'] = block.select_one('[data-hook=review-body] span') and block.select_one('[data-hook=review-body] span').get_text(strip=True)
        date = block.select_one('[data-hook=review-date]')
        r['date'] = re.search(r'on\s+(.*)', date.get_text()).group(1) if date else None
        reviews.append(r)
    return reviews

def save_to_csv(products, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    data = []
    for p in products:
        base = {k: v for k, v in p.items() if k != 'reviews'}
        if p.get('reviews'):
            for r in p['reviews']:
                merged = base.copy()
                merged.update({f'review_{k}': v for k, v in r.items()})
                data.append(merged)
        else:
            data.append(base)
    df = pd.DataFrame(data)
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=False, index=False)
    else:
        df.to_csv(filename, index=False)
    print(f"✅ Saved {len(data)} rows to {filename}")

# ------------------------------
# MAIN EXECUTION
# ------------------------------
if __name__ == "__main__":
    try:
        with open(LAST_PAGE_FILE, "r") as f:
            start_page = int(f.read().strip())
    except:
        start_page = 1

    session = get_session()
    all_products = []

    for page in range(start_page, 401):
        print(f"\n📄 Scraping page {page}/400")
        try:
            with open(LAST_PAGE_FILE, "w") as f:
                f.write(str(page))

            page_url = BASE_CATEGORY_URL.format(page)
            links = extract_product_links(session, page_url)
            print(f"🔗 Found {len(links)} product links")

            for i, link in enumerate(links, 1):
                print(f"📦 ({i}/{len(links)}) Scraping {link}")
                product = scrape_product_page(session, link)
                if product:
                    all_products.append(product)

            save_to_csv(all_products, SAVE_FILE)
            all_products.clear()

            # Anti-block cooldown
            if page % 5 == 0:
                print("😴 Sleeping for 60 seconds to avoid blocks...")
                time.sleep(60)
            else:
                time.sleep(random.uniform(10, 20))

        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            time.sleep(60)

    print("🎉 Scraping complete.")
