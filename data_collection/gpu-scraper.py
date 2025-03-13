import argparse
import json
import time
import re
import random
from datetime import datetime
import pandas as pd
import logging
from langdetect import detect

# For web scraping
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC  
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='gpu-google_maps_scraper.log',
    filemode='a'
)
logger = logging.getLogger('GPUGoogleMapsScraper')

class GoogleMapsReviewsScraper:
    def __init__(self, headless=True):
        self.driver = None
        self.headless = headless
        self._setup_webdriver()
    
    def _setup_webdriver(self):
        # Modified for GPU optimization and fast loading
        options = Options()
        if self.headless:
            options.add_argument('--headless=new')
        # GPU-specific flags for NVIDIA RTX 3070
        options.add_argument('--enable-gpu')
        options.add_argument('--use-gl=desktop')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--incognito')
        options.page_load_strategy = "eager"
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(15)
            self.driver.implicitly_wait(1)
            logger.info("GPU Optimized Fast WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Error setting up WebDriver: {e}")
            exit(1)
    
    def search_bank_agencies(self, bank_name, city):
        logger.info(f"Searching for {bank_name} agencies in {city}")
        query = f"{bank_name} {city} morocco"
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        agencies = []
        try:
            self.driver.get(url)
            time.sleep(1)  # minimal wait
            try:
                consent = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Accept') or contains(text(), 'Accept')]"))
                )
                consent.click()
                time.sleep(0.5)
            except Exception:
                logger.info("No consent pop-up")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.Nv2PK"))
            )
            # Try extended container first
            container = None
            try:
                container = self.driver.find_element(By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde.ecceSd")
                logger.info("Using extended container for scrolling")
            except Exception:
                logger.info("Falling back to window scrolling")
            last_count = 0
            for _ in range(7):
                if container:
                    self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
                else:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                elems = self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
                if len(elems) == last_count:
                    break
                last_count = len(elems)
            for i in range(last_count):
                try:
                    elems = self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
                    elem = elems[i]
                    name = elem.find_element(By.CSS_SELECTOR, "div.qBF1Pd").text.strip()
                    if bank_name.lower() not in name.lower():
                        continue
                    try:
                        loc = elem.find_element(By.CSS_SELECTOR, "div.AeaXub .Io6YTe").text.strip()
                    except Exception:
                        loc = f"{city}, Morocco"
                    agency = {"name": name, "location": loc, "city": city, "bank": bank_name}
                    try:
                        ActionChains(self.driver).move_to_element(elem).click().perform()
                        time.sleep(1)
                        agency["url"] = self.driver.current_url
                        agencies.append(agency)
                        logger.info(f"Added agency: {name} in {city}")
                        self.driver.back()
                        time.sleep(1)
                    except Exception as e:
                        logger.error(f"Error clicking agency {name}: {e}")
                except Exception as e:
                    logger.error(f"Error processing agency index {i}: {e}")
            return agencies
        except Exception as e:
            logger.error(f"Error in search_bank_agencies: {e}")
            return []
    
    def get_reviews(self, agency, max_reviews=20):
        logger.info(f"Getting reviews for {agency['name']} in {agency['city']}")
        reviews = []
        try:
            self.driver.get(agency["url"])
            WebDriverWait(self.driver, 8).until(lambda d: "maps/place" in d.current_url)
            try:
                btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'more reviews') or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'plus d'avis')]"))
                )
                btn.click()
                time.sleep(0.5)
            except Exception:
                logger.warning("Review button not found; proceeding")
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.jftiEf"))
            )
            while True:
                try:
                    more = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, \"Plus d'avis\")]"))
                    )
                    more.click()
                    logger.info("Clicked 'Plus d'avis' button")
                    time.sleep(0.5)
                except Exception:
                    break
            container = None
            for sel in ["div[role='feed']", "div.m6QErb"]:
                try:
                    container = self.driver.find_element(By.CSS_SELECTOR, sel)
                    break
                except:
                    continue
            last = 0
            for _ in range(7):
                if container:
                    self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
                else:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                elems = self.driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
                if len(elems) == last:
                    break
                last = len(elems)
            review_elems = self.driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
            logger.info(f"Collected {len(review_elems)} review elements")
            for i, elem in enumerate(review_elems[:max_reviews]):
                try:
                    text = ""
                    for sel in ["span.wiI7pd", "span.review-full-text"]:
                        try:
                            text = elem.find_element(By.CSS_SELECTOR, sel).text.strip()
                            if text:
                                break
                        except:
                            continue
                    if not text:
                        continue
                    reviewer = "Anonymous"
                    try:
                        rev = elem.find_element(By.CSS_SELECTOR, "div.d4r55")
                        reviewer = rev.text.split("\n")[0].strip()
                    except:
                        pass
                    rating = 0
                    try:
                        rating_elem = elem.find_element(By.CSS_SELECTOR, "span.kvMYJc")
                        aria = rating_elem.get_attribute("aria-label")
                        m = re.search(r"([\d.]+)", aria)
                        if m:
                            rating = int(float(m.group(1)))
                    except:
                        try:
                            stars = elem.find_elements(By.CSS_SELECTOR, "img[src*='star_active']")
                            rating = len(stars)
                        except:
                            rating = 0
                    review_date = datetime.now().strftime("%Y-%m-%d")
                    language = "unknown"
                    try:
                        language = detect(text)
                    except:
                        pass
                    reviews.append({
                        "agency_name": agency["name"],
                        "bank": agency["bank"],
                        "location": agency["location"],
                        "city": agency["city"],
                        "reviewer": reviewer,
                        "text": text,
                        "rating": rating,
                        "date": review_date,
                        "language": language,
                        "url": agency["url"]
                    })
                    logger.info(f"Added review {i+1} for {agency['name']} (rating: {rating})")
                except Exception as e:
                    logger.error(f"Error extracting review {i+1}: {e}")
            logger.info(f"Total reviews collected: {len(reviews)} for {agency['name']}")
            return reviews
        except Exception as e:
            logger.error(f"Error getting reviews for {agency['name']}: {e}")
            return []
    
    def save_to_json(self, reviews, output_file):
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reviews, f, ensure_ascii=False, indent=4)
        logger.info(f"Saved {len(reviews)} reviews to {output_file}")
        print(f"Saved {len(reviews)} reviews to {output_file}")
    
    def save_to_csv(self, reviews, output_file):
        df = pd.DataFrame(reviews)
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Saved {len(reviews)} reviews to {output_file}")
        print(f"Saved {len(reviews)} reviews to {output_file}")
    
    def close(self):
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")

def main():
    parser = argparse.ArgumentParser(description='GPU Optimized Fast Google Maps Reviews Scraper optimized for NVIDIA RTX 3070')
    parser.add_argument('--output', type=str, required=True, help='Output file path (JSON or CSV)')
    parser.add_argument('--banks', type=str, required=True, help='Comma-separated list of bank names')
    parser.add_argument('--cities', type=str, default='Casablanca,Rabat,Marrakech,Tangier,Fes',
                        help='Comma-separated list of cities')
    parser.add_argument('--max_reviews', type=int, default=35, help='Maximum reviews to collect per agency')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    args = parser.parse_args()
    
    scraper = GoogleMapsReviewsScraper(headless=args.headless)
    all_reviews = []
    bank_names = [name.strip() for name in args.banks.split(',')]
    cities = [city.strip() for city in args.cities.split(',')]
    
    for bank in bank_names:
        logger.info(f"Processing bank: {bank}")
        print(f"Processing bank: {bank}")
        for city in cities:
            logger.info(f"  Searching in city: {city}")
            print(f"  Searching in city: {city}")
            agencies = scraper.search_bank_agencies(bank, city)
            logger.info(f"  Found {len(agencies)} agencies in {city}")
            print(f"  Found {len(agencies)} agencies in {city}")
            for i, agency in enumerate(agencies):
                logger.info(f"    Processing agency {i+1}/{len(agencies)}: {agency['name']}")
                print(f"    Processing agency {i+1}/{len(agencies)}: {agency['name']}")
                reviews = scraper.get_reviews(agency, max_reviews=args.max_reviews)
                all_reviews.extend(reviews)
                logger.info(f"    Collected {len(reviews)} reviews for {agency['name']}")
                print(f"    Collected {len(reviews)} reviews for {agency['name']}")
                time.sleep(random.uniform(1.5, 3))
            time.sleep(1)
    if not all_reviews:
        logger.warning("No reviews collected!")
        print("No reviews collected!")
    if args.output.endswith('.json'):
        scraper.save_to_json(all_reviews, args.output)
    elif args.output.endswith('.csv'):
        scraper.save_to_csv(all_reviews, args.output)
    else:
        logger.error("Unsupported output format. Use .json or .csv")
        print("Error: Unsupported output format. Use .json or .csv")
    scraper.close()

if __name__ == "__main__":
    main()
