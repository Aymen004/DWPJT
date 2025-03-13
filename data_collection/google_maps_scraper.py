"""
Google Maps Reviews Scraper

This script collects customer reviews for bank agencies in Morocco from Google Maps.
It uses Selenium for web scraping to extract detailed information about bank reviews.

Usage:
    python google_maps_scraper.py --output reviews.json --banks "Attijariwafa Bank,Bank of Africa,CIH Bank" --cities "Casablanca,Rabat,Marrakech,Tangier,Fes"
"""

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
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='google_maps_scraper.log',
    filemode='a'
)

logger = logging.getLogger('GoogleMapsScraper')

class GoogleMapsReviewsScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with web scraping method."""
        self.driver = None
        self.headless = headless
        self._setup_webdriver()
    
    def _setup_webdriver(self):
        """Set up the Selenium WebDriver for scraping."""
        options = Options()
        if self.headless:
            options.add_argument('--headless')  # Run in headless mode
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-infobars')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.implicitly_wait(10)
            logger.info("WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Error setting up WebDriver: {e}")
            print(f"Error setting up WebDriver: {e}")
            print("Make sure you have Chrome and ChromeDriver installed.")
            exit(1)
    
    def search_bank_agencies(self, bank_name, city):
        """Search for bank agencies in a specific city in Morocco."""
        logger.info(f"Searching for {bank_name} agencies in {city}, Morocco")
        search_query = f"{bank_name} {city} morocco"
        search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
        
        agencies = []
        try:
            self.driver.get(search_url)
            time.sleep(3)  # Allow page to load
            
            # Handle potential cookies consent popup
            try:
                consent_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Accept') or contains(@aria-label, 'Agree') or contains(text(), 'Accept') or contains(text(), 'Agree')]"))
                )
                consent_button.click()
                time.sleep(1)
            except (TimeoutException, ElementClickInterceptedException, NoSuchElementException):
                logger.info("No consent pop-up found or couldn't click it")
            
            # Wait for search results to load - modern Google Maps results
            agency_elements = WebDriverWait(self.driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.Nv2PK"))
            )
            
            # --- New code: scroll down using new extended agencies container ---
            try:
                agencies_container = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde.ecceSd"))
                )
                logger.info("Found extended agencies container (new feed); using it for scrolling")
                last_count = len(agencies_container.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
                for _ in range(30):
                    self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", agencies_container)
                    time.sleep(1)
                    new_count = len(agencies_container.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
                    if new_count == last_count:
                        break
                    last_count = new_count
                total_agencies = last_count
            except Exception:
                logger.info("New extended agencies container not found; falling back to previous method")
                try:
                    scroll_box = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.section-listbox"))
                    )
                    logger.info("Found agencies container; using it for scrolling")
                    last_count = len(self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
                    for _ in range(20):
                        scroll_box.send_keys(Keys.PAGE_DOWN)
                        time.sleep(1)
                        new_count = len(self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
                        if new_count == last_count:
                            break
                        last_count = new_count
                    total_agencies = last_count
                except Exception:
                    logger.info("Agencies container not found; falling back to window scroll")
                    last_count = len(self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
                    for _ in range(10):
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1)
                        new_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
                        if len(new_elements) == last_count:
                            break
                        last_count = len(new_elements)
                    total_agencies = last_count
            logger.info(f"Found {total_agencies} agencies for {bank_name} in {city}")
            # Remove any agency limit; process all loaded agencies
            max_agencies = total_agencies
            
            for i in range(max_agencies):
                try:
                    # We need to find elements again as the DOM might have changed
                    agency_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
                    element = agency_elements[i]
                    
                    # Extract basic information
                    name_element = element.find_element(By.CSS_SELECTOR, "div.qBF1Pd")
                    name = name_element.text.strip()
                    
                    # Check if this is actually a bank agency (filter out irrelevant results)
                    if not any(bank.lower() in name.lower() for bank in [bank_name.lower(), "bank", "banque", "atm", "agence"]):
                        continue
                        
                    # Get location
                    try:
                        # Try extracting location from the new element structure
                        location = element.find_element(By.CSS_SELECTOR, "div.AeaXub .Io6YTe").text.strip()
                    except NoSuchElementException:
                        try:
                            location = element.find_element(By.CSS_SELECTOR, "div.W4Efsd > div:nth-child(1) > span:nth-child(1)").text.strip()
                        except NoSuchElementException:
                            try:
                                location = element.find_element(By.CSS_SELECTOR, "div.W4Efsd div[jsan]").text.strip()
                            except NoSuchElementException:
                                location = f"{city}, Morocco"
                    
                    # Get rating if available
                    try:
                        rating_text = element.find_element(By.CSS_SELECTOR, "span.MW4etd").text.strip()
                        rating = float(rating_text)
                    except (NoSuchElementException, ValueError):
                        rating = None
                    
                    # Create agency object
                    agency = {
                        "name": name,
                        "location": location,
                        "city": city,
                        "bank": bank_name,
                        "rating": rating
                    }
                    
                    # Click on the agency to navigate to its details page
                    try:
                        ActionChains(self.driver).move_to_element(name_element).click().perform()
                        time.sleep(2)  # Wait for the details page to load
                        
                        # Add Google Maps URL
                        agency["url"] = self.driver.current_url
                        
                        agencies.append(agency)
                        logger.info(f"Added agency: {name} in {city}")
                        
                        # Go back to search results
                        self.driver.back()
                        time.sleep(2)
                    except (ElementClickInterceptedException, StaleElementReferenceException) as e:
                        logger.warning(f"Could not click on agency {name}: {e}")
                        # Try with JavaScript click
                        try:
                            self.driver.execute_script("arguments[0].click();", name_element)
                            time.sleep(2)
                            
                            # Add Google Maps URL
                            agency["url"] = self.driver.current_url
                            
                            agencies.append(agency)
                            logger.info(f"Added agency: {name} in {city} (using JavaScript click)")
                            
                            # Go back to search results
                            self.driver.back()
                            time.sleep(2)
                        except Exception:
                            logger.error(f"Failed to process agency {name}")
                except Exception as e:
                    logger.error(f"Error processing agency at index {i}: {e}")
            
            return agencies
            
        except Exception as e:
            logger.error(f"Error searching for {bank_name} agencies in {city}: {e}")
            return []
    
    def get_reviews(self, agency, max_reviews=20):
        """Get all non-empty reviews for a specific agency by clicking 'more reviews' / 'plus d'avis'."""
        logger.info(f"Getting reviews for {agency['name']} in {agency['city']}")
        reviews = []
        try:
            self.driver.get(agency["url"])
            WebDriverWait(self.driver, 10).until(lambda d: "maps/place" in d.current_url)
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Accept')]"))
                ).click()
            except Exception:
                pass
            # Open reviews column – look for a review button or one with "plus d'avis" text
            try:
                review_btn = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'review') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'plus d'avis') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'more reviews')]"))
                )
                review_btn.click()
            except Exception:
                logger.warning("Review button not found; proceeding anyway")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.jftiEf"))
            )
            # NEW: Click repeatedly on the "Plus d'avis" button to load all reviews
            while True:
                try:
                    more_btn = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, \"Plus d'avis\")]"))
                    )
                    more_btn.click()
                    logger.info("Clicked 'Plus d'avis' button to load additional reviews")
                    time.sleep(1)  # brief pause after click
                except Exception:
                    logger.info("No 'Plus d'avis' button found, proceeding")
                    break
            # Continuously click "more reviews"/"plus d'avis" button if it exists
            while True:
                try:
                    more_btn = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'more reviews') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'plus d'avis')]"))
                    )
                    more_btn.click()
                    time.sleep(1)
                    logger.info("Clicked 'more reviews' button to load additional reviews")
                except Exception:
                    logger.info("No more 'more reviews' button found")
                    break
            # Rapidly scroll the review container until no new reviews load
            scrollable = None
            for sel in ["div[role='feed']", "div.m6QErb"]:
                try:
                    scrollable = self.driver.find_element(By.CSS_SELECTOR, sel)
                    break
                except:
                    continue
            if scrollable:
                last_count = 0
                for _ in range(30):
                    self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
                    time.sleep(0.5)
                    review_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
                    if len(review_elements) == last_count:
                        break
                    last_count = len(review_elements)
            review_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
            logger.info(f"After scrolling: found {len(review_elements)} review elements")
            # Process all review elements and ignore empty reviews
            for i, elem in enumerate(review_elements):
                try:
                    reviewer = "Anonymous"
                    try:
                        names = elem.find_elements(By.CSS_SELECTOR, "div.d4r55, .WNxzHc")
                        if names:
                            reviewer = names[0].text.split("\n")[0].strip()
                    except:
                        pass
                    rating = None
                    try:
                        rating_span = elem.find_element(By.CSS_SELECTOR, "span.kvMYJc")
                        rating_aria = rating_span.get_attribute("aria-label")
                        match = re.search(r'([\d,.]+)', rating_aria)
                        if match:
                            num_str = match.group(1).replace(',', '.')
                            rating = int(float(num_str))
                        else:
                            rating = 0
                    except Exception:
                        try:
                            rating = len(elem.find_elements(By.CSS_SELECTOR, "img[src*='star_active']"))
                        except:
                            rating = 0
                    review_text = ""
                    for sel in ["span.wiI7pd", "span.review-full-text", "div.MyEned"]:
                        try:
                            txt_elems = elem.find_elements(By.CSS_SELECTOR, sel)
                            if txt_elems and txt_elems[0].text.strip():
                                review_text = txt_elems[0].text.strip()
                                break
                        except:
                            continue
                    if not review_text:
                        continue  # skip empty reviews
                    review_date = datetime.now().strftime("%Y-%m-%d")
                    language = "unknown"
                    try:
                        language = detect(review_text)
                    except:
                        pass
                    reviews.append({
                        "agency_name": agency["name"],
                        "bank": agency["bank"],
                        "location": agency["location"],
                        "city": agency["city"],
                        "reviewer": reviewer,
                        "text": review_text,
                        "rating": rating,
                        "date": review_date,
                        "language": language,
                        "url": agency["url"]
                    })
                    logger.info(f"Added review {i+1} for {agency['name']} (rating: {rating})")
                except Exception as e:
                    logger.error(f"Error extracting review {i+1} for {agency['name']}: {e}")
            logger.info(f"Collected {len(reviews)} reviews for {agency['name']}")
            return reviews
        except Exception as e:
            logger.error(f"Error getting reviews for {agency['name']}: {e}")
            return []
    
    def save_to_json(self, reviews, output_file):
        """Save reviews to a JSON file."""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reviews, f, ensure_ascii=False, indent=4)
        logger.info(f"Saved {len(reviews)} reviews to {output_file}")
        print(f"Saved {len(reviews)} reviews to {output_file}")
    
    def save_to_csv(self, reviews, output_file):
        """Save reviews to a CSV file."""
        df = pd.DataFrame(reviews)
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Saved {len(reviews)} reviews to {output_file}")
        print(f"Saved {len(reviews)} reviews to {output_file}")
    
    def close(self):
        """Close the WebDriver."""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")

def main():
    parser = argparse.ArgumentParser(description='Google Maps Reviews Scraper for Bank Agencies in Morocco')
    parser.add_argument('--output', type=str, required=True, help='Output file path (JSON or CSV)')
    parser.add_argument('--banks', type=str, required=True, help='Comma-separated list of bank names')
    parser.add_argument('--cities', type=str, default='Casablanca,Rabat,Marrakech,Tangier,Fes', 
                       help='Comma-separated list of Moroccan cities (default: Casablanca,Rabat,Marrakech,Tangier,Fes)')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--max_reviews', type=int, default=35, help='Maximum number of reviews to collect per agency')
    
    args = parser.parse_args()
    
    # Initialize the scraper
    scraper = GoogleMapsReviewsScraper(headless=args.headless)
    
    all_reviews = []
    bank_names = [name.strip() for name in args.banks.split(',')]
    cities = [city.strip() for city in args.cities.split(',')]
    
    try:
        for bank_name in bank_names:
            logger.info(f"Processing bank: {bank_name}")
            print(f"Processing bank: {bank_name}")
            
            for city in cities:
                logger.info(f"  Searching in city: {city}")
                print(f"  Searching in city: {city}")
                
                agencies = scraper.search_bank_agencies(bank_name, city)
                logger.info(f"  Found {len(agencies)} agencies in {city}")
                print(f"  Found {len(agencies)} agencies in {city}")
                
                for i, agency in enumerate(agencies):
                    logger.info(f"    Processing agency {i+1}/{len(agencies)}: {agency['name']}")
                    print(f"    Processing agency {i+1}/{len(agencies)}: {agency['name']}")
                    
                    reviews = scraper.get_reviews(agency, max_reviews=args.max_reviews)
                    all_reviews.extend(reviews)
                    
                    logger.info(f"    Collected {len(reviews)} reviews for {agency['name']}")
                    print(f"    Collected {len(reviews)} reviews for {agency['name']}")
                    
                    # Sleep to avoid hitting rate limits (random delay between 2-5 seconds)
                    time.sleep(random.uniform(2, 5))
                
                # Sleep between cities
                time.sleep(1)
        
        # Save the results based on file extension
        output_file = args.output
        if not all_reviews:
            logger.warning("No reviews were collected!")
            print("Warning: No reviews were collected!")
        
        if output_file.endswith('.json'):
            scraper.save_to_json(all_reviews, output_file)
        elif output_file.endswith('.csv'):
            scraper.save_to_csv(all_reviews, output_file)
        else:
            logger.error("Unsupported output format. Use .json or .csv")
            print("Error: Unsupported output format. Use .json or .csv")
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\nProcess interrupted by user")
        
        # Save collected reviews so far
        if all_reviews:
            temp_output = f"partial_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            scraper.save_to_json(all_reviews, temp_output)
            print(f"Saved {len(all_reviews)} reviews collected so far to {temp_output}")
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
    
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
