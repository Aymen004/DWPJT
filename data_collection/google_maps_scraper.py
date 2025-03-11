"""
Google Maps Reviews Scraper

This script collects customer reviews for bank agencies in Morocco from Google Maps.
It uses Selenium for web scraping to extract detailed information about bank reviews.

Usage:
    python google_maps_scraper.py --output reviews.json --banks "Attijariwafa Bank,Bank of Africa,CIH Bank" --cities "Casablanca,Rabat,Marrakech,Tangier,Fes"
"""

import argparse
import json
import os
import time
import re
import random
from datetime import datetime
import pandas as pd
import logging
from langdetect import detect, LangDetectException

# For web scraping
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
            
            logger.info(f"Found {len(agency_elements)} agencies for {bank_name} in {city}")
            
            # Limit to max 10 agencies per city-bank combination to avoid exceeding limits
            max_agencies = min(len(agency_elements), 10)
            
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
        """Get reviews for a specific agency."""
        logger.info(f"Getting reviews for {agency['name']} in {agency['city']}")
        
        reviews = []
        try:
            # Navigate to the agency page
            self.driver.get(agency["url"])
            time.sleep(5)  # Increased wait time for page to load
            
            # Check if we need to accept cookies
            try:
                cookie_buttons = self.driver.find_elements(By.XPATH, 
                    "//button[contains(@aria-label, 'Accept') or contains(text(), 'Accept') or contains(text(), 'Agree')]")
                if cookie_buttons:
                    cookie_buttons[0].click()
                    time.sleep(1)
                    logger.info("Accepted cookies dialog")
            except Exception as e:
                logger.info(f"No cookie dialog or couldn't handle it: {e}")
            
            # Look for reviews section - try multiple methods as Google Maps changes frequently
            logger.info("Looking for reviews section")
            found_reviews_section = False
            
            # Method 1: Look for review count button
            try:
                review_elements = self.driver.find_elements(By.XPATH, 
                    "//button[contains(@aria-label, 'review') or contains(@aria-label, 'avis') or contains(@aria-label, '★')]")
                
                if review_elements:
                    logger.info(f"Found review button (Method 1): {review_elements[0].get_attribute('aria-label')}")
                    review_elements[0].click()
                    time.sleep(3)
                    found_reviews_section = True
            except Exception as e:
                logger.warning(f"Method 1 failed: {e}")
            
            # Method 2: Try finding the reviews tab - recent Google Maps versions
            if not found_reviews_section:
                try:
                    tabs = self.driver.find_elements(By.CSS_SELECTOR, "button.hh2c6")
                    for tab in tabs:
                        try:
                            if "review" in tab.text.lower() or "avis" in tab.text.lower():
                                logger.info(f"Found reviews tab (Method 2): {tab.text}")
                                tab.click()
                                time.sleep(3)
                                found_reviews_section = True
                                break
                        except:
                            continue
                except Exception as e:
                    logger.warning(f"Method 2 failed: {e}")
            
            # Method 3: Try clicking on ratings section
            if not found_reviews_section:
                try:
                    rating_sections = self.driver.find_elements(By.XPATH, 
                        "//div[contains(@aria-label, 'star') or contains(@aria-label, 'étoile')]")
                    if rating_sections:
                        for section in rating_sections:
                            if section.is_displayed() and section.is_enabled():
                                logger.info("Found ratings section (Method 3)")
                                self.driver.execute_script("arguments[0].click();", section)
                                time.sleep(3)
                                found_reviews_section = True
                                break
                except Exception as e:
                    logger.warning(f"Method 3 failed: {e}")
            
            # Method 4: Try to find by specific section structure
            if not found_reviews_section:
                try:
                    sections = self.driver.find_elements(By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf")
                    if sections and len(sections) >= 2:
                        logger.info("Found reviews section by structure (Method 4)")
                        self.driver.execute_script("arguments[0].scrollIntoView();", sections[1])
                        time.sleep(1)
                        found_reviews_section = True
                except Exception as e:
                    logger.warning(f"Method 4 failed: {e}")
            
            if not found_reviews_section:
                logger.warning("Could not find reviews section using any method")
                # Take screenshot for debugging
                try:
                    screenshot_path = f"debug_screenshot_{agency['name'].replace(' ', '_')}.png"
                    self.driver.save_screenshot(screenshot_path)
                    logger.info(f"Saved debug screenshot to {screenshot_path}")
                except:
                    pass
                return []
            
            # Wait for reviews to load - try different possible selectors
            review_containers = []
            selectors_to_try = [
                "div.jftiEf", 
                "div.gws-localreviews__google-review", 
                "div[data-review-id]",
                "div.jJc9Ad",
                "div[class*='review']"
            ]
            
            for selector in selectors_to_try:
                try:
                    logger.info(f"Trying to find reviews with selector: {selector}")
                    review_containers = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if review_containers:
                        logger.info(f"Found {len(review_containers)} review containers with selector: {selector}")
                        break
                except Exception as e:
                    logger.warning(f"Selector {selector} failed: {e}")
            
            if not review_containers:
                logger.warning("Could not find any review elements")
                return []
            
            # Try to identify scrollable container
            scrollable_containers = []
            scroll_selectors = [
                "div[role='feed']",
                "div.m6QErb",
                "div.m6QErb.DxyBCb.kA9KIf.dS8AEf",
                "div.DxyBCb.kA9KIf.dS8AEf",
                "div.lXJj5c.Hk4XGb"
            ]
            
            for selector in scroll_selectors:
                try:
                    containers = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for container in containers:
                        if container.is_displayed():
                            scrollable_containers.append(container)
                except:
                    pass
            
            # Scroll multiple times to load more reviews
            if scrollable_containers:
                scrollable_div = scrollable_containers[0]
                logger.info("Scrolling to load more reviews")
                
                for i in range(min(15, max(1, max_reviews // 3))):
                    try:
                        self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
                        time.sleep(0.5)
                        
                        # Try to expand any collapsed reviews
                        try:
                            more_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.w8nwRe")
                            for btn in more_buttons[:5]:  # Limit to first few to avoid spending too much time
                                if btn.is_displayed():
                                    self.driver.execute_script("arguments[0].click();", btn)
                                    time.sleep(0.2)
                        except:
                            pass
                    except Exception as e:
                        logger.warning(f"Error during scrolling iteration {i}: {e}")
            else:
                logger.warning("Could not find scrollable container")
            
            # Re-fetch review elements after scrolling
            for selector in selectors_to_try:
                try:
                    review_containers = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if review_containers:
                        logger.info(f"After scrolling: found {len(review_containers)} review elements with selector: {selector}")
                        break
                except:
                    pass
            
            # Process each review
            for i, review_element in enumerate(review_containers[:max_reviews]):
                try:
                    # Get reviewer name (useful for debugging)
                    reviewer = "Anonymous"
                    try:
                        name_elements = review_element.find_elements(By.CSS_SELECTOR, "div.d4r55, .WNxzHc")
                        if name_elements:
                            reviewer = name_elements[0].text.strip()
                    except:
                        pass
                    
                    # Get rating
                    rating = None
                    try:
                        # Try multiple methods to extract rating
                        rating_methods = [
                            # Method 1: From aria-label
                            lambda: int(re.search(r'(\d+)', 
                                review_element.find_element(By.CSS_SELECTOR, "span[aria-label*='star' i]").get_attribute("aria-label")).group(1)),
                            
                            # Method 2: Count filled stars
                            lambda: len(review_element.find_elements(By.CSS_SELECTOR, "img[src*='star_active'], span.vzX5Ic")),
                            
                            # Method 3: From specific rating element
                            lambda: int(float(review_element.find_element(By.CSS_SELECTOR, "span.kvMYJc").get_attribute("aria-label").split()[0])),
                        ]
                        
                        for method in rating_methods:
                            try:
                                rating = method()
                                if rating and 1 <= rating <= 5:
                                    break
                            except:
                                continue
                    except:
                        pass
                    
                    # Get review text
                    review_text = ""
                    try:
                        # Try different possible text containers
                        text_selectors = [
                            "span.wiI7pd",
                            "span.review-full-text",
                            "div.MyEned",
                            "div.review-content"
                        ]
                        
                        for selector in text_selectors:
                            try:
                                elements = review_element.find_elements(By.CSS_SELECTOR, selector)
                                if elements:
                                    review_text = elements[0].text.strip()
                                    if review_text:
                                        break
                            except:
                                continue
                        
                        # If still no text, try to expand review first
                        if not review_text:
                            try:
                                more_buttons = review_element.find_elements(By.CSS_SELECTOR, "button.w8nwRe, button[aria-label*='More'], button[jsaction*='pane.review']")
                                for btn in more_buttons:
                                    if btn.is_displayed():
                                        self.driver.execute_script("arguments[0].click();", btn)
                                        time.sleep(0.5)
                                        
                                        # Try again to get text after expanding
                                        for selector in text_selectors:
                                            try:
                                                elements = review_element.find_elements(By.CSS_SELECTOR, selector)
                                                if elements:
                                                    review_text = elements[0].text.strip()
                                                    if review_text:
                                                        break
                                            except:
                                                continue
                            except:
                                pass
                    except:
                        pass
                    
                    # Get review date
                    review_date = datetime.now().strftime("%Y-%m-%d")
                    try:
                        date_selectors = ["span.rsqaWe", "span.review-date", "span[class*='date']"]
                        
                        for selector in date_selectors:
                            try:
                                date_elements = review_element.find_elements(By.CSS_SELECTOR, selector)
                                if date_elements:
                                    date_text = date_elements[0].text.strip()
                                    if date_text:
                                        # Convert relative date to actual date
                                        current_date = datetime.now()
                                        if "week" in date_text.lower() or "semaine" in date_text.lower():
                                            weeks = int(re.search(r'(\d+)', date_text).group(1))
                                            review_date = (current_date - pd.Timedelta(weeks=weeks)).strftime("%Y-%m-%d")
                                        elif "month" in date_text.lower() or "mois" in date_text.lower():
                                            months = int(re.search(r'(\d+)', date_text).group(1))
                                            review_date = (current_date - pd.Timedelta(days=months*30)).strftime("%Y-%m-%d")
                                        elif "year" in date_text.lower() or "an" in date_text.lower():
                                            years = int(re.search(r'(\d+)', date_text).group(1))
                                            review_date = (current_date - pd.Timedelta(days=years*365)).strftime("%Y-%m-%d")
                                        elif "day" in date_text.lower() or "jour" in date_text.lower() or "today" in date_text.lower():
                                            if "today" in date_text.lower():
                                                days = 0
                                            else:
                                                days = int(re.search(r'(\d+)', date_text).group(1))
                                            review_date = (current_date - pd.Timedelta(days=days)).strftime("%Y-%m-%d")
                                        break
                            except:
                                continue
                    except:
                        pass
                    
                    # Detect language if review text exists
                    language = "unknown"
                    if review_text:
                        try:
                            language = detect(review_text)
                        except LangDetectException:
                            pass
                    
                    # Only add reviews with actual content
                    if review_text or rating:
                        # Create review object
                        review = {
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
                        }
                        
                        reviews.append(review)
                        logger.info(f"Added review {i+1} for {agency['name']}: '{review_text[:30]}{'...' if len(review_text) > 30 else ''}' (rating: {rating})")
                    
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
    parser.add_argument('--max_reviews', type=int, default=20, help='Maximum number of reviews to collect per agency')
    
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
