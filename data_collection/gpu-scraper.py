import argparse
import json
import time
import re
import random
from datetime import datetime
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
from langdetect import detect, LangDetectException
import numpy as np

# For GPU utilization
try:
    import cupy as cp
    HAS_CUPY = True
except ImportError:
    HAS_CUPY = False
    
try:
    import cudf
    HAS_CUDF = True
except ImportError:
    HAS_CUDF = False

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

logger = logging.getLogger('GPUOptimizedGoogleMapsScraper')

class GPUOptimizedGoogleMapsScraper:
    def __init__(self, headless=True, max_workers=8, use_gpu=True):
        """Initialize the scraper with optimized settings for NVIDIA GPUs."""
        self.drivers = []
        self.headless = headless
        self.max_workers = max_workers
        self.use_gpu = use_gpu and (HAS_CUPY or HAS_CUDF)
        
        # If GPU is enabled, report capabilities
        if self.use_gpu:
            logger.info(f"GPU acceleration enabled: CuPy: {HAS_CUPY}, cuDF: {HAS_CUDF}")
            if HAS_CUPY:
                logger.info(f"CUDA Version: {cp.cuda.runtime.runtimeGetVersion()}")
                logger.info(f"GPU Device: {cp.cuda.runtime.getDeviceProperties(0)['name'].decode()}")
        else:
            logger.info("GPU acceleration disabled or required libraries not found")
            
        self.review_selectors = {
            'containers': ["div.jftiEf", "div.gws-localreviews__google-review", "div[data-review-id]", "div.jJc9Ad"],
            'reviewer': ["div.d4r55", ".WNxzHc"],
            'rating': ["span[aria-label*='star' i]", "img[src*='star_active']", "span.vzX5Ic", "span.kvMYJc"],
            'text': ["span.wiI7pd", "span.review-full-text", "div.MyEned", "div.review-content"],
            'date': ["span.rsqaWe", "span.review-date", "span[class*='date']"],
            'more_buttons': ["button.w8nwRe", "button[aria-label*='More']", "button[jsaction*='pane.review']"]
        }
    
    def _setup_webdriver(self):
        """Create an optimized WebDriver instance with GPU acceleration if available."""
        options = Options()
        if self.headless:
            options.add_argument('--headless=new')  # Modern headless mode
        
        # Performance optimizations
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # GPU Acceleration settings
        if self.use_gpu:
            options.add_argument('--enable-gpu')
            options.add_argument('--enable-webgl')
            options.add_argument('--ignore-gpu-blocklist')
            options.add_argument('--use-gl=desktop')  # Use desktop OpenGL
        else:
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
        
        # Other optimizations
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--blink-settings=imagesEnabled=false')  # Disable images
        options.add_argument('--disable-infobars')
        options.add_argument('--incognito')  # Use incognito to avoid caching
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-translate')
        options.add_argument('--lang=en-US')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')
        
        # Page and JavaScript optimizations
        options.add_argument('--js-flags=--max-old-space-size=4096')  # Increase JS memory
        
        options.add_experimental_option('prefs', {
            'profile.default_content_setting_values': {
                'cookies': 2,  # Block cookies
                'images': 2,   # Block images
                'notifications': 2,  # Block notifications
                'plugins': 2,  # Block plugins
                'popups': 2,   # Block popups
                'geolocation': 2,  # Block geolocation
                'auto_select_certificate': 2,  # Block auto select certificate
                'fullscreen': 2,  # Block fullscreen
                'mouselock': 2,  # Block mouselock
                'mixed_script': 2,  # Block mixed script
                'media_stream': 2,  # Block media stream
                'media_stream_mic': 2,  # Block media stream mic
                'media_stream_camera': 2,  # Block media stream camera
                'automatic_downloads': 2,  # Block automatic downloads
                'midi_sysex': 2,  # Block midi sysex
                'push_messaging': 2,  # Block push messaging
                'ssl_cert_decisions': 2,  # Block ssl cert decisions
                'metro_switch_to_desktop': 2,  # Block metro switch to desktop
                'protected_media_identifier': 2,  # Block protected media identifier
                'app_banner': 2,  # Block app banner
                'site_engagement': 2,  # Block site engagement
                'durable_storage': 2  # Block durable storage
            }
        })
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(25)  # Timeout for page load
            driver.set_script_timeout(25)     # Timeout for scripts
            driver.implicitly_wait(3)         # Reduced implicit wait
            return driver
        except Exception as e:
            logger.error(f"Error setting up WebDriver: {e}")
            raise
    
    def _initialize_drivers(self):
        """Initialize multiple WebDriver instances for parallel processing."""
        for _ in range(self.max_workers):
            try:
                driver = self._setup_webdriver()
                self.drivers.append(driver)
            except Exception as e:
                logger.error(f"Failed to initialize driver: {e}")
        
        if not self.drivers:
            raise Exception("Failed to initialize any WebDrivers")
    
    def _dismiss_popups(self, driver):
        """Try to dismiss any consent or cookie popups."""
        try:
            # Common selectors for consent buttons
            for selector in [
                "button[jsaction*='dismiss']", 
                "button[jsaction*='consent']",
                "button[jsaction*='agree']",
                "button[aria-label*='Accept']", 
                "button[aria-label*='Agree']", 
                "[aria-label*='cookie'] button", 
                "[aria-label*='consent'] button"
            ]:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                for button in buttons:
                    if button.is_displayed():
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.3)
                        return True
        except:
            pass
        return False
    
    def search_bank_agencies(self, bank_name, city, driver_index=0):
        """Search for bank agencies in a specific city with optimized queries."""
        logger.info(f"Searching for {bank_name} agencies in {city}")
        driver = self.drivers[driver_index % len(self.drivers)]
        
        # Use more specific search query for better results
        search_query = f"{bank_name} agence {city} morocco"
        search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
        
        agencies = []
        try:
            driver.get(search_url)
            time.sleep(1.5)  # Reduced wait time
            
            # Dismiss any popups
            self._dismiss_popups(driver)
            
            # Wait for search results - using explicit wait with shorter timeout
            try:
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK, div.fontHeadlineSmall"))
                )
            except TimeoutException:
                logger.warning(f"Timeout waiting for search results for {bank_name} in {city}")
                return []
            
            # Get agency elements - try multiple selectors as Google Maps changes often
            agency_elements = []
            for selector in ["div.Nv2PK", "div.fontHeadlineSmall"]:
                agency_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if agency_elements:
                    break
            
            logger.info(f"Found {len(agency_elements)} potential agencies for {bank_name} in {city}")
            
            # Limit to max 8 agencies per city-bank combination for speed
            max_agencies = min(len(agency_elements), 8)
            
            for i in range(max_agencies):
                try:
                    # Re-fetch elements to avoid stale references
                    for selector in ["div.Nv2PK", "div.fontHeadlineSmall"]:
                        agency_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if agency_elements and i < len(agency_elements):
                            element = agency_elements[i]
                            break
                    else:
                        continue
                    
                    # Extract basic information - name is most critical
                    try:
                        name_element = element.find_element(By.CSS_SELECTOR, "div.qBF1Pd, span.fontHeadlineSmall")
                        name = name_element.text.strip()
                    except NoSuchElementException:
                        continue
                    
                    # Quick filter: Skip if not a bank-related result
                    if not any(term.lower() in name.lower() for term in [bank_name.lower(), "bank", "banque", "atm", "agence"]):
                        continue
                    
                    # Get minimal location info
                    location = f"{city}, Morocco"
                    try:
                        loc_elements = element.find_elements(By.CSS_SELECTOR, "div.W4Efsd > div:nth-child(1) > span:nth-child(1), div[aria-label*='address']")
                        if loc_elements:
                            location = loc_elements[0].text.strip() or location
                    except:
                        pass
                    
                    # Click on the agency to get its URL - use JavaScript for faster execution
                    try:
                        driver.execute_script("arguments[0].click();", name_element)
                    except:
                        try:
                            ActionChains(driver).move_to_element(name_element).click().perform()
                        except:
                            continue
                    
                    # Wait for details to load
                    time.sleep(1)
                    
                    # Get current URL which contains the place ID
                    url = driver.current_url
                    
                    # Create agency object with minimal info needed for review collection
                    agency = {
                        "name": name,
                        "location": location,
                        "city": city,
                        "bank": bank_name,
                        "url": url
                    }
                    
                    agencies.append(agency)
                    logger.info(f"Added agency: {name} in {city}")
                    
                    # Go back to results
                    driver.back()
                    time.sleep(0.8)
                    
                except Exception as e:
                    logger.error(f"Error processing agency at index {i}: {e}")
            
            return agencies
            
        except Exception as e:
            logger.error(f"Error searching for {bank_name} agencies in {city}: {e}")
            return []
    
    def get_reviews(self, agency, max_reviews=30, driver_index=0):
        """Get reviews for a specific agency with speed optimizations."""
        logger.info(f"Getting reviews for {agency['name']} in {agency['city']}")
        driver = self.drivers[driver_index % len(self.drivers)]
        
        reviews = []
        try:
            # Navigate to the agency page
            driver.get(agency["url"])
            time.sleep(1.5)
            
            # Dismiss any popups
            self._dismiss_popups(driver)
            
            # Try to find and click reviews section using multiple methods
            review_section_found = False
            
            # Method 1: Look for review buttons with star ratings - using fast JavaScript execution
            try:
                driver.execute_script("""
                    const reviewButtons = document.querySelectorAll('button[aria-label*="star"], button[aria-label*="review"], button[aria-label*="avis"]');
                    for (const button of reviewButtons) {
                        if (button.offsetParent !== null) {
                            button.click();
                            return true;
                        }
                    }
                    return false;
                """)
                time.sleep(1)
                review_section_found = True
            except:
                pass
            
            # Method 2: Look for reviews tab
            if not review_section_found:
                try:
                    driver.execute_script("""
                        const tabs = document.querySelectorAll('button.hh2c6, div.RWPxGd button');
                        for (const tab of tabs) {
                            if (tab.textContent.toLowerCase().includes('review') || tab.textContent.toLowerCase().includes('avis')) {
                                tab.click();
                                return true;
                            }
                        }
                        return false;
                    """)
                    time.sleep(1)
                    review_section_found = True
                except:
                    pass
            
            # Method 3: Scroll to reviews section
            if not review_section_found:
                try:
                    driver.execute_script("""
                        document.querySelectorAll('div.m6QErb, div.DxyBCb, div.fontBodyMedium').forEach(function(el) {
                            if (el.textContent.includes('review') || el.textContent.includes('avis')) {
                                el.scrollIntoView({behavior: 'instant'});
                            }
                        });
                    """)
                    time.sleep(0.8)
                    review_section_found = True
                except:
                    pass
            
            # Find scrollable container for reviews and scroll fast using JS
            scrollable_container = None
            for selector in ["div[role='feed']", "div.m6QErb", "div.DxyBCb.kA9KIf.dS8AEf", "div.lXJj5c.Hk4XGb"]:
                containers = driver.find_elements(By.CSS_SELECTOR, selector)
                for container in containers:
                    if container.is_displayed():
                        scrollable_container = container
                        break
                if scrollable_container:
                    break
            
            # Fast scroll to load reviews quickly
            if scrollable_container:
                # GPU enhanced scrolling - quick pulses with short pauses
                scroll_script = """
                    var container = arguments[0];
                    var currentHeight = 0;
                    var distance = 800;  // Longer distance per scroll
                    
                    for (var i = 0; i < 6; i++) {  // More iterations for more reviews
                        currentHeight += distance;
                        container.scrollTop = currentHeight;
                    }
                    
                    return container.scrollTop;
                """
                driver.execute_script(scroll_script, scrollable_container)
                time.sleep(0.5)  # Single short pause after all scrolling
            
            # Collect review containers using JavaScript for speed
            review_containers = []
            review_selectors = ", ".join(self.review_selectors['containers'])
            review_containers_js = f"""
                return Array.from(document.querySelectorAll('{review_selectors}')).filter(el => el.offsetParent !== null);
            """
            review_containers = driver.execute_script(review_containers_js)
            
            # Process reviews (up to max_reviews) in a faster batch
            processed_count = 0
            review_data = []
            
            for review_element in review_containers[:max_reviews]:
                try:
                    # Try expanding the review if visible - using JavaScript
                    driver.execute_script("""
                        const moreButtons = arguments[0].querySelectorAll('button.w8nwRe, button[aria-label*="More"], button[jsaction*="pane.review"]');
                        for (const btn of moreButtons) {
                            if (btn.offsetParent !== null) {
                                btn.click();
                                return true;
                            }
                        }
                        return false;
                    """, review_element)
                    
                    # Extract data using JavaScript for speed
                    review_data_js = f"""
                        const review = arguments[0];
                        let rating = null;
                        let reviewText = "";
                        
                        // Get rating
                        const ratingEl = review.querySelector('span[aria-label*="star" i]');
                        if (ratingEl) {{
                            const ariaLabel = ratingEl.getAttribute('aria-label');
                            const ratingMatch = ariaLabel.match(/(\\d+)/);
                            if (ratingMatch) {{
                                rating = parseInt(ratingMatch[1]);
                            }}
                        }}
                        
                        if (!rating) {{
                            const stars = review.querySelectorAll('img[src*="star_active"], span.vzX5Ic');
                            if (stars.length > 0) {{
                                rating = stars.length;
                            }}
                        }}
                        
                        // Get review text
                        const textSelectors = {str(self.review_selectors['text'])};
                        for (const selector of textSelectors) {{
                            const textEl = review.querySelector(selector);
                            if (textEl && textEl.textContent.trim()) {{
                                reviewText = textEl.textContent.trim();
                                break;
                            }}
                        }}
                        
                        return {{
                            rating: rating,
                            text: reviewText
                        }};
                    """
                    
                    review_info = driver.execute_script(review_data_js, review_element)
                    
                    # Skip empty reviews
                    if not review_info['text'] and not review_info['rating']:
                        continue
                    
                    # Create review object with essential info
                    review = {
                        "agency_name": agency["name"],
                        "bank": agency["bank"],
                        "city": agency["city"],
                        "text": review_info['text'],
                        "rating": review_info['rating'],
                        "url": agency["url"]
                    }
                    
                    review_data.append(review)
                    processed_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error extracting review for {agency['name']}: {e}")
            
            # Process language detection in parallel with GPU if available
            if review_data:
                if self.use_gpu and HAS_CUDF and len(review_data) > 5:
                    try:
                        # Use GPU-accelerated dataframe for processing
                        reviews_df = cudf.DataFrame(review_data)
                        
                        # Filter out records with empty text
                        text_mask = reviews_df['text'].str.len() > 0
                        texts_to_process = reviews_df.loc[text_mask, 'text'].to_pandas().tolist()
                        
                        # Process language detection in batches on CPU (langdetect doesn't support GPU)
                        languages = []
                        for text in texts_to_process:
                            try:
                                languages.append(detect(text))
                            except LangDetectException:
                                languages.append("unknown")
                        
                        # Assign back to dataframe
                        reviews_df.loc[text_mask, 'language'] = languages
                        
                        # Convert back to list of dicts
                        reviews = reviews_df.to_pandas().to_dict('records')
                    except Exception as e:
                        logger.error(f"Error in GPU-accelerated language detection: {e}")
                        # Fallback to CPU processing
                        reviews = review_data
                        for review in reviews:
                            if review.get('text'):
                                try:
                                    review['language'] = detect(review['text'])
                                except LangDetectException:
                                    review['language'] = "unknown"
                else:
                    # CPU-based processing
                    reviews = review_data
                    for review in reviews:
                        if review.get('text'):
                            try:
                                review['language'] = detect(review['text'])
                            except LangDetectException:
                                review['language'] = "unknown"
            
            logger.info(f"Collected {len(reviews)} reviews for {agency['name']}")
            return reviews
            
        except Exception as e:
            logger.error(f"Error getting reviews for {agency['name']}: {e}")
            return []
    
    def process_bank_city(self, bank_name, city, max_reviews=30, driver_index=0):
        """Process a single bank-city combination."""
        all_reviews = []
        try:
            agencies = self.search_bank_agencies(bank_name, city, driver_index)
            
            for agency in agencies:
                reviews = self.get_reviews(agency, max_reviews, driver_index)
                all_reviews.extend(reviews)
                
                # Smaller random delay between agencies
                time.sleep(random.uniform(0.3, 1.0))
        except Exception as e:
            logger.error(f"Error processing {bank_name} in {city}: {e}")
        
        return all_reviews
    
    def run(self, bank_names, cities, max_reviews=30, output_file="reviews.json"):
        """Run the scraper with parallel processing for speed."""
        self._initialize_drivers()
        all_reviews = []
        
        try:
            # Create tasks for parallel processing
            tasks = []
            driver_index = 0
            
            for bank_name in bank_names:
                for city in cities:
                    tasks.append((bank_name, city, max_reviews, driver_index))
                    driver_index += 1
            
            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(tasks))) as executor:
                # Submit tasks
                future_to_task = {
                    executor.submit(self.process_bank_city, bank, city, max_reviews, idx): (bank, city)
                    for idx, (bank, city, max_reviews, _) in enumerate(tasks)
                }
                
                # Collect results
                for future in future_to_task:
                    bank, city = future_to_task[future]
                    try:
                        reviews = future.result()
                        all_reviews.extend(reviews)
                        print(f"Collected {len(reviews)} reviews for {bank} in {city}")
                    except Exception as e:
                        logger.error(f"Error in task for {bank} in {city}: {e}")
            
            # Process and save results with GPU acceleration if available
            if all_reviews:
                if self.use_gpu and HAS_CUDF:
                    try:
                        # Use GPU-accelerated DataFrame processing
                        print(f"Processing {len(all_reviews)} reviews with GPU acceleration...")
                        
                        # Convert to GPU DataFrame for faster processing
                        gdf = cudf.DataFrame(all_reviews)
                        
                        # Perform any needed transformations or filtering
                        # Example: Filter out reviews without text or with very short text
                        if 'text' in gdf.columns:
                            gdf = gdf[gdf['text'].str.len() > 5]
                        
                        # Convert back to CPU for saving
                        all_reviews = gdf.to_pandas().to_dict('records')
                        print(f"GPU processing complete. {len(all_reviews)} reviews remain after filtering.")
                    except Exception as e:
                        logger.error(f"Error in GPU data processing: {e}")
                
                # Save results
                if output_file.endswith('.json'):
                    self.save_to_json(all_reviews, output_file)
                elif output_file.endswith('.csv'):
                    self.save_to_csv(all_reviews, output_file)
                else:
                    output_file = f"{output_file}.json"
                    self.save_to_json(all_reviews, output_file)
            else:
                logger.warning("No reviews collected!")
                print("Warning: No reviews were collected!")
            
            return all_reviews
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
            print("\nProcess interrupted by user")
            
            # Save partial results
            if all_reviews:
                temp_output = f"partial_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                self.save_to_json(all_reviews, temp_output)
                print(f"Saved {len(all_reviews)} reviews collected so far to {temp_output}")
            
            return all_reviews
            
        except Exception as e:
            logger.error(f"Error in scraper run: {e}")
            print(f"Error in scraper run: {e}")
            return all_reviews
            
        finally:
            self.close()
    
    def save_to_json(self, reviews, output_file):
        """Save reviews to a JSON file."""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reviews, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(reviews)} reviews to {output_file}")
        print(f"Saved {len(reviews)} reviews to {output_file}")
    
    def save_to_csv(self, reviews, output_file):
        """Save reviews to a CSV file with GPU acceleration if available."""
        if self.use_gpu and HAS_CUDF and len(reviews) > 1000:
            try:
                # Use GPU-accelerated CSV writing
                gdf = cudf.DataFrame(reviews)
                gdf.to_csv(output_file, index=False)
            except Exception as e:
                logger.error(f"Error in GPU CSV export: {e}")
                # Fallback to pandas
                df = pd.DataFrame(reviews)
                df.to_csv(output_file, index=False, encoding='utf-8')
        else:
            # Standard pandas export
            df = pd.DataFrame(reviews)
            df.to_csv(output_file, index=False, encoding='utf-8')
            
        logger.info(f"Saved {len(reviews)} reviews to {output_file}")
        print(f"Saved {len(reviews)} reviews to {output_file}")
    
    def close(self):
        """Close all WebDriver instances."""
        for driver in self.drivers:
            try:
                driver.quit()
            except:
                pass
        logger.info("All WebDriver instances closed")
        
        # Clean up GPU memory if used
        if self.use_gpu and HAS_CUPY:
            try:
                cp.get_default_memory_pool().free_all_blocks()
                logger.info("GPU memory cleaned")
            except:
                pass

def main():
    parser = argparse.ArgumentParser(description='GPU-Optimized Google Maps Reviews Scraper for Bank Agencies in Morocco')
    parser.add_argument('--output', type=str, default='bank_reviews.json', help='Output file path (JSON or CSV)')
    parser.add_argument('--banks', type=str, required=True, help='Comma-separated list of bank names')
    parser.add_argument('--cities', type=str, default='Casablanca,Rabat,Marrakech', 
                       help='Comma-separated list of Moroccan cities')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode', default=True)
    parser.add_argument('--max_reviews', type=int, default=30, help='Maximum number of reviews to collect per agency')
    parser.add_argument('--max_workers', type=int, default=8, help='Maximum number of parallel browser instances')
    parser.add_argument('--use_gpu', action='store_true', help='Enable GPU acceleration', default=True)
    
    args = parser.parse_args()
    
    # Parse banks and cities
    bank_names = [name.strip() for name in args.banks.split(',')]
    cities = [city.strip() for city in args.cities.split(',')]
    
    # Initialize and run the scraper
    scraper = GPUOptimizedGoogleMapsScraper(headless=args.headless, max_workers=args.max_workers, use_gpu=args.use_gpu)
    scraper.run(bank_names, cities, args.max_reviews, args.output)

if __name__ == "__main__":
    main()
