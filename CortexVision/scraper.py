from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
import os
import requests
from bs4 import BeautifulSoup
import urllib.parse
import base64

class WebScraper:
    def __init__(self):
        """Initialize the web scraper with Chrome WebDriver"""
        self.initialize_driver()
        
    def initialize_driver(self):
        """Initialize and configure the Chrome WebDriver"""
        chrome_options = webdriver.ChromeOptions()
        # Add options for better scraping
        chrome_options.add_argument('--headless')  # Run in headless mode
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)  # 10 seconds timeout
                
    def scrape_data(self, search_term="chair", max_products=5):
        """
        Scrape product data from the search results
        Args:
            search_term (str): The product to search for
            max_products (int): Maximum number of products to scrape
        Returns:
            pd.DataFrame: Scraped data in a pandas DataFrame
        """
        if not self.search_products(search_term):
            return pd.DataFrame()
            
        try:
            # Wait for product grid to load
            product_grid = self.wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "div[data-component-type='s-search-result']")
                )
            )
            
            products_data = []
            for product in product_grid[:max_products]:
                try:
                    # Extract product details
                    title = product.find_element(
                        By.CSS_SELECTOR, "h2 span.a-text-normal"
                    ).text
                    
                    image_element = product.find_element(
                        By.CSS_SELECTOR, "img.s-image"
                    )
                    image_url = image_element.get_attribute("src")
                    
                    # Get price if available
                    try:
                        price = product.find_element(
                            By.CSS_SELECTOR, "span.a-price-whole"
                        ).text
                    except:
                        price = "N/A"
                        
                    products_data.append({
                        "title": title,
                        "image_url": image_url,
                        "price": price
                    })
                    
                    # Print the data for verification
                    print(f"\nProduct found:")
                    print(f"Title: {title}")
                    print(f"Image URL: {image_url}")
                    print(f"Price: {price}")
                    print("-" * 50)
                    
                except Exception as e:
                    print(f"Error extracting product details: {str(e)}")
                    continue
                    
            return pd.DataFrame(products_data)
            
        except Exception as e:
            print(f"Error during scraping: {str(e)}")
            return pd.DataFrame()
    
    def save_to_csv(self, data, filename='scraped_data.csv'):
        """
        Save scraped data to CSV
        Args:
            data (pd.DataFrame): The data to save
            filename (str): The name of the CSV file
        """
        data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        
    def close(self):
        """Close the WebDriver"""
        self.driver.quit()
        
    def download_google_images(self, search_query, num_images=5, output_dir='downloaded_images'):
        """
        Search Google Images and download images
        Args:
            search_query (str): The search term
            num_images (int): Number of images to download
            output_dir (str): Directory to save images
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        query = urllib.parse.quote(search_query)
        url = f"https://www.google.com/search?q={query}&tbm=isch"

        try:
            print(f"Navigating to Google Images...")
            self.driver.get(url)
            time.sleep(0.7)  # Wait a bit longer

            print(f"Page title: {self.driver.title}")
            print(f"Current URL: {self.driver.current_url}")

            # Try different selectors with debugging
            selectors = [
                #("div.isv-r img", "div.isv-r img"),  # Main image container
                #("img.rg_i", "Thumbnail images"),
                #("img[jsname='Q4LuWd']", "JSName images"),
                ("div.H8Rx8c img", "Image containers")
            ]
            
            img_results = []
            print("\nTrying different selectors:")
            for selector, desc in selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"- {desc}: found {len(elements)} elements")
                if elements:
                    img_results = elements
                    print(f"Using selector: {selector}")
                    break
            
            if not img_results:
                print("\nDebug: Page source preview:")
                print(self.driver.page_source[:500])
                print("\nNo images found with any selector")
                return False

            print(f"\nFound {len(img_results)} potential images")
            downloaded_count = 0
            start_time = time.time()

            for idx, img in enumerate(img_results):
                if downloaded_count >= num_images:
                    break

                try:
                    print(f"\nProcessing image {idx + 1}:")
                    
                    # Ensure element is visible to trigger lazy loading
                    try:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", img)
                        time.sleep(0.2)
                    except Exception:
                        # ignore scrolling errors
                        pass
                    
                    # Try to get all possible attributes (covers lazy-loaded attributes)
                    src = img.get_attribute('src')
                    data_src = img.get_attribute('data-src')
                    data_iurl = img.get_attribute('data-iurl') or img.get_attribute('data-url')
                    srcset = img.get_attribute('srcset')
                    
                    print(f"- src: {src}")
                    print(f"- data-src: {data_src}")
                    print(f"- data-iurl: {data_iurl}")
                    print(f"- srcset: {srcset}")

                    # If srcset exists, try to extract the first URL
                    if srcset and not (src and src.strip() and not src.startswith('data:')):
                        try:
                            first_srcset = srcset.split(',')[0].strip().split(' ')[0]
                            if first_srcset:
                                srcset_url = first_srcset
                            else:
                                srcset_url = None
                        except Exception:
                            srcset_url = None
                    else:
                        srcset_url = None

                    # prioritize attributes: src, data-src, data-iurl, srcset
                    image_url = None
                    for candidate in (src, data_src, data_iurl, srcset_url):
                        if candidate and candidate.strip():
                            image_url = candidate.strip()
                            break

                    # Some URLs are protocol-relative or relative -> normalize
                    if image_url and image_url.startswith('//'):
                        image_url = 'https:' + image_url
                    elif image_url and image_url.startswith('/'):
                        image_url = urllib.parse.urljoin(self.driver.current_url, image_url)

                    # If it's a data URL (base64), decode and save directly
                    if image_url and image_url.startswith('data:'):
                        try:
                            header, b64data = image_url.split(',', 1)
                            if ';base64' in header:
                                # get MIME type and convert to extension
                                mime = header.split(';')[0].split(':')[-1] if ':' in header else 'image/jpeg'
                                ext = mime.split('/')[-1].lower()
                                if ext == 'jpeg':
                                    ext = 'jpg'
                                if not ext:
                                    ext = 'jpg'
                                file_name = f"{search_query}_{downloaded_count}.{ext}"
                                file_path = os.path.join(output_dir, file_name)
                                image_bytes = base64.b64decode(b64data)
                                with open(file_path, 'wb') as f:
                                    f.write(image_bytes)
                                print(f"✓ Decoded and saved data URL as: {file_name}")
                                downloaded_count += 1
                                continue
                            else:
                                print("- Data URL not base64; skipping")
                                continue
                        except Exception as e:
                            print(f"- Failed to decode data URL: {e}")
                            continue

                    if not image_url:
                        print("- Skipping: No valid URL found")
                        continue

                    print(f"- Attempting download from: {image_url}")
                    
                    with requests.Session() as session:
                        session.headers.update({
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                        })
                        try:
                            response = session.get(image_url, timeout=5)
                            size = int(response.headers.get('content-length', 0))
                            # allow smaller images too (don't require >1000 bytes for all cases)
                            if response.status_code == 200 and size != 0:
                                # try to get extension from response headers or fallback to jpg
                                ctype = response.headers.get('content-type', '')
                                ext = 'jpg'
                                if ctype and '/' in ctype:
                                    ext_candidate = ctype.split('/')[-1].split(';')[0]
                                    if ext_candidate == 'jpeg':
                                        ext = 'jpg'
                                    elif ext_candidate:
                                        ext = ext_candidate
                                file_name = f"{search_query}_{downloaded_count}.{ext}"
                                file_path = os.path.join(output_dir, file_name)
                                
                                with open(file_path, 'wb') as f:
                                    f.write(response.content)
                                
                                print(f"✓ Successfully downloaded: {file_name}")
                                downloaded_count += 1
                            else:
                                print(f"- Skip: Bad response (status: {response.status_code}, size: {size} bytes)")
                        except Exception as e:
                            print(f"- Download failed: {str(e)}")
                            continue

                except Exception as e:
                    print(f"- Error processing image: {str(e)}")
                    continue

            print(f"\nSummary: Downloaded {downloaded_count}/{num_images} images in {int(time.time() - start_time)} seconds")
            return downloaded_count > 0

        except Exception as e:
            print(f"Critical error: {str(e)}")
            return False