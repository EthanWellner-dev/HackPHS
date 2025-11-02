from scraper import WebScraper
import os

def test_google_images():
    print("\n=== Testing Google Image Download ===")
    scraper = WebScraper()
    try:
        test_query = "Orange Water Bottle"
        test_dir = "test_images"
        
        # Clean up previous test images if they exist
        if os.path.exists(test_dir):
            for file in os.listdir(test_dir):
                os.remove(os.path.join(test_dir, file))
        
        success = scraper.download_google_images(
            search_query=test_query,
            num_images=100,
            output_dir=test_dir
        )
        
        if success and os.path.exists(test_dir):
            downloaded_files = os.listdir(test_dir)
            if downloaded_files:
                print(f"\n✓ Successfully downloaded {len(downloaded_files)} images")
                print(f"✓ Images saved in: {test_dir}/")
                return True
        return False
    except Exception as e:
        print(f"✗ Google image download failed: {str(e)}")
        return False

def run_test():
    try:
        scraper = WebScraper()
        
        print("Starting test suite...")
        
        google_result = test_google_images()
        
        print("\n=== Test Results Summary ===")
        print(f"Google Images: {'✓ PASSED' if google_result else '✗ FAILED'}")
        
    except Exception as e:
        print(f"\nTest suite failed: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    run_test()