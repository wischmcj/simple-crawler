from selenium import webdriver
import time

DRIVER_PATH = '/path/to/chromedriver'
driver = webdriver.Chrome(executable_path=DRIVER_PATH)
driver.get("https://example.com/infinite-scroll-page")

# Function to scroll to the bottom
def scroll_to_bottom(driver):
    old_position = driver.execute_script("return window.pageYOffset;")
    while True:
        # Execute JavaScript to scroll down
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # Wait for the page to load
        time.sleep(3)  # This delay will depend on the connection speed and server response time
        new_position = driver.execute_script("return window.pageYOffset;")
        if new_position == old_position:
            break  # Exit the loop if the page hasn't scrolled, meaning end of page
        old_position = new_position

scroll_to_bottom(driver)

# Now you can perform any actions on the content loaded
# Example: extract data, take screenshots, etc.

driver.quit()