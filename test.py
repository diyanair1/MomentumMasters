from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

options = webdriver.ChromeOptions() 
options.add_argument("--start-maximized") # passing a predifined argument to  the option which makes a full screen browser screen
driver = webdriver.Chrome(options=options) # starting the chrome browser

url = "https://www.barchart.com/stocks/highs-lows?orderBy=lastPrice&orderDir=desc&page=1"
driver.get(url)

def get_shadow_root(driver, host_element):
    return driver.execute_script("return arguments[0].shadowRoot", host_element) # getting the shadow root(where the full grid is stored) from the tag given

time.sleep(5)  # Wait for the page to load completely

print("Scrolling to find 'Show All' button...")
try:
    show_all_button = WebDriverWait(driver, 10).until(  # waits 10 secs for button to show up
        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.show-all[data-ng-click='getAll()']")) #uses selector to select an element by searching for it on the page(using A tag)
    )
    # Scroll to the button to ensure it's in view
    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", show_all_button) # scrolling till the bottom to find the button
    time.sleep(1) # buffer
    show_all_button.click()
    print("Clicked 'Show All' button")
    time.sleep(5)  # Wait for all data to load after clicking
except Exception as e:
    print(f"Could not click 'Show All' button: {e}")

# driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
driver.execute_script("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'});") # scrolling to see ALL the data(as the show all button is clicked)
time.sleep(5)  # Wait for the page to load completely

# 1) find the shadow host
host = WebDriverWait(driver, 10).until( # scroll/ wait for ten secs until element selected from the bc tag
    EC.presence_of_element_located((By.CSS_SELECTOR, "bc-data-grid"))
)

# 2) get its shadow root
shadow_root = get_shadow_root(driver, host) # gets shadow root from the tag

# 3) within the shadow root, find the grid container
grid = shadow_root.find_element(By.CSS_SELECTOR, "div._grid") # contains the full stock table and all rows of it

# 4) all data rows
rows = grid.find_elements(By.CSS_SELECTOR, "set-class.row._grid_columns") # list of rows. each row/element has the data of each stock

data = []
for row in rows:
    try:
        symbol = row.find_element(By.CSS_SELECTOR, "div._cell.symbol").text.strip() # strip() removes extra spaces from each side and removes and \n or \t etc
        symbol_name  = row.find_element(By.CSS_SELECTOR, "div._cell.symbolName").text.strip()
        last_price   = row.find_element(By.CSS_SELECTOR, "div._cell.lastPrice").text.strip()
        volume       = row.find_element(By.CSS_SELECTOR, "div._cell.volume").text.strip()
        high_hits = row.find_element(By.CSS_SELECTOR, "div._cell.highHits1y").text.strip()
        high_percent_diff  = row.find_element(By.CSS_SELECTOR, "div._cell.highPercent1y").text.strip()

        data.append({
            "symbol": symbol,
            "name": symbol_name,
            "last_price": last_price,
            "volume": volume,
            "high_hits": high_hits,
            "high_percent_diff": high_percent_diff,
        })
    except Exception as e:
        print(f"Error processing row: {e}")
        # if any cell is missing, just skip that row
        continue

df = pd.DataFrame(data)
print(df)

# for row in data:
#     print(row)
# print(f"Total rows extracted: {len(data)}")
driver.quit()