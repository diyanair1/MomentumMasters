from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# --------------------------
# Database connection
# --------------------------
conn = psycopg2.connect(
    host="localhost",
    database="mmasters",
    user="postgres",
    password="mmaster@2025"
)
cur = conn.cursor()

# --------------------------
# Selenium setup
# --------------------------
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument("--headless")
driver = webdriver.Chrome(options=options)

url = "https://www.barchart.com/stocks/highs-lows?orderBy=lastPrice&orderDir=desc&page=1"
driver.get(url)

def get_shadow_root(driver, host_element):
    return driver.execute_script("return arguments[0].shadowRoot", host_element)

time.sleep(5)  # Wait for page load

# Click 'Show All' button if present
try:
    show_all_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.show-all[data-ng-click='getAll()']"))
    )
    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", show_all_button)
    time.sleep(1)
    show_all_button.click()
    time.sleep(5)
except Exception as e:
    print(f"Could not click 'Show All': {e}")

# Scroll to bottom
driver.execute_script("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'});")
time.sleep(5)

# Access shadow DOM grid
host = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "bc-data-grid")))
shadow_root = get_shadow_root(driver, host)
grid = shadow_root.find_element(By.CSS_SELECTOR, "div._grid")
rows = grid.find_elements(By.CSS_SELECTOR, "set-class.row._grid_columns")

# --------------------------
# Scrape data
# --------------------------
data = []
for row in rows:
    try:
        symbol = row.find_element(By.CSS_SELECTOR, "div._cell.symbol").text.strip()
        name = row.find_element(By.CSS_SELECTOR, "div._cell.symbolName").text.strip()
        last_price = row.find_element(By.CSS_SELECTOR, "div._cell.lastPrice").text.strip().replace(",", "")
        volume = row.find_element(By.CSS_SELECTOR, "div._cell.volume").text.strip().replace(",", "")
        high_hits = row.find_element(By.CSS_SELECTOR, "div._cell.highHits1y").text.strip()
        high_percent_diff = row.find_element(By.CSS_SELECTOR, "div._cell.highPercent1y").text.strip().replace("%", "")

        data.append((
            symbol,
            name,
            float(last_price) if last_price else None,
            int(volume) if volume else None,
            float(high_hits) if high_hits else None,
            float(high_percent_diff)/100 if high_percent_diff else None,
            datetime.now()  # date_scraped
        ))
    except Exception as e:
        print(f"Error processing row: {e}")
        continue

# --------------------------
# Insert into PostgreSQL
# --------------------------
insert_query = """
INSERT INTO market_data
(symbol, stock_name, price, volume, w52_high, w52_high_pct, date_scraped)
VALUES %s
ON CONFLICT (symbol, date_scraped) DO NOTHING;
"""

if data:
    execute_values(cur, insert_query, data)
    conn.commit()
    print(f"{len(data)} rows inserted into market_data")
else:
    print("No data to insert")

# --------------------------
# Cleanup
# --------------------------
cur.close()
conn.close()
driver.quit()
