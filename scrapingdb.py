
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
    host="library-db.kurian.ca",
    database="mmasters",
    user="postgres",
    password="mmaster@2025"
)
cur = conn.cursor()


# --------------------------
# Selenium setup (from scraping.py)
# --------------------------
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--start-maximized")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-infobars")
options.add_argument("--disable-extensions")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--enable-javascript")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
driver = webdriver.Chrome(options=options)

url = "https://www.barchart.com/stocks/highs-lows?orderBy=lastPrice&orderDir=desc&page=1"
driver.get(url)

def get_shadow_root(driver, host_element):
    return driver.execute_script("return arguments[0].shadowRoot", host_element)

time.sleep(5)  # Wait for the page to load completely

print("Scrolling to find 'Show All' button...")
try:
    show_all_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.show-all[data-ng-click='getAll()']"))
    )
    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", show_all_button)
    time.sleep(1)
    show_all_button.click()
    print("Clicked 'Show All' button")
    time.sleep(5)
except Exception as e:
    print(f"Could not click 'Show All' button: {e}")

driver.execute_script("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'});")
time.sleep(5)

# 1) find the shadow host
try:
    host = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "bc-data-grid"))
    )
except Exception as e:
    print(f"Could not find shadow host: {e}")
    driver.quit()
    cur.close()
    conn.close()
    exit(1)

# 2) get its shadow root
shadow_root = get_shadow_root(driver, host)

# 3) within the shadow root, find the grid container
grid = shadow_root.find_element(By.CSS_SELECTOR, "div._grid")

# 4) all data rows
rows = grid.find_elements(By.CSS_SELECTOR, "set-class.row._grid_columns")

# Scrape data directly as list of tuples for DB insert
data = []
for row in rows:
    try:
        symbol = row.find_element(By.CSS_SELECTOR, "div._cell.symbol").text.strip()
        symbol_name  = row.find_element(By.CSS_SELECTOR, "div._cell.symbolName").text.strip()
        last_price   = row.find_element(By.CSS_SELECTOR, "div._cell.lastPrice").text.strip().replace(",", "")
        volume       = row.find_element(By.CSS_SELECTOR, "div._cell.volume").text.strip().replace(",", "")
        high_hits = row.find_element(By.CSS_SELECTOR, "div._cell.highHits1y").text.strip()
        high_percent_diff  = row.find_element(By.CSS_SELECTOR, "div._cell.highPercent1y").text.strip().replace("%", "")

        # Handle 'unch' and other non-numeric values
        def parse_float(val):
            try:
                if not val or val.lower() == 'unch':
                    return None
                return float(val)
            except Exception:
                return None

        def parse_int(val):
            try:
                if not val or val.lower() == 'unch':
                    return None
                return int(val)
            except Exception:
                return None

        data.append((
            symbol,
            symbol_name,
            parse_float(last_price),
            parse_int(volume),
            parse_float(high_hits),
            parse_float(high_percent_diff)/100 if parse_float(high_percent_diff) is not None else None,
            datetime.now().date()  # Only the date part
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
ON CONFLICT (symbol, date_scraped)
DO UPDATE SET
    stock_name = EXCLUDED.stock_name,
    price = EXCLUDED.price,
    volume = EXCLUDED.volume,
    w52_high = EXCLUDED.w52_high,
    w52_high_pct = EXCLUDED.w52_high_pct;
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
