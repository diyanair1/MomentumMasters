import psycopg2
from datetime import datetime

# --------------------------
# Database connection
# --------------------------
conn = psycopg2.connect(
    host="192.168.1.202",
    database="mmasters",
    user="nidhi",
    password="mmaster@2025",
    port=5432
)
cur = conn.cursor()

# --------------------------
# Config
# --------------------------
TOTAL_PORTFOLIO_VALUE = 500  # for testing
DAY_ID = 3  # Today’s day_id

# --------------------------
# Helper functions
# --------------------------

def get_top_5_stocks():
    """
    Select top 5 stocks based on closest to 52-week high (w52_high_pct).
    """
    cur.execute("""
        SELECT symbol, stock_name, price
        FROM market_data
        WHERE price IS NOT NULL
          AND volume IS NOT NULL
          AND w52_high IS NOT NULL
        ORDER BY w52_high_pct DESC
        LIMIT 5;
    """)
    return cur.fetchall()  # list of tuples: (symbol, stock_name, price)

def get_active_positions(day_id):
    """
    Get currently active positions for the day.
    """
    cur.execute("""
        SELECT symbol, shares, initial_investment
        FROM portfolio_positions
        WHERE status = 'active' AND day_id = %s;
    """, (day_id,))
    return cur.fetchall()

def insert_new_positions(day_id, new_stocks):
    """
    Add new stocks to portfolio_positions and log in algorithm_decision.
    """
    initial_investment = TOTAL_PORTFOLIO_VALUE / 5
    now = datetime.now()

    for symbol, stock_name, price in new_stocks:
        shares = initial_investment / price
        # Insert into portfolio_positions
        cur.execute("""
            INSERT INTO portfolio_positions
            (symbol, stock_name, added_at, entry_price, initial_investment, shares, status, created_at, updated_at, day_id)
            VALUES (%s, %s, %s, %s, %s, %s, 'active', %s, %s, %s)
        """, (symbol, stock_name, now, price, initial_investment, shares, now, now, day_id))

        # Log decision
        cur.execute("""
            INSERT INTO algorithm_decision (day_id, action, symbol, created_at)
            VALUES (%s, 'add', %s, %s)
            ON CONFLICT (day_id, symbol, action) DO NOTHING;
        """, (day_id, symbol, now))

def compute_portfolio_performance(day_id):
    """
    Compute portfolio performance for active stocks and insert snapshot.
    """
    # Fetch active positions and latest prices
    cur.execute("""
        SELECT p.symbol, p.shares, p.initial_investment, m.price
        FROM portfolio_positions p
        JOIN market_data m ON p.symbol = m.symbol
        WHERE p.status = 'active' AND p.day_id = %s;
    """, (day_id,))
    positions = cur.fetchall()

    total_investment = sum([row[2] for row in positions])
    current_value = sum([row[1] * row[3] for row in positions])
    total_return = current_value - total_investment
    return_percentage = (total_return / total_investment) * 100 if total_investment else 0

    # Insert into portfolio_performance
    cur.execute("""
        INSERT INTO portfolio_performance
        (day_id, total_investment, current_value, total_return, return_percentage, active_positions, snapshot_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (day_id, total_investment, current_value, total_return, return_percentage, len(positions), datetime.now()))

# --------------------------
# Main Script
# --------------------------

try:
    # Start transaction
    conn.autocommit = False

    top_5 = get_top_5_stocks()
    if len(top_5) < 5:
        raise ValueError("Not enough valid symbols to select top 5.")

    # Check existing active positions
    active = get_active_positions(DAY_ID)
    active_symbols = [row[0] for row in active]

    # Determine which stocks to add
    top_symbols = [row[0] for row in top_5]
    to_add = [row for row in top_5 if row[0] not in active_symbols]

    # Add new positions and log decisions
    insert_new_positions(DAY_ID, to_add)

    # Compute portfolio performance snapshot
    compute_portfolio_performance(DAY_ID)

    # Commit all changes
    conn.commit()
    print(f"Portfolio update for day_id={DAY_ID} complete.")
    print(f"Added symbols: {[row[0] for row in to_add]}")

except Exception as e:
    conn.rollback()
    print("Error during portfolio update:", e)

finally:
    cur.close()
    conn.close()
