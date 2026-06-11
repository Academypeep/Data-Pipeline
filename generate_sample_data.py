"""Generate a realistic sample sales CSV for pipeline testing."""
import csv, random, os
from datetime import date, timedelta

REGIONS = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]
CATEGORIES = ["Hardware", "Software", "Services", "Training", "Licensing"]

def generate(rows=500, output="data/raw_sales.csv"):
    os.makedirs("data", exist_ok=True)
    start = date(2023, 1, 1)
    with open(output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "region", "category", "revenue", "units", "cost"])
        for _ in range(rows):
            d = start + timedelta(days=random.randint(0, 730))
            region   = random.choice(REGIONS)
            category = random.choice(CATEGORIES)
            units    = random.randint(1, 50)
            revenue  = round(units * random.uniform(500, 5000), 2)
            cost     = round(revenue * random.uniform(0.4, 0.75), 2)
            # Introduce ~5% missing values to demonstrate cleaning
            if random.random() < 0.05:
                revenue = ""
            writer.writerow([d.strftime("%d/%m/%Y"), region, category, revenue, units, cost])
    print(f"Generated {rows} rows -> {output}")

if __name__ == "__main__":
    generate()
