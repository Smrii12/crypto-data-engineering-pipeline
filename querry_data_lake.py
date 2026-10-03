import os
import json
import sqlite3
from datetime import datetime

print("Initializing Analytical Serving Tire SQL Engine...")
today_date=datetime.now().strftime("%Y-%m-%d")
production_path=f"data_lake/production/date={today_date}"

if not os.path.exists(production_path) or not os.listdir(production_path):
    print(f"Error: No production files discovered for date={today_date}. Run your pipline script first")
    exit()

all_records=[]
for file_name in os.listdir(production_path):
    if file_name.endswith(".json"):
        with open(os.path.join(production_path,file_name),'r') as f:
            file_data=json.load(f)
            all_records.extend(file_data)
print(f"Loaded {len(all_records)} records")

connection=sqlite3.connect(":memory:")
cursor=connection.cursor()

cursor.execute("""CREATE TABLE market_metrics (token_id TEXT,usd_price REAL,usd_market_cap REAL,usd_24h_volume REAL)""")

for row in all_records:
    cursor.execute(""" INSERT INTO market_metrics VALUES (?,?,?,?)""",(row['token_id'],row['usd_price'],row['usd_market_cap'],row['usd_24h_volume']))
    connection.commit()
print(f"SQL Engine Loaded Successfully. Querrying {len(all_records)} crypto assets records ...")

print("\n     TOP 10 BY MARKET CAP     ")
print(f"{"TOKEN ID ":<20}| {"USD PRICE":>12}|{"MARKET CAP":>18}")
print("-"*60)
q1="""SELECT token_id,usd_price,usd_market_cap FROM market_metrics ORDER BY usd_market_cap DESC LIMIT 10"""
cursor.execute(q1)
result=cursor.fetchall()
for row in result:
    print(f"{row[0]:<20}|${row[1]:>12,.2f} | ${row[2]:>18,.0f}")

print("\n     TOP 10 BY 24H VOLUME     ")
print(f"{"TOKEN ID ":<20}| {"USD PRICE":>12}|{"24H VOLUME":>18}")
print("-"*60)
q2="""SELECT token_id,usd_price,usd_24h_volume FROM market_metrics ORDER BY usd_24h_volume DESC LIMIT 10"""
cursor.execute(q2)
result=cursor.fetchall()
for row in result:
    print(f"{row[0]:<20}|${row[1]:>12,.2f} | ${row[2]:>18,.0f}")

print("\n     PRICE FILTER     ")
print(f"{"TOKEN ID ":<20}| {"USD PRICE":>12}|{"MARKET CAP":>18}")
print("-"*60)
q3=""" SELECT token_id,usd_price,usd_market_cap FROM market_metrics WHERE usd_price >100 ORDER BY usd_price DESC"""
cursor.execute(q3)
result=cursor.fetchall()
for row in result:
    print(f"{row[0]:<20}|${row[1]:>12,.2f} | ${row[2]:>18,.0f}")

print("\n     MARKET STATISTICS     ")
q4="""SELECT COUNT(*) AS total_assets,AVG(usd_price) as average_price,MAX(usd_price) as highest_price,MIN(usd_price) as lowest_price FROM market_metrics"""
cursor.execute(q4)
result=cursor.fetchone()
print(f"Total Assets : {result[0]}")
print(f"Average Price : ${result[1]:.2f}")
print(f"Highest Price : ${result[2]:.2f}")
print(f"Lowest Price : ${result[3]:.2f}")

print("\n     PRICE DISTRIBUTION     ")
q5="""
SELECT 
    CASE
        WHEN usd_price<1 THEN 'Under $1'
        WHEN usd_price<100 THEN '$1-$100'
        ELSE 'Above $100'
    END AS price_category,
    COUNT(*) AS asset_count FROM market_metrics GROUP BY price_category
"""
cursor.execute(q5)
result=cursor.fetchall()
for row in result:
    print(f"{row[0]:<15} | {row[1]}")

connection.close()
print("Analytical Processing completed!")