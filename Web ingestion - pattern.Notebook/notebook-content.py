# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2fc9f370-5018-46eb-a774-9ed60d52a11b",
# META       "default_lakehouse_name": "BCTS_Lakehouse",
# META       "default_lakehouse_workspace_id": "7a798b59-e76b-4af6-9ffb-fa0a2959519c",
# META       "known_lakehouses": [
# META         {
# META           "id": "2fc9f370-5018-46eb-a774-9ed60d52a11b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Example notebook for Web scraping and ingesting into On-Prem ODS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time, csv
import requests
from bs4 import BeautifulSoup

headers = {"User-Agent": "Mozilla/5.0 (Fabric scraper; contact: data-team@example.com)"}

def get_awards_for_keyword(keyword):
    # Replace with the current Contract Awards URL you confirmed in the portal UI
    url = "https://www.bcbid.gov.bc.ca/page.aspx/en/usr/awards"
    # If the portal requires keyword input to render results, pass it via params or form as observed in DevTools.
    params = {"keyword": keyword}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    # TODO: adapt selectors to the live DOM; the guide confirms keyword-driven results after CAPTCHA. [12](https://www.bchousing.org/about/doing-business/bid-centre)
    rows = []
    for tr in soup.select("table tbody tr"):
        tds = tr.find_all("td")
        link = tr.find("a", href=True)
        rows.append({
            "award_id": tds[0].get_text(strip=True) if len(tds) > 0 else "",
            "title": link.get_text(strip=True) if link else "",
            "organization": tds[2].get_text(strip=True) if len(tds) > 2 else "",
            "award_date": tds[3].get_text(strip=True) if len(tds) > 3 else "",
            "award_amount": tds[4].get_text(strip=True) if len(tds) > 4 else "",
            "details_url": link["href"] if link else ""
        })
    return rows

lower_mainland_keywords = [
    "Vancouver","Surrey","Burnaby","Richmond","Coquitlam","Delta",
    "Langley","New Westminster","North Vancouver","West Vancouver",
    "Port Coquitlam","Port Moody","Pitt Meadows","Maple Ridge","White Rock",
    "Abbotsford","Chilliwack","Mission","Hope","Harrison Hot Springs","Kent",
    "Metro Vancouver","Fraser Valley"
]

all_rows = []
for kw in lower_mainland_keywords:
    time.sleep(1.5)
    all_rows.extend(get_awards_for_keyword(kw))

print(f"Saved {len(all_rows)} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save to Lakehouse Files area or workspace
with open("bc_bid_contract_awards_lower_mainland.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=all_rows[0].keys() if all_rows else [])
    if all_rows:
        w.writeheader(); w.writerows(all_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
results = pd.DataFrame(all_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results_spark_df = spark.createDataFrame(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(results_spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results_spark_df.write.mode("overwrite").format("delta").saveAsTable('TEST_PATTERNS.SCRAPED_DATA')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
