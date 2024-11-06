from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging
from scraper import ClicFlyerScraper 

logging.basicConfig(level=logging.INFO)

app = FastAPI()
scraper = ClicFlyerScraper()
output_dir = Path("./app/data")
output_dir.mkdir(parents=True, exist_ok=True)

@app.post("/scrape_data/")
async def scrape_data():
    try:
        retailer_names, retailer_ids, offer_counts = scraper.get_home_retailers()
        (
            retailer_names_out,
            retailer_ids_out,
            ids,
            category_names,
            subcategory_names,
            coupon_codes,
            coupon_types,
            buynow_ids,
        ) = scraper.get_flyer_offers(retailer_names, retailer_ids, offer_counts)
        
        details = scraper.get_offer_details(
            retailer_names_out, retailer_ids_out, ids, category_names,
            subcategory_names, coupon_codes, coupon_types, buynow_ids
        )

        filepath = scraper.save_data(details, output_dir=output_dir)
        return {"status": "Data scraped and saved successfully", "file_path": str(filepath)}

    except Exception as e:
        logging.error(f"Scraping failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Scraping failed")


@app.get("/latest_data/")
async def latest_data():
    try:
        latest_file = max(output_dir.glob("*.json"), key=os.path.getctime)
        with open(latest_file, "r", encoding="utf-8") as file:
            content = file.read()
        return {"data": content}

    except Exception as e:
        logging.error(f"Retrieving data failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve data")
