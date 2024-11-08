from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging
from scraper import ClicFlyerScraper 
from db_handler import MongoDBHandler
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

app = FastAPI()
scraper = ClicFlyerScraper()
db_handler = MongoDBHandler()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

scheduler = AsyncIOScheduler()

async def scheduled_scrape():
    """Function to run scheduled scraping"""
    try:
        retailer_names, retailer_ids, offer_counts = scraper.get_home_retailers()
        retailer_data = scraper.get_flyer_offers(retailer_names, retailer_ids, offer_counts)
        details = scraper.get_offer_details(*retailer_data)

        db_handler.update_offers(details)
        logging.info("Scheduled scraping completed successfully")
    except Exception as e:
        logging.error(f"Scheduled scraping failed: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Trigger scraping on startup"""
    logging.info("Starting scraping on application startup...")
    await scheduled_scrape() 

scheduler.add_job(scheduled_scrape, 'cron', hour=0)
scheduler.start()

@app.post("/scrape_data/")
async def scrape_data():
    try:
        retailer_names, retailer_ids, offer_counts = scraper.get_home_retailers()
        retailer_data = scraper.get_flyer_offers(retailer_names, retailer_ids, offer_counts)
        details = scraper.get_offer_details(*retailer_data)
        
        stats = db_handler.update_offers(details)
        
        return {
            "message": "Data scraped and stored successfully",
            "stats": stats
        }
    except Exception as e:
        logging.error(f"Scraping failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Scraping failed")

@app.get("/latest_data/")
async def latest_data(limit: int = 100):
    try:
        data = db_handler.get_latest_offers(limit)
        return {"data": data}
    except Exception as e:
        logging.error(f"Retrieving data failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve data")
