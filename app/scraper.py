
import requests
import pandas as pd
import os
import math
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Any
import logging
from datetime import datetime
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clicflyer_scraper.log'),
        logging.StreamHandler()
    ]
)

class ClicFlyerScraper:
    def __init__(self):
        self.base_headers = {
            "appversion": "5.20.2",
            "cityid": "2",
            "devicetype": "android",
            "language": "en",
            "uniqueid": "0c4644f0d8bf4f23",
            "User-Agent": "okhttp/4.9.3",
            "authtoken": "B1336930-E2E7-4104-BB80-585F",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        self.base_url = "https://api.clicflyer.com/api/ClicFlyerAPI"
        self.session = requests.Session()
        
    def _make_request(self, endpoint: str, method: str = 'POST', data: Dict = None) -> Dict:
        """Make HTTP request with error handling and retries"""
        url = f"{self.base_url}/{endpoint}"
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=self.base_headers,
                    data=data,
                    timeout=30
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logging.error(f"Failed to make request to {endpoint}: {str(e)}")
                    raise
                logging.warning(f"Attempt {attempt + 1} failed, retrying...")
                continue

    def get_home_retailers(self) -> Tuple[List[str], List[int], List[int]]:
        """Get retailer names, IDs and offer counts"""
        logging.info("Fetching home retailers...")
        
        response = self._make_request("GetHomeRetailers_V3")
        retailers_data = response.get('data', [])
        
        retailer_names = []
        retailer_ids = []
        offer_counts = []

        for retailer in retailers_data:
            offers = retailer.get('offerCount', 0)
            pages = math.ceil(offers / 12) if offers > 0 else 0
            
            if pages > 0:
                retailer_names.append(retailer['Name_en'])
                retailer_ids.append(retailer['Id'])
                offer_counts.append(pages)
        
        logging.info(f"Found {len(retailer_ids)} retailers")
        return retailer_names, retailer_ids, offer_counts

    def get_flyer_offers(self, retailer_names: List[str], retailer_ids: List[int], 
                        offer_counts: List[int]) -> Tuple[List[str], List[int], List[int], 
                                                        List[str], List[str], List[str], List[str], List[str]]:
        """Get flyer offers for all retailers"""
        logging.info("Fetching flyer offers...")
        
        retailer_names_out = []
        retailer_ids_out = []
        ids = []
        category_names = []
        subcategory_names = []
        coupon_codes = []
        coupon_types = []
        buynow_ids = []

        for retailer_name, retailer_id, total_pages in zip(retailer_names, retailer_ids, offer_counts):
            for page in range(1, total_pages + 1):
                data = {
                    'userid': 0,
                    'retailerid': retailer_id,
                    'sorting': 0,
                    'pageno': page,
                    'pagesize': 12,
                    'cityid': 2,
                    'IsBuyNowToogle': False,
                    'OfferImageTypeId': 1,
                }
                
                response = self._make_request("GetFlyerOffersSort_V1", data=data)
                flyers = response.get('data', [])
                
                for flyer in flyers:
                    retailer_names_out.append(retailer_name)
                    retailer_ids_out.append(retailer_id)
                    ids.append(flyer['Id'])
                    category_names.append(flyer.get('CategoryName', ''))
                    subcategory_names.append(flyer.get('SubCategoryName', ''))
                    coupon_codes.append(flyer.get('CouponCode', ''))
                    coupon_types.append(flyer.get('CouponType', ''))
                    buynow_ids.append(flyer.get('Buynowid', ''))
                
                logging.info(f"Processed page {page}/{total_pages} for retailer {retailer_name} ({retailer_id})")
        
        logging.info(f"Found {len(ids)} total flyers")
        return (retailer_names_out, retailer_ids_out, ids, category_names, 
                subcategory_names, coupon_codes, coupon_types, buynow_ids)

    def get_offer_detail(self, retailer_name: str, retailer_id: int, offer_id: int, 
                        category: str, subcategory: str, coupon_code: str, 
                        coupon_type: str, buynow_id: str) -> Dict[str, Any]:
        """Fetch details for a single offer"""
        base_url = 'https://clicflyer.com/shoppers/en/saudi-arabia/jeddah/Retailers/offer/'
        
        data = {
            'userid': 0,
            'offerid': offer_id,
            'cityid': 2,
            'language': 'en',
            'OfferImageTypeId': 1,
        }
        
        response = self._make_request("GetOfferDetail_V1", data=data)
        offer_data = response.get('data', {})
        
        return {
            'Retailer Name': retailer_name,
            'Retailer ID': retailer_id,
            'Category': category,
            'Sub Category': subcategory,
            'Id': offer_data.get('Id'),
            'Name_en': offer_data.get('Name_en'),
            'Name_local': offer_data.get('Name_local'),
            'Image': offer_data.get('Image'),
            'PromoPrice': offer_data.get('PromoPrice'),
            'RegularPrice': offer_data.get('RegularPrice'),
            'OfferStartDate': offer_data.get('OfferStartDate'),
            'OfferEndDate': offer_data.get('OfferEndDate'),
            'Logo': offer_data.get('Logo'),
            'ShoppingCartId': offer_data.get('ShoppingCartId'),
            'DaysLeft': offer_data.get('DaysLeft'),
            'Width': offer_data.get('Width'),
            'Height': offer_data.get('Height'),
            'ShareUrl': offer_data.get('ShareUrl'),
            'IsCustomText': offer_data.get('IsCustomText'),
            'Custom_en': offer_data.get('Custom_en'),
            'Custom_local': offer_data.get('Custom_local'),
            'OfferDiscount': offer_data.get('OfferDiscount'),
            'Currency': offer_data.get('Currency'),
            'IsBuyNow': offer_data.get('IsBuyNow'),
            'BuyNowUrl': offer_data.get('BuyNowUrl'),
            'buyNowUrlLocal': offer_data.get('buyNowUrlLocal'),
            'App_Value_en': offer_data.get('App_Value_en'),
            'App_Value_local': offer_data.get('App_Value_local'),
            'CouponCode': coupon_code,
            'CouponType': coupon_type,
            'Buynowid': buynow_id,
            'OfferTagsdetails': offer_data.get('OfferTagsdetails'),
            'Page URL': f"{base_url}{offer_id}"
        }

    def get_offer_details(self, retailer_names: List[str], retailer_ids: List[int], 
                         ids: List[int], category_names: List[str], subcategory_names: List[str],
                         coupon_codes: List[str], coupon_types: List[str], 
                         buynow_ids: List[str]) -> List[Dict[str, Any]]:
        """Get details for all offers using thread pool"""
        logging.info("Fetching offer details...")
        details = []
        total = len(ids)
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(
                    self.get_offer_detail,
                    retailer_name,
                    retailer_id,
                    offer_id,
                    category,
                    subcategory,
                    coupon_code,
                    coupon_type,
                    buynow_id
                )
                for retailer_name, retailer_id, offer_id, category, subcategory, 
                    coupon_code, coupon_type, buynow_id
                in zip(retailer_names, retailer_ids, ids, category_names, 
                      subcategory_names, coupon_codes, coupon_types, buynow_ids)
            ]
            
            for future in as_completed(futures):
                try:
                    details.append(future.result())
                    if len(details) % 100 == 0:
                        logging.info(f"Processed {len(details)}/{total} offers")
                except Exception as e:
                    logging.error(f"Error processing offer: {str(e)}")
        
        return details

def save_data(self, details: List[Dict[str, Any]], output_dir: str = None) -> Dict[str, Any]:
        """Convert data to JSON format and return it."""
        if output_dir is None:
            output_dir = os.getcwd()

        df = pd.DataFrame(details)
        json_data = df.to_json(orient='records', lines=True, force_ascii=False)
        json_data = json_data.replace('\\/', '/')
        parsed_json = [json.loads(line) for line in json_data.splitlines()]
        logging.info(f"Data converted to JSON successfully.")
        return parsed_json

