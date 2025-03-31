import os
import re
import time
from typing import Optional
import pandas as pd

import requests
from pydantic import BaseModel
from requests import HTTPError
from tqdm import tqdm

API_KEY = os.getenv("GOOGLE_API_KEY")
BASE_URL = 'https://www.googleapis.com/civicinfo/v2/representatives'

STATE_HOUSE_ID = 'sldl'
STATE_SENATE_ID = 'sldu'
US_HOUSE_ID = 'cd:'

TOO_MANY_REQUESTS_STATUS_CODE = 429


import signal

should_stop = False

def handle_sigint(signum, frame):
    global should_stop
    print("Caught Ctrl+C, will stop soon.")
    should_stop = True

signal.signal(signal.SIGINT, handle_sigint)

class AdaptiveRateLimiter:
    def __init__(self, initial_delay=1.0, initial_alpha=2, max_delay=30.0, min_delay=0.1):
        self.delay = initial_delay
        self.initial_alpha = initial_alpha
        self.update_count = 0
        self.max_delay = max_delay
        self.min_delay = min_delay

    def _compute_alpha(self):
        return self.initial_alpha / (1 + self.update_count)

    def _apply_ema(self, new_target):
        self.update_count += 1
        alpha = self._compute_alpha()
        self.delay = max(
            self.min_delay,
            min(
                self.max_delay,
                (alpha * new_target) + ((1 - alpha) * self.delay)
            )
        )

    def on_success(self):
        target = self.delay * 0.9
        self._apply_ema(target)

    def on_rate_limit(self):
        target = self.delay * 2.0
        self._apply_ema(target)
        print(f"[429] Rate limited. Sleeping {self.delay:.2f}s...")
        self.sleep()

    def sleep(self):
        time.sleep(self.delay)

    def request(self, make_request_fn, params, max_attempts=5):
        for attempt in range(max_attempts):
            try:
                result = make_request_fn(params)
                self.on_success()
                return result
            except HTTPError as e:
                if e.response.status_code == TOO_MANY_REQUESTS_STATUS_CODE:
                    self.on_rate_limit()
                else:
                    raise
        return make_request_fn(params)


class DistrictAndRep(BaseModel):
    district_number: Optional[int] = None
    representative_name: Optional[str] = None
    party: Optional[str] = None

class RowItem(BaseModel):
    state_house: Optional[DistrictAndRep] = None  # sldl
    state_senate: Optional[DistrictAndRep] = None  # sldu
    us_house: Optional[DistrictAndRep] = None  # cd = congressional district

def extract_district_and_rep(data: dict, info: dict) -> DistrictAndRep:
    district_name = info['name']
    district_number = re.search(r'\d+', district_name).group(0)
    office_index = info['officeIndices'][0]
    official_index = data['offices'][office_index]['officialIndices'][0]
    official_info = data['officials'][official_index]
    official_name = official_info['name']
    party = official_info.get('party')[0]
    return DistrictAndRep(
        district_number = int(district_number),
        representative_name=official_name,
        party=party
    )

def district_name(district: DistrictAndRep, term: str) -> str:
    return f"{district.party} {term} District {district.district_number}"

def add_if_dont_exist(existing: list, new: list):
    for item in new:
        if item not in existing:
            existing.append(item)

def process_row(df: pd.DataFrame, idx, rate_limiter):
    row = df.iloc[idx]
    address = row['address']
    districts = get_legislative_districts(address, rate_limiter)
    if districts.state_house is not None:
        df.at[idx, 'State House District'] = district_name(districts.state_house, 'House')
        df.at[idx, 'State House Rep.'] = districts.state_house.representative_name
    if districts.state_senate is not None:
        df.at[idx, 'State Senate District'] = district_name(districts.state_senate, 'Senate')
        df.at[idx, 'State Senate Rep.'] = districts.state_senate.representative_name
    if districts.us_house is not None:
        df.at[idx, 'US House District'] = district_name(districts.us_house, 'US House')
        df.at[idx, 'US House Rep.'] = districts.us_house.representative_name


def process_csv(file_path):
    tqdm.pandas()
    df = pd.read_csv(file_path)
    # Filter rows where new_col1 is missing
    state_house_rep_column = "State House Rep."
    if state_house_rep_column not in df.columns:
        df[state_house_rep_column] = pd.NA  # or None, or "" if working with strings
    mask = df[state_house_rep_column].isna()

    rate_limiter = AdaptiveRateLimiter()
    # Apply with progress bar
    try:
        for idx in tqdm(df[mask].index):
            if should_stop:
                break
            process_row(df, idx, rate_limiter)
            rate_limiter.sleep()
    except Exception as e:
        df.to_csv(file_path, index=False)
        raise e

    # Save back to same file
    df.to_csv(file_path, index=False)



def get_legislative_districts(address, rate_limiter: AdaptiveRateLimiter) -> RowItem:
    params = {
        'key': API_KEY,
        'address': address,
    }
    data = rate_limiter.request(make_request, params)

    divisions = data.get('divisions', {})

    row_item = RowItem()
    for division_id, info in divisions.items():
        if STATE_HOUSE_ID in division_id:
            row_item.state_house = extract_district_and_rep(data, info)
        if STATE_SENATE_ID in division_id:
            row_item.state_senate = extract_district_and_rep(data, info)
        if US_HOUSE_ID in division_id:
            row_item.us_house = extract_district_and_rep(data, info)

    return row_item

def make_request(params) -> dict:
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()



if __name__ == '__main__':
    process_csv('data/registered_addresses.csv')