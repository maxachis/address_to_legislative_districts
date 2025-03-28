import re
from typing import Optional
import pandas as pd

import requests
from pydantic import BaseModel
from tqdm import tqdm

API_KEY = 'AIzaSyARlIFVBPzqb8qW3696iPT2dwMZNRxbdsk'
BASE_URL = 'https://www.googleapis.com/civicinfo/v2/representatives'

STATE_HOUSE_ID = 'sldl'
STATE_SENATE_ID = 'sldu'
US_HOUSE_ID = 'cd:'

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

def process_row(df: pd.DataFrame, idx):
    row = df.iloc[idx]
    address = row['address']
    districts = get_legislative_districts(address)
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

    # Apply with progress bar
    try:
        for idx in tqdm(df[mask].index):
            process_row(df, idx)
    except Exception as e:
        df.to_csv(file_path, index=False)
        raise e

    # Save back to same file
    df.to_csv(file_path, index=False)



def get_legislative_districts(address) -> RowItem:
    params = {
        'key': API_KEY,
        'address': address,
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

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

if __name__ == '__main__':
    process_csv('data/unregistered_addresses.csv')