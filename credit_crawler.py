import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
from PIL import Image
import subprocess
import camelot
import re
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
DU_NO_URL = os.getenv('DU_NO_URL')
IMG_FOLDER = os.getenv('IMG_FOLDER')
IMG_NAME = os.getenv('IMG_NAME')
PDF_NAME = os.getenv('PDF_NAME')
DATA_PATH = os.getenv('DATA_PATH')
LOG_FOLDER = os.getenv('LOG_FOLDER')
NOTI_EMAIL = os.getenv('NOTI_EMAIL')
CUSTOM_PACKAGE = os.getenv('CUSTOM_PACKAGE_PATH')

import sys
sys.path.append(CUSTOM_PACKAGE)
from noti_utilities.create_logger import workflow_logger

IMG_SAVE_PATH = os.path.join(IMG_FOLDER, IMG_NAME)
os.makedirs(IMG_FOLDER, exist_ok=True)

# Logger
log_file = os.path.join(LOG_FOLDER, "credit_crawler.log")
logger = workflow_logger(
    name='credit_crawler',
    log_file=log_file
).get_logger()


def is_up_to_date(old_date, new_date):

    if old_date < new_date:
        print(f'New data is available for {new_date.month}/{new_date.year}')
        logger.info(f'New data is available for {new_date.month}/{new_date.year}')
        return False
    else:
        print('Data is up to date')
        logger.info('Data is up to date')
        return True
    
def get_date(df: pd.DataFrame) -> pd.Timestamp:

    # Extract date

    date_str = [item for item in df.iloc[1,:].values if item != ""][0]
    search = re.search(r"Thang\s+(\d+)\s+Nam\s+(\d+)", date_str, re.IGNORECASE)
    date = datetime(
        year=int(search.group(2)),
        month=int(search.group(1)),
        day=1
    )
    date = pd.to_datetime(date) + pd.offsets.MonthEnd(0)

    return date

def get_year_month(df: pd.DataFrame) -> pd.Timestamp:

    # Extract date

    date_str = [item for item in df.iloc[1,:].values if item != ""][0]
    search = re.search(r"Thang\s+(\d+)\s+Nam\s+(\d+)", date_str, re.IGNORECASE)
    year=int(search.group(2))
    month=int(search.group(1))
    
    return year, month

# Read old data
old_data = pd.read_excel(DATA_PATH)
old_date = (
    pd.to_datetime(
        old_data['Năm'].astype(str) + '-' + old_data['Tháng'].astype(str),
        format='%Y-%m'
    ) + pd.offsets.MonthEnd(0)
).max()

# Download image

soup = BeautifulSoup(
    requests.get(DU_NO_URL).text,
    'html.parser'
)
img_url = 'https://sbv.gov.vn' + soup.find('img', {'class':'w-100'}).get('src')

request_img = requests.get(img_url)
request_img.raise_for_status()

with open('img/data_img.png', 'wb') as f:
    f.write(request_img.content)

# Convert to PDF
subprocess.run(
    ["tesseract", IMG_SAVE_PATH, PDF_NAME, "pdf"],
    check=True
)

# Convert to dataframe
df = camelot.read_pdf(
    f'{PDF_NAME}.pdf',
    flavor='stream'
)[0].df

current_date = get_date(df)

if not is_up_to_date(old_date=old_date, new_date=current_date):

    logger.info(f'Updating data for {current_date}')
    current_year, current_month = get_year_month(df)

    df_processed = df.loc[4:,:].replace("", None).reset_index(drop=True).dropna(how='all', axis=1)
    df_processed.columns = ['code', 'field', 'value_bln_vnd', 'change']

    df_processed['value'] = df_processed['value_bln_vnd'].str.replace(".", "").str.replace(",", ".").astype(float)
    # df_processed['change'] = df_processed['change'].str.replace(',', ".").astype(float) / 100

    df_processed['field_vn'] = [
        'Nông nghiệp, lâm nghiệp và thủy sản',
        'Công nghiệp và xây dựng',
        'Công nghiệp',
        'Xây dựng',
        'Hoạt động Thương mại, Vận tải và Viễn thông',
        'Thương mại',
        'Vận tải và Viễn thông',
        'Các hoạt động dịch vụ khác',
        'Tổng cộng'
    ]

    df_processed = df_processed.drop(
        ['value_bln_vnd', 'field', 'code', 'change'],
        axis=1
    )

    df_final = df_processed.pivot_table(
        values='value',
        columns='field_vn',
    ).rename_axis(None, axis=1).reset_index(drop=True)

    df_final['Năm'] = current_year
    df_final['Tháng'] = current_month

    updated_data = pd.concat([old_data, df_final], ignore_index=True)
    
    updated_data.to_excel(DATA_PATH, index=False)
    
    print('Data updated sucessfully!')
    logger.info('Data updated sucessfully!')