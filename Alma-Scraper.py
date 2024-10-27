import requests
from lxml import html
import pandas as pd
import os
import csv
import time

# Base configuration and headers
base_querystring = { # NOQA
    "_wrapper_format": "drupal_ajax",
    "field_geofield_proximity[value]": "100",  # 100-mile proximity
    "field_address_country_code": "US",
    "view_name": "provider_locator",
    "view_display_id": "block_1",
    "view_args": "",
    "view_path": "/node/196",
    "view_base_path": "assets-test",
    "view_dom_id": "34747321387aa7656e53dedf778b0c9d27fa086e0d19ab07a129e8fe4ae0efa4",
    "pager_element": "0",
    "_drupal_ajax": "1",
    "ajax_page_state[theme]": "alma",
    "ajax_page_state[theme_token]": "",
    "ajax_page_state[libraries]": "eJyFj0sOwjAMRC8U4iMhh7ipkRNHddISTo8oCCo2bObzNJtByQhJNKCcrA3hklwQvA8IrB6veHMTt5Wjwds_nQs3N2lpuJFpJjhkv1F4Vvs78DZzdkk1CZ2xoIzGF4Nf4CoumBass0FcekXxX-J7qT0I20zR2bBGGQIauZVpM9j19eUIssYu9ADTE2UY"
}

headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "priority": "u=1, i",
    "referer": "https://almainc.com/physician-locator",
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}

device_names = {
    "352": "Accent Prime",
    "353": "Alma Duo",
    "389": "Alma Harmony",
    "344": "Alma Hybrid",
    "395": "Alma IQ",
    "345": "Alma TED",
    "363": "Alma Veil",
    "355": "BeautiFill",
    "346": "CBD+ Skincare",
    "354": "FemiLift",
    "358": "Harmony XL Pro",
    "364": "LMNT.one",
    "356": "Opus",
    "357": "Soprano ICE"
}

devices_no = ["352", "353", "389", "344", "395", "345", "363", "355", "346", "354", "358", "364", "356", "357"]


def read_file_data(filename):
    """Read data from a file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read().strip().split("\n")


def create_csv_file(filename):
    """Create a new CSV file."""
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['COMPANY NAME', 'PHONE', 'TAGS'])


def remove_duplicates_using_pandas(filename):
    """Remove duplicates from a CSV file using pandas based on COMPANY NAME and TAGS."""
    df = pd.read_csv(filename)

    # Remove duplicates where both COMPANY NAME and TAGS are the same
    df.drop_duplicates(subset=['COMPANY NAME', 'TAGS'], inplace=True)

    print(f"==> Total new records: {len(df)}")

    # Save the cleaned data back to the CSV file
    df.to_csv(filename, index=False)


# Initialize CSV file if it doesn't exist
if not os.path.exists("Alma-Data.csv"):
    create_csv_file("Alma-Data.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]


# Function to process each response and extract the required data
def process_device_data(response, device, zip_code):
    try:
        if response.status_code != 200:
            print(f"Failed to retrieve data for device {device} in Zip code {zip_code}")
            return

        json_response = response.json()
        if not json_response:
            print(f"No data returned for device {device} in Zip code {zip_code}.")
            return

        print(
            f"Processing Device: {device_names[device]} (ID: {device}) for Zip Code: {zip_code} with Status Code: {response.status_code}")

        for item in json_response:
            if 'data' in item and isinstance(item['data'], str):
                html_content = item['data']
                if not html_content.strip():
                    continue  # Skip empty HTML content

                tree = html.fromstring(html_content)
                clinic_names = tree.xpath(
                    '//div[contains(@class, "views-field-title-1")]//h2[@class="field-content"]/text()')
                phone_numbers = tree.xpath('//div[contains(@class, "views-field-field-phone-number")]//a/@href')
                phone_numbers = [phone.replace('tel:', '') for phone in phone_numbers]

                if not clinic_names:
                    print(f"No clinic names found for device {device}.")
                if not phone_numbers:
                    print(f"No phone numbers found for device {device}.")

                for name, phone in zip(clinic_names, phone_numbers):
                    record = [name, phone, device_names[device]]

                    with open('Alma-Data.csv', mode='a', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file)
                        writer.writerow(record)
                    remove_duplicates_using_pandas("Alma-Data.csv")
    except Exception as e:
        print(f"Error processing data for device {device}: {str(e)}")


# Loop through each chunk of zip codes
for zip_code in zip_codes:
    url = "https://almainc.com/views/ajax"

    # Prepare the requests for all devices for the current zip code
    for device in devices_no:
        querystring = base_querystring.copy()
        querystring["field_device_types_target_id"] = device
        querystring["field_geofield_proximity[source_configuration][origin_address]"] = f"{zip_code}, United States"

        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=10)
            process_device_data(response, device, zip_code)
            time.sleep(5)  # Rate limiting
        except requests.RequestException as e:
            print(f"Request failed for device {device} in Zip code {zip_code}: {str(e)}")

    # Mark zip code as done
    with open('already-done.txt', 'a', encoding='utf-8') as file:
        file.write(f"{zip_code}\n")
