from playwright.sync_api import sync_playwright # NOQA
import requests
from lxml import html
import re
import csv
import pandas as pd
import os


def read_file_data(filename):  # NOQA
    """Read data from a file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read().strip().split("\n")


def create_csv_file(filename):
    """Create a new CSV file."""
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['COMPANY NAME', 'WEBSITE', 'PHONE', 'EMAIL', 'TAGS'])  # Added ZIP CODE


def remove_duplicates_using_pandas(filename):
    """Remove duplicates from a CSV file using pandas."""
    df = pd.read_csv(filename)
    df['WEBSITE'] = df['WEBSITE'].fillna(df['COMPANY NAME'])
    df.drop_duplicates(subset=['WEBSITE'], inplace=True)

    print(f"==> Total new records: {len(df)}")
    df.to_csv(filename, index=False)


def get_lat_and_lon(us_zip_code):
    """Get latitude and longitude for a US zip code from df based on Zipcode."""
    df = pd.read_csv("Zipcodes.csv")
    df['zip'] = df['zip'].astype(str).str.zfill(5)

    row = df[df['zip'] == us_zip_code]

    if not row.empty:
        return row['latitude'].values[0], row['longitude'].values[0]
    return None, None


if not os.path.exists("Sciton-Data.csv"):
    create_csv_file("Sciton-Data.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

tags_no = [
    "517", "542", "506", "535", "216", "534", "507", "219", "526", "527", "508", "520", "522", "509", "510", "521",
    "523", "539", "524", "519", "511", "513", "514"

]

tags_name = [
    "ALLURA", "BARE IT", "BBL", "BBL HEROic", "CelluSmooth", "ClearScan ALX", "ClearScan YAG", "ClearSense",
    "ClearSilk", "ClearV", "Contour TRL", "diVa", "Forever Bare BBL", "Forever Clear", "Forever Young", "Halo", "Hero",
    "Heroic", "Moxi", "Pro V", "ProFractional", "SkinTyte", "ThermaScan"

]


def clean_response_content(response_text):  # NOQA
    cleaned_content = re.sub(r'\\/', '/', response_text)  # NOQA
    cleaned_content = re.sub(r'\\n', '', cleaned_content)  # NOQA
    return cleaned_content


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    nonce = None

    page.on('response', lambda response:  # Noqa
    handle_response(response) if response.url == 'https://sciton.com/wp-json/ll/api/v1/cpt/filter' else None
            )


    def handle_response(response):  # NOQA
        global nonce  # Use the global variable for nonce
        if response.ok:
            nonce = response.headers.get('x-wp-nonce')
            print("x-wp-nonce:", nonce)  # Print the nonce for debugging


    page.goto('https://sciton.com/find-my-provider/', wait_until='load', timeout=0)

    print("Scraper is running....")

    page.locator('//input[@name="fmp-address"]').fill('00501')
    page.click('//button[@class="btn"]')

    page.wait_for_selector('//div[@class="provider"]', state='visible', timeout=30000)  # Wait for the results to load

    if nonce:
        url = 'https://sciton.com/wp-json/ll/api/v1/cpt/filter'
        headers = {
            "accept": "*/*",
            "accept-language": "en,ur;q=0.9,en-GB;q=0.8,en-US;q=0.7",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "cookie": "_zitok=a13e9bffedd9b14759151724619848",
            "origin": "https://sciton.com",
            "priority": "u=1, i",
            "referer": "https://sciton.com/find-my-provider/?fmp-name=&fmp-address=00501&fmp-country=US&fmp-radius=50&fmp-branded-treatments=&fmp-treatments=&fmp-designations=",
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
            "x-wp-nonce": nonce  # Include the nonce in the headers
        }

        browser.close()


        def extract_data(tag_no, tag_name, zip_code):  # NOQA
            latitude, longitude = get_lat_and_lon(zip_code)

            if latitude is None or longitude is None:
                print(f"Invalid zip code: {zip_code}. Skipping...")
                return []

            # Payload for the POST request
            payload = (
                f"name=&lat={latitude}&long={longitude}&distance=50&treatments="
                f"&brandedTreatments={tag_no}&designations=&country=US&paged=1&address={zip_code}"
            )

            response = requests.post(url, data=payload, headers=headers)
            cleaned_content = clean_response_content(response.text)
            tree = html.fromstring(cleaned_content)

            cards_xpath = tree.xpath('//div[contains(@class,"provider")]')
            providers = []  # NOQA

            no_data_message = tree.xpath('//div[@class="-mb-5 row"]//p/text()')
            if no_data_message:
                print(f"No providers found for {tag_name} in zip code {zip_code}.")
                return providers

            for provider in cards_xpath:
                name = provider.xpath('.//h2/text()')
                links_xpath = provider.xpath('.//a/@href')
                link = [link.replace('\\', '') for link in links_xpath]
                phone = provider.xpath('.//a[contains(@href, "tel:")]/text()')

                providers.append({
                    'name': name[0].strip() if name else "NA",
                    'website': link[0] if link else "NA",
                    'phone': phone[0] if phone else "NA",
                    'tags': tag_name,
                })

            print(f"Found {len(providers)} data for {tag_name} in zip code {zip_code} with {response.status_code}")

            return providers


        all_providers = []  # NOQA

        for zip_code in zip_codes:
            providers_per_zip = []
            for tag_no, tag_name in zip(tags_no, tags_name):
                providers = extract_data(tag_no, tag_name, zip_code)
                providers_per_zip.extend(providers)

            if providers_per_zip:
                with open("Sciton-Data.csv", mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    for provider in providers_per_zip:
                        writer.writerow([provider['name'], provider['website'], provider['phone'], "",
                                         provider['tags']])

                remove_duplicates_using_pandas("Sciton-Data.csv")

                with open("already-done.txt", 'a', encoding='utf-8') as file:
                    file.write(f"{zip_code}\n")

        print("Data scraping completed.")
