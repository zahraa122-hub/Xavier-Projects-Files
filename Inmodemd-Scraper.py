from playwright.sync_api import sync_playwright
import time
import csv
import os
import pandas as pd
import subprocess


def read_file_data(filename):  # NOQA
    """Read data from a file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read().strip().split("\n")


def create_csv_file(filename):
    """Create a new CSV file."""
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['COMPANY NAME', 'WEBSITE', 'PHONE', 'EMAIL', 'TAGS'])


def remove_duplicates_using_pandas(filename):
    """Remove duplicates from a CSV file using pandas."""
    df = pd.read_csv(filename)
    df['WEBSITE'] = df['WEBSITE'].fillna(df['COMPANY NAME'])
    df.drop_duplicates(subset=['WEBSITE'], inplace=True)
    print(f"==> Total new records: {len(df)}")
    df.to_csv(filename, index=False)


# Check if the CSV file already exists; if not, create it
if not os.path.exists("inmodemd-Data.csv"):  # NOQA
    create_csv_file("inmodemd-Data.csv")

# Read zip codes from the provided CSV file
df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

list_of_devices = ["AccuTite", "BodyTite", "Define", "EmpowerRF", "EvolveX", "FaceTite", "Forma", "FormaV",
                   "Hair Removal", "Lumecca", "Morpheus8 Burst/Morpheus8", "Morpheus8 Burst Deep/Morpheus8 Body",
                   "Plus", "QuantumRF", "Tone", "Vasculaze", "VTone"]


def main():
    with sync_playwright() as p:
        browser = p.firefox.launch(proxy={
            'server': 'p.webshare.io:80',
            'username': 'cdmyxxyb-rotate',
            'password': '5ygd5zdc6vlp'
        }, headless=True)

        page = browser.new_page()

        for zipcode in zip_codes:  # Process each zipcode
            all_data = []

            for device in list_of_devices:
                page.goto('https://inmodemd.com/find-a-provider/', timeout=0, wait_until='load') # NOQA

                try:
                    page.wait_for_selector('//span[@class="subtitle"]', timeout=3000).click()
                except: # NOQA
                    pass

                try:
                    page.wait_for_selector('//button[@id="onetrust-accept-btn-handler"]', timeout=4000)
                    page.click('//button[@id="onetrust-accept-btn-handler"]')
                except: # NOQA
                    pass

                page.context.clear_cookies()

                print("SCRAPER IS RUNNING...")

                # Fill the zipcode
                page.locator('//input[@name="full_address"]').fill(zipcode)

                try:
                    # Try to select the location
                    select_location = page.locator(
                        "//div[@class='pac-item']/span[not(@class='pac-item-query') and not(@class='pac-icon')][2]").nth(
                        0)
                    select_location.click()

                except: # NOQA
                    print(f"No location Found for zipcode: {zipcode}. Skipping this zipcode.")
                    break

                select_radius = page.locator('//select[@name="searchradius"]') # NOQA
                select_radius.select_option('50 miles')

                page.click('//select[@id="treatment"]')
                try:
                    page.select_option('//select[@id="treatment"]', device)
                except:
                    print(f"Error selecting device {device} for zipcode: {zipcode}")
                    continue  # Skip to the next device

                time.sleep(2)

                submit = page.locator('//input[@value="Search"]')
                submit.click()

                try:
                    page.wait_for_selector('//div[@id="clinicslist"]//div[@class="col-12 item s"]', timeout=10000)
                    print(f"RESULTS FOUND FOR ZIPCODE: {zipcode} AND DEVICE: {device}")

                    cards_xpath = '//div[@class="listresult"]//div//div//div//div[@class="col-12"]'
                    cards = page.query_selector_all(cards_xpath)

                    for card in cards:
                        company_name = card.query_selector('//div[@class="dr-name"]')
                        website = card.query_selector('//div[@class="dr-website"]//a')
                        phone = card.query_selector('//div[@class="dr-phone"]')

                        company_name_text = company_name.inner_text() if company_name else ""
                        website_text = website.get_attribute('href') if website else ""
                        phone_text = phone.inner_text() if phone else ""

                        record = [company_name_text, website_text, phone_text, "", device]
                        all_data.append(record)

                        with open("inmodemd-Data.csv", mode='a', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)
                            writer.writerow(record)

                        remove_duplicates_using_pandas("inmodemd-Data.csv")

                except:
                    print(f"NO RESULTS FOR ZIPCODE: {zipcode} AND DEVICE: {device}")

            with open("already-done.txt", "a") as file:
                file.write(f"{zipcode}\n")


def run_scraping():
    while True:
        try:
            main()
        except:
            print("RESTARTING THE SCRIPT...")
            time.sleep(5)
            subprocess.run(["python", __file__])
            continue


run_scraping()
