from playwright.sync_api import sync_playwright
import csv
import os
import pandas as pd
import time


def read_file_data(filename):  # NOQA
    """Read data from a file."""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as file:
            return file.read().strip().split("\n")
    return []


def create_csv_file(filename):  # NOQA
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


def append_to_done_file(filename, zipcode):
    """Append a completed zip code to the already done file."""
    with open(filename, mode='a', encoding='utf-8') as file:
        file.write(f"{zipcode}\n")


if not os.path.exists("Aspenlaser-Data.csv"):  # NOQA
    create_csv_file("Aspenlaser-Data.csv")

# Read zip codes from the provided CSV file
df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

list_of_devices = ["Acupuncturist - LAc", "Apex", "Aspen Laser Provider", "Athletics", "Chiropractic",
                   "Chiropractor - DC", "Dental", "Dentist - DMD/DDS", "Distributions", "Equine", "Laser Therapy", "MD",
                   "Massage", "Massage Therapist - MT", "Medical", "Medical Doctor - MD/DO", "Orthodontics",
                   "Physical Therapist - PT", "Physical Therapy", "Podiatry", "Veterinary", "Wellness"]

with sync_playwright() as p:
    browser = p.firefox.launch(headless=True)
    page = browser.new_page()
    page.goto("https://www.aspenlaser.com/find-a-medical-professional", timeout=0, wait_until='load')

    print("Scraping started...")

    for zipcode in zip_codes:
        for device in list_of_devices:

            # SEARCHING PART
            page.wait_for_selector('//input[@id="search-location"]').fill(zipcode)
            page.keyboard.press("Space")

            time.sleep(1)

            select_location = page.locator(
                "//div[@class='pac-item']/span[not(@class='pac-item-query') and not(@class='pac-icon')][2]").nth(0)
            select_location.click()

            page.select_option('//select[@id="store_locatore_search_radius"]', '50000 Mile')

            select_device = page.locator('//select[@id="store_locator_category"]')
            select_device.select_option(label=device)

            page.click('//button[@id="store_locatore_search_btn"]')
            page.wait_for_timeout(3000)

            try:
                page.wait_for_selector('//div[@class="shop-box"]', timeout=5000)
            except:  # NOQA
                print(f"No results found for device: {device}")
                continue

            # SCRAPING PART
            company_names = page.query_selector_all('//div[@class="shop-box"]//h4')
            website_links = page.query_selector_all('//div[@class="shop-box"]//a[@class="website-link"]')
            phone_numbers = page.query_selector_all('//div[@class="shop-box"]//a[1]')

            print(f"Results for device: {device} for zipcode: {zipcode}")

            for name, website, phone in zip(company_names, website_links, phone_numbers):
                company_name = name.inner_text() if name else "N/A"
                website = website.get_attribute("href") if website else "N/A"
                phone = phone.inner_text() if phone else "N/A"
                tags = device

                with open("Aspenlaser-Data.csv", "a", encoding="utf-8", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow([company_name, website, phone, "", tags])

                remove_duplicates_using_pandas("Aspenlaser-Data.csv")

        append_to_done_file("already-done.txt", zipcode)

    browser.close()
