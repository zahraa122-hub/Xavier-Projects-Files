from playwright.sync_api import sync_playwright
import csv
import pandas as pd
import os


def read_file_data(filename):  # NOQA
    """Read data from a file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read().strip().split("\n")


def create_csv_file(filename):
    """Create a new CSV file with headers."""
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['COMPANY NAME', 'WEBSITE', 'PHONE', 'EMAIL', 'TAGS'])


def remove_duplicates_using_pandas(filename):
    """Remove duplicates from a CSV file using pandas."""
    df = pd.read_csv(filename)
    df['WEBSITE'] = df['WEBSITE'].fillna(df['COMPANY NAME'])
    df.drop_duplicates(subset=['WEBSITE'], inplace=True)

    print(f"==> Total new records in CSV: {len(df)}")
    df.to_csv(filename, index=False)


if not os.path.exists("Cutera-Data.csv"):
    create_csv_file("Cutera-Data.csv")

df = pd.read_csv("Zipcodes.csv")
cities_states_countries = df[['city', 'state', 'country']].dropna().values.tolist()
already_done_locations = set(read_file_data("already-done.txt"))

locations = [(city, state, country) for city, state, country in cities_states_countries
             if f"{city}, {state}, {country}" not in already_done_locations]


def get_text_from_xpath(locator, timeout=2000):
    """Wait for a locator to be visible and return its text or 'NA' if not available."""
    try:
        locator.wait_for(timeout=timeout)
        return locator.inner_text()
    except Exception:  # NOQA
        return 'NA'


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    print(f"Scraper is running...")

    for city, state, country in locations:
        page.goto('https://cutera.com/us-en/patient/find-a-provider/')

        # Fill city and state in the address field
        page.locator('//input[@type="address"]').fill(f"{city},{state},{country}")
        page.locator('//form[@id="find-provider-form"]//button[@type="submit"]').click()

        try:
            page.wait_for_selector('//div[@class="location-result-item"]', timeout=20000)
        except Exception:
            print(f"==> No results found for {city}, {state}, {country}. Moving to the next location.")
            with open("already-done.txt", 'a', encoding='utf-8') as file:
                file.write(f"{city}, {state}, {country}\n")
            continue

        total_cards = page.query_selector_all('//div[@class="location-result-item"]')
        print(f"==> Total Records for {city}, {state}, {country}: {len(total_cards)}")

        data = []

        for index in range(1, len(total_cards) + 1):
            card = page.locator(f'(//div[@class="location-result-item"])[{index}]')

            print(f"==> Scraping record {index}")

            company_name = get_text_from_xpath(card.locator('//h3'))
            website_locator = card.locator("//a[contains(text(), 'View Website')]")
            website_link = website_locator.get_attribute('href') if website_locator.is_visible(timeout=2000) else 'NA'
            website_link = website_link.replace('(', '').replace(')', '') if website_link != 'NA' else 'NA'
            phone = get_text_from_xpath(card.locator('//a[@class="location-phone-number"]'))
            email = get_text_from_xpath(card.locator('//a[@class="location-email"]'))
            request_button = card.locator("//button[contains(@class, 'request-btn')]")
            tags = request_button.get_attribute('data-treatments') if request_button.is_visible(timeout=2000) else 'NA'

            record = [company_name, website_link, phone, email, tags]
            data.append(record)

            # Append each record to the CSV file
            with open("Cutera-Data.csv", 'a', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(record)

        remove_duplicates_using_pandas("Cutera-Data.csv")

        with open("already-done.txt", 'a', encoding='utf-8') as file:
            file.write(f"{city}, {state}, {country}\n")

    browser.close()
