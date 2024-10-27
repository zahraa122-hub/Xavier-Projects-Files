import grequests
from lxml import html
import pandas as pd
import csv
import os


def read_file_data(filename):  # NOQA
    """Read data from a file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read().strip().split("\n")


def create_csv_file(filename):
    """Create a new CSV file with specified headers."""
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['COMPANY NAME', 'WEBSITE', 'PHONE', 'EMAIL', 'TAGS'])


def remove_duplicates_using_pandas(filename):
    """Remove duplicates from a CSV file using pandas."""
    df = pd.read_csv(filename)

    # Fill NA in the WEBSITE column with COMPANY NAME
    df['WEBSITE'] = df['WEBSITE'].fillna(df['COMPANY NAME'])

    # Drop duplicates based on the WEBSITE
    df.drop_duplicates(subset=['WEBSITE'], inplace=True)

    print(f"==> Total new records: {len(df)}")
    df.to_csv(filename, index=False)


# Create the CSV file if it doesn't exist
if not os.path.exists("Summus-Laser-Data.csv"):
    create_csv_file("Summus-Laser-Data.csv")

# Read and process zip codes
df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
zip_codes = df['zip'].tolist()

# Read already processed zip codes
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]


def fetch_data_async(zip_code, practice, headers, url, payload_template):
    """Generate grequests for fetching provider data."""
    payload = payload_template.format(zip_code, practice)
    return grequests.post(url, data=payload, headers=headers)


def extract_data(response, zip_code, practice):
    """Extract provider information from the fetched response."""
    tree = html.fromstring(response.text)
    cards = tree.xpath('//div[@class="card mb-3"]')

    data = []
    for card in cards:
        company_name = card.xpath('.//h3/text()')  # NOQA
        website = card.xpath('.//a')

        website_url = website[0].get('href') if website else 'NA'
        phone = card.xpath('.//p[@class="mb-1"]/text()')
        phone = phone[0].strip() if phone else 'NA'
        tags = ''  # Set the practice type as a tag
        email = ''

        # Prepare the record for CSV
        record = [company_name[0] if company_name else 'NA', website_url, phone, email, tags]
        data.append(record)

        # Write record to CSV
        with open("Summus-Laser-Data.csv", 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(record)

    remove_duplicates_using_pandas("Summus-Laser-Data.csv")

    # Write the processed zip code to already-done.txt
    with open("already-done.txt", 'a', encoding='utf-8') as file:
        file.write(f"{zip_code}\n")


def main():
    url = "https://gatewayplus.summuslaser.com/find-provider-iframe"
    practice_types = ['Medical', 'Veterinary', 'Dental']
    payload_template = "_token=0WNvLXE2bjL4nyk3zKZ4cSJYJMYgdNpLbaaDaTSR&submit_with_neuropathy_providers=0&zip={}&practice={}&distance=100" # NOQA

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7", # NOQA
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://gatewayplus.summuslaser.com",
        "referer": "https://gatewayplus.summuslaser.com/find-provider-iframe",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36" # NOQA
    }

    # Fetch and extract provider data concurrently for each zip code and practice type
    for zip_code in zip_codes:
        print(f"Processing zip code: {zip_code}")

        # Create grequests for all practice types
        requests_list = [fetch_data_async(zip_code, practice, headers, url, payload_template)
                         for practice in practice_types]

        # Send all requests concurrently
        responses = grequests.map(requests_list)

        # Process each response
        for response, practice in zip(responses, practice_types):
            if response and response.status_code == 200:
                extract_data(response, zip_code, practice)
            else:
                print(f"Failed to fetch data for zip code {zip_code}, practice {practice}")


if __name__ == '__main__':
    main()
