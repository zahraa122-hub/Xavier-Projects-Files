import pandas as pd
import csv
import os
import cloudscraper


def read_file_data(filename):  # NOQA
    """Read data from a file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read().strip().split("\n")


def create_csv_file(filename):
    """Create a new CSV file.
    COMPANY NAME
    WEBSITE
    PHONE
    EMAIL
    TAGS
    """
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['COMPANY NAME', 'WEBSITE', 'PHONE', 'EMAIL', 'TAGS'])


def remove_duplicates_using_pandas(filename):
    """Remove duplicates from a CSV file using pandas."""
    df = pd.read_csv(filename)

    # if na in website column then add company name into website column
    df['WEBSITE'] = df['WEBSITE'].fillna(df['COMPANY NAME'])

    # drop duplicates based on website
    df.drop_duplicates(subset=['WEBSITE'], inplace=True)

    print(f"==> Total new records: {len(df)}")
    df.to_csv(filename, index=False)


def get_lat_and_lon(us_zip_code):
    """Get latitude and longitude for a US zip code from df based on Zipcode."""
    df = pd.read_csv("Zipcodes.csv")
    # convert zip code to string and fill with zeros
    df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
    # convert all columns to string
    df = df.apply(lambda x: x.astype(str))

    row = df[df['zip'] == us_zip_code]

    if not row.empty:
        return row['latitude'].values[0], row['longitude'].values[0]
    return None, None


if not os.path.exists("Lutronic-Data.csv"):
    create_csv_file("Lutronic-Data.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

# Use cloudscraper to bypass Cloudflare protection
scraper = cloudscraper.create_scraper()

for us_zip_code in zip_codes:
    lat, lng = get_lat_and_lon(us_zip_code)

    if lat is None or lng is None:
        print(f"==> Latitude and longitude not found for ZIP: {us_zip_code}")
        continue

    url = "https://www.lutronic.com/wp-admin/admin-ajax.php"

    payload = {
        "action": "proxyProviderSearch",
        "userLat": lat,
        "userLng": lng,
        "userRadius": "100",
        "userEquipment": "",
        "userTreatment": ""
    }

    try:
        response = scraper.post(url, data=payload)
        response.raise_for_status()  # Raises an exception for HTTP errors
        data = response.json()

        print(f"==> Fetched data for ZIP code: {us_zip_code}")
    except Exception as e:
        print(f"Error fetching data for ZIP code {us_zip_code}: {e}")
        continue  # Skip this ZIP code and proceed with the next one

    records = []  # NOQA
    for single_list in data:
        company_name = single_list.get('CompanyName', '')  # NOQA
        website = single_list.get('URL', '')
        phone = single_list.get('PhoneNumber', '')
        email = single_list.get('EmailAddress', '')
        tags = single_list.get('EquipmentList', '')

        record = [company_name, website, phone, email, tags]

        records.append(record)

        # Append records to CSV file
    with open("Lutronic-Data.csv", mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(records)

    remove_duplicates_using_pandas("Lutronic-Data.csv")

    # Add ZIP code to already-done.txt
    with open("already-done.txt", mode='a', encoding='utf-8') as done_file:
        done_file.write(f"{us_zip_code}\n")
