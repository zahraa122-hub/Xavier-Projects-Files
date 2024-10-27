import requests
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
    df.drop_duplicates(subset=['WEBSITE', 'COMPANY NAME'], inplace=True)

    print(f"==> Total new records after removing duplicates: {len(df)}")
    df.to_csv(filename, index=False)


# Create the CSV file if it doesn't exist
if not os.path.exists("Cartessa-Data.csv"):
    create_csv_file("Cartessa-Data.csv")

url = "https://provider.cartessaaesthetics.com/scs/devices/devices.json"
response = requests.get(url)

print("""Scraping Started""")

if response.status_code == 200:
    data = response.json()
    if isinstance(data, dict):
        records = []
        for key, item in data.items():
            if isinstance(item, dict) and 'name' in item:
                company_name = item.get('store_name', '')
                website = item.get('web', '')
                email = ""
                phone = item.get('phone', '')
                tags = item.get('device_name', '')

                # Prepare the record
                record = [company_name, website, phone, email, tags]
                records.append(record)

        # Remove duplicates and write to CSV
        with open('Cartessa-Data.csv', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for record in records:
                writer.writerow(record)

        # remove_duplicates_using_pandas("Cartessa-Data.csv")
    else:
        print("Data is not a dictionary:", data)
else:
    print(f"Request failed with status code {response.status_code}")
