import grequests
import pandas as pd
import csv
import os


def list_into_chunks(lst, n):  # NOQA
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


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

    # drop duplicates based on  website
    df.drop_duplicates(subset=['WEBSITE'], inplace=True)

    print(f"==> Total new records: {len(df)}")
    df.to_csv(filename, index=False)


def get_lat_and_lon(us_zip_code):
    """Get latitude and longitude for a US zip code from df based on Zipcode."""
    df = pd.read_csv("Zipcodes.csv")
    # convert zip code to string and fill with zeros
    df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
    # conert all columns to string
    df = df.apply(lambda x: x.astype(str))

    row = df[df['zip'] == us_zip_code]

    if not row.empty:
        return row['latitude'].values[0], row['longitude'].values[0]
    return None, None


if not os.path.exists("Aerolase-Data-2.csv"):
    create_csv_file("Aerolase-Data-2.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

chunks_of_zipcodes = list(list_into_chunks(zip_codes, 30))

for chunk_index, single_chunk in enumerate(chunks_of_zipcodes):
    print(f"chunk_index = {chunk_index + 1} / {len(chunks_of_zipcodes)}")

    # MAKE URLS
    urls = []
    for single_chunk_zip_code in single_chunk:
        lat, lon = get_lat_and_lon(single_chunk_zip_code)
        if lat and lon:
            urls.append(
                f"https://api.storepoint.co/v1/15f49331371dbe/locations?lat={lat}&long={lon}&radius=100")

            print(f"==> Scraping: {single_chunk_zip_code}")

    # MAKE API REQUESTS AND COLLECT DATA
    data = []
    responses = grequests.map([grequests.get(url) for url in urls])

    for single_response in responses:
        try:
            response_data = single_response.json()
        except:
            continue

        # Check if response_data is a dictionary and contains the 'results' key
        if isinstance(response_data, dict) and 'results' in response_data and isinstance(response_data['results'],
                                                                                         dict):
            # Get the 'locations' from the 'results' dictionary
            locations = response_data['results'].get('locations', None)

            # Check if locations is a list
            if isinstance(locations, list):
                # Iterate through each item in the locations list

                for item in locations:
                    # Check if the current item is None (null)
                    if item is None:
                        print(f"==> Location is null")
                        continue

                    company_name = item.get('name', '')
                    website = item.get('website', '')
                    phone = item.get('phone', '')
                    email = item.get('email', '')
                    tags = item.get('tags', '')

                    record = [company_name, website, phone, email, tags]
                    print(record)
                    data.append(record)

                    with open("Aerolase-Data-2.csv", 'a', encoding='utf-8', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(record)

            else:
                print("==> 'locations' is not a list or is empty.")
        else:
            print("==> 'results' not found or is invalid.")

    # remove_duplicates_using_pandas("Aerolase-Data.csv")

    with open("already-done.txt", 'a', encoding='utf-8') as file:
        for zip_code in single_chunk:
            file.write(f"{zip_code}\n")
