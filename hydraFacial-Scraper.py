import grequests
import csv
import pandas as pd
import os
import time


def read_file_data(filename):  # NOQA
    """Read data from a file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read().strip().split("\n")


def list_into_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def create_csv_file(filename):  # NOQA
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
    # Convert zip code to string and fill with zeros
    df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
    # Convert all columns to string
    df = df.apply(lambda x: x.astype(str))  # NOQA

    row = df[df['zip'] == us_zip_code]

    if not row.empty:
        return row['latitude'].values[0], row['longitude'].values[0]
    return None, None


def write_to_already_done(zip_code):
    """Write processed zip codes to already-done.txt"""
    with open("already-done.txt", 'a', encoding='utf-8') as file:
        file.write(f"{zip_code}\n")


def exception_handler(request, exception):
    print(f"Request failed: {exception}")


def send_requests(zip_codes, lat_lons):
    url = "https://ws.bullseyelocations.com/RestSearch.svc/DoSearch2"
    headers = {
        "accept": "application/json, text/plain, /",
        "accept-language": "en,ur;q=0.9,en-GB;q=0.8,en-US;q=0.7",
        "origin": "https://hflocator.bullseyelocations.com",
        "priority": "u=1, i",
        "referer": "https://hflocator.bullseyelocations.com/",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    }

    # Create a list of unsent requests
    requests_list = []
    for zip_code, (lat, lon) in zip(zip_codes, lat_lons):
        if lat and lon:
            querystring = {
                "distanceUnit": "mi",
                "latitude": lat,
                "longitude": lon,
                "radius": "50",
                "loading": "false",
                "ready": "true",
                "postalCode": zip_code,
                "_reference": "A9",
                "isManualSearch": "true",
                "pageSize": "1000",
                "ClientId": "8161",
                "ApiKey": "80b3f274-6ab7-4e41-9761-aff11f7d2aac"
            }
            req = grequests.get(url, headers=headers, params=querystring)
            requests_list.append(req)

    # Send all requests asynchronously
    responses = grequests.map(requests_list, exception_handler=exception_handler)
    return responses


if not os.path.exists("Hydra-Facial-Data.csv"):
    create_csv_file("Hydra-Facial-Data.csv")

# Load zip codes
df = pd.read_csv("Zipcodes.csv")
df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data(
    "already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

chunks_of_zipcodes = list(list_into_chunks(zip_codes, 30))

for chunk_index, single_chunk in enumerate(chunks_of_zipcodes):
    print(f"chunk_index = {chunk_index + 1} / {len(chunks_of_zipcodes)}")

    # Get lat/lon for zip codes in this chunk
    lat_lons = [get_lat_and_lon(zip_code) for zip_code in single_chunk]

    # Send asynchronous requests
    responses = send_requests(single_chunk, lat_lons)

    # Process the responses
    for response, zip_code in zip(responses, single_chunk):
        if response and response.status_code == 200:
            try:
                json_data = response.json()
                for item in json_data['ResultList']:
                    company_name = item.get('Name', '')
                    website = item.get('URL', '')
                    phone = item.get('PhoneNumber', '')
                    email = item.get('EmailAddress', '')
                    tags = item.get('CategoryNames', '')

                    # Append the results to the CSV file
                    with open("Hydra-Facial-Data.csv", mode='a', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file)
                        writer.writerow([company_name, website, phone, email, tags])

                remove_duplicates_using_pandas("Hydra-Facial-Data.csv")
                write_to_already_done(zip_code)
                print(f"Processed ZIP code: {zip_code} with status code: {response.status_code}")

            except Exception as e:
                print(f"Error processing ZIP code {zip_code}: {e}")
        else:
            print(f"Skipping ZIP code {zip_code}")

    # Sleep to avoid overwhelming the server
    time.sleep(2)
