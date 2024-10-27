import grequests
import pandas as pd
import csv
import os

url = "https://hosted.meetsoci.com/thermage/rest/locatorsearch"

querystring = {"like": "0.8641097231556281", "isSOCiLocator": "true"}
headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en,ur;q=0.9,en-GB;q=0.8,en-US;q=0.7",
    "content-type": "application/json",
    "origin": "https://hosted.meetsoci.com",
    "priority": "u=1, i",
    "referer": "https://hosted.meetsoci.com/thermage/en.html?form=getlist_search&country=US",
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}


def list_into_chunks(lst, n):  # NOQA
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def read_file_data(filename):
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


def get_lat_and_lon(us_zip_code):
    """Get latitude and longitude for a US zip code."""
    df = pd.read_csv("Zipcodes.csv")
    df['zip'] = df['zip'].astype(str).str.zfill(5)
    row = df[df['zip'] == us_zip_code]
    if not row.empty:
        return row['latitude'].values[0], row['longitude'].values[0]
    return None, None


# Initialize CSV file if it doesn't exist
if not os.path.exists("Thermage-Data.csv"):  # NOQA
    create_csv_file("Thermage-Data.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

chunks_of_zipcodes = list(list_into_chunks(zip_codes, 30))

# Loop through each chunk of zip codes
all_records = []
for chunk_index, single_chunk in enumerate(chunks_of_zipcodes):
    print(f"Processing chunk {chunk_index + 1}/{len(chunks_of_zipcodes)}")
    print(f"Zip codes: {single_chunk}")

    # Create list of requests using grequests
    requests_list = []
    for zipcode in single_chunk:
        lat, lon = get_lat_and_lon(zipcode)
        if lat is None or lon is None:
            print(f"Skipping zipcode {zipcode} - no lat/lon found.")
            continue  # Skip if lat/lon not found

        payload = {"request": {
            "appkey": "446B5128-16CF-11EF-ADB1-7BDCD2F445A6",
            "formdata": {
                "geoip": False,
                "dataview": "store_default",
                "limit": 100,
                "order": "_distance",
                "geolocs": {"geoloc": [
                    {
                        "addressline": zipcode,
                        "country": "",
                        "latitude": lat,
                        "longitude": lon,
                    }
                ]},
                "searchradius": "15|25|50|100|250",
                "where": {"or": {
                    "thermageflx": {"eq": "1"},
                    "thermagenxt": {"eq": "1"},
                    "thermagecpt": {"eq": "1"},
                    "clearbrilliant": {"eq": ""},
                    "clearbrilliantpermea": {"eq": ""},
                    "clearbrilliantpelo": {"eq": ""},
                    "clearbrillianttouch": {"eq": ""},
                    "fraxeldual155_1927": {"eq": ""},
                    "fraxel155": {"eq": ""},
                    "fraxel1927": {"eq": ""},
                    "fraxel_re_pair": {"eq": ""},
                    "fraxel_re_pairsst": {"eq": ""},
                    "vaserlipo": {"eq": ""},
                    "vasersmooth": {"eq": ""},
                    "vaserhidef": {"eq": ""}
                }},
                "false": "0"
            }
        }}

        req = grequests.post(url, json=payload, headers=headers, params=querystring)
        requests_list.append(req)

        # Send the requests concurrently
    responses = grequests.map(requests_list)  # NOQA

    # Process the responses
    for response in responses:
        if response is not None and response.status_code == 200:
            try:
                json_data = response.json()
                if 'response' in json_data and 'collection' in json_data['response']:
                    for data in json_data['response']['collection']:
                        company_name = data.get('name', '')
                        website = data.get('link', '')
                        phone = data.get('phone', '')
                        email = data.get('email', '')
                        # check which value is 1 and get its key
                        tags = [key for key, value in data.items() if value == "1"]
                        tags = ', '.join(tags)

                        record = [company_name, website, phone, email, tags]
                        all_records.append(record)

                else:
                    pass

            except ValueError as e:
                pass

    # Write all records to the CSV file after processing the chunk
    with open("Thermage-Data.csv", 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(all_records)

    # Remove duplicates after processing all records
    remove_duplicates_using_pandas("Thermage-Data.csv")

    # Record already done zip codes after processing the chunk
    with open("already-done.txt", 'a', encoding='utf-8') as file:
        for zip_code in single_chunk:
            file.write(f"{zip_code}\n")

    # Clear the records for the next chunk
    all_records.clear()
