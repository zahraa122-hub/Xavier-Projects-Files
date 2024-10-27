import grequests
import pandas as pd
import csv
import os

payload = ""
headers = {
    "cookie": "sid=Bc0l7rYIKbcFHa4uJRwL3yilFuSHpvBOuB8; dwanonymous_bfda064952a7770e50aaa66c8f067dd2=abYGPQbwDdVEaNaV7BDM6LfFyU; __cq_dnt=1; dw_dnt=1; dwsid=PyzOKmokFh_LK_c2fPXCwRJE_SBbis_mdinKU3JXnb3n689QEo4r-jIwHJGsFpH--2GRriban1FUPvXEdUCarg%3D%3D",
    "accept": "*/*",
    "accept-language": "en,ur;q=0.9,en-GB;q=0.8,en-US;q=0.7",
    "origin": "https://ultherapy.com",
    "priority": "u=1, i",
    "referer": "https://ultherapy.com/",
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
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


if not os.path.exists("Ultherapy-Data.csv"):
    create_csv_file("Ultherapy-Data.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)  # Ensure all zip codes have 5 digits
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

chunks_of_zipcodes = list(list_into_chunks(zip_codes, 30))

for chunk_index, single_chunk in enumerate(chunks_of_zipcodes):
    print(f"chunk_index = {chunk_index + 1} / {len(chunks_of_zipcodes)}")  # NOQA

    # MAKE URLS
    urls = []
    for single_chunk_zip_code in single_chunk:
        lat, lon = get_lat_and_lon(single_chunk_zip_code)
        if lat and lon:
            urls.append(
                f"https://locator.merzusa.com/on/demandware.store/Sites-MerzNA-Site/default/Widget-GetData?scope=stores:{lat},{lon},100&brand=ultherapy")

            print(f"==> Scraping: {single_chunk_zip_code}")

    data = []
    responses = grequests.map([grequests.get(url) for url in urls])

    for single_response in responses:
        try:
            response_data = single_response.json()
        except:
            continue

        for item in response_data['stores']:
            company_name = item['name']
            website = item['practiceWebsite']
            phone = item['phone']
            email = ""
            tag = item['brandsEligibleFor']
            tags = ", ".join(tag)

            record = [company_name, website, phone, email, tags]
            data.append(record)

            with open("Ultherapy-Data.csv", 'a', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(record)

        remove_duplicates_using_pandas("Ultherapy-Data.csv")

        with open("already-done.txt", 'a', encoding='utf-8') as file:
            for zip_code in single_chunk:
                file.write(f"{zip_code}\n")
