import requests
import csv
import pandas as pd
import os


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

    print(f"==> Saved new records: {len(df)}")
    df.to_csv(filename, index=False)


def get_lat_and_lon(us_zip_code):
    """Get latitude and longitude for a US zip code."""
    df = pd.read_csv("Zipcodes.csv")
    df['zip'] = df['zip'].astype(str).str.zfill(5)

    row = df[df['zip'] == us_zip_code]

    if not row.empty:
        return row['latitude'].values[0], row['longitude'].values[0]
    return None, None


if not os.path.exists("Zeltiq-Data.csv"):
    create_csv_file("Zeltiq-Data.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

url = "https://api.alle.com/graphql"

base_url = "https://alle.com/search/"
headers = {
    "accept": "*/*",
    "content-type": "application/json",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}


def payload_and_headers(lat, lon, page=0, limit=100):
    payload = {
        "operationName": "SearchQuery",
        "variables": {
            "limit": limit,
            "offset": page,
            "searchInput": {
                "sort": {
                    "column": "DEFAULT",
                    "order": "ASCENDING"
                },
                "filters": {
                    "proximity": {
                        "geoPoint": {
                            "latitude": lat,
                            "longitude": lon
                        },
                        "radiusInMiles": 50
                    },
                    "hours": {},
                    "profile": {
                        "productIds": [],
                        "treatmentAreaIds": []
                    }
                }
            }
        },
        "query": "query SearchQuery($limit: Int!, $offset: Int!, $searchInput: ProviderSearchInput!) { providerSearch(limit: $limit, offset: $offset, searchInput: $searchInput) { offsetPageInfo { totalResults limit offset nextOffset previousOffset } edges { displayDistance node { id providerOrganizationId parentProviderOrganizationId displayName profileSlug practiceType profileCompletenessPercentage address { address1 address2 city state zipcode } avatarImageUrl phoneNumber productIds treatmentAreaIds geoLocation { latitude longitude } consultationRequestSettings { feeTowardsTreatmentCost } indicators { nodes { label slug } } optInMarketingEvents { nodes { id title providerIsEnrolled } } businessHours { day open close closed } googleData { placeId reviewsRating totalNumReviews } } } } }"
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


def main():
    all_records = []
    limit = 100

    for zip_code in zip_codes:
        lat, lon = get_lat_and_lon(zip_code)
        if lat is None or lon is None:
            print(f"==> No data found for ZIP code: {zip_code}")
            continue

        page = 0
        while True:
            json_data = payload_and_headers(lat, lon, page, limit)

            total_records = json_data['data']['providerSearch']['offsetPageInfo']['totalResults']
            print(f"==> Total Records for ZIP {zip_code}: {total_records}, Fetching from offset: {page}")

            for item in json_data['data']['providerSearch']['edges']:
                company_name = item['node']['displayName']
                website_url = item['node']['profileSlug']
                website = base_url + website_url
                phone = item['node']['phoneNumber']
                email = ''
                tags = item['node']['treatmentAreaIds']
                tags = ', '.join(tags) if tags else ''

                record = [company_name, website, phone, email, tags]
                all_records.append(record)

                # Save records to CSV after processing all ZIP codes
                with open("Zeltiq-Data.csv", mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerows(all_records)

            remove_duplicates_using_pandas("Zeltiq-Data.csv")

            page += limit
            if page >= total_records:
                break

        with open("already-done.txt", "a") as file:
            file.write(f"{zip_code}\n")


if __name__ == "__main__":
    main()
