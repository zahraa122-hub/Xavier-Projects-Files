import grequests
import requests
from lxml import html
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


# Initialize CSV file if it doesn't exist
if not os.path.exists("Cynsure-Data.csv"):  # NOQA
    create_csv_file("Cynsure-Data.csv")

df = pd.read_csv("Zipcodes.csv")  # NOQA
df['zip'] = df['zip'].astype(str).str.zfill(5)
zip_codes = df['zip'].tolist()
already_done_zip_codes = read_file_data("already-done.txt")
zip_codes = [code for code in zip_codes if code not in already_done_zip_codes]

for zip_code in zip_codes:

    print(f"==> Processing zip code: {zip_code}") # NOQA


    def get_device_links(url, device_id, headers):  # NOQA
        querystring = {
            "country": "US",
            "zipcode": zip_code,
            "proximity": "100",
            "product-id[]": device_id,
            "campaign-code": "default",
            "session-id": "default"
        }
        req = grequests.get(url, headers=headers, params=querystring)
        return req


    def extract_link_data(single_link, headers):  # NOQA
        response = requests.get(single_link, headers=headers)  # NOQA

        data = {
            'company_name': 'N/A',  # Default value
            'website': 'N/A',  # Default value
            'phone': 'N/A'  # Default value
        }

        if response.status_code == 200:
            tree = html.fromstring(response.text)

            data['company_name'] = extract_xpath(tree, "//h1//text()")
            data['website'] = tree.xpath('//div[@class="c-subheader__buttons"]//a')[0].get("href") if tree.xpath(
                '//div[@class="c-subheader__buttons"]//a') else 'N/A'
            data['phone'] = extract_xpath(tree, "//p//a[@href[contains(., 'tel:')]]//text()")

        else:
            print("Failed to retrieve data for:", single_link)

        return data


    def extract_xpath(tree, xpath_expression):  # NOQA
        """Extracts text from the given xpath expression."""
        try:
            return tree.xpath(xpath_expression)[0].strip() if tree.xpath(xpath_expression) else 'N/A'
        except IndexError:
            return 'N/A'


    url = "https://www.cynosure.com/results"

    list_of_devices = ["61", "34", "23", "68", "1", "20", "69", "58", "35", "2", "12", "3", "7", "65", "39",
                       "38", "74", "33", "37", "32", "31", "30", "64", "29", "75", "28", "81", "82", "27", "4",
                       "73", "44", "26", "25", "77", "80", "76", "84", "79", "78", "46", "24", "83", "22",
                       "21", "19", "18", "72", "17", "16", "15", "14", "13", "11", "10", "5", "9", "47", "48",
                       "6", "62", "40", "8", "59", "60"]

    list_of_device_names = ["Elite iQ", "Icon", "MonaLisa Touch", "MyEllevate", "Nitronox", "PicoSure", "PicoSure Pro",
                            "Potenza", "SculpSure®", "SculpSure® – Submental", "Smartlipo TriPlex", "TempSure® RF",
                            "Vectus", "ViaSure", "Acclaim", "Accolade", "Accufit", "Adivive", "Affirm", "Affirm CO2",
                            "Apogee", "Apogee+", "B.E. - Beautiful Energy Skincare", "Cellulaze", "Clarity II",
                            "Cynergy", "DermaV", "eCO2 Plus", "Elite", "Elite+", "Elite iQ Pro", "ELITE MD",
                            "Elite MPX", "Emerge", "Genius", "Healite", "Hollywood Spectra", "Infini", "LaseMD",
                            "LaseMD Ultra", "MEDLITE", "MedLite C6", "Mosaic 3D", "PelleFirm", "Pelleve", "PinPointe",
                            "PrecisionTx", "Replenishing Face Mask", "RevLite", "RevLite SI", "SlimLipo", "Smartlipo",
                            "Smartlipo MPX", "SmartSkin", "SmartSkin+", "SmartXide DOT", "SmoothShapes XV",
                            "STARLUX 300", "STARLUX 500", "StarLux", "StimSure", "Synchro REPLA:Y", "TriActive",
                            "Vivace", "ZWave Pro"]

    all_links = set()

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cookie": "PHPSESSID=878f22233630d2d89d0e3cf48ac7c8eb; _mkto_trk=id:016-OLH-093&token:_mch-cynosure.com-1729670005802-93774; afl_wc_utm_1_sess_landing=https%3A%2F%2Fwww.cynosure.com%2Fresults%2F%3Fcountry%3DUS%26zipcode%3D33160%26proximity%3D300%26campaign-code%3Ddefault%26session-id%3Ddefault; afl_wc_utm_1_sess_referer=https%3A%2F%2Fwww.google.com%2F; _gcl_au=1.1.2098350870.1729670008; _gid=GA1.2.66916702.1729670008; _hjSession_824318=eyJpZCI6IjY5MzBlM2NkLTc2N2MtNDk4Yy04NTUwLTk0YTdlZWI4YzRmZiIsImMiOjE3Mjk2NzAwMDg2NjUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=; _pa_user=c10eE5EHa1729670009; cebs=1; _sp_ses.7642=*; cnv_sp=d7fe6c69-6ed8-4dc7-9f50-48ffd1868e44; _hjSessionUser_824318=eyJpZCI6Ijc3Mjg4M2EwLWRhMGEtNTRlNy04YTNmLTNlNWNkMjFkNzkxZiIsImNyZWF0ZWQiOjE3Mjk2NzAwMDg2NjUsImV4aXN0aW5nIjp0cnVlfQ==; _ce.clock_data=-63%2C39.52.233.85%2C1%2Ca455ebc67d0b5007e2a055414dd14d78%2CChrome%2CPK; cebsp_=4; _ce.s=v~409d8a26f7e6c6468f17f2e0e9fe0babe6c8489e~lcw~1729670039718~vir~new~lva~1729670009907~vpv~0~v11.cs~403937~v11.s~e5071ef0-9113-11ef-8407-fbbdf09e7848~v11.sla~1729670039718~v11.send~1729670045147~lcw~1729670045147; locatorGeocode=true; locatorCountry=US; locatorZip=75207; amps_query=%7B%22country%22%3A%22US%22%2C%22zipcode%22%3A%2200601%22%2C%22proximity%22%3A%22100%22%2C%22product-id%22%3A%5B%2261%22%5D%2C%22campaign-code%22%3A%22default%22%2C%22session-id%22%3A%22default%22%7D; _gat_UA-1290973-1=1; _gat_UA-1290973-16=1; _ga=GA1.2.244190240.1729670008; _ga_7LY6836TP5=GS1.2.1729670009.1.1.1729670523.0.0.0; afl_wc_utm_1_main=%7B%22updated_ts%22%3A1729670523%2C%22cookie_expiry%22%3A90%2C%22consent_type%22%3A%22optout%22%2C%22sess_ts%22%3A1729670005%7D; _sp_id.7642=5ada6f7c-01c4-4963-89ce-b8ea8c4d49d6.1729670010.1.1729670535.1729670010.898a0849-5de0-4758-a220-448ff99ebb81; _ga_G09EJGMDL5=GS1.1.1729670008.1.1.1729670534.56.0.0",
        "user-agent": "Mozilla/5.0"
    }

    requests_list = [get_device_links(url, device_id, headers) for device_id in list_of_devices]

    responses = grequests.map(requests_list)

    for response, device_id, device_name in zip(responses, list_of_devices, list_of_device_names):
        if response and response.status_code == 200:
            tree = html.fromstring(response.text)
            get_links = tree.xpath("//a[text()='More Details']")

            for link in get_links:
                href = link.get("href")
                all_links.add((href, device_name))  # Save link and device name
        else:
            print(f"Failed to retrieve data for device {device_id}: {response.status_code}")

    links_with_device_names = list(all_links)
    print("==> Total links: ", len(links_with_device_names))

    for single_link, device_name in links_with_device_names:
        link_data = extract_link_data(single_link, headers)
        company_name = link_data.get('company_name', 'N/A')
        website = link_data.get('website', 'N/A')
        phone = link_data.get('phone', 'N/A')

        record = [company_name, website, phone, 'N/A', device_name]  # Save device name into CSV

        with open("Cynsure-Data.csv", mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(record)

        remove_duplicates_using_pandas("Cynsure-Data.csv")

        # save the links that have been processed
        with open("already-done.txt", 'a') as file:
            file.write(f"{single_link}\n")

        print(f"Saved record for company: {company_name} (Device: {device_name})")

    with open("already-done.txt", 'a') as file:
        file.write(f"{zip_code}\n")
