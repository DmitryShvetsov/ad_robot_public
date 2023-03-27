import sys
import requests
import os
import json
import re


from script_settings import (
credentials_path,
dir_name,
camp_ids_filter,
big_white_image,
cut_url_postfixes,
page_text_from,
page_text_to,
prod_page_pattern,
im_hash_default,  # May be different for different campaigns.
just_non_image_ads,
just_im_hash_default_ads,  # To update just default image ads.
)

sys.path.append(credentials_path)
from credentials_common import (
    modules_dirs,
)

for dir in modules_dirs:
    sys.path.append(dir)
sys.path.append(os.path.join(credentials_path, dir_name))

from credentials import (
    token_direct,
    login_direct,
    Use_Operator_Units,
    CHEKING_TEXT_ON_OFF,
)

from constants_common import headers_200

from services_common import Common_instance
from services_feeds import CatCampXMLGenerator
from services_ya_dir import YaDirectHelper

common_instance = Common_instance()
path_vm_itself = common_instance.get_path_vm_itself(os.path.abspath(__file__))

ya_dir_instance = YaDirectHelper(token_direct, login_direct, Use_Operator_Units)

ads = ya_dir_instance.get_ads(
    None,
    None,
    camp_ids_filter,
    None,
    None,
    ["Id"],
    ["Href", "AdImageHash"],
    None,
    None,
    None,
    None,
    None,
)

ad_id_url = {}
account_urls = set()
for ad in ads:
    if just_non_image_ads and ad["TextAd"]["AdImageHash"]:
        continue

    if just_im_hash_default_ads and ad["TextAd"]["AdImageHash"] == im_hash_default:
        continue

    url = ad["TextAd"]["Href"]
    for cut_url_postfix in cut_url_postfixes:
        url = re.split(cut_url_postfix, url)[0]

    ad_id_url[ad["Id"]] = url
    account_urls.add(url)

feed = CatCampXMLGenerator([], "", "", "")
url_image_hash = {}
ad_id_hash = {}
counter = 0
for url in account_urls:
    counter += 1
    # Get image url.
    url_respond = requests.get(url, headers=headers_200)

    url_respond.encoding = "utf-8"
    page_text = url_respond.text

    (
        is_pattern,
        status_code,
        is_ConnectionError,
        is_TooManyRedirects,
    ) = common_instance.check_status_text_of_page(url, headers_200, CHEKING_TEXT_ON_OFF)

    if (
        not is_pattern
        or status_code != 200
        or is_ConnectionError
        or is_TooManyRedirects
    ):
        image_url = ""
    else:
        prod_url = re.split(prod_page_pattern, page_text)

        # Handle error.
        if len(prod_url) > 1:
            prod_url = prod_url[1]

            prod_url = re.split('"', prod_url)[0]
            prod_url = f"https://www.tsum.ru/product/{prod_url}"

            # Get image url.
            prod_url_respond = requests.get(prod_url, headers=headers_200)
            prod_url_respond.encoding = "utf-8"
            page_text = prod_url_respond.text

            image_url = re.split(page_text_from, page_text)[1]
            image_url = re.split(page_text_to, image_url)[0]

        else:
            print(f'No prod_page_pattern at the {url}')
            image_url = ""

    # Get image hash.
    if not image_url:
        im_hash = im_hash_default
    else:
        img_name = re.split("/", image_url)[-1]
        img_name = re.split(r"\?", img_name)[0]  # Make different for convenience.

        common_instance.download_file(image_url, path_vm_itself, img_name, headers_200)

        common_instance.make_image_square(path_vm_itself, img_name, big_white_image)

        im_hash = feed.get_hashes(path_vm_itself, img_name, "", login_direct, token_direct)

    url_image_hash[url] = im_hash

    if counter % 50 == 0 or counter == len(account_urls):
        for ad_id in ad_id_url:
            if ad_id_url[ad_id] in url_image_hash:
                ad_id_hash[ad_id] = url_image_hash[ad_id_url[ad_id]]

        ads_final = []
        for ad_id in ad_id_hash:
            dic = {"Id": ad_id, "TextAd": {"AdImageHash": ad_id_hash[ad_id]}}
            ads_final.append(dic)

        ads_final_json = json.dumps(ads_final, indent=4, ensure_ascii=False)
        with open(os.path.join(path_vm_itself, "ads_final.json"), "w", encoding="utf-8") as ouf:
            ouf.write(ads_final_json)

pass
