import sys
import os
import json


from script_settings import (
credentials_path,
dir_name,
file_update_data
)

sys.path.append(credentials_path)
from credentials_common import modules_dirs
for dir in modules_dirs:
    sys.path.append(dir)
sys.path.append(os.path.join(credentials_path, dir_name))

from credentials import (
Use_Operator_Units,  # 'true' or 'false'.
token_direct,
login_direct,
)

from services_ya_dir import YaDirectHelper

with open(file_update_data, encoding='utf-8') as json_data:
    ads_data_update = json.load(json_data)

"""TextImageAds_exclude = []
for ad in ads_data_update:
    if ad.get('TextAd'):
        del ad['TextAd']['Title2']
        del ad['TextAd']['AdImageHash']
    elif ad.get('DynamicTextAd'):
        del ad['DynamicTextAd']['AdImageHash']
    elif ad.get('TextImageAd'):
        TextImageAds_exclude.append(ad["Id"])
    else:
        sys.exit(1)
for ad_id in TextImageAds_exclude:
    for ad in ads_data_update:
        if ad_id == ad["Id"]:
            ads_data_update.remove(ad)
            break"""

ya_dir_instance = YaDirectHelper(token_direct, login_direct, Use_Operator_Units)
ya_dir_instance.ya_dir_utf8()

result_to_see = ya_dir_instance.update_ads(ads_data_update)

pass
