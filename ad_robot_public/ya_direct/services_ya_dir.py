import sys
import json
import requests
import re
import os
import datetime  # To get time for Direct report name.
from time import sleep


from constants_common import (
    YA_DIR_API_CAMPAIGNS_URL,
    YA_DIR_API_GROUPS_URL,
    YA_DIR_API_KEYWORDS_URL,
    YA_DIR_API_ADS_URL,
    YA_DIR_API_KEYWORD_BIDS_URL,
    YA_DIR_API_SITELINKS_URL,
    YA_DIR_KEYWORDS_RESEARCH_URL,
    YA_DIR_API4_URL,
    YA_DIR_FORBIDDEN_ADS_SYMBOLS,
    YA_DIR_API_BID_ADJUST,
)
from services_common import Common_instance

#  Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b""):
            return x.decode("utf8")
        else:
            return x

class YaDirectHelper:
    def __init__(self, token_direct, login_direct, Use_Operator_Units):
        self.headers = {
            "Authorization": f"Bearer {token_direct}",
            "Client-Login": login_direct,
            "Accept-Language": "en",
            "Use-Operator-Units": Use_Operator_Units,  # To use the agency API scores.
        }
        self.token_direct = token_direct
        self.login_direct = login_direct

    def ya_dir_utf8(self):
        #  Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
        if sys.version_info < (3,):
            def u(x):
                try:
                    return x.encode("utf8")
                except UnicodeDecodeError:
                    return x
        else:
            def u(x):
                if type(x) == type(b""):
                    return x.decode("utf8")
                else:
                    return x

    def get_ad_groups(
        self,
        Ids,
        CampaignIds,
        Name,
        Statuses,
        ServingStatuses,
        FieldNames,
        DynamicTextAdGroupFieldNames,
    ):
        if Ids and CampaignIds:
            raise ValueError("Cannot be both campaigns and ad groups filtering.")
        elif not Ids and not CampaignIds:
            return set(), []

        if Ids:
            selection_level = "Ids"
            all_selectors = Ids
            group_size = 10000
        elif CampaignIds:
            selection_level = "CampaignIds"
            all_selectors = CampaignIds
            group_size = 10

        selection_groups = Common_instance.divide_to_small_groups(
            all_selectors, group_size
        )
        ad_groups_ids = []
        ad_groups = []
        # Iterate in parts with Yandex.Direct API requirements.
        for elements in selection_groups:
            offset = 0  # Number of objects to get next iteration.
            while True:  # To get all objects using as many iteration as need.
                body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {selection_level: elements},
                        "FieldNames": FieldNames,
                        "Page": {"Offset": offset},
                    },
                }

                if Statuses:
                    body["params"]["SelectionCriteria"]["Statuses"] = Statuses
                if ServingStatuses:
                    body["params"]["SelectionCriteria"][
                        "ServingStatuses"
                    ] = ServingStatuses
                if DynamicTextAdGroupFieldNames:
                    body["params"][
                        "DynamicTextAdGroupFieldNames"
                    ] = DynamicTextAdGroupFieldNames

                # Кодирование тела запроса в JSON.
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_GROUPS_URL, jsonBody, headers=self.headers
                )
                result = result.json()
                if "result" in result:
                    if "AdGroups" in result["result"]:
                        # Iterate and get groups ids.
                        for ad_group_data in result["result"]["AdGroups"]:
                            if Name and not re.match(Name, ad_group_data["Name"]):
                                continue
                            if (
                                "Id" in FieldNames
                                and ad_group_data["Id"] not in ad_groups_ids
                            ):
                                ad_groups_ids.append(ad_group_data["Id"])
                            ad_groups.append(ad_group_data)

                # Check whether all objects were got or not.
                if "LimitedBy" not in result["result"]:
                    break
                else:
                    offset = result["result"]["LimitedBy"]

        return ad_groups_ids, ad_groups

    def ad_groups_update(self, ad_groups_data):
        def add_results(body_data, results_lst):
            jsonBody = json.dumps(body_data, ensure_ascii=False).encode("utf8")
            result_data = requests.post(
                YA_DIR_API_GROUPS_URL, jsonBody, headers=self.headers
            )
            result_data = result_data.json()
            results_lst += result_data["result"]["UpdateResults"]
            # Raise error to handle it.
            for res in result_data["result"]["UpdateResults"]:
                if "Errors" in res:
                    raise Exception("Errors in ad_groups_update")
            return results_lst

        counter = 0
        results = []
        body = {"method": "update", "params": {"AdGroups": []}}
        for ad_group_data in ad_groups_data:
            counter += 1
            body["params"]["AdGroups"].append(ad_group_data)

            if counter % 1000 == 0:
                results = add_results(body, results)
                body["params"]["AdGroups"] = []

        if counter % 1000 != 0:
            results = add_results(body, results)

        return results

    def add_ad_group(self, ad_group_name, camp_id, regions, neg_kws):
        body = {
            "method": "add",
            "params": {
                "AdGroups": [
                    {
                        "Name": ad_group_name,
                        "CampaignId": camp_id,
                        "RegionIds": regions,
                    }
                ]
            },
        }

        # Add negative keywords to body.
        if neg_kws:  # Don't use empty.
            body["params"]["AdGroups"][0]["NegativeKeywords"] = {"Items": neg_kws}

        # Кодирование тела запроса в JSON.
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API_GROUPS_URL, jsonBody, headers=self.headers)
        result = result.json()

        # Get created group id.
        adGroupId = result["result"]["AddResults"][0]["Id"]
        return adGroupId

    def get_campaigns(self, Ids, States):
        campaigns = []
        offset = 0  # Number of objects to get next iteration.
        while True:
            body = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {},
                    "FieldNames": ["Id", "Name", "State", "Status", "NegativeKeywords"],
                    "Page": {"Offset": offset},
                },
            }

            if Ids:
                body["params"]["SelectionCriteria"]["Ids"] = Ids
            if States:
                body["params"]["SelectionCriteria"]["States"] = States

            # Кодирование тела запроса в JSON.
            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            result = requests.post(
                YA_DIR_API_CAMPAIGNS_URL, jsonBody, headers=self.headers
            )
            result = result.json()

            # Obtains campaigns data.
            for campaign in result["result"]["Campaigns"]:
                campaigns.append(campaign)

            # Check whether all objects were got or not.
            if "LimitedBy" not in result["result"]:
                break
            else:
                offset = result["result"]["LimitedBy"]

        return campaigns

    def create_campaign(
        self, new_camp_name, search_strat_type, networks_strat_type, neg_phrases
    ):
        # Set networks strategy type:
        # SERVING_OFF or MAXIMUM_COVERAGE for networks or HIGHEST_POSITION for search.
        today = datetime.datetime.today()
        body = {
            "method": "add",
            "params": {
                "Campaigns": [
                    {
                        "Name": new_camp_name,
                        "StartDate": str(today.strftime("%Y-%m-%d")),
                        # Create stopped handle strategy campaigns for now.
                        "DailyBudget": {"Amount": 300 * 1000000, "Mode": "STANDARD"},
                        # We create text campaign.
                        "TextCampaign": {
                            "BiddingStrategy": {
                                "Search": {"BiddingStrategyType": search_strat_type},
                                "Network": {"BiddingStrategyType": networks_strat_type},
                            }
                        },
                    }
                ]
            },
        }

        if neg_phrases:
            body["params"]["Campaigns"][0]["NegativeKeywords"] = {"Items": neg_phrases}

        # Кодирование тела запроса в JSON.
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API_CAMPAIGNS_URL, jsonBody, headers=self.headers)
        result = result.json()
        # Sleep for a second not to create campaigns with the same names.
        sleep(1)
        camp_id = result["result"]["AddResults"][0]["Id"]

        return camp_id

    def campaign_set_state(self, CampaignIds, state):
        selection_groups = Common_instance.divide_to_small_groups(
            CampaignIds, 10
        )  # 1000 is maximum.

        for selection_group in selection_groups:
            body = {
                "method": state,
                "params": {"SelectionCriteria": {"Ids": selection_group}},
            }

            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            result = requests.post(
                YA_DIR_API_CAMPAIGNS_URL, jsonBody, headers=self.headers
            )
            result = result.json()

    def get_keyword_bids(
        self, CampaignIds, AdGroupIds, KeywordIds, TrafficVolume_needs, use_prev
    ):
        """if CampaignIds and AdGroupIds:
            raise ValueError('Cannot be both campaigns and ad groups filtering.')
        elif CampaignIds and KeywordIds:
            raise ValueError('Cannot be both campaigns and keywords filtering.')
        elif AdGroupIds and KeywordIds:
            raise ValueError('Cannot be both ad groups and keywords filtering.')"""

        # Mutate items to integers for correctly comparison.
        if CampaignIds:
            CampaignIds = [int(x) for x in CampaignIds]
        if AdGroupIds:
            AdGroupIds = [int(x) for x in AdGroupIds]
        if KeywordIds:
            KeywordIds = [int(x) for x in KeywordIds]

        if KeywordIds:
            selection_level = "KeywordIds"
            all_selectors = KeywordIds
            group_size = 10000
        elif AdGroupIds:
            selection_level = "AdGroupIds"
            all_selectors = AdGroupIds
            group_size = 1000
        elif CampaignIds:
            selection_level = "CampaignIds"
            all_selectors = CampaignIds
            group_size = 10

        # Iterate by partitions to avoid too many selection elements error.
        selection_groups = Common_instance.divide_to_small_groups(
            all_selectors, group_size
        )

        all_kw_bids = []
        for elements in selection_groups:
            print(len(selection_groups))
            offset = 0  # Number of keyword to get next iteration.
            while True:
                body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {selection_level: elements},
                        "FieldNames": [
                            "KeywordId",
                            "StrategyPriority",
                            "AdGroupId",
                            "CampaignId",
                        ],
                        "SearchFieldNames": ["Bid", "AuctionBids"],
                        "NetworkFieldNames": ["Bid", "Coverage"],
                        "Page": {"Limit": 10000, "Offset": offset},
                    },
                }
                # Кодирование тела запроса в JSON
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_KEYWORD_BIDS_URL, jsonBody, headers=self.headers
                )
                result = result.json()
                # Handle when there is no bids.
                if "KeywordBids" in result["result"]:
                    kw_bids = result["result"]["KeywordBids"]

                    if TrafficVolume_needs:
                        for kw_bid_data in kw_bids:
                            if (
                                kw_bid_data.get("Search")
                                and kw_bid_data["Search"].get("AuctionBids")
                                and kw_bid_data["Search"]["AuctionBids"].get(
                                    "AuctionBidItems"
                                )
                            ):
                                traff_forecasts_cut = []
                                AuctionBidItems = kw_bid_data["Search"]["AuctionBids"][
                                    "AuctionBidItems"
                                ]

                                for traff_forecast in AuctionBidItems:
                                    if (
                                        traff_forecast["TrafficVolume"]
                                        in TrafficVolume_needs
                                    ):
                                        traff_forecasts_cut.append(traff_forecast)

                                # Handle case API returned less traffic volume items.
                                if use_prev and len(traff_forecasts_cut) < len(
                                    TrafficVolume_needs
                                ):
                                    for TrafficVolume in TrafficVolume_needs:
                                        for traff_forecast in AuctionBidItems:
                                            if (
                                                traff_forecast["TrafficVolume"]
                                                <= TrafficVolume
                                            ):
                                                traff_forecast[
                                                    "TrafficVolume"
                                                ] = TrafficVolume  # To match later.

                                                if (
                                                    traff_forecast
                                                    not in traff_forecasts_cut
                                                ):
                                                    traff_forecasts_cut.append(
                                                        traff_forecast
                                                    )
                                                break

                                kw_bid_data["Search"]["AuctionBids"][
                                    "AuctionBidItems"
                                ] = traff_forecasts_cut

                    all_kw_bids += kw_bids

                # Check whether all keywords were got or not.
                if "LimitedBy" not in result["result"]:
                    break
                else:
                    offset = result["result"]["LimitedBy"]

        # Filter data.
        kws_to_del = []
        for kw in all_kw_bids:
            if CampaignIds and kw["CampaignId"] not in CampaignIds:
                kws_to_del.append(kw)
                continue
            if AdGroupIds and kw["AdGroupId"] not in AdGroupIds:
                kws_to_del.append(kw)
                continue
            if KeywordIds and kw["KeywordId"] not in KeywordIds:
                kws_to_del.append(kw)

        for kw in kws_to_del:
            all_kw_bids.remove(kw)

        return all_kw_bids

    def direct_statistics(
        self,
        path_vm,
        dir_name,
        YA_DIR_API_REPORTS_URL,
        fileName,
        DateFrom,
        DateTo,
        FieldNames,
        ReportType,
        IncludeVAT,
        Goals,
        Filters,
    ):
        headers = self.headers
        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": DateFrom,
                    "DateTo": DateTo,
                },
                "FieldNames": FieldNames,
                "ReportName": "direct_statistics "
                + str(datetime.datetime.now())
                + " "
                + dir_name,
                "ReportType": ReportType,
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": IncludeVAT,
                "IncludeDiscount": "YES",
            }
        }

        if Goals:
            body["params"]["Goals"] = Goals
        if Filters:
            body["params"]["SelectionCriteria"]["Filter"] = Filters

        # Encoding the request message body as JSON.
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")

        # Clear file to write results there.
        with open(os.path.join(path_vm, fileName), "w") as ouf:
            ouf.write("")

        while True:
            try:
                req = requests.post(YA_DIR_API_REPORTS_URL, jsonBody, headers=headers)
                if req.status_code == 400:
                    print("Invalid request parameters, or the report queue is full")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON code for the request: {}".format(u(body)))
                    print(
                        "JSON code for the server response: \n{}".format(u(req.json()))
                    )
                    sys.exit(1)
                    break
                elif req.status_code == 200:
                    print("Report created successfully")
                    with open(os.path.join(path_vm, fileName), "a") as ouf:
                        ouf.write("Report contents: \n{}".format(u(req.text)))
                    break
                elif req.status_code == 201:
                    print("Report successfully added to the offline queue")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Request will be resent in {} seconds".format(retryIn))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 202:
                    print("Report is being created in offline mode")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Request will be resent in {} seconds".format(retryIn))
                    print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 500:
                    print(
                        "Error occurred when creating the report. Please repeat the request again later"
                    )
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print(
                        "JSON code for the server's response: \n{}".format(
                            u(req.json())
                        )
                    )
                    break
                elif req.status_code == 502:
                    print("Exceeded the server limit on report creation time.")
                    print(
                        "Please try changing the request parameters: reduce the time period and the amount of data requested."
                    )
                    print("JSON code for the request: {}".format(body))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print(
                        "JSON code for the server's response: \n{}".format(
                            u(req.json())
                        )
                    )
                    break
                else:
                    print("Unexpected error")
                    print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    print("JSON code for the request: {}".format(body))
                    print(
                        "JSON code for the server's response: \n{}".format(
                            u(req.json())
                        )
                    )
                    break

            # Error handling if the connection with the Yandex.Direct API server wasn't established
            except ConnectionError:
                # In this case, we recommend repeating the request again later
                print("Error connecting to the Yandex.Direct API server")
                # Forced exit from loop
                break

            # If any other error occurred
            except:
                # In this case, you should analyze the application's actions
                print("Unexpected error")
                # Forced exit from loop
                break

    def get_ads(
        self,
        Ids,
        AdGroupIds,
        CampaignIds,
        States,
        Statuses,
        FieldNames,
        TextAdFieldNames,
        DynamicTextAdFieldNames,
        TextImageAdFieldNames,
        MobileAppAdFieldNames,
        MobileAppImageAdFieldNames,
        TextAdPriceExtensionFieldNames,
    ):
        if (
            (Ids and AdGroupIds)
            or (Ids and CampaignIds)
            or (AdGroupIds and CampaignIds)
        ):
            raise Exception("Cannot be more than 1 selection criteria")

        ads = []
        if Ids:
            selection_level = "Ids"
            all_selectors = Ids
            group_size = 10000
        elif AdGroupIds:
            selection_level = "AdGroupIds"
            all_selectors = AdGroupIds
            group_size = 1000
        elif CampaignIds:
            selection_level = "CampaignIds"
            all_selectors = CampaignIds
            group_size = 10

        # Iterate by partitions to avoid too many selection elements error.
        selection_groups = Common_instance.divide_to_small_groups(
            all_selectors, group_size
        )

        for elements in selection_groups:
            offset = 0  # Number of objects to get next iteration.
            while True:
                body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {selection_level: elements},
                        "FieldNames": FieldNames,
                        "Page": {"Offset": offset},
                    },
                }

                if TextAdFieldNames:
                    body["params"]["TextAdFieldNames"] = TextAdFieldNames
                if DynamicTextAdFieldNames:
                    body["params"]["DynamicTextAdFieldNames"] = DynamicTextAdFieldNames
                if TextImageAdFieldNames:
                    body["params"]["TextImageAdFieldNames"] = TextImageAdFieldNames
                if MobileAppAdFieldNames:
                    body["params"]["MobileAppAdFieldNames"] = MobileAppAdFieldNames
                if MobileAppImageAdFieldNames:
                    body["params"][
                        "MobileAppImageAdFieldNames"
                    ] = MobileAppImageAdFieldNames
                if TextAdPriceExtensionFieldNames:
                    body["params"][
                        "TextAdPriceExtensionFieldNames"
                    ] = TextAdPriceExtensionFieldNames

                if States:
                    body["params"]["SelectionCriteria"]["States"] = States
                if Statuses:
                    body["params"]["SelectionCriteria"]["Statuses"] = Statuses

                # Кодирование тела запроса в JSON
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_ADS_URL, jsonBody, headers=self.headers
                )
                result = result.json()
                # To prevent the error.
                if "result" in result:
                    if "Ads" in result["result"]:
                        for ad in result["result"]["Ads"]:
                            ads.append(ad)

                # Check whether all objects were got or not.
                if "LimitedBy" not in result["result"]:
                    break
                else:
                    offset = result["result"]["LimitedBy"]

        return ads

    def update_ads(self, ads_data_update):
        result_to_see = ""
        counter = 0
        body = {"method": "update", "params": {"Ads": []}}
        for ad_data in ads_data_update:
            counter += 1
            body["params"]["Ads"].append(ad_data)

            if counter % 1000 == 0:
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_ADS_URL, jsonBody, headers=self.headers
                )
                result_to_see += f"{result.json()}\n\n"
                body["params"]["Ads"] = []

        if counter % 1000 != 0:
            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            result = requests.post(YA_DIR_API_ADS_URL, jsonBody, headers=self.headers)
            result_to_see += f"{result.json()}\n\n"

        return result_to_see

    def ads_set_state(self, state, ids):
        counter = 0
        result = (
            {}
        )  # To prevent UnboundLocalError: local variable 'result' referenced before assignment.

        body = {"method": state, "params": {"SelectionCriteria": {"Ids": []}}}
        for ad_id in ids:
            counter += 1
            body["params"]["SelectionCriteria"]["Ids"].append(ad_id)

            if counter % 10000 == 0:
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_ADS_URL, jsonBody, headers=self.headers
                )
                result = result.json()
                body["params"]["SelectionCriteria"]["Ids"] = []

        if counter % 10000 != 0:
            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            result = requests.post(YA_DIR_API_ADS_URL, jsonBody, headers=self.headers)
            result = result.json()

        return result

    def get_sitelinks(self, Ids, FieldNames):
        SitelinksSets = []
        if Ids:
            selection_level = "Ids"
            all_selectors = Ids
            group_size = 10000

        # Iterate by partitions to avoid too many selection elements error.
        selection_groups = Common_instance.divide_to_small_groups(
            all_selectors, group_size
        )

        for elements in selection_groups:
            offset = 0  # Number of keyword to get next iteration.
            while True:
                body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {selection_level: elements},
                        "FieldNames": FieldNames,
                        "Page": {"Limit": 10000, "Offset": offset},
                    },
                }
                jsonBody = json.dumps(body, ensure_ascii=False).encode(
                    "utf8"
                )  # Кодирование тела запроса в JSON
                result = requests.post(
                    YA_DIR_API_SITELINKS_URL, jsonBody, headers=self.headers
                )
                result = result.json()

                # Handle case there is no sitelinks.
                if "SitelinksSets" in result["result"]:
                    SitelinksSets += result["result"]["SitelinksSets"]

                # Check whether all keywords were got or not.
                if "LimitedBy" not in result["result"]:
                    break
                else:
                    offset = result["result"]["LimitedBy"]

        return SitelinksSets

    def add_ads(
        self,
        AdGroupId,
        Title,
        Title2,
        Texts,
        Href,
        mobile,  # ["YES", "NO"]
        DisplayUrlPath,
        VCardId,
        AdImageHash,
        SitelinkSetId,
        callouts_ids,
        VideoExtension,
        PriceExtension,
        TurboPageId,
        BusinessId,
        PreferVCardOverBusiness,
    ):
        body = {"method": "add", "params": {"Ads": []}}
        # Add mobile and desktop ads.
        for Mobile in mobile:
            # count_image = 0 # To choose format of image.
            for Text in Texts:
                # count_image += 1
                # Set 2 regular and 1 wide image.
                # if count_image == 3:
                #    AdImageHash = AdImageHash_wide
                # else:
                #    AdImageHash = AdImageHash_regular

                # Set body content.
                body_content = {
                    "AdGroupId": AdGroupId,
                    "TextAd": {
                        "Title": Title,
                        "Text": Text,
                        "Href": Href,
                        "Mobile": Mobile,
                    },
                }

                if Title2:
                    body_content["TextAd"]["Title2"] = Title2

                if SitelinkSetId:
                    body_content["TextAd"]["SitelinkSetId"] = SitelinkSetId

                if callouts_ids:
                    body_content["TextAd"]["AdExtensionIds"] = callouts_ids

                if DisplayUrlPath:
                    body_content["TextAd"]["DisplayUrlPath"] = DisplayUrlPath

                if VCardId:
                    body_content["TextAd"]["VCardId"] = VCardId

                if AdImageHash:
                    body_content["TextAd"]["AdImageHash"] = AdImageHash

                if VideoExtension:
                    body_content["TextAd"]["VideoExtension"] = VideoExtension

                if PriceExtension:
                    body_content["TextAd"]["PriceExtension"] = PriceExtension

                if TurboPageId:
                    body_content["TextAd"]["TurboPageId"] = TurboPageId

                if BusinessId:
                    body_content["TextAd"]["BusinessId"] = BusinessId

                if PreferVCardOverBusiness:
                    body_content["TextAd"][
                        "PreferVCardOverBusiness"
                    ] = PreferVCardOverBusiness

                # Replace forbidden characters.
                if body_content.get("TextAd"):
                    for elem in body_content["TextAd"]:
                        if type(body_content["TextAd"][elem]) == str:
                            body_content["TextAd"][elem] = re.sub(
                                YA_DIR_FORBIDDEN_ADS_SYMBOLS,
                                "",
                                body_content["TextAd"][elem],
                            )

                body["params"]["Ads"].append(body_content)

        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API_ADS_URL, jsonBody, headers=self.headers)
        result = result.json()

        created_ads = []
        for AdId in result["result"]["AddResults"]:
            created_ads.append(AdId["Id"])

        return created_ads

    def get_keywords(self, Ids, AdGroupIds, CampaignIds, States):
        kw_id_data = {}
        if Ids:
            selection_level = "Ids"
            all_selectors = Ids
            group_size = 10000
        elif AdGroupIds:
            selection_level = "AdGroupIds"
            all_selectors = AdGroupIds
            group_size = 1000
        elif CampaignIds:
            selection_level = "CampaignIds"
            all_selectors = CampaignIds
            group_size = 10

        # Iterate by partitions to avoid too many selection elements error.
        selection_groups = Common_instance.divide_to_small_groups(
            all_selectors, group_size
        )
        for elements in selection_groups:
            offset = 0  # Number of keyword to get next iteration.
            while True:
                body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {
                            selection_level: elements,
                        },
                        "FieldNames": ["Id", "Keyword", "AdGroupId", "CampaignId"],
                        "Page": {"Limit": 10000, "Offset": offset},
                    },
                }

                if States:
                    body["params"]["SelectionCriteria"]["States"] = States

                # Кодирование тела запроса в JSON.
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_KEYWORDS_URL, jsonBody, headers=self.headers
                )
                result = result.json()

                # Handle case there is no keywords for DSA campaigns.
                if "Keywords" in result["result"]:
                    for i in result["result"]["Keywords"]:
                        kw_id_data[i["Id"]] = {
                            "Keyword": i["Keyword"],
                            "AdGroupId": i["AdGroupId"],
                            "CampaignId": i["CampaignId"],
                        }

                # Check whether all keywords were got or not.
                if "LimitedBy" not in result["result"]:
                    break
                else:
                    offset = result["result"]["LimitedBy"]

        return kw_id_data

    def add_keywords(self, new_kws, adGroupId, Bid, ContextBid):
        if len(new_kws) > 200:
            raise Exception("Cannot be more than 200 keywords per ad group.")

        body = {"method": "add", "params": {"Keywords": []}}

        for kw in new_kws:
            kw_data = {
                "Keyword": kw,
                "AdGroupId": adGroupId,
            }
            if Bid:
                kw_data["Bid"] = Bid
            if ContextBid:
                kw_data["ContextBid"] = ContextBid

            body["params"]["Keywords"].append(kw_data)

        # Кодирование тела запроса в JSON
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API_KEYWORDS_URL, jsonBody, headers=self.headers)
        result = result.json()

    def set_keyword_bids(self, bids):
        counter = 0
        body = {"method": "set", "params": {"KeywordBids": []}}
        for bid in bids:
            counter += 1
            body["params"]["KeywordBids"].append(bid)

            if counter % 10000 == 0:
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_KEYWORD_BIDS_URL, jsonBody, headers=self.headers
                )
                body["params"]["KeywordBids"] = []

        if counter % 10000 != 0:
            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            result = requests.post(
                YA_DIR_API_KEYWORD_BIDS_URL, jsonBody, headers=self.headers
            )

        return result.json()

    def kws_no_search_volume(self, path_vm, kws, regions):
        # Check all_potential_kws for rare status.
        # Get keywords could be rare showing.
        kws_no_search_volume = set()  # To store rare showing keywords to remove them.
        kws_has_search_volume = set()

        # Remove already got no search volume keywords.
        file_kws_no_search_volume = "kws_no_search_volume.csv"
        file_kws_has_search_volume = "kws_has_search_volume.csv"

        Common_instance.deploy_dir_files(
            None,
            [
                os.path.join(path_vm, file_kws_no_search_volume),
                os.path.join(path_vm, file_kws_has_search_volume),
            ],
        )

        with open(
            os.path.join(path_vm, file_kws_no_search_volume), encoding="utf-8"
        ) as ouf:
            for line in ouf:
                kws_no_search_volume.add(line.strip())
        kws = [x for x in kws if x not in kws_no_search_volume]

        with open(
            os.path.join(path_vm, file_kws_has_search_volume), encoding="utf-8"
        ) as ouf:
            for line in ouf:
                kws_has_search_volume.add(line.strip())
        kws = [x for x in kws if x not in kws_has_search_volume]

        # Divide keywords to smaller groups because of Yandex.Direct requirements.
        selection_groups = Common_instance.divide_to_small_groups(
            kws, 7000
        )  # Not 10000 to speed up the process.

        counter_to_see = 1
        for phrases_group in selection_groups:
            # Handle case all keyword search volume are already obtained.
            if not phrases_group:
                continue

            with open(
                os.path.join(path_vm, "to_see_step_kws_no_search_volume.csv"),
                "w",
                encoding="utf-8",
            ) as inf:
                inf.write(
                    f"{counter_to_see} of len selection_groups {len(selection_groups)}"
                )
            counter_to_see += 1

            body = {
                "method": "hasSearchVolume",
                "params": {
                    "SelectionCriteria": {
                        "Keywords": phrases_group,
                        "RegionIds": regions,
                    },
                    "FieldNames": ["Keyword", "AllDevices", "MobilePhones", "Desktops"]
                    # "Tablets", "RegionIds",
                },
            }
            # Кодирование тела запроса в JSON
            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            # Handle error 'requests.exceptions.ConnectionError: ('Connection aborted.',
            # RemoteDisconnected('Remote end closed connection without response'))'
            times_to_try = 11
            for try_time in range(times_to_try):
                try:
                    result = requests.post(
                        YA_DIR_KEYWORDS_RESEARCH_URL, jsonBody, headers=self.headers
                    )
                    break
                except:
                    print(f"KeywordsResearchURL exception {try_time}")
                    sleep(9)
                    if try_time == times_to_try - 1:
                        raise Exception(
                            f"KeywordsResearchURL more than {times_to_try} attempts."
                        )
            result = result.json()

            for i in result["result"]["HasSearchVolumeResults"]:
                if (
                    i["AllDevices"] == "NO"
                    or i["MobilePhones"] == "NO"
                    or i["Desktops"] == "NO"
                ):  # or i['Tablets'] == 'NO'
                    kws_no_search_volume.add(i["Keyword"])

                    with open(
                        os.path.join(path_vm, file_kws_no_search_volume),
                        "a",
                        encoding="utf-8",
                    ) as inf:
                        inf.write(f'{i["Keyword"]}\n')
                else:
                    with open(
                        os.path.join(path_vm, file_kws_has_search_volume),
                        "a",
                        encoding="utf-8",
                    ) as inf:
                        inf.write(f'{i["Keyword"]}\n')

            sleep(
                3.1
            )  # No more than 20 requests per 60 seconds according to Yandex requirements.

        return kws_no_search_volume

    def add_neg_kws_ad_groups(self, ad_group_id_addit_neg_kws):
        # Add negative keywords to the ad groups.
        Ids = []
        for ad_gr_id in ad_group_id_addit_neg_kws:
            Ids.append(ad_gr_id)

        # Get current ad groups negative kws.
        _, ad_groups = self.get_ad_groups(
            Ids, None, None, None, None, ["Id", "NegativeKeywords"], None
        )

        for ad_group in ad_groups:
            if ad_group["NegativeKeywords"]:
                ad_group["NegativeKeywords"]["Items"] += list(
                    ad_group_id_addit_neg_kws[str(ad_group["Id"])]
                )
            else:
                ad_group["NegativeKeywords"] = {}
                ad_group["NegativeKeywords"]["Items"] = list(
                    ad_group_id_addit_neg_kws[str(ad_group["Id"])]
                )

        for ad_group in ad_groups:
            length_neg_kws = 0
            for neg_kw in ad_group["NegativeKeywords"]["Items"]:
                neg_kw = neg_kw.replace(" ", "")
                length_neg_kws += len(neg_kw)
                if length_neg_kws > 4096:
                    raise Exception("length_neg_kws > 4096")

        # Update ad groups.
        result = self.ad_groups_update(ad_groups)
        return result

    def get_wordstat_kws(self, phrases, regions):
        # Create report to get keywords.
        # Get search volume of keywords.
        body = {
            "token": self.token_direct,
            "Client-Login": self.login_direct,
            "method": "CreateNewWordstatReport",
            "param": {"Phrases": phrases, "GeoID": regions},
        }

        # Кодирование тела запроса в JSON.
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API4_URL, jsonBody)
        result = result.json()
        # Get report id.
        report_id = result["data"]

        # Wait for report is ready.
        is_break = False
        counter_infinity = 0  # Counter to prevent infinity iteration.
        while True:
            if is_break:  # Stop iteration if report is ready.
                break
            body = {
                "token": self.token_direct,
                "Client-Login": self.login_direct,
                "method": "GetWordstatReportList",
            }

            # Кодирование тела запроса в JSON
            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            result = requests.post(YA_DIR_API4_URL, jsonBody)
            result = result.json()

            for report in result["data"]:
                if report["ReportID"] == report_id and report["StatusReport"] == "Done":
                    is_break = True
                    break
                elif (
                    report["ReportID"] == report_id
                    and report["StatusReport"] == "Failed "
                ):
                    print("Report is failed")
                    sys.exit(1)
            sleep(1)  # Wait for respond be done.
            counter_infinity += 1
            if counter_infinity > 300:  # Stop loop if it goes for too long time.
                print("Infinity loop")
                sys.exit(1)

        # Get report data.
        body = {
            "token": self.token_direct,
            "Client-Login": self.login_direct,
            "method": "GetWordstatReport",
            "param": report_id,
        }
        # Кодирование тела запроса в JSON
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API4_URL, jsonBody)
        result = result.json()

        if len(result["data"]) > 0:
            obtained_phrases = []
            for kw in result["data"][0]["SearchedWith"]:
                obtained_phrases.append(kw["Phrase"])
        else:
            obtained_phrases = phrases

        return obtained_phrases

    def kws_set_state(self, state, ids):
        counter = 0
        body = {"method": state, "params": {"SelectionCriteria": {"Ids": []}}}
        for kw_id in ids:
            counter += 1
            body["params"]["SelectionCriteria"]["Ids"].append(kw_id)

            if counter % 10000 == 0:
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_KEYWORDS_URL, jsonBody, headers=self.headers
                )
                body["params"]["SelectionCriteria"]["Ids"] = []

        if counter % 10000 != 0:
            jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
            result = requests.post(
                YA_DIR_API_KEYWORDS_URL, jsonBody, headers=self.headers
            )

    def kws_deduplicate(self, kws_all, kws_potential):
        # Replace "-" to " " to avoid error 'В ключевой фразе неправильное использование знака "-"'.
        for kw_data in kws_all:
            kw = kw_data["Keyword"]
            pattern = r"-"
            kw = re.sub(pattern, " ", kw)
            pattern = r" +"
            kw = re.sub(pattern, " ", kw).strip()
            kw_data["Keyword"] = kw

        for kw_data in kws_potential:
            kw = kw_data["Keyword"]
            pattern = r"-"
            kw = re.sub(pattern, " ", kw)
            pattern = r" +"
            kw = re.sub(pattern, " ", kw).strip()
            kw_data["Keyword"] = kw

        body = {
            "method": "deduplicate",
            "params": {"Keywords": kws_all, "Operation": ["MERGE_DUPLICATES"]},
        }
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(
            YA_DIR_KEYWORDS_RESEARCH_URL, jsonBody, headers=self.headers
        )
        result = result.json()

        if result["result"].get("Delete"):
            kw_ids_duplicates = result["result"]["Delete"]["Ids"]
        else:
            kw_ids_duplicates = []

        kws_duplicates = []
        new_obtained_phrases = []
        for kw_id in kw_ids_duplicates:
            for kw_data in kws_potential:
                if kw_id == kw_data["Id"]:
                    kws_duplicates.append(kw_data["Keyword"])
                    kws_potential.remove(kw_data)
                    break

        for kw_data in kws_potential:
            new_obtained_phrases.append(kw_data["Keyword"])

        return new_obtained_phrases, kws_duplicates

    def get_bid_adjustments(self, AdGroupIds, CampaignIds, Types, Levels):
        if AdGroupIds and CampaignIds:
            raise ValueError("Cannot be both AdGroupIds and CampaignIds filtering.")

        adjust_data = []
        if AdGroupIds:
            selection_level = "AdGroupIds"
            all_selectors = AdGroupIds
            group_size = 1000
        elif CampaignIds:
            selection_level = "CampaignIds"
            all_selectors = CampaignIds
            group_size = 10

        # Iterate by partitions to avoid too many selection elements error.
        selection_groups = Common_instance.divide_to_small_groups(
            all_selectors, group_size
        )

        for elements in selection_groups:
            offset = 0
            while True:
                body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {
                            selection_level: elements,
                            "Types": Types,
                            "Levels": Levels,
                        },
                        "FieldNames": [
                            "Id",
                            "CampaignId",
                            "AdGroupId",
                            "Level",
                            "Type",
                        ],
                        "MobileAdjustmentFieldNames": [
                            "BidModifier",
                            "OperatingSystemType",
                        ],
                        "TabletAdjustmentFieldNames": [
                            "BidModifier",
                            "OperatingSystemType",
                        ],
                        "DesktopAdjustmentFieldNames": ["BidModifier"],
                        "DesktopOnlyAdjustmentFieldNames": ["BidModifier"],
                        "DemographicsAdjustmentFieldNames": [
                            "Gender",
                            "Age",
                            "BidModifier",
                            "Enabled",
                        ],
                        "RetargetingAdjustmentFieldNames": [
                            "RetargetingConditionId",
                            "BidModifier",
                            "Accessible",
                            "Enabled",
                        ],
                        "RegionalAdjustmentFieldNames": [
                            "RegionId",
                            "BidModifier",
                            "Enabled",
                        ],
                        "VideoAdjustmentFieldNames": ["BidModifier"],
                        "SmartAdAdjustmentFieldNames": ["BidModifier"],
                        "SerpLayoutAdjustmentFieldNames": [
                            "SerpLayout",
                            "BidModifier",
                            "Enabled",
                        ],
                        "IncomeGradeAdjustmentFieldNames": [
                            "Grade",
                            "BidModifier",
                            "Enabled",
                        ],
                        "Page": {"Offset": offset},
                    },
                }

                # Кодирование тела запроса в JSON.
                jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
                result = requests.post(
                    YA_DIR_API_BID_ADJUST, jsonBody, headers=self.headers
                )
                result = result.json()

                if "result" in result and "BidModifiers" in result["result"]:
                    adjust_data += result["result"]["BidModifiers"]

                # Check whether all keywords were got or not.
                if "LimitedBy" not in result["result"]:
                    break
                else:
                    offset = result["result"]["LimitedBy"]

        return adjust_data

    def add_bid_adjustments(self, BidModifiers):
        body = {"method": "add", "params": {"BidModifiers": BidModifiers}}
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API_BID_ADJUST, jsonBody, headers=self.headers)
        result = result.json()

        for res in result["result"]["AddResults"]:
            if res.get("Errors"):
                raise Exception(f'add_bid_adjustments Error {res["Errors"]}')

        return result
