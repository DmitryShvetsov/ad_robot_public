from xml.etree import ElementTree
from lxml import etree
import os
from services_common import Common_instance
import json
import requests
import base64
import sys
import boto3
import re
import html


from constants_common import YA_DIR_API_IMAGES_URL
common_instance = Common_instance()

class CatCampXMLGenerator:
    def __init__(self, feed_list, path_vm, new_xml_file_name, on_off_tag):
        self.feed_list = feed_list
        self.path_vm = path_vm
        self.new_xml_file_name = new_xml_file_name
        self.path_to_file = path_vm + new_xml_file_name
        self.on_off_tag = on_off_tag

    def get_hashes(
        self, path_vm_itself, image_name, default_image_hash, login_direct, token_direct
    ):
        # Get images hashes.
        # list_img_names=[path_vm_itself + image_name]

        headers = {
            "Authorization": f"Bearer {token_direct}",
            "Client-Login": login_direct,
            "Accept-Language": "en",
        }

        # For i in list_img_names:
        image = open(os.path.join(path_vm_itself, image_name), "rb").read()
        imageData = base64.b64encode(image).decode("utf-8")
        # Загрузить картинку REGULAR в аккаунт Директа.
        body = {
            "method": "add",
            "params": {"AdImages": [{"ImageData": imageData, "Name": image_name}]},
        }
        # Кодирование тела запроса в JSON
        jsonBody = json.dumps(body, ensure_ascii=False).encode("utf8")
        result = requests.post(YA_DIR_API_IMAGES_URL, jsonBody, headers=headers)
        result = result.json()

        # Remove image file.
        os.remove(os.path.join(path_vm_itself, image_name))

        # Handle errors.
        if "result" in result:
            if "AdImageHash" in result["result"]["AddResults"][0]:
                return result["result"]["AddResults"][0]["AdImageHash"]

        return default_image_hash

    def fill_xml(self):
        # Create new xml tree and write data from first tree to the second one.

        start_of_file = (
            '<rss version="2.0" xmlns:g="http://base.google.com/ns/1.0">\n<channel>'
        )
        end_of_file = "</channel>\n</rss>"
        if self.on_off_tag:
            start_of_file += f"\n<on_off_tag>{self.on_off_tag}</on_off_tag>"

        with open(
            os.path.join(self.path_vm, self.new_xml_file_name), "w", encoding="utf-8"
        ) as ouf:
            ouf.write(start_of_file)

        for element in self.feed_list:
            # Create first douther element.
            root_new = ElementTree.Element("rss")
            # Create new tree.
            tree_new = ElementTree.ElementTree(root_new)

            # Add new tags to the new feed.
            group = ElementTree.SubElement(root_new, "group")
            for key in element:
                value = element[key]
                # Create subtag.
                subtag = ElementTree.SubElement(group, key)
                # Add text of new subtag.
                subtag.text = value

            # Write the tree to the file.
            tree_new = tree_new.getroot()  # Tech action to mutate tree to the string.
            xml_str = ElementTree.tostring(tree_new, encoding="unicode")
            xml_str = xml_str.replace("</rss>", "").replace("<rss>", "")

            # Write big file with line breaking to easy-reading.
            xml_str = etree.fromstring(xml_str, parser=etree.XMLParser(huge_tree=True))
            xml_str = etree.tostring(xml_str, encoding="utf-8", pretty_print=True)
            xml_str = xml_str.decode("utf-8")

            xml_str = xml_str.strip()
            with open(
                os.path.join(self.path_vm, self.new_xml_file_name),
                "a",
                encoding="utf-8",
            ) as ouf:
                ouf.write(f"\n{xml_str}")

        with open(
            os.path.join(self.path_vm, self.new_xml_file_name), "a", encoding="utf-8"
        ) as ouf:
            ouf.write(f"\n{end_of_file}")

    def worker(self):
        self.fill_xml()

    def upload_file_into_bucket(
        self,
        YA_CLOUD_KEY_ID,
        YA_CLOUD_SECRET_KEY,
        path_vm_itself,
        bucket,
        bucket_path,
        new_xml_file_name,
        ExtraArgs=None,
    ):
        # Upload file into a bucket.
        session = boto3.session.Session()
        s3 = session.client(
            service_name="s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=YA_CLOUD_KEY_ID,
            aws_secret_access_key=YA_CLOUD_SECRET_KEY,
        )

        # From a file.
        bucket_path = bucket_path.strip("/")
        new_xml_file_name = new_xml_file_name.strip("/")
        s3.upload_file(
            os.path.join(path_vm_itself, new_xml_file_name),
            bucket,
            os.path.join(bucket_path, new_xml_file_name),
            ExtraArgs=ExtraArgs,
        )


class FeedHelper:
    def get_feed_data(self, sourse_file, path_vm):
        sourse_file_name = re.split("/", sourse_file)[-1]

        # Download feed by chunks to save RAM.
        common_instance.download_file(sourse_file, path_vm, sourse_file_name)

        # Get actual data from source file.
        source_file_data = {}
        with open(os.path.join(path_vm, sourse_file_name), encoding="utf-8") as ouf:
            for line in ouf:
                line = line.strip()
                tag = re.split(">", line)[0]
                tag = re.sub(r"<|>|/", "", tag)
                tag_value = re.sub(rf"<{tag}>|</{tag}>|<{tag}/>", "", line)

                if tag == "channel" or tag == "rss" or tag.startswith("rss "):
                    continue

                elif tag == "on_off_tag":
                    on_off_tag = tag_value

                elif re.search(r"<group>", line):
                    group_dict = {}

                # product_type is for dividing by campaigns. We usually advertise
                # one product type at the same ad campaign.

                elif tag == "name_kw":
                    # Replace all not spaces, not numeric and not letters symbols from ad group names because it cannot be
                    # a keyword because of Yandex.Direct requirements.
                    pattern = r"[^a-zA-Zа-яА-Я0-9!\+\"\'\[\] ]"
                    tag_value = re.sub(pattern, "", tag_value)
                    # Replace left or right spaces and double spaces if it is.
                    tag_value = tag_value.strip().lower()
                    group_dict[tag] = re.sub(r" +", " ", tag_value)

                elif re.search(r"</group>", line):
                    key = group_dict[on_off_tag]
                    if on_off_tag == "url":
                        # Handle case url with '/' or not.
                        key = key.rstrip("/")
                    source_file_data[key] = group_dict

                else:
                    group_dict[tag] = html.unescape(tag_value)

        return source_file_data, on_off_tag
