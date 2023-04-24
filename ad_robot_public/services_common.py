import urllib.request
from xml.etree import ElementTree
import re
import psycopg2
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import requests
from time import sleep
from datetime import datetime
from datetime import date, timedelta
import pymorphy2
from transliterate import translit
from email.mime.base import MIMEBase
from email import encoders
from skimage.io import imread, imsave
from PIL import Image


class Feed:
    def get_source_data(self, sourse_file, title2):
        # Get actual data from source file.
        source_file_data = {}
        # Check the sourse file type.
        if sourse_file.startswith("http") and sourse_file.endswith(".xml"):
            # Get feed.
            with urllib.request.urlopen(sourse_file) as f:
                feed_row_content = f.read().decode("utf-8")
            root = ElementTree.fromstring(feed_row_content)

            # Iterate items of feed and get their data.
            # Name space to get elements with 'g:'.
            namespaces = {"xmlns:g": "http://base.google.com/ns/1.0"}

            # Iterate the sourse feed.
            for element in root.iter("group"):
                # Null variables to case it will not be obtained.
                name_kw = ""
                title = ""
                url = ""
                product_type = ""  # For divide by campaigns. We usually advertise
                # one product type at the same ad campaign.
                main = ""  # To define whether add other ad groups keywords as negative
                # or not.
                AdImageHash = ""  # Image hash.

                for child in element:
                    # Get variables values.
                    if child.tag == "name_kw":
                        name_kw = child.text
                    if child.tag == "title":
                        title = child.text
                    if child.tag == "url":
                        url = child.text
                    if child.tag == "product_type":
                        product_type = child.text
                    if child.tag == "main":
                        main = child.text
                    if child.tag == "display_link":
                        display_link = child.text
                    if child.tag == "AdImageHash":
                        AdImageHash = child.text

                # Fulfil dictionary.
                source_file_data[name_kw.lower()] = {
                    "product_type": product_type,
                    "title": title,
                    "url": url,
                    "main": main,
                    "display_link": display_link,
                    "title2": title2,
                    "AdImageHash": AdImageHash,
                }

        # Replace all not spaces, not numeric and not letters symbols from ad group names because it cannot be a keyword
        # because of Yandex.Direct requarements.
        source_file_data_upgr = {}  # To store file data with cleaned ad groups names.
        for ad_gr_name in source_file_data:
            pattern = r"[^a-zA-Zа-яА-Я0-9!\+\"\' ]"
            upgr_key = re.sub(pattern, "", ad_gr_name)

            # Replace left or right spaces and double spaces if it is.
            upgr_key = upgr_key.rstrip().lstrip()
            pattern = r" +"
            upgr_key = re.sub(pattern, " ", upgr_key)

            # Do not use ad group if it's name has more than 7 words or 4096 characters
            # because of Yandex.Direct requarements.
            if len(upgr_key) <= 4096 and upgr_key.count(" ") <= 6:
                source_file_data_upgr[upgr_key] = source_file_data[ad_gr_name]
        return source_file_data_upgr


class Common_instance:
    @classmethod
    def divide_to_small_groups(self, variety_name, group_size):
        # Divide variety to small groups.
        # Handle case when lenth of variety_name not bigger than group_size.
        if len(variety_name) > group_size:
            groups_amount = len(variety_name) // group_size
            residue = len(variety_name) % group_size
            divided_groups = []  # To store grouped elements.
            for group_number in range(groups_amount):
                divided_groups.append(
                    variety_name[
                        group_number * group_size : (group_number + 1) * group_size
                    ]
                )
            # Add residue elements.
            if residue > 0:
                divided_groups.append(
                    variety_name[
                        (group_number + 1)
                        * group_size : (group_number + 1)
                        * group_size
                        + residue
                        + 1
                    ]
                )
        # Return the variety if it already not bigger than group_size.
        else:
            divided_groups = [list(variety_name)]
        return divided_groups

    def sql_query(self, sql_statement, dbname, user, password, host, port):
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        cursor = conn.cursor()
        cursor.execute(sql_statement)
        conn.commit()

        if sql_statement.lower().startswith("select"):
            respond = []

            for row in cursor:
                respond.append(row)
            return respond
        else:
            return ""

    def get_path_vm_itself(self, path_vm_itself):
        # Path where the script is located.
        path_vm_itself_list = re.split(r"/", path_vm_itself)
        del path_vm_itself_list[-1]
        path_vm_itself = ""
        for i in path_vm_itself_list:
            path_vm_itself += i + "/"
        return path_vm_itself

    @classmethod
    def deploy_dir_files(self, dirs, files):
        # Deploy directories and files for this site if there is not.
        # Check whether needed directories exists or not.
        # If not than create it.
        if dirs:
            for dir in dirs:
                if not os.path.exists(dir):
                    os.makedirs(dir)

        if files:
            for file in files:
                if not os.path.exists(file):
                    with open(file, "a", encoding="utf-8") as ouf:
                        ouf.write("")

    def download_file(self, url, path_vm, file_name, headers=""):
        # Download file by chunks to save RAM.
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(os.path.join(path_vm, file_name), "wb") as f:
                for chunk in r.iter_content(chunk_size=4096):
                    if chunk:
                        f.write(chunk)

    def send_email(
        self,
        subject,
        BodyText,
        recipients,
        recipients_copy,
        email_sender,
        email_password,
        file_paths=None,
    ):
        # Send email.
        for mail in recipients:
            # create message object instance
            msg = MIMEMultipart()
            message = BodyText

            # Attach files.
            if file_paths:
                for file_path in file_paths:
                    if os.path.exists(file_path):
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(open(file_path, "rb").read())
                        encoders.encode_base64(part)
                        part.add_header(
                            "Content-Disposition",
                            "attachment; filename= %s" % os.path.basename(file_path),
                        )
                        msg.attach(part)

            # Set up the parameters of the message.
            msg["From"] = email_sender
            msg["To"] = mail
            msg["Cc"] = recipients_copy
            msg["Subject"] = subject
            # add in the message body
            msg.attach(MIMEText(message, "plain"))
            # create server
            server = smtplib.SMTP("smtp.gmail.com: 587")
            server.starttls()
            # Login Credentials for sending the mail
            server.login(msg["From"], email_password)
            # send the message via the server.
            server.sendmail(
                msg["From"],
                msg["To"].split(",") + msg["Cc"].split(","),
                msg.as_string(),
            )
            server.quit()

    def logs_caution_email(
        self,
        log_success_file,
        days_to_check,
        subject,
        BodyText,
        recipients,
        recipients_copy,
        email_sender,
        email_password,
    ):
        # Send email if robot was not worked for some days.
        with open(log_success_file) as inf:
            log_success_text = inf.read()

        is_processed = False
        for day in range(days_to_check + 1):
            if str(date.today() - timedelta(day)) in log_success_text:
                is_processed = True
                break

        if not is_processed:
            self.send_email(
                subject,
                f"{BodyText}\nChecked for {days_to_check} days.",
                recipients,
                recipients_copy,
                email_sender,
                email_password,
            )

    def check_status_text_of_page(self, url, headers_200, checking_text):
        # Returns is pattern in page text, status_code, is_ConnectionError, is_TooManyRedirects error.
        is_ConnectionError = False
        try:
            url_respond = requests.get(url, headers=headers_200)
        except requests.exceptions.TooManyRedirects:
            return None, None, None, True
        except requests.exceptions.ConnectionError:
            is_ConnectionError = True
            for i in range(5):
                print("ConnectionError. sleep(12)")
                sleep(12)
                try:
                    url_respond = requests.get(url, headers=headers_200)
                except requests.exceptions.ConnectionError:
                    continue
                is_ConnectionError = False
                break

        if is_ConnectionError:
            print(f"is_ConnectionError {url}")
            return None, None, True, False

        if url_respond.status_code != 200:
            # Give 2 the second chances.
            sleep(3)
            url_respond = requests.get(url, headers=headers_200)
            if url_respond.status_code != 200:
                sleep(3)
                url_respond = requests.get(url, headers=headers_200)
                if url_respond.status_code != 200:
                    return None, url_respond.status_code, False, False

        # Check for text existing on the page.
        url_respond.encoding = "utf-8"
        page_text = url_respond.text

        if checking_text:
            for pattern in checking_text:
                is_match = bool(re.search(pattern, page_text))
                if checking_text[pattern] != is_match:
                    return False, url_respond.status_code, False, False

        return True, url_respond.status_code, False, False

    def save_logs(self, file, rows, max_file_size_mb):
        # Deploy logs file if there is not.
        file_dir = re.split(r"/", file)[0:-1]
        file_dir = "/".join(file_dir)
        self.deploy_dir_files([file_dir], [file])

        # Clear log file if it's too big.
        if os.path.getsize(file) / 1000000 > max_file_size_mb:  # Mb.
            rows_to_save = []
            with open(file, encoding="utf-8") as ouf:
                for line in ouf:
                    rows_to_save.append(line)

                from_index = int(len(rows_to_save) * 0.4)
                rows_to_save = rows_to_save[from_index:]

            with open(file, "w", encoding="utf-8") as inf:
                inf.write("")

            for row in rows_to_save:
                with open(file, "a", encoding="utf-8") as inf:
                    inf.write(f"{row}")

        rows.insert(0, str(datetime.now()))
        for row in rows:
            with open(file, "a", encoding="utf-8") as inf:
                inf.write(f"\n{row}")

    def normalization(self, output_type, source, exclusion_words):
        # Keywords normalization.
        output_plenty = output_type()
        morph = pymorphy2.MorphAnalyzer()

        for name_kw in source:
            # Remove special symbols and PREPOSITIONS for matching.
            pattern = r"[^a-zA-Zа-яА-Я0-9 ]"
            kw = re.sub(pattern, "", name_kw)
            kw = re.sub(r" +", " ", kw).strip()

            kw_list = [x for x in kw.split()]
            if exclusion_words:
                kw_list = [x for x in kw_list if x not in exclusion_words]

            kw_set_norm = set()
            for kw in kw_list:
                kw = morph.parse(kw)[0]
                kw = kw.normal_form.strip()  # strip prevents space case.
                if kw:
                    kw_set_norm.add(kw)

            if type(output_plenty) == dict:
                output_plenty[name_kw] = kw_set_norm
            elif type(output_plenty) == list:
                output_plenty.append(kw_set_norm)
            else:
                raise ValueError("output_plenty should be dict or list type only.")

        return output_plenty

    def cross_phrases(self, potential_phrases, exclusion_phrases):
        # Differ sets of phrases. Differ if subset is True.
        final_phrases = set()  # To store returned "family of sets".
        # We use dictionary because we need save original phrase form to use it in ad titles.
        # Differ subset cases.
        for potential_phrase in potential_phrases:
            is_subset = False  # To check whether at list one phrase is subset.
            # Check no one ya_direct_phrase is superset of potential_phrase
            # (I.e. they are not crossing).
            for exclusion_phrase in exclusion_phrases:
                if potential_phrases[potential_phrase].issuperset(exclusion_phrase):
                    is_subset = True
                    break

            if not is_subset:
                final_phrases.add(potential_phrase)

        return final_phrases

    def translit_text(self, ru_text):
        # Make Russian letters as English, replace spaces and quoters.
        text = translit(ru_text, language_code="ru", reversed=True)
        text = text.replace("'", "").replace(" ", "_")
        return text

    def make_image_square(self, path_vm, image_name, big_white_image):
        # Function to square image to prevent distortion of image later.
        # Read image.
        img = imread(os.path.join(path_vm, image_name))
        # Get size of image.
        size_img = img.shape

        # ------------------------------------------
        # If item image is black and white then convert it to RGB
        # to prevent concatination proccess error and correct pixels measuring.
        if len(size_img) < 3:
            black_white_img = Image.open(os.path.join(path_vm, image_name)).convert(
                "RGB"
            )
            black_white_img.save(os.path.join(path_vm, image_name))
            # Read renewed image again.
            img = imread(os.path.join(path_vm, image_name))
            # Get size of image.
            size_img = img.shape
        # ------------------------------------------

        # Count difference between width and height of image.
        diff_width_height = size_img[1] - size_img[0]
        # Stop the function if image is already squared.
        if diff_width_height == 0:
            return "pass"

        # Read white square space.
        img_white = imread(os.path.join(path_vm, big_white_image))
        if diff_width_height > 0:  # Use the bigger side.
            img_white_square = img_white[0 : size_img[1], 0 : size_img[1]]
        else:
            img_white_square = img_white[0 : size_img[0], 0 : size_img[0]]

        # Add image to the square.
        # Count half of diff_width_height.
        half = abs(int(diff_width_height / 2))
        if diff_width_height < 0:
            img_white_square[0 : size_img[0], half : half + size_img[1]] = img
        else:
            img_white_square[half : half + size_img[0], 0 : size_img[1]] = img
        # Save new image.
        imsave(os.path.join(path_vm, image_name), img_white_square)
