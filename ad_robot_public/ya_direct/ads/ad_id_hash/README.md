Допустим, у вас есть крупные поисковые категорийные рекламные кампании в Яндекс.Директ.
Они могут состоять из тысяч групп объявлений и вести на холодильники "Заря", компьютеры "Эльбрус", недорогие посудомойки и т.д. и т.д.

Вопрос: как запустить РСЯ на основе этих кампаний, где взять картинки? 

Ответ:
Сначала мы копируем поисковые кампании и настраиваем их на сети. Объявления пока без картинок.

Предлагается скрипт на Питоне, который выполняет следующие действия:
1. Получает url посадочных страниц объявлений аккаунта с привязкой к id объявления.
2. Посещает посадочные страницы, находит на нах первый товар.
3. Переходит на страницу товара и скачивает картинку этого товара.
4. Хэширует картинку по стандартам API Яндекс.Директ.
5. Записывает словарь в json файл пары id объявления: хэш изображения. 

В результате вы получите файл ads_final.json с данными для обновления.
Обновить объявления (добавить картинки) можно с помощью ad_robot_public/ad_robot_public/ya_direct/ads/update/ads_update.py

Это поможет вам быстро запустить крупные РСЯ-кампании.