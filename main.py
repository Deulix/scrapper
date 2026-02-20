import json
import time

import requests
from bs4 import BeautifulSoup

SITE_URL = "https://inote.by/index.php?route=product/category&path=451"


def scrapper(url):
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Ошибка доступа! {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")

    products = soup.find_all("div", class_="product-layout")

    results = []

    for item in products:
        name = item.find("div", class_="us-module-title").find("a").text.strip()
        is_available_part = item.find(
            "a", class_="us-module-cart-btn button-cart"
        ).text.strip()

        is_available = str(is_available_part).lower().strip() == "в корзину"
        print(is_available)

        try:
            price = item.find("span", class_="us-module-price-old").text.strip()
            price_with_off = item.find(
                "span", class_="us-module-price-new"
            ).text.strip()
        except AttributeError:
            price = item.find("span", class_="us-module-price-actual").text.strip()
            price_with_off = None

        data = {
            "timestamp": int(time.time()),
            "source": "inote.by",
            "name": name,
            "price": price,
            "price_with_off": price_with_off,
            "available": is_available,
        }
        results.append(data)
        print(f"Найдено: {name} | Цена: {price}")

    return results


def save_to_jsonl(data_list):
    with open("raw_ram_data.jsonl", "a", encoding="utf-8") as f:
        for entry in data_list:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main():
    data = scrapper(SITE_URL)
    if data:
        save_to_jsonl(data)
        print(f"Сохранено {len(data)} позиций в raw_ram_data.jsonl")

    else:
        return

    url = SITE_URL + "&page="
    i = 2
    while i < 100:
        new_url = url + str(i)
        data = scrapper(new_url)
        if not data:
            break
        save_to_jsonl(data)
        print(f"Сохранено {len(data)} позиций в raw_ram_data.jsonl")

        i += 1


if __name__ == "__main__":
    main()
