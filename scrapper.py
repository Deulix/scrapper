import time

import httpx
from bs4 import BeautifulSoup


async def scrapper(client: httpx.AsyncClient, url):
    headers = {"User-Agent": "Mozilla/5.0"}

    response: httpx.Response = await client.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Ошибка доступа! {response.status_code}")

    soup = BeautifulSoup(response.text, "lxml")

    products = soup.find_all("div", class_="product-layout")

    results = []

    for item in products:
        name = item.find("div", class_="us-module-title").find("a").text.strip()
        is_available_part = item.find(
            "a", class_="us-module-cart-btn button-cart"
        ).text.strip()

        is_available = str(is_available_part).lower().strip() == "в корзину"
        description = item.find(
            "div", class_="us-product-list-description"
        ).text.strip()

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
            "description": description,
        }
        results.append(data)
        print(f"Найдено: {name} | Цена: {price}")

    return results
