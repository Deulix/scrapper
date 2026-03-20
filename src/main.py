import asyncio
import json
import os
from datetime import datetime as dt
from typing import Callable

import aiofiles
import httpx

from src.scrapper.scrapper import scrapper
from src.settings.config import RAW_DATA_DIR, SITE_URL

mapping = {
    "Оперативная память": 451,
    "Видеокарты": 169,
    "Процессоры": 433,
    "Материнские платы": 436,
    "SSD": 556,
}

time_delta = None


def async_time_decorator(func: Callable):
    async def wrapper(*args, **kwargs):
        start_time = dt.now()
        result = await func(*args, **kwargs)
        end_time = dt.now()
        global time_delta
        time_delta = end_time - start_time
        if time_delta.seconds >= 60:
            work_time = (
                f"{time_delta.seconds // 60} минут {time_delta.seconds % 60} секунд"
            )
        else:
            work_time = f"{time_delta.seconds} секунд"

        print("\n=== СКРАППЕР ===")
        print(f"Начало работы: {start_time.strftime('%d.%m.%Y, %H:%M:%S')}")
        print(f"Конец работы: {end_time.strftime('%d.%m.%Y, %H:%M:%S')}")
        print(f"Время работы: {work_time}\n")

        return result

    return wrapper


file_lock = asyncio.Lock()


async def save_to_jsonl(data_list):
    lines = (
        "\n".join([json.dumps(entry, ensure_ascii=False) for entry in data_list]) + "\n"
    )
    async with file_lock:
        async with aiofiles.open(
            RAW_DATA_DIR / "products_raw_data.jsonl", "a", encoding="utf-8"
        ) as f:
            await f.write(lines)


async def category_process(client, category_name, category_id):
    url = f"{SITE_URL}{category_id}"
    data = await scrapper(client, url)

    if not data:
        print(f"ОШИБКА ЧТЕНИЯ КАТЕГОРИИ [{category_name}]")
        return

    await save_to_jsonl(data)

    url += "&page="
    page = 2
    chunk_size = 2
    found = True

    while found:
        tasks = [scrapper(client, f"{url}{p}") for p in range(page, page + chunk_size)]
        page += chunk_size
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                continue

            if not result:
                found = False
                break

            await save_to_jsonl(result)


@async_time_decorator
async def main():
    async with httpx.AsyncClient() as client:
        category_tasks = [
            category_process(client, category_name, category_id)
            for category_name, category_id in mapping.items()
        ]

        await asyncio.gather(*category_tasks)


if __name__ == "__main__":
    try:
        os.remove(RAW_DATA_DIR / "products_raw_data.jsonl")
    except FileNotFoundError:
        print("Файл products_raw_data.jsonl не найден для удаления")
    asyncio.run(main())
    with open(RAW_DATA_DIR / "products_raw_data.jsonl", encoding="utf-8") as file:
        count = sum(1 for _ in file)
        print(
            f"Количество строк: {count}, ~{(count / time_delta.total_seconds()):.2f} строк/cек"
        )
