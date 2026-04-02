## 📊 Web scrapper
Скрапер для получения данных о компьютерных комплектующих с сайта inote.by

## ⚙️ Установка
1. **Клонируйте репозиторий**
```bash
git clone https://github.com/Deulix/scrapper.git
```
2. **Перейдите в папку проекта**
```bash
cd '.\scrapper\'
```
3. **Запустите скрипт**
```
docker compose up -d
```

## 📁 Структура проекта
```
scrapper/
├── data/
│   ├── clean/
│   │   └── data.csv
│   └── raw/
│       └── products_raw_data.jsonl
├── image
│   └── README
│       └── ...
├── logs/
│   └── ...
├── src/
│   ├── scrapper/
│   │   └── scrapper.py
│   ├── settings/
│   │   └── config.py
│   ├── spark/
│   │   └── transform.py
│   └── main.py
├── tests/
│   └── ...
├── .dockerignore
├── .env
├── .gitignore
├── .python-version
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── README.md
└── uv.lock
```
# 📷 Примеры работы скрапера
- Результат работы в консоли

![1773964296043](image/README/1773964296043.png)

- Сырые данные

![1773964400945](image/README/1773964400945.png)

- Очищенные данные

![1773964429301](image/README/1773964429301.png)